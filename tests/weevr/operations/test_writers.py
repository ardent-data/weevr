"""Tests for target mapping (apply_target_mapping) and Delta writers (write_target)."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from spark_helpers import create_delta_table
from weevr.model.target import ColumnMapping, Target
from weevr.model.types import SparkExpr
from weevr.model.write import WriteConfig
from weevr.operations.writers import apply_target_mapping, quote_identifier, write_target


class TestQuoteIdentifier:
    """Unit tests for quote_identifier SQL escaping (no Spark needed)."""

    def test_plain_name(self) -> None:
        assert quote_identifier("column_name") == "`column_name`"

    def test_name_with_backtick(self) -> None:
        assert quote_identifier("col`umn") == "`col``umn`"

    def test_name_with_spaces(self) -> None:
        assert quote_identifier("my column") == "`my column`"

    def test_empty_string(self) -> None:
        assert quote_identifier("") == "``"


@pytest.fixture()
def source_df(spark: SparkSession):
    """Source DataFrame with mixed columns for mapping tests."""
    return spark.createDataFrame(
        [
            {"id": 1, "name": "alice", "amount": 100, "status": "active"},
            {"id": 2, "name": "bob", "amount": 50, "status": "inactive"},
        ]
    )


@pytest.mark.spark
class TestAutoModeNewTarget:
    """auto mode when target does not yet exist — all columns pass through."""

    def test_auto_new_no_columns_config_passes_all(self, source_df, spark: SparkSession) -> None:
        target = Target(path="/nonexistent/target")
        result = apply_target_mapping(source_df, target, spark)
        assert set(result.columns) == set(source_df.columns)

    def test_auto_new_with_drop_removes_column(self, source_df, spark: SparkSession) -> None:
        target = Target(
            path="/nonexistent/target",
            columns={"status": ColumnMapping(drop=True)},
        )
        result = apply_target_mapping(source_df, target, spark)
        assert "status" not in result.columns
        assert "id" in result.columns

    def test_auto_new_with_expr_adds_column(self, source_df, spark: SparkSession) -> None:
        target = Target(
            path="/nonexistent/target",
            columns={"doubled": ColumnMapping(expr=SparkExpr("amount * 2"))},
        )
        result = apply_target_mapping(source_df, target, spark)
        assert "doubled" in result.columns
        values = {r["doubled"] for r in result.collect()}
        assert 200 in values

    def test_auto_new_with_type_cast(self, source_df, spark: SparkSession) -> None:
        target = Target(
            path="/nonexistent/target",
            columns={"amount": ColumnMapping(type="string")},
        )
        result = apply_target_mapping(source_df, target, spark)
        assert result.schema["amount"].dataType == StringType()

    def test_auto_new_with_default_fills_nulls(self, spark: SparkSession) -> None:
        schema = StructType(
            [
                StructField("id", LongType()),
                StructField("val", StringType(), nullable=True),
            ]
        )
        df = spark.createDataFrame([(1, None), (2, "existing")], schema=schema)
        target = Target(
            path="/nonexistent/target",
            columns={"val": ColumnMapping(default="fallback")},
        )
        result = apply_target_mapping(df, target, spark)
        values = {r["val"] for r in result.collect()}
        assert "fallback" in values
        assert "existing" in values
        assert None not in values

    def test_auto_new_preserves_row_count(self, source_df, spark: SparkSession) -> None:
        target = Target(path="/nonexistent/target")
        result = apply_target_mapping(source_df, target, spark)
        assert result.count() == source_df.count()


@pytest.mark.spark
class TestAutoModeExistingTarget:
    """auto mode when target table already exists — select matching columns only."""

    def test_auto_existing_selects_matching_columns(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        # Create existing target with subset of columns
        existing_path = tmp_delta_path("existing_target")
        create_delta_table(spark, existing_path, [{"id": 0, "name": "seed"}])

        source_df = spark.createDataFrame(
            [
                {"id": 1, "name": "alice", "extra_col": "ignored"},
            ]
        )
        target = Target(alias=existing_path)
        result = apply_target_mapping(source_df, target, spark)

        # Only columns from existing target schema should remain
        assert "extra_col" not in result.columns
        assert "id" in result.columns
        assert "name" in result.columns

    def test_auto_existing_column_order_follows_existing_schema(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        existing_path = tmp_delta_path("ordered_target")
        # Use explicit schema to control column ordering in the existing table
        schema = StructType([StructField("b", LongType()), StructField("a", LongType())])
        spark.createDataFrame([(1, 2)], schema=schema).write.format("delta").save(existing_path)

        source_df = spark.createDataFrame(
            [(10, 20, 30)],
            schema=StructType(
                [
                    StructField("a", LongType()),
                    StructField("b", LongType()),
                    StructField("c", LongType()),
                ]
            ),
        )
        target = Target(alias=existing_path)
        result = apply_target_mapping(source_df, target, spark)

        # Column order should match existing target schema (b, a), not source (a, b)
        assert result.columns == ["b", "a"]

    def test_auto_existing_with_expr_applied_before_selection(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        existing_path = tmp_delta_path("expr_target")
        create_delta_table(spark, existing_path, [{"id": 0, "label": "x"}])

        source_df = spark.createDataFrame([{"id": 1, "name": "alice"}])
        target = Target(
            alias=existing_path,
            columns={"label": ColumnMapping(expr=SparkExpr("upper(name)"))},
        )
        result = apply_target_mapping(source_df, target, spark)

        assert "label" in result.columns
        assert result.collect()[0]["label"] == "ALICE"


@pytest.mark.spark
class TestExplicitMode:
    """explicit mode — keep only declared non-dropped columns."""

    def test_explicit_selects_only_declared_columns(self, source_df, spark: SparkSession) -> None:
        target = Target(
            alias="test",
            mapping_mode="explicit",
            columns={
                "id": ColumnMapping(),
                "name": ColumnMapping(),
            },
        )
        result = apply_target_mapping(source_df, target, spark)
        assert set(result.columns) == {"id", "name"}
        assert "amount" not in result.columns

    def test_explicit_with_expr_applies_expression(self, source_df, spark: SparkSession) -> None:
        target = Target(
            alias="test",
            mapping_mode="explicit",
            columns={
                "id": ColumnMapping(),
                "upper_name": ColumnMapping(expr=SparkExpr("upper(name)")),
            },
        )
        result = apply_target_mapping(source_df, target, spark)
        assert set(result.columns) == {"id", "upper_name"}
        names = {r["upper_name"] for r in result.collect()}
        assert "ALICE" in names

    def test_explicit_with_type_cast(self, source_df, spark: SparkSession) -> None:
        target = Target(
            alias="test",
            mapping_mode="explicit",
            columns={"amount": ColumnMapping(type="string")},
        )
        result = apply_target_mapping(source_df, target, spark)
        assert result.columns == ["amount"]
        assert result.schema["amount"].dataType == StringType()

    def test_explicit_excludes_dropped_columns(self, source_df, spark: SparkSession) -> None:
        target = Target(
            alias="test",
            mapping_mode="explicit",
            columns={
                "id": ColumnMapping(),
                "name": ColumnMapping(),
                "amount": ColumnMapping(drop=True),
            },
        )
        result = apply_target_mapping(source_df, target, spark)
        assert "amount" not in result.columns
        assert {"id", "name"} <= set(result.columns)

    def test_explicit_no_columns_returns_as_is(self, source_df, spark: SparkSession) -> None:
        target = Target(alias="test", mapping_mode="explicit")
        result = apply_target_mapping(source_df, target, spark)
        assert set(result.columns) == set(source_df.columns)

    def test_explicit_default_fills_nulls(self, spark: SparkSession) -> None:
        schema = StructType(
            [
                StructField("id", LongType()),
                StructField("score", LongType(), nullable=True),
            ]
        )
        df = spark.createDataFrame([(1, None), (2, 99)], schema=schema)
        target = Target(
            alias="test",
            mapping_mode="explicit",
            columns={"score": ColumnMapping(default=0)},
        )
        result = apply_target_mapping(df, target, spark)
        values = {r["score"] for r in result.collect()}
        assert 0 in values
        assert 99 in values
        assert None not in values

    def test_explicit_preserves_row_count(self, source_df, spark: SparkSession) -> None:
        target = Target(
            alias="test",
            mapping_mode="explicit",
            columns={"id": ColumnMapping()},
        )
        result = apply_target_mapping(source_df, target, spark)
        assert result.count() == source_df.count()


# ---------------------------------------------------------------------------
# write_target tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def simple_df(spark: SparkSession):
    """Small DataFrame for write tests."""
    return spark.createDataFrame(
        [
            {"id": 1, "val": "a"},
            {"id": 2, "val": "b"},
        ]
    )


@pytest.mark.spark
class TestWriteTargetOverwrite:
    """Overwrite write mode."""

    def test_overwrite_creates_new_table(
        self, spark: SparkSession, simple_df, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("overwrite_new")
        target = Target(path=path)
        write_config = WriteConfig(mode="overwrite")
        rows = write_target(spark, simple_df, target, write_config, path)
        assert rows == 2
        assert spark.read.format("delta").load(path).count() == 2

    def test_overwrite_replaces_existing_table(
        self, spark: SparkSession, simple_df, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("overwrite_existing")
        create_delta_table(spark, path, [{"id": 99, "val": "old"}])
        target = Target(path=path)
        write_config = WriteConfig(mode="overwrite")
        write_target(spark, simple_df, target, write_config, path)
        result = spark.read.format("delta").load(path)
        assert result.count() == 2
        assert result.filter("id = 99").count() == 0

    def test_overwrite_returns_row_count(
        self, spark: SparkSession, simple_df, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("overwrite_count")
        target = Target(path=path)
        rows = write_target(spark, simple_df, target, None, path)
        assert rows == 2

    def test_overwrite_with_partition(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("overwrite_partition")
        df = spark.createDataFrame([{"id": 1, "region": "eu"}, {"id": 2, "region": "us"}])
        target = Target(path=path, partition_by=["region"])
        write_target(spark, df, target, WriteConfig(mode="overwrite"), path)
        result = spark.read.format("delta").load(path)
        assert result.count() == 2


@pytest.mark.spark
class TestWriteTargetAppend:
    """Append write mode."""

    def test_append_adds_rows_to_existing_table(
        self, spark: SparkSession, simple_df, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("append_existing")
        create_delta_table(spark, path, [{"id": 0, "val": "seed"}])
        target = Target(path=path)
        write_target(spark, simple_df, target, WriteConfig(mode="append"), path)
        result = spark.read.format("delta").load(path)
        assert result.count() == 3

    def test_append_returns_row_count(self, spark: SparkSession, simple_df, tmp_delta_path) -> None:
        path = tmp_delta_path("append_count")
        create_delta_table(spark, path, [{"id": 0, "val": "seed"}])
        target = Target(path=path)
        rows = write_target(spark, simple_df, target, WriteConfig(mode="append"), path)
        assert rows == 2


@pytest.mark.spark
class TestWriteTargetMerge:
    """Merge write mode — all on_match / on_no_match_target / on_no_match_source combos."""

    def test_merge_update_on_match(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("merge_update")
        create_delta_table(spark, path, [{"id": 1, "val": "old"}, {"id": 2, "val": "keep"}])
        incoming = spark.createDataFrame([{"id": 1, "val": "new"}])
        target = Target(path=path)
        write_config = WriteConfig(mode="merge", match_keys=["id"], on_match="update")
        write_target(spark, incoming, target, write_config, path)
        result = spark.read.format("delta").load(path)
        assert result.filter("id = 1").collect()[0]["val"] == "new"
        assert result.filter("id = 2").collect()[0]["val"] == "keep"

    def test_merge_ignore_on_match(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("merge_ignore_match")
        create_delta_table(spark, path, [{"id": 1, "val": "original"}])
        incoming = spark.createDataFrame([{"id": 1, "val": "should_not_update"}])
        target = Target(path=path)
        write_config = WriteConfig(
            mode="merge",
            match_keys=["id"],
            on_match="ignore",
            on_no_match_target="ignore",
        )
        write_target(spark, incoming, target, write_config, path)
        result = spark.read.format("delta").load(path)
        assert result.collect()[0]["val"] == "original"

    def test_merge_insert_on_no_match_target(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("merge_insert")
        create_delta_table(spark, path, [{"id": 1, "val": "existing"}])
        incoming = spark.createDataFrame([{"id": 2, "val": "new"}])
        target = Target(path=path)
        write_config = WriteConfig(
            mode="merge",
            match_keys=["id"],
            on_no_match_target="insert",
        )
        write_target(spark, incoming, target, write_config, path)
        result = spark.read.format("delta").load(path)
        assert result.count() == 2

    def test_merge_ignore_on_no_match_target(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("merge_ignore_target")
        create_delta_table(spark, path, [{"id": 1, "val": "existing"}])
        incoming = spark.createDataFrame([{"id": 2, "val": "not_inserted"}])
        target = Target(path=path)
        write_config = WriteConfig(
            mode="merge",
            match_keys=["id"],
            on_match="ignore",
            on_no_match_target="ignore",
        )
        write_target(spark, incoming, target, write_config, path)
        result = spark.read.format("delta").load(path)
        assert result.count() == 1

    def test_merge_delete_on_no_match_source(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("merge_delete_source")
        create_delta_table(
            spark,
            path,
            [
                {"id": 1, "val": "keep_matched"},
                {"id": 2, "val": "delete_unmatched"},
            ],
        )
        incoming = spark.createDataFrame([{"id": 1, "val": "updated"}])
        target = Target(path=path)
        write_config = WriteConfig(
            mode="merge",
            match_keys=["id"],
            on_match="update",
            on_no_match_source="delete",
        )
        write_target(spark, incoming, target, write_config, path)
        result = spark.read.format("delta").load(path)
        assert result.count() == 1
        assert result.collect()[0]["id"] == 1

    def test_merge_ignore_on_no_match_source(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("merge_ignore_source")
        create_delta_table(
            spark,
            path,
            [
                {"id": 1, "val": "matched"},
                {"id": 2, "val": "unmatched_kept"},
            ],
        )
        incoming = spark.createDataFrame([{"id": 1, "val": "updated"}])
        target = Target(path=path)
        write_config = WriteConfig(
            mode="merge",
            match_keys=["id"],
            on_match="update",
            on_no_match_source="ignore",
        )
        write_target(spark, incoming, target, write_config, path)
        result = spark.read.format("delta").load(path)
        assert result.count() == 2

    def test_merge_on_new_target_writes_all_rows(
        self, spark: SparkSession, tmp_delta_path, simple_df
    ) -> None:
        path = tmp_delta_path("merge_new_target")
        target = Target(path=path)
        write_config = WriteConfig(mode="merge", match_keys=["id"])
        write_target(spark, simple_df, target, write_config, path)
        result = spark.read.format("delta").load(path)
        assert result.count() == 2

    def test_merge_soft_delete_seeds_schema_on_new_target(
        self, spark: SparkSession, tmp_delta_path, simple_df
    ) -> None:
        """Initial merge with soft_delete adds the soft-delete column to the schema."""
        path = tmp_delta_path("merge_sd_seed")
        target = Target(path=path)
        write_config = WriteConfig(
            mode="merge",
            match_keys=["id"],
            on_no_match_source="soft_delete",
            soft_delete_column="is_deleted",
        )
        write_target(spark, simple_df, target, write_config, path)
        result = spark.read.format("delta").load(path)
        assert "is_deleted" in result.columns
        # All rows should have null (no soft-delete flag yet)
        assert all(r["is_deleted"] is None for r in result.collect())

    def test_merge_soft_delete_marks_unmatched_rows(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Merge with soft_delete sets flag on unmatched target rows."""
        from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

        path = tmp_delta_path("merge_sd_mark")
        schema = StructType(
            [
                StructField("id", LongType()),
                StructField("val", StringType()),
                StructField("is_deleted", BooleanType(), nullable=True),
            ]
        )
        create_delta_table(
            spark,
            path,
            [
                {"id": 1, "val": "a", "is_deleted": None},
                {"id": 2, "val": "b", "is_deleted": None},
            ],
            schema=schema,
        )
        incoming = spark.createDataFrame([{"id": 1, "val": "a_updated"}])
        target = Target(path=path)
        write_config = WriteConfig(
            mode="merge",
            match_keys=["id"],
            on_match="update",
            on_no_match_source="soft_delete",
            soft_delete_column="is_deleted",
        )
        write_target(spark, incoming, target, write_config, path)
        result = spark.read.format("delta").load(path)
        rows = {r["id"]: r for r in result.collect()}
        # id=1 matched — soft-delete column cleared to null
        assert rows[1]["is_deleted"] is None
        # id=2 unmatched — soft-delete flag set to True
        assert rows[2]["is_deleted"] is True

    def test_merge_soft_delete_without_column_raises_validation_error(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """soft_delete without soft_delete_column is caught by model validation."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="soft_delete_column"):
            WriteConfig(
                mode="merge",
                match_keys=["id"],
                on_no_match_source="soft_delete",
            )

    def test_merge_soft_delete_active_value_seeds_schema(
        self, spark: SparkSession, tmp_delta_path, simple_df
    ) -> None:
        """First merge with soft_delete_active_value seeds the column with that value."""
        path = tmp_delta_path("merge_sd_default_seed")
        target = Target(path=path)
        write_config = WriteConfig(
            mode="merge",
            match_keys=["id"],
            on_no_match_source="soft_delete",
            soft_delete_column="is_deleted",
            soft_delete_active_value=False,
        )
        write_target(spark, simple_df, target, write_config, path)
        result = spark.read.format("delta").load(path)
        assert "is_deleted" in result.columns
        # First write seeds with soft_delete_active_value, not null
        assert all(r["is_deleted"] is False for r in result.collect())

    def test_merge_soft_delete_active_value_on_match_and_insert(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """soft_delete_active_value is written to matched/inserted rows;
        orphans still get soft_delete_value."""
        from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

        path = tmp_delta_path("merge_sd_default_match")
        schema = StructType(
            [
                StructField("id", LongType()),
                StructField("val", StringType()),
                StructField("is_deleted", BooleanType(), nullable=True),
            ]
        )
        create_delta_table(
            spark,
            path,
            [
                {"id": 1, "val": "a", "is_deleted": True},  # previously soft-deleted
                {"id": 2, "val": "b", "is_deleted": False},  # active
                {"id": 3, "val": "c", "is_deleted": False},  # will become orphan
            ],
            schema=schema,
        )
        # id=1 re-appears (update); id=4 is new (insert); id=3 disappears (orphan → soft_delete)
        incoming = spark.createDataFrame([{"id": 1, "val": "a_new"}, {"id": 4, "val": "d_new"}])
        target = Target(path=path)
        write_config = WriteConfig(
            mode="merge",
            match_keys=["id"],
            on_match="update",
            on_no_match_target="insert",
            on_no_match_source="soft_delete",
            soft_delete_column="is_deleted",
            soft_delete_value=True,
            soft_delete_active_value=False,
        )
        write_target(spark, incoming, target, write_config, path)
        result = spark.read.format("delta").load(path)
        rows = {r["id"]: r for r in result.collect()}
        # id=1 re-appeared: soft_delete_active_value (False) should be written
        assert rows[1]["is_deleted"] is False
        # id=2 was not in incoming source: becomes orphan → soft_delete_value (True)
        assert rows[2]["is_deleted"] is True
        # id=3 was not in incoming source: becomes orphan → soft_delete_value (True)
        assert rows[3]["is_deleted"] is True
        # id=4 was inserted: soft_delete_active_value (False) should be written
        assert rows[4]["is_deleted"] is False

    def test_merge_soft_delete_active_value_none_preserves_null(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """soft_delete_active_value=None (default) keeps existing null behaviour
        on update/insert."""
        from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

        path = tmp_delta_path("merge_sd_default_none")
        schema = StructType(
            [
                StructField("id", LongType()),
                StructField("val", StringType()),
                StructField("is_deleted", BooleanType(), nullable=True),
            ]
        )
        create_delta_table(
            spark,
            path,
            [
                {"id": 1, "val": "a", "is_deleted": True},  # previously soft-deleted
                {"id": 2, "val": "b", "is_deleted": False},  # will become orphan
            ],
            schema=schema,
        )
        incoming = spark.createDataFrame([{"id": 1, "val": "a_updated"}])
        target = Target(path=path)
        write_config = WriteConfig(
            mode="merge",
            match_keys=["id"],
            on_match="update",
            on_no_match_source="soft_delete",
            soft_delete_column="is_deleted",
            # soft_delete_active_value omitted → None
        )
        write_target(spark, incoming, target, write_config, path)
        result = spark.read.format("delta").load(path)
        rows = {r["id"]: r for r in result.collect()}
        # id=1 matched → null (existing behaviour preserved)
        assert rows[1]["is_deleted"] is None
        # id=2 orphan → soft_delete_value (True)
        assert rows[2]["is_deleted"] is True


@pytest.mark.spark
class TestUncertainExistenceGuard:
    """A failed existence probe must never route append/merge onto the
    destructive first-write (overwrite) branch."""

    @staticmethod
    def _break_probe(monkeypatch: pytest.MonkeyPatch) -> None:
        import weevr.delta as delta_mod

        def _boom(spark_arg, path_arg):  # type: ignore[no-untyped-def]
            raise RuntimeError("transient metastore outage")

        monkeypatch.setattr(delta_mod, "probe_delta_table_exists", _boom)

    def test_append_uncertain_raises_and_preserves_content(
        self, spark: SparkSession, simple_df, tmp_delta_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from weevr.errors.exceptions import ExecutionError

        path = tmp_delta_path("uncertain_append")
        create_delta_table(spark, path, [{"id": 99, "val": "keep"}])
        self._break_probe(monkeypatch)

        target = Target(path=path)
        with pytest.raises(ExecutionError) as exc_info:
            write_target(spark, simple_df, target, WriteConfig(mode="append"), path)
        message = str(exc_info.value)
        assert path in message
        assert "append" in message
        assert "probe" in message.lower()

        monkeypatch.undo()
        result = spark.read.format("delta").load(path)
        assert [r["val"] for r in result.collect()] == ["keep"]

    def test_merge_uncertain_raises_and_preserves_content(
        self, spark: SparkSession, simple_df, tmp_delta_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from weevr.errors.exceptions import ExecutionError

        path = tmp_delta_path("uncertain_merge")
        create_delta_table(spark, path, [{"id": 99, "val": "keep"}])
        self._break_probe(monkeypatch)

        target = Target(path=path)
        write_config = WriteConfig(mode="merge", match_keys=["id"])
        with pytest.raises(ExecutionError) as exc_info:
            write_target(spark, simple_df, target, write_config, path)
        message = str(exc_info.value)
        assert path in message
        assert "merge" in message

        monkeypatch.undo()
        result = spark.read.format("delta").load(path)
        assert [r["val"] for r in result.collect()] == ["keep"]

    def test_probe_failure_logs_warning_naming_target(
        self,
        spark: SparkSession,
        simple_df,
        tmp_delta_path,
        monkeypatch: pytest.MonkeyPatch,
        caplog,
    ) -> None:
        from weevr.errors.exceptions import ExecutionError

        path = tmp_delta_path("uncertain_logged")
        self._break_probe(monkeypatch)

        with pytest.raises(ExecutionError):
            write_target(spark, simple_df, Target(path=path), WriteConfig(mode="append"), path)
        assert path in caplog.text
        assert "transient metastore outage" in caplog.text

    def test_overwrite_uncertain_proceeds(
        self, spark: SparkSession, simple_df, tmp_delta_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Overwrite is destructive by contract — uncertainty does not block it."""
        path = tmp_delta_path("uncertain_overwrite")
        create_delta_table(spark, path, [{"id": 99, "val": "old"}])
        self._break_probe(monkeypatch)

        write_target(spark, simple_df, Target(path=path), WriteConfig(mode="overwrite"), path)

        monkeypatch.undo()
        result = spark.read.format("delta").load(path)
        assert result.count() == 2
        assert result.filter("id = 99").count() == 0

    def test_cdc_first_write_uncertain_raises_and_preserves_content(
        self, spark: SparkSession, tmp_delta_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from weevr.errors.exceptions import ExecutionError
        from weevr.model.load import CdcConfig
        from weevr.operations.writers import execute_cdc_merge

        path = tmp_delta_path("uncertain_cdc")
        create_delta_table(spark, path, [{"id": 99, "val": "keep"}])
        self._break_probe(monkeypatch)

        incoming = spark.createDataFrame([{"id": 1, "val": "a", "op": "I"}])
        write_config = WriteConfig(mode="merge", match_keys=["id"])
        cdc_config = CdcConfig(operation_column="op", insert_value="I")
        with pytest.raises(ExecutionError) as exc_info:
            execute_cdc_merge(spark, incoming, path, write_config, cdc_config)
        assert path in str(exc_info.value)

        monkeypatch.undo()
        result = spark.read.format("delta").load(path)
        assert [r["val"] for r in result.collect()] == ["keep"]

    def test_clean_absent_first_write_unaffected(
        self, spark: SparkSession, simple_df, tmp_delta_path
    ) -> None:
        """A probe that cleanly answers False keeps today's first-write behavior."""
        path = tmp_delta_path("clean_absent_append")
        rows = write_target(spark, simple_df, Target(path=path), WriteConfig(mode="append"), path)
        assert rows == 2
        assert spark.read.format("delta").load(path).count() == 2


@pytest.mark.spark
class TestCommitMetricsCounts:
    """Write counts come from guarded commit metrics on single-pass paths."""

    @pytest.fixture()
    def count_spy(self, monkeypatch: pytest.MonkeyPatch) -> list[int]:
        """Count DataFrame.count() calls — the eager pattern being removed.

        The generic action counter also sees the driver-side
        ``history(1).collect()`` metadata reads, which are the design's
        accepted (log-scale) cost; this spy isolates fact-scale counts.
        """
        from pyspark.sql import DataFrame

        calls: list[int] = []
        original = DataFrame.count

        def _spy(inner_self):  # type: ignore[no-untyped-def]
            calls.append(1)
            return original(inner_self)

        monkeypatch.setattr(DataFrame, "count", _spy)
        return calls

    def test_overwrite_and_append_counts_parity(
        self, spark: SparkSession, tmp_delta_path, count_spy: list[int]
    ) -> None:
        from weevr.model.target import Target
        from weevr.model.write import WriteConfig

        path = tmp_delta_path("cm_tgt")
        target = Target(path=path)
        df3 = spark.createDataFrame([{"id": i} for i in range(3)])
        df2 = spark.createDataFrame([{"id": i} for i in range(2)])

        n1 = write_target(spark, df3, target, WriteConfig(mode="overwrite"), path)
        n2 = write_target(spark, df2, target, WriteConfig(mode="append"), path)
        assert count_spy == []  # no eager data counts

        assert n1 == 3
        assert n2 == 2
        assert spark.read.format("delta").load(path).count() == 5

    def test_foreign_commit_degrades_to_zero_with_warning(
        self, spark: SparkSession, tmp_delta_path, caplog
    ) -> None:
        from weevr.operations.target_handle import TargetHandle

        path = tmp_delta_path("cm_guard")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(path)  # v0
        handle = TargetHandle(spark, path)
        pre = handle.current_version()
        assert pre == 0

        # Two commits land after capture: the engine's own would be v1;
        # a foreign writer pushes latest to v2
        spark.createDataFrame([{"id": 2}]).write.format("delta").mode("append").save(path)
        spark.createDataFrame([{"id": 3}]).write.format("delta").mode("append").save(path)

        metrics = handle.commit_metrics_after(pre)
        assert metrics is None  # degrade, never misattribute
        assert any("foreign commit" in r.message for r in caplog.records)

    def test_dimension_first_write_count_from_metrics(
        self, spark: SparkSession, tmp_delta_path, count_spy: list[int]
    ) -> None:
        from weevr.model.dimension import DimensionConfig
        from weevr.operations.dimension import DimensionMergeBuilder, execute_dimension_merge
        from weevr.operations.hashing import compute_dimension_keys

        path = tmp_delta_path("cm_dim_first")
        dim = DimensionConfig(
            business_key=["k"],
            surrogate_key={"name": "_sk", "columns": ["k"]},  # type: ignore[arg-type]
        )
        src = spark.createDataFrame([{"k": "A", "v": 1}, {"k": "B", "v": 2}])
        src = compute_dimension_keys(src, dim)
        builder = DimensionMergeBuilder(dim)

        result = execute_dimension_merge(spark, src, builder, path, "2026-01-01")
        assert count_spy == []  # pre-count gone

        assert result.rows_inserted == 2


@pytest.mark.spark
class TestCommitStamp:
    """Engine writes stamp their commits; attribution finds the own stamp
    exactly, regardless of interleaved foreign commits."""

    @staticmethod
    def _stamp(run_id: str = "run-abc", thread: str = "t1"):
        from weevr.operations.target_handle import CommitStamp

        return CommitStamp(run_id=run_id, thread=thread, loom="loomy", weave="weavy", mode="append")

    @staticmethod
    def _latest_user_metadata(spark: SparkSession, path: str) -> str | None:
        from weevr.delta import resolve_delta_table

        row = resolve_delta_table(spark, path).history(1).select("userMetadata").collect()[0]
        return row[0]

    def test_stamp_visible_and_parseable_in_history(
        self, spark: SparkSession, simple_df, tmp_delta_path
    ) -> None:
        import json

        path = tmp_delta_path("stamp_visible")
        stamp = self._stamp()
        write_target(
            spark, simple_df, Target(path=path), WriteConfig(mode="overwrite"), path, stamp=stamp
        )

        raw = self._latest_user_metadata(spark, path)
        assert raw is not None
        assert len(raw.encode()) < 1024
        data = json.loads(raw)
        assert data["engine"] == "weevr"
        assert isinstance(data["version"], int)
        assert data["run_id"] == "run-abc"
        assert data["thread"] == "t1"
        assert data["loom"] == "loomy"
        assert data["weave"] == "weavy"
        assert data["mode"] == "append"

    def test_unstamped_write_leaves_no_user_metadata(
        self, spark: SparkSession, simple_df, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("stamp_absent")
        write_target(spark, simple_df, Target(path=path), WriteConfig(mode="overwrite"), path)
        assert self._latest_user_metadata(spark, path) is None

    def test_interleaved_foreign_commit_count_is_exact(
        self, spark: SparkSession, simple_df, tmp_delta_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The F-006 guard-degrade scenario, upgraded: a foreign commit
        between pre-version capture and the engine write no longer costs
        the engine its own count."""
        from weevr.operations.target_handle import TargetHandle

        path = tmp_delta_path("stamp_interleave")
        create_delta_table(spark, path, [{"id": 0, "val": "seed"}])
        handle = TargetHandle(spark, path)

        original_cv = handle.current_version

        def _capture_then_foreign():
            version = original_cv()
            frame = spark.createDataFrame([{"id": 100, "val": "foreign"}])
            frame.write.format("delta").mode("append").save(path)
            return version

        monkeypatch.setattr(handle, "current_version", _capture_then_foreign)
        rows = write_target(
            spark,
            simple_df,
            Target(path=path),
            WriteConfig(mode="append"),
            path,
            handle=handle,
            stamp=self._stamp(),
        )
        assert rows == 2  # the engine's own commit, not 0 and not the foreign 1

    def test_foreign_only_window_is_unattributable(
        self, spark: SparkSession, tmp_delta_path, caplog
    ) -> None:
        """No own stamp in the window → absent, never another commit's metrics."""
        from weevr.operations.target_handle import TargetHandle

        path = tmp_delta_path("stamp_foreign_only")
        create_delta_table(spark, path, [{"id": 0}])
        handle = TargetHandle(spark, path)
        pre = handle.current_version()

        # An engine-shaped stamp from a DIFFERENT run lands in the window.
        other = self._stamp(run_id="run-other")
        frame = spark.createDataFrame([{"id": 1}])
        frame.write.format("delta").mode("append").option("userMetadata", other.to_json()).save(
            path
        )

        metrics = handle.commit_metrics_after(pre, stamp=self._stamp())
        assert metrics is None
        assert "unattributable" in caplog.text

    def test_own_stamp_wins_over_foreign_engine_shaped_stamp(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Attribution matches on the run_id+thread core — an engine-shaped
        stamp with a different run_id in the same window is not attributed."""
        from weevr.operations.target_handle import TargetHandle

        path = tmp_delta_path("stamp_core_match")
        create_delta_table(spark, path, [{"id": 0}])
        handle = TargetHandle(spark, path)
        pre = handle.current_version()

        own = self._stamp()
        other = self._stamp(run_id="run-other")
        spark.createDataFrame([{"id": 1}, {"id": 2}, {"id": 3}]).write.format("delta").mode(
            "append"
        ).option("userMetadata", other.to_json()).save(path)
        spark.createDataFrame([{"id": 4}, {"id": 5}]).write.format("delta").mode("append").option(
            "userMetadata", own.to_json()
        ).save(path)

        metrics = handle.commit_metrics_after(pre, stamp=own)
        assert metrics is not None
        assert int(metrics["numOutputRows"]) == 2  # the own commit, not the foreign 3


@pytest.mark.spark
class TestVersionReadFailureDistinction:
    """A failed history read on an existing table is not a fresh table."""

    def test_unreadable_version_skips_metrics_without_foreign_commit_claim(
        self, spark: SparkSession, tmp_delta_path, monkeypatch: pytest.MonkeyPatch, caplog
    ) -> None:
        from weevr.operations import target_handle as th_mod
        from weevr.operations.target_handle import TargetHandle

        path = tmp_delta_path("vrf_tgt")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(path)
        handle = TargetHandle(spark, path)
        assert handle.exists() is True

        original = th_mod._delta.resolve_delta_table

        def _boom(spark_arg, path_arg):  # type: ignore[no-untyped-def]
            raise RuntimeError("history unavailable")

        monkeypatch.setattr(th_mod._delta, "resolve_delta_table", _boom)
        pre = handle.current_version()
        assert pre is None  # degraded — but flagged as a read failure

        monkeypatch.setattr(th_mod._delta, "resolve_delta_table", original)
        metrics = handle.commit_metrics_after(pre)
        assert metrics is None
        messages = " ".join(r.message for r in caplog.records)
        assert "unreadable" in messages
        assert "foreign commit" not in messages  # never the phantom diagnosis
