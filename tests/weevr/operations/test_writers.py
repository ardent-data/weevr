"""Tests for target mapping (apply_target_mapping) and Delta writers (write_target)."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from spark_helpers import create_delta_table
from weevr.model.target import ColumnMapping, Target
from weevr.model.types import SparkExpr
from weevr.model.write import WriteConfig
from weevr.operations.writers import _quote_identifier, apply_target_mapping, write_target


class TestQuoteIdentifier:
    """Unit tests for _quote_identifier SQL escaping (no Spark needed)."""

    def test_plain_name(self) -> None:
        assert _quote_identifier("column_name") == "`column_name`"

    def test_name_with_backtick(self) -> None:
        assert _quote_identifier("col`umn") == "`col``umn`"

    def test_name_with_spaces(self) -> None:
        assert _quote_identifier("my column") == "`my column`"

    def test_empty_string(self) -> None:
        assert _quote_identifier("") == "``"


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
            mapping_mode="explicit",
            columns={"amount": ColumnMapping(type="string")},
        )
        result = apply_target_mapping(source_df, target, spark)
        assert result.columns == ["amount"]
        assert result.schema["amount"].dataType == StringType()

    def test_explicit_excludes_dropped_columns(self, source_df, spark: SparkSession) -> None:
        target = Target(
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
        target = Target(mapping_mode="explicit")
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
