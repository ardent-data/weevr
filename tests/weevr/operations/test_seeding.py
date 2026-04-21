"""Tests for seed execution operations."""

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_helpers import create_delta_table
from weevr.errors.exceptions import ExecutionError
from weevr.model.dimension import DimensionConfig
from weevr.model.seed import SeedConfig
from weevr.operations.seeding import (
    build_seed_dataframe,
    build_system_member_rows,
    check_seed_trigger,
    execute_seeds,
)


@pytest.mark.spark
class TestCheckSeedTriggerFirstWrite:
    """check_seed_trigger with on='first_write'."""

    def test_table_missing_triggers(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("fw_missing")
        config = SeedConfig(on="first_write", rows=[{"id": 1}])
        assert check_seed_trigger(spark, path, config) is True

    def test_table_exists_does_not_trigger(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("fw_exists")
        create_delta_table(spark, path, [{"id": 1, "name": "alice"}])
        config = SeedConfig(on="first_write", rows=[{"id": 99}])
        assert check_seed_trigger(spark, path, config) is False


@pytest.mark.spark
class TestCheckSeedTriggerEmpty:
    """check_seed_trigger with on='empty'."""

    def test_table_missing_triggers(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("empty_missing")
        config = SeedConfig(on="empty", rows=[{"id": 1}])
        assert check_seed_trigger(spark, path, config) is True

    def test_table_with_zero_rows_triggers(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("empty_zero")
        schema = StructType([StructField("id", LongType(), True)])
        create_delta_table(spark, path, [], schema=schema)
        config = SeedConfig(on="empty", rows=[{"id": 1}])
        assert check_seed_trigger(spark, path, config) is True

    def test_table_with_rows_does_not_trigger(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("empty_has_rows")
        create_delta_table(spark, path, [{"id": 1, "name": "alice"}])
        config = SeedConfig(on="empty", rows=[{"id": 99}])
        assert check_seed_trigger(spark, path, config) is False


@pytest.mark.spark
class TestBuildSeedDataframe:
    """build_seed_dataframe — schema casting and null filling."""

    def test_values_cast_to_target_schema(self, spark: SparkSession) -> None:
        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("label", StringType(), True),
            ]
        )
        rows = [{"id": 1, "label": "hello"}]
        df = build_seed_dataframe(spark, rows, schema)
        assert df.schema == schema
        collected = df.collect()
        assert len(collected) == 1
        assert collected[0]["id"] == 1
        assert collected[0]["label"] == "hello"

    def test_missing_column_fills_null(self, spark: SparkSession) -> None:
        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("label", StringType(), True),
            ]
        )
        rows = [{"id": 1}]
        df = build_seed_dataframe(spark, rows, schema)
        collected = df.collect()
        assert collected[0]["label"] is None

    def test_extra_columns_in_row_are_ignored(self, spark: SparkSession) -> None:
        schema = StructType([StructField("id", LongType(), True)])
        rows = [{"id": 1, "extra_col": "ignored"}]
        df = build_seed_dataframe(spark, rows, schema)
        assert df.columns == ["id"]

    def test_multiple_rows_preserved(self, spark: SparkSession) -> None:
        schema = StructType([StructField("id", LongType(), True)])
        rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        df = build_seed_dataframe(spark, rows, schema)
        assert df.count() == 3

    def test_invalid_cast_raises_execution_error(self, spark: SparkSession) -> None:
        schema = StructType([StructField("id", IntegerType(), True)])
        rows = [{"id": "not_a_number"}]
        with pytest.raises(ExecutionError):
            build_seed_dataframe(spark, rows, schema)


@pytest.mark.spark
class TestExecuteSeeds:
    """execute_seeds — end-to-end behaviour."""

    def test_first_write_missing_table_inserts_rows(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("exec_fw_new")
        config = SeedConfig(on="first_write", rows=[{"id": 1, "label": "a"}])
        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("label", StringType(), True),
            ]
        )
        result = execute_seeds(spark, config, path, schema)
        assert result.triggered is True
        assert result.rows_inserted == 1
        assert result.trigger_condition == "first_write"
        assert result.skipped_reason is None
        written = spark.read.format("delta").load(path)
        assert written.count() == 1

    def test_first_write_existing_table_skips(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("exec_fw_exists")
        create_delta_table(spark, path, [{"id": 99, "label": "existing"}])
        config = SeedConfig(on="first_write", rows=[{"id": 1, "label": "seed"}])
        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("label", StringType(), True),
            ]
        )
        result = execute_seeds(spark, config, path, schema)
        assert result.triggered is False
        assert result.rows_inserted == 0
        assert result.skipped_reason is not None

    def test_empty_trigger_empty_table_inserts_rows(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("exec_empty_zero")
        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("label", StringType(), True),
            ]
        )
        create_delta_table(spark, path, [], schema=schema)
        config = SeedConfig(on="empty", rows=[{"id": 1, "label": "seed"}])
        result = execute_seeds(spark, config, path, schema)
        assert result.triggered is True
        assert result.rows_inserted == 1
        written = spark.read.format("delta").load(path)
        assert written.count() == 1

    def test_empty_trigger_has_rows_skips(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("exec_empty_has_rows")
        create_delta_table(spark, path, [{"id": 99, "label": "existing"}])
        config = SeedConfig(on="empty", rows=[{"id": 1, "label": "seed"}])
        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("label", StringType(), True),
            ]
        )
        result = execute_seeds(spark, config, path, schema)
        assert result.triggered is False
        assert result.rows_inserted == 0

    def test_multiple_seed_rows_all_inserted(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("exec_multi")
        config = SeedConfig(
            on="first_write",
            rows=[{"id": 1, "label": "a"}, {"id": 2, "label": "b"}, {"id": 3, "label": "c"}],
        )
        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("label", StringType(), True),
            ]
        )
        result = execute_seeds(spark, config, path, schema)
        assert result.triggered is True
        assert result.rows_inserted == 3

    def test_result_trigger_condition_matches_config(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("exec_condition")
        config = SeedConfig(on="empty", rows=[{"id": 1}])
        schema = StructType([StructField("id", LongType(), True)])
        result = execute_seeds(spark, config, path, schema)
        assert result.trigger_condition == "empty"

    def test_seeds_appended_when_table_exists_and_triggered(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("exec_append")
        schema = StructType([StructField("id", LongType(), True)])
        # Create empty table — "empty" mode should trigger and append
        create_delta_table(spark, path, [], schema=schema)
        config = SeedConfig(on="empty", rows=[{"id": 42}])
        result = execute_seeds(spark, config, path, schema)
        assert result.triggered is True
        written = spark.read.format("delta").load(path)
        assert written.count() == 1
        assert written.collect()[0]["id"] == 42


@pytest.mark.spark
class TestBuildSystemMemberRows:
    """Tests for build_system_member_rows."""

    def _make_dim_config(self, **overrides):  # type: ignore[no-untyped-def]
        base = {
            "business_key": ["customer_id"],
            "surrogate_key": {"name": "_sk", "columns": ["customer_id"]},
        }
        return DimensionConfig(**{**base, **overrides})

    def test_default_system_members(self, spark: SparkSession) -> None:
        """Default members are unknown (-1) and not_applicable (-2)."""
        dim = self._make_dim_config(seed_system_members=True)
        df = spark.createDataFrame([{"customer_id": "C1", "_sk": 1, "name": "Alice"}])
        rows = build_system_member_rows(dim, df)
        assert len(rows) == 2
        sks = {r["_sk"] for r in rows}
        assert sks == {-1, -2}

    def test_custom_system_members(self, spark: SparkSession) -> None:
        """Custom system_members override defaults."""
        dim = self._make_dim_config(
            seed_system_members=True,
            system_members=[  # type: ignore[arg-type]
                {"sk": -10, "code": "custom", "label": "Custom"},
            ],
        )
        df = spark.createDataFrame([{"customer_id": "C1", "_sk": 1, "name": "Alice"}])
        rows = build_system_member_rows(dim, df)
        assert len(rows) == 1
        assert rows[0]["_sk"] == -10

    def test_sk_column_gets_negative_value(self, spark: SparkSession) -> None:
        """SK column receives the configured negative value."""
        dim = self._make_dim_config(seed_system_members=True)
        df = spark.createDataFrame([{"customer_id": "C1", "_sk": 1}])
        rows = build_system_member_rows(dim, df)
        assert all(r["_sk"] < 0 for r in rows)

    def test_string_columns_get_semantic_label(self, spark: SparkSession) -> None:
        """String columns filled with semantic label from resolve_type_defaults."""
        dim = self._make_dim_config(seed_system_members=True)
        df = spark.createDataFrame([{"customer_id": "C1", "_sk": 1, "name": "Alice"}])
        rows = build_system_member_rows(dim, df)
        unknown_row = next(r for r in rows if r["_sk"] == -1)
        assert unknown_row["name"] == "Unknown"

    def test_numeric_columns_get_zero(self, spark: SparkSession) -> None:
        """Numeric columns filled with 0."""
        dim = self._make_dim_config(seed_system_members=True)
        schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("_sk", LongType(), True),
                StructField("amount", DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(
            [{"customer_id": "C1", "_sk": 1, "amount": 99.9}],
            schema=schema,
        )
        rows = build_system_member_rows(dim, df)
        assert rows[0]["amount"] == 0.0

    def test_boolean_columns_get_false(self, spark: SparkSession) -> None:
        """Boolean columns filled with False."""
        dim = self._make_dim_config(seed_system_members=True)
        schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("_sk", LongType(), True),
                StructField("is_active", BooleanType(), True),
            ]
        )
        df = spark.createDataFrame(
            [{"customer_id": "C1", "_sk": 1, "is_active": True}],
            schema=schema,
        )
        rows = build_system_member_rows(dim, df)
        assert rows[0]["is_active"] is False

    def test_label_column_gets_label(self, spark: SparkSession) -> None:
        """label_column receives the member's label."""
        dim = self._make_dim_config(
            seed_system_members=True,
            label_column="description",
        )
        df = spark.createDataFrame([{"customer_id": "C1", "_sk": 1, "description": "test"}])
        rows = build_system_member_rows(dim, df)
        unknown_row = next(r for r in rows if r["_sk"] == -1)
        assert unknown_row["description"] == "Unknown"

    def test_label_column_none_skips(self, spark: SparkSession) -> None:
        """When label_column is None, no label assignment."""
        dim = self._make_dim_config(seed_system_members=True)
        df = spark.createDataFrame([{"customer_id": "C1", "_sk": 1, "description": "test"}])
        rows = build_system_member_rows(dim, df)
        # description gets type default, not label
        unknown_row = next(r for r in rows if r["_sk"] == -1)
        assert unknown_row["description"] == "Unknown"

    def test_business_key_gets_code(self, spark: SparkSession) -> None:
        """BK columns get the member's code."""
        dim = self._make_dim_config(seed_system_members=True)
        df = spark.createDataFrame([{"customer_id": "C1", "_sk": 1}])
        rows = build_system_member_rows(dim, df)
        unknown_row = next(r for r in rows if r["_sk"] == -1)
        assert unknown_row["customer_id"] == "unknown"

    def test_integer_business_key_gets_type_default(self, spark: SparkSession) -> None:
        """IntegerType BK column gets 0, not the string code (dim_calendar pattern)."""
        dim = self._make_dim_config(
            business_key=["calendar_id"],
            surrogate_key={"name": "_sk", "columns": ["calendar_id"]},
            seed_system_members=True,
        )
        schema = StructType(
            [
                StructField("calendar_id", IntegerType(), True),
                StructField("_sk", LongType(), True),
            ]
        )
        df = spark.createDataFrame([{"calendar_id": 20240601, "_sk": 1}], schema=schema)
        rows = build_system_member_rows(dim, df)
        assert rows[0]["calendar_id"] == 0
        assert rows[1]["calendar_id"] == 0

    def test_long_business_key_gets_type_default(self, spark: SparkSession) -> None:
        """LongType BK column gets 0, not the string code."""
        dim = self._make_dim_config(
            business_key=["account_id"],
            surrogate_key={"name": "_sk", "columns": ["account_id"]},
            seed_system_members=True,
        )
        schema = StructType(
            [
                StructField("account_id", LongType(), True),
                StructField("_sk", LongType(), True),
            ]
        )
        df = spark.createDataFrame([{"account_id": 1_000_000, "_sk": 1}], schema=schema)
        rows = build_system_member_rows(dim, df)
        assert rows[0]["account_id"] == 0
        assert rows[1]["account_id"] == 0

    def test_date_business_key_gets_type_default(self, spark: SparkSession) -> None:
        """DateType BK column gets date(1970, 1, 1), not the string code."""
        dim = self._make_dim_config(
            business_key=["as_of_date"],
            surrogate_key={"name": "_sk", "columns": ["as_of_date"]},
            seed_system_members=True,
        )
        schema = StructType(
            [
                StructField("as_of_date", DateType(), True),
                StructField("_sk", LongType(), True),
            ]
        )
        df = spark.createDataFrame([{"as_of_date": date(2024, 6, 1), "_sk": 1}], schema=schema)
        rows = build_system_member_rows(dim, df)
        assert rows[0]["as_of_date"] == date(1970, 1, 1)
        assert rows[1]["as_of_date"] == date(1970, 1, 1)

    def test_decimal_business_key_gets_type_default(self, spark: SparkSession) -> None:
        """DecimalType BK column gets Decimal(0) at the column's scale."""
        dim = self._make_dim_config(
            business_key=["account_id"],
            surrogate_key={"name": "_sk", "columns": ["account_id"]},
            seed_system_members=True,
        )
        schema = StructType(
            [
                StructField("account_id", DecimalType(12, 2), True),
                StructField("_sk", LongType(), True),
            ]
        )
        df = spark.createDataFrame([{"account_id": Decimal("19.99"), "_sk": 1}], schema=schema)
        rows = build_system_member_rows(dim, df)
        assert rows[0]["account_id"] == Decimal(0)
        assert rows[1]["account_id"] == Decimal(0)

    def test_composite_bk_mixed_string_and_integer(self, spark: SparkSession) -> None:
        """Composite BK: string column gets member.code, integer column gets 0."""
        dim = self._make_dim_config(
            business_key=["customer_id", "tenant_id"],
            surrogate_key={"name": "_sk", "columns": ["customer_id", "tenant_id"]},
            seed_system_members=True,
        )
        schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("tenant_id", IntegerType(), True),
                StructField("_sk", LongType(), True),
            ]
        )
        df = spark.createDataFrame(
            [{"customer_id": "C1", "tenant_id": 42, "_sk": 1}], schema=schema
        )
        rows = build_system_member_rows(dim, df)
        # String BK column receives member.code directly (lowercase);
        # resolve_type_defaults would have returned "Unknown" (title-cased).
        assert rows[0]["customer_id"] == "unknown"
        assert rows[1]["customer_id"] == "not_applicable"
        # Integer BK column keeps the type default from resolve_type_defaults.
        assert rows[0]["tenant_id"] == 0
        assert rows[1]["tenant_id"] == 0

    def test_scd_columns_set_for_track_history(self, spark: SparkSession) -> None:
        """SCD columns set when track_history is True."""
        dim = self._make_dim_config(
            seed_system_members=True,
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "cd": {
                    "columns": "auto",
                    "on_change": "version",
                },
            },
        )
        df = spark.createDataFrame([{"customer_id": "C1", "_sk": 1, "name": "test"}])
        rows = build_system_member_rows(dim, df)
        unknown_row = next(r for r in rows if r["_sk"] == -1)
        assert unknown_row["_valid_from"] == "1970-01-01"
        assert unknown_row["_valid_to"] == "9999-12-31"
        assert unknown_row["_is_current"] is True
