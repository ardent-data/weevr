"""Tests for seed execution operations."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from spark_helpers import create_delta_table
from weevr.errors.exceptions import ExecutionError
from weevr.model.seed import SeedConfig
from weevr.operations.seeding import (
    build_seed_dataframe,
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
