"""Tests for fact target validation."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType

from weevr.model.fact import FactConfig
from weevr.operations.fact import (
    check_fact_sentinels,
    validate_fact_schema,
    validate_fact_target,
)

pytestmark = pytest.mark.spark


class TestValidateFactTarget:
    """Test fact target validation."""

    def test_all_fk_columns_present(self, spark: SparkSession):
        """All FK columns present produces no ERROR diagnostics."""
        schema = StructType(
            [
                StructField("dim_date_id", IntegerType(), True),
                StructField("dim_product_id", IntegerType(), True),
                StructField("measure", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame([(1, 2, 100)], schema)
        fact_config = FactConfig(foreign_keys=["dim_date_id", "dim_product_id"])
        diagnostics = validate_fact_target(df, fact_config)
        errors = [d for d in diagnostics if d.startswith("ERROR:")]
        assert errors == []

    def test_single_fk_column_missing(self, spark: SparkSession):
        """Missing FK column returns ERROR diagnostic."""
        schema = StructType(
            [
                StructField("dim_date_id", IntegerType(), True),
                StructField("measure", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame([(1, 100)], schema)
        fact_config = FactConfig(foreign_keys=["dim_date_id", "dim_product_id"])
        diagnostics = validate_fact_target(df, fact_config)
        errors = [d for d in diagnostics if d.startswith("ERROR:")]
        assert len(errors) == 1
        assert errors[0] == "ERROR: FK column 'dim_product_id' not found in output DataFrame"

    def test_multiple_fk_columns_one_missing(self, spark: SparkSession):
        """Multiple FK columns with one missing returns one ERROR."""
        schema = StructType(
            [
                StructField("dim_date_id", IntegerType(), True),
                StructField("dim_product_id", IntegerType(), True),
                StructField("measure", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame([(1, 2, 100)], schema)
        fact_config = FactConfig(foreign_keys=["dim_date_id", "dim_product_id", "dim_store_id"])
        diagnostics = validate_fact_target(df, fact_config)
        errors = [d for d in diagnostics if d.startswith("ERROR:")]
        assert len(errors) == 1
        assert errors[0] == "ERROR: FK column 'dim_store_id' not found in output DataFrame"

    def test_multiple_fk_columns_all_missing(self, spark: SparkSession):
        """Multiple missing FK columns returns multiple ERRORs."""
        schema = StructType([StructField("measure", IntegerType(), True)])
        df = spark.createDataFrame([(100,)], schema)
        fact_config = FactConfig(foreign_keys=["dim_date_id", "dim_product_id", "dim_store_id"])
        diagnostics = validate_fact_target(df, fact_config)
        errors = [d for d in diagnostics if d.startswith("ERROR:")]
        assert len(errors) == 3
        assert "ERROR: FK column 'dim_date_id' not found in output DataFrame" in errors
        assert "ERROR: FK column 'dim_product_id' not found in output DataFrame" in errors
        assert "ERROR: FK column 'dim_store_id' not found in output DataFrame" in errors

    def test_empty_dataframe_columns(self, spark: SparkSession):
        """Empty DataFrame with FK columns returns ERRORs for all missing."""
        schema = StructType([])
        df = spark.createDataFrame([], schema)
        fact_config = FactConfig(foreign_keys=["dim_date_id", "dim_product_id"])
        diagnostics = validate_fact_target(df, fact_config)
        errors = [d for d in diagnostics if d.startswith("ERROR:")]
        assert len(errors) == 2
        assert "ERROR: FK column 'dim_date_id' not found in output DataFrame" in errors
        assert "ERROR: FK column 'dim_product_id' not found in output DataFrame" in errors

    def test_fk_column_no_sentinel_values_warns(self, spark: SparkSession):
        """FK column with no sentinel values produces WARN diagnostic."""
        schema = StructType(
            [
                StructField("sk_customer", IntegerType(), True),
                StructField("amount", IntegerType(), True),
            ]
        )
        # All positive values — no sentinel (-1 or -4)
        df = spark.createDataFrame([(1, 100), (2, 200)], schema)
        fact_config = FactConfig(foreign_keys=["sk_customer"])
        diagnostics = validate_fact_target(df, fact_config)
        warns = [d for d in diagnostics if d.startswith("WARN:")]
        assert len(warns) == 1
        assert "sk_customer" in warns[0]
        assert "sentinel" in warns[0]

    def test_fk_column_with_sentinel_no_warn(self, spark: SparkSession):
        """FK column containing sentinel values produces no WARN."""
        schema = StructType(
            [
                StructField("sk_customer", IntegerType(), True),
                StructField("amount", IntegerType(), True),
            ]
        )
        # Contains -1 (missing sentinel)
        df = spark.createDataFrame([(-1, 0), (1, 100)], schema)
        fact_config = FactConfig(foreign_keys=["sk_customer"])
        diagnostics = validate_fact_target(df, fact_config)
        warns = [d for d in diagnostics if d.startswith("WARN:")]
        assert len(warns) == 0


class TestValidateFactSchema:
    """Test the schema-only FK existence check."""

    def test_no_spark_actions(self, spark: SparkSession, spark_action_counter: dict[str, int]):
        """Existence validation is driver-side only."""
        schema = StructType([StructField("dim_date_id", IntegerType(), True)])
        df = spark.createDataFrame([(1,)], schema)
        fact_config = FactConfig(foreign_keys=["dim_date_id", "dim_product_id"])
        diagnostics = validate_fact_schema(df, fact_config)
        assert diagnostics == ["ERROR: FK column 'dim_product_id' not found in output DataFrame"]
        assert spark_action_counter["total"] == 0


class TestCheckFactSentinels:
    """Test the single-pass sentinel advisory check."""

    def test_single_action_for_multiple_fks(
        self, spark: SparkSession, spark_action_counter: dict[str, int]
    ):
        """All FK columns are checked in one Spark action."""
        schema = StructType(
            [
                StructField("sk_customer", IntegerType(), True),
                StructField("sk_product", IntegerType(), True),
                StructField("sk_date", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame([(-1, 1, 1), (2, 2, 2)], schema)
        fact_config = FactConfig(foreign_keys=["sk_customer", "sk_product", "sk_date"])
        diagnostics = check_fact_sentinels(df, fact_config)
        warned_cols = {d.split("'")[1] for d in diagnostics}
        assert warned_cols == {"sk_product", "sk_date"}
        assert spark_action_counter["total"] == 1

    def test_exact_beyond_1000_distinct_values(self, spark: SparkSession):
        """A sentinel among >1000 distinct FK values is always found.

        Regression for the sampled probe: distinct().limit(1000) could
        miss the sentinel and emit a false WARN.
        """
        schema = StructType([StructField("sk_customer", IntegerType(), True)])
        rows = [(v,) for v in range(1, 1501)] + [(-1,)]
        df = spark.createDataFrame(rows, schema)
        fact_config = FactConfig(foreign_keys=["sk_customer"])
        diagnostics = check_fact_sentinels(df, fact_config)
        assert diagnostics == []

    def test_all_null_fk_warns(self, spark: SparkSession):
        """An all-null FK column contains no sentinels and WARNs."""
        schema = StructType([StructField("sk_customer", IntegerType(), True)])
        df = spark.createDataFrame([(None,), (None,)], schema)
        fact_config = FactConfig(foreign_keys=["sk_customer"])
        diagnostics = check_fact_sentinels(df, fact_config)
        assert len(diagnostics) == 1
        assert "sk_customer" in diagnostics[0]

    def test_empty_dataframe_warns(self, spark: SparkSession):
        """An empty DataFrame with present FK columns WARNs."""
        schema = StructType([StructField("sk_customer", IntegerType(), True)])
        df = spark.createDataFrame([], schema)
        fact_config = FactConfig(foreign_keys=["sk_customer"])
        diagnostics = check_fact_sentinels(df, fact_config)
        assert len(diagnostics) == 1

    def test_duplicate_fk_names_single_action(
        self, spark: SparkSession, spark_action_counter: dict[str, int]
    ):
        """Duplicate FK names are deduplicated in the aggregation.

        FactConfig does not enforce FK uniqueness, so a duplicated name
        must not produce ambiguous aggregation aliases. Diagnostics keep
        one entry per declared FK, matching the old per-column loop.
        """
        schema = StructType([StructField("sk_customer", IntegerType(), True)])
        df = spark.createDataFrame([(1,), (2,)], schema)
        fact_config = FactConfig(foreign_keys=["sk_customer", "sk_customer"])
        diagnostics = check_fact_sentinels(df, fact_config)
        assert len(diagnostics) == 2
        assert all("sk_customer" in d for d in diagnostics)
        assert spark_action_counter["total"] == 1

    def test_aggregation_failure_swallowed_as_advisory(self, spark: SparkSession, monkeypatch):
        """A failing sentinel aggregation returns no diagnostics and
        raises nothing — the check is advisory only."""
        from pyspark.sql import DataFrame

        schema = StructType([StructField("sk_customer", IntegerType(), True)])
        df = spark.createDataFrame([(1,)], schema)

        def _boom(self, *args, **kwargs):
            raise RuntimeError("aggregation failed")

        monkeypatch.setattr(DataFrame, "agg", _boom)
        fact_config = FactConfig(foreign_keys=["sk_customer"])
        diagnostics = check_fact_sentinels(df, fact_config)
        assert diagnostics == []

    def test_absent_fk_columns_skipped(
        self, spark: SparkSession, spark_action_counter: dict[str, int]
    ):
        """Missing FK columns are the existence check's job — skipped here."""
        schema = StructType([StructField("amount", IntegerType(), True)])
        df = spark.createDataFrame([(100,)], schema)
        fact_config = FactConfig(foreign_keys=["sk_customer"])
        diagnostics = check_fact_sentinels(df, fact_config)
        assert diagnostics == []
        assert spark_action_counter["total"] == 0
