"""Tests for fact target validation."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType

from weevr.model.fact import FactConfig
from weevr.operations.fact import validate_fact_target

pytestmark = pytest.mark.spark


class TestValidateFactTarget:
    """Test fact target validation."""

    def test_all_fk_columns_present(self, spark: SparkSession):
        """All FK columns present in output DataFrame returns empty list."""
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
        assert diagnostics == []

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
        assert len(diagnostics) == 1
        assert diagnostics[0] == "ERROR: FK column 'dim_product_id' not found in output DataFrame"

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
        assert len(diagnostics) == 1
        assert diagnostics[0] == "ERROR: FK column 'dim_store_id' not found in output DataFrame"

    def test_multiple_fk_columns_all_missing(self, spark: SparkSession):
        """Multiple missing FK columns returns multiple ERRORs."""
        schema = StructType([StructField("measure", IntegerType(), True)])
        df = spark.createDataFrame([(100,)], schema)
        fact_config = FactConfig(foreign_keys=["dim_date_id", "dim_product_id", "dim_store_id"])
        diagnostics = validate_fact_target(df, fact_config)
        assert len(diagnostics) == 3
        assert "ERROR: FK column 'dim_date_id' not found in output DataFrame" in diagnostics
        assert "ERROR: FK column 'dim_product_id' not found in output DataFrame" in diagnostics
        assert "ERROR: FK column 'dim_store_id' not found in output DataFrame" in diagnostics

    def test_empty_dataframe_columns(self, spark: SparkSession):
        """Empty DataFrame with FK columns returns ERRORs for all missing."""
        schema = StructType([])
        df = spark.createDataFrame([], schema)
        fact_config = FactConfig(foreign_keys=["dim_date_id", "dim_product_id"])
        diagnostics = validate_fact_target(df, fact_config)
        assert len(diagnostics) == 2
        assert "ERROR: FK column 'dim_date_id' not found in output DataFrame" in diagnostics
        assert "ERROR: FK column 'dim_product_id' not found in output DataFrame" in diagnostics

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
