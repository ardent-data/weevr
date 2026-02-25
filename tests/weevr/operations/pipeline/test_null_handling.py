"""Tests for null-handling pipeline step handlers — fill_null, coalesce."""

import pytest
from pyspark.sql import SparkSession

from weevr.model.pipeline import CoalesceParams, FillNullParams
from weevr.operations.pipeline.null_handling import apply_coalesce, apply_fill_null

pytestmark = pytest.mark.spark


class TestApplyFillNull:
    """Test fill_null step handler."""

    def test_numeric_defaults(self, spark: SparkSession):
        """Null numeric column filled with default."""
        df = spark.createDataFrame([(1, None), (2, 10)], ["id", "amount"])
        params = FillNullParams(columns={"amount": 0})
        result = apply_fill_null(df, params)
        rows = {r["id"]: r["amount"] for r in result.collect()}
        assert rows[1] == 0
        assert rows[2] == 10

    def test_string_defaults(self, spark: SparkSession):
        """Null string column filled with default."""
        df = spark.createDataFrame([(1, None), (2, "alice")], ["id", "name"])
        params = FillNullParams(columns={"name": "unknown"})
        result = apply_fill_null(df, params)
        rows = {r["id"]: r["name"] for r in result.collect()}
        assert rows[1] == "unknown"
        assert rows[2] == "alice"

    def test_no_nulls_unchanged(self, spark: SparkSession):
        """No nulls present → data unchanged."""
        df = spark.createDataFrame([(1, 10), (2, 20)], ["id", "amount"])
        params = FillNullParams(columns={"amount": 0})
        result = apply_fill_null(df, params)
        rows = {r["id"]: r["amount"] for r in result.collect()}
        assert rows[1] == 10
        assert rows[2] == 20


class TestApplyCoalesce:
    """Test coalesce step handler."""

    def test_first_non_null(self, spark: SparkSession):
        """First non-null value selected from source columns."""
        df = spark.createDataFrame(
            [(None, "work@x.com"), ("p@x.com", "w@x.com")],
            ["personal", "work"],
        )
        params = CoalesceParams(columns={"email": ["personal", "work"]})
        result = apply_coalesce(df, params)
        rows = result.collect()
        assert rows[0]["email"] == "work@x.com"
        assert rows[1]["email"] == "p@x.com"

    def test_all_null(self, spark: SparkSession):
        """All sources null → output is null."""
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType([StructField("a", StringType()), StructField("b", StringType())])
        df = spark.createDataFrame([(None, None)], schema=schema)
        params = CoalesceParams(columns={"out": ["a", "b"]})
        result = apply_coalesce(df, params)
        assert result.collect()[0]["out"] is None

    def test_new_column(self, spark: SparkSession):
        """Output column name doesn't exist yet → created."""
        df = spark.createDataFrame([(1, 2)], ["a", "b"])
        params = CoalesceParams(columns={"merged": ["a", "b"]})
        result = apply_coalesce(df, params)
        assert "merged" in result.columns
        assert result.collect()[0]["merged"] == 1
