"""Tests for the case_when pipeline step handler."""

import pytest
from pyspark.sql import SparkSession

from weevr.model.pipeline import CaseWhenBranch, CaseWhenParams
from weevr.model.types import SparkExpr
from weevr.operations.pipeline.conditional import apply_case_when

pytestmark = pytest.mark.spark


class TestApplyCaseWhen:
    """Test case_when step handler."""

    def test_basic_branches_with_otherwise(self, spark: SparkSession):
        """Two branches + otherwise assigns correct values."""
        df = spark.createDataFrame([(10,), (150,), (50,)], ["amount"])
        params = CaseWhenParams(
            column="tier",
            cases=[
                CaseWhenBranch(
                    when=SparkExpr("amount > 100"),
                    then=SparkExpr("'high'"),
                ),
                CaseWhenBranch(
                    when=SparkExpr("amount > 40"),
                    then=SparkExpr("'medium'"),
                ),
            ],
            otherwise=SparkExpr("'low'"),
        )
        result = apply_case_when(df, params)
        rows = {r["amount"]: r["tier"] for r in result.collect()}
        assert rows[10] == "low"
        assert rows[150] == "high"
        assert rows[50] == "medium"

    def test_without_otherwise(self, spark: SparkSession):
        """Unmatched rows get null when otherwise is absent."""
        df = spark.createDataFrame([(10,), (200,)], ["amount"])
        params = CaseWhenParams(
            column="tier",
            cases=[
                CaseWhenBranch(
                    when=SparkExpr("amount > 100"),
                    then=SparkExpr("'high'"),
                ),
            ],
        )
        result = apply_case_when(df, params)
        rows = {r["amount"]: r["tier"] for r in result.collect()}
        assert rows[200] == "high"
        assert rows[10] is None

    def test_overwrites_existing_column(self, spark: SparkSession):
        """Output column already exists → replaced."""
        df = spark.createDataFrame([("old", 10)], ["tier", "amount"])
        params = CaseWhenParams(
            column="tier",
            cases=[
                CaseWhenBranch(
                    when=SparkExpr("amount > 5"),
                    then=SparkExpr("'new'"),
                ),
            ],
            otherwise=SparkExpr("'default'"),
        )
        result = apply_case_when(df, params)
        assert result.collect()[0]["tier"] == "new"

    def test_null_handling(self, spark: SparkSession):
        """Null values in condition columns handled correctly."""
        df = spark.createDataFrame([(None,), (10,)], ["amount"])
        params = CaseWhenParams(
            column="flag",
            cases=[
                CaseWhenBranch(
                    when=SparkExpr("amount > 5"),
                    then=SparkExpr("'yes'"),
                ),
            ],
            otherwise=SparkExpr("'no'"),
        )
        result = apply_case_when(df, params)
        rows = result.collect()
        flags = {r["flag"] for r in rows}
        assert "yes" in flags
        assert "no" in flags
