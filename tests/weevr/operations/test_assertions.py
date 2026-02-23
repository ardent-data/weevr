"""Tests for post-execution assertion evaluator."""

import pytest
from pyspark.sql import SparkSession

from weevr.model.types import SparkExpr
from weevr.model.validation import Assertion
from weevr.operations.assertions import evaluate_assertions

pytestmark = pytest.mark.spark


@pytest.fixture()
def target_df(spark: SparkSession, tmp_delta_path):
    """Write a sample Delta table and return its path."""
    path = tmp_delta_path("assertion_target")
    df = spark.createDataFrame([
        {"id": 1, "name": "alice", "amount": 100},
        {"id": 2, "name": "bob", "amount": 200},
        {"id": 3, "name": None, "amount": 300},
    ])
    df.write.format("delta").mode("overwrite").save(path)
    return path


@pytest.fixture()
def dup_target_df(spark: SparkSession, tmp_delta_path):
    """Write a Delta table with duplicate rows and return its path."""
    path = tmp_delta_path("dup_target")
    df = spark.createDataFrame([
        {"id": 1, "name": "alice"},
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ])
    df.write.format("delta").mode("overwrite").save(path)
    return path


class TestRowCountAssertion:
    def test_within_bounds(self, spark, target_df):
        assertions = [Assertion(type="row_count", min=1, max=10)]
        results = evaluate_assertions(spark, assertions, target_df)
        assert len(results) == 1
        assert results[0].passed is True
        assert results[0].assertion_type == "row_count"

    def test_below_min(self, spark, target_df):
        assertions = [Assertion(type="row_count", min=5)]
        results = evaluate_assertions(spark, assertions, target_df)
        assert results[0].passed is False
        assert "below minimum" in results[0].details

    def test_above_max(self, spark, target_df):
        assertions = [Assertion(type="row_count", max=2)]
        results = evaluate_assertions(spark, assertions, target_df)
        assert results[0].passed is False
        assert "above maximum" in results[0].details


class TestColumnNotNullAssertion:
    def test_no_nulls(self, spark, target_df):
        assertions = [Assertion(type="column_not_null", columns=["id", "amount"])]
        results = evaluate_assertions(spark, assertions, target_df)
        assert results[0].passed is True

    def test_nulls_present(self, spark, target_df):
        assertions = [Assertion(type="column_not_null", columns=["name"])]
        results = evaluate_assertions(spark, assertions, target_df)
        assert results[0].passed is False
        assert "'name' has 1 nulls" in results[0].details
        assert results[0].columns == ["name"]


class TestUniqueAssertion:
    def test_all_unique(self, spark, target_df):
        assertions = [Assertion(type="unique", columns=["id"])]
        results = evaluate_assertions(spark, assertions, target_df)
        assert results[0].passed is True

    def test_duplicates(self, spark, dup_target_df):
        assertions = [Assertion(type="unique", columns=["id"])]
        results = evaluate_assertions(spark, assertions, dup_target_df)
        assert results[0].passed is False
        assert "1 duplicate" in results[0].details


class TestExpressionAssertion:
    def test_true_predicate(self, spark, target_df):
        assertions = [
            Assertion(
                type="expression",
                expression=SparkExpr("COUNT(*) > 0"),
            ),
        ]
        results = evaluate_assertions(spark, assertions, target_df)
        assert results[0].passed is True

    def test_false_predicate(self, spark, target_df):
        assertions = [
            Assertion(
                type="expression",
                expression=SparkExpr("COUNT(*) > 100"),
            ),
        ]
        results = evaluate_assertions(spark, assertions, target_df)
        assert results[0].passed is False


class TestSeverityPropagation:
    def test_severity_on_result(self, spark, target_df):
        assertions = [Assertion(type="row_count", min=1, max=10, severity="error")]
        results = evaluate_assertions(spark, assertions, target_df)
        assert results[0].severity == "error"

    def test_default_severity_warn(self, spark, target_df):
        assertions = [Assertion(type="row_count", min=1)]
        results = evaluate_assertions(spark, assertions, target_df)
        assert results[0].severity == "warn"


class TestEvaluationError:
    def test_bad_sql_returns_failed(self, spark, target_df):
        assertions = [
            Assertion(
                type="expression",
                expression=SparkExpr("INVALID_SQL_HERE !!!"),
            ),
        ]
        results = evaluate_assertions(spark, assertions, target_df)
        assert results[0].passed is False
        assert "Evaluation error" in results[0].details or "error" in results[0].details.lower()
