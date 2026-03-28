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
    df = spark.createDataFrame(
        [
            {"id": 1, "name": "alice", "amount": 100},
            {"id": 2, "name": "bob", "amount": 200},
            {"id": 3, "name": None, "amount": 300},
        ]
    )
    df.write.format("delta").mode("overwrite").save(path)
    return path


@pytest.fixture()
def dup_target_df(spark: SparkSession, tmp_delta_path):
    """Write a Delta table with duplicate rows and return its path."""
    path = tmp_delta_path("dup_target")
    df = spark.createDataFrame(
        [
            {"id": 1, "name": "alice"},
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
        ]
    )
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

    def test_empty_columns_fails(self, spark, target_df):
        assertions = [Assertion(type="column_not_null", columns=[])]
        results = evaluate_assertions(spark, assertions, target_df)
        assert results[0].passed is False
        assert "No columns specified" in results[0].details

    def test_none_columns_fails(self, spark, target_df):
        assertions = [Assertion(type="column_not_null")]
        results = evaluate_assertions(spark, assertions, target_df)
        assert results[0].passed is False
        assert "No columns specified" in results[0].details


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

    def test_empty_columns_fails(self, spark, target_df):
        assertions = [Assertion(type="unique", columns=[])]
        results = evaluate_assertions(spark, assertions, target_df)
        assert results[0].passed is False
        assert "No columns specified" in results[0].details


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


# ---------------------------------------------------------------------------
# fk_sentinel_rate assertions
# ---------------------------------------------------------------------------


@pytest.fixture()
def fk_target_df(spark: SparkSession, tmp_delta_path):
    """Write a fact table with FK sentinel values and return its path."""
    path = tmp_delta_path("fk_assertion_target")
    df = spark.createDataFrame(
        [
            {"plant_id": 1, "company_id": 10},
            {"plant_id": 2, "company_id": -1},
            {"plant_id": -4, "company_id": -4},
            {"plant_id": 3, "company_id": 10},
            {"plant_id": -1, "company_id": 10},
        ]
    )
    df.write.format("delta").mode("overwrite").save(path)
    return path


class TestFkSentinelRate:
    """Test fk_sentinel_rate assertion evaluator."""

    def test_single_column_under_threshold(self, spark, fk_target_df):
        """Rate under max_rate passes."""
        assertions = [
            Assertion(
                type="fk_sentinel_rate",
                column="plant_id",
                sentinel=-4,
                max_rate=0.25,
            )
        ]
        results = evaluate_assertions(spark, assertions, fk_target_df)
        assert results[0].passed is True

    def test_single_column_over_threshold(self, spark, fk_target_df):
        """Rate over max_rate fails."""
        assertions = [
            Assertion(
                type="fk_sentinel_rate",
                column="plant_id",
                sentinel=-4,
                max_rate=0.10,
            )
        ]
        results = evaluate_assertions(spark, assertions, fk_target_df)
        assert results[0].passed is False

    def test_columns_list(self, spark, fk_target_df):
        """Multiple columns each checked independently."""
        assertions = [
            Assertion(
                type="fk_sentinel_rate",
                columns=["plant_id", "company_id"],
                sentinel=-4,
                max_rate=0.25,
            )
        ]
        results = evaluate_assertions(spark, assertions, fk_target_df)
        assert results[0].passed is True

    def test_named_sentinel_groups_shared_rate(self, spark, fk_target_df):
        """Named sentinel groups with shared max_rate."""
        assertions = [
            Assertion(
                type="fk_sentinel_rate",
                column="plant_id",
                sentinels={"invalid": -4, "unknown": -1},  # type: ignore[arg-type]
                max_rate=0.25,
            )
        ]
        results = evaluate_assertions(spark, assertions, fk_target_df)
        # -4 = 1/5 = 20%, -1 = 1/5 = 20%, both under 25%
        assert results[0].passed is True

    def test_named_sentinel_groups_per_group_rate(self, spark, fk_target_df):
        """Per-group max_rate with one group failing."""
        assertions = [
            Assertion(
                type="fk_sentinel_rate",
                column="plant_id",
                sentinels={  # type: ignore[arg-type]
                    "invalid": {"value": -4, "max_rate": 0.25},
                    "unknown": {"value": -1, "max_rate": 0.10},
                },
            )
        ]
        results = evaluate_assertions(spark, assertions, fk_target_df)
        # -4 = 20% (under 25%), -1 = 20% (over 10%) → fail
        assert results[0].passed is False

    def test_empty_table_passes(self, spark, tmp_delta_path):
        """Empty table has 0% rate — passes."""
        path = tmp_delta_path("fk_empty")
        df = spark.createDataFrame([], "plant_id: int")
        df.write.format("delta").mode("overwrite").save(path)
        assertions = [
            Assertion(
                type="fk_sentinel_rate",
                column="plant_id",
                sentinel=-4,
                max_rate=0.05,
            )
        ]
        results = evaluate_assertions(spark, assertions, path)
        assert results[0].passed is True
