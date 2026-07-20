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


# ---------------------------------------------------------------------------
# Scan hygiene: one read per evaluation, one aggregation per assertion type
# ---------------------------------------------------------------------------


class TestScanHygiene:
    def test_one_read_for_many_assertions(self, spark, target_df, monkeypatch):
        """The target is read once no matter how many assertions run."""
        import weevr.operations.assertions as assertions_mod

        calls = {"n": 0}
        real_read = assertions_mod.read_delta

        def counting_read(spark_arg, path_arg):
            calls["n"] += 1
            return real_read(spark_arg, path_arg)

        monkeypatch.setattr(assertions_mod, "read_delta", counting_read)
        assertions = [
            Assertion(type="row_count", min=1),
            Assertion(type="column_not_null", columns=["id", "amount"]),
            Assertion(type="unique", columns=["id"]),
            Assertion(type="expression", expression=SparkExpr("count(*) >= 1")),
        ]
        results = evaluate_assertions(spark, assertions, target_df)
        assert [r.passed for r in results] == [True, True, True, True]
        assert calls["n"] == 1

    def test_missing_table_errors_every_assertion(self, spark, tmp_delta_path):
        """A failed read surfaces as an evaluation error on each assertion."""
        path = tmp_delta_path("assertion_missing")
        assertions = [
            Assertion(type="row_count", min=1),
            Assertion(type="unique", columns=["id"]),
        ]
        results = evaluate_assertions(spark, assertions, path)
        assert len(results) == 2
        assert all(r.passed is False for r in results)
        assert all("Evaluation error" in r.details for r in results)
        assert results[1].columns == ["id"]

    def test_column_not_null_single_aggregation(self, spark, target_df, spark_action_counter):
        """Multi-column not-null resolves in one aggregation, not one scan per column."""
        assertions = [Assertion(type="column_not_null", columns=["id", "name", "amount"])]
        results = evaluate_assertions(spark, assertions, target_df)
        assert results[0].passed is False
        assert "'name' has 1 nulls" in results[0].details
        assert spark_action_counter["total"] == 1

    def test_column_not_null_empty_table_passes(self, spark, tmp_delta_path):
        path = tmp_delta_path("not_null_empty")
        spark.createDataFrame([], "id: int, name: string").write.format("delta").save(path)
        results = evaluate_assertions(
            spark, [Assertion(type="column_not_null", columns=["id", "name"])], path
        )
        assert results[0].passed is True

    def test_unique_single_pass(self, spark, dup_target_df, spark_action_counter):
        """Unique check computes total and distinct in one action over the table."""
        results = evaluate_assertions(
            spark, [Assertion(type="unique", columns=["id", "name"])], dup_target_df
        )
        assert results[0].passed is False
        assert "1 duplicate rows" in results[0].details
        assert spark_action_counter["total"] == 1

    def test_unique_counts_null_keys_as_distinct_values(self, spark, tmp_delta_path):
        """NULL key rows group together — same answer distinct() gave."""
        path = tmp_delta_path("null_key_unique")
        spark.createDataFrame([(1,), (None,), (None,)], "id INT").write.format("delta").save(path)
        results = evaluate_assertions(spark, [Assertion(type="unique", columns=["id"])], path)
        assert results[0].passed is False
        assert "1 duplicate rows" in results[0].details

    def test_fk_sentinel_single_aggregation(self, spark, fk_target_df, spark_action_counter):
        """Columns x groups resolve in one aggregation, not one scan per pair."""
        assertions = [
            Assertion(
                type="fk_sentinel_rate",
                columns=["plant_id", "company_id"],
                sentinels={"invalid": -4, "unknown": -1},  # type: ignore[arg-type]
                max_rate=0.25,
            )
        ]
        results = evaluate_assertions(spark, assertions, fk_target_df)
        assert results[0].passed is True
        assert spark_action_counter["total"] == 1
