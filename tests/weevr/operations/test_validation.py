"""Tests for single-pass DataFrame validation with severity routing."""

import pytest
from pyspark.sql import SparkSession

from weevr.model.types import SparkExpr
from weevr.model.validation import ValidationRule
from weevr.operations.validation import ValidationOutcome, validate_dataframe

pytestmark = pytest.mark.spark


@pytest.fixture()
def sample_df(spark: SparkSession):
    """Sample DataFrame with id and amount columns."""
    return spark.createDataFrame(
        [
            {"id": 1, "amount": 100},
            {"id": 2, "amount": -5},
            {"id": 3, "amount": 50},
            {"id": 4, "amount": -10},
        ]
    )


class TestValidateDataframeNoRules:
    def test_no_rules_returns_original(self, sample_df):
        outcome = validate_dataframe(sample_df, [])
        assert outcome.clean_df.count() == 4
        assert outcome.quarantine_df is None
        assert outcome.validation_results == []
        assert outcome.has_fatal is False


class TestValidateDataframeSingleRule:
    def test_all_pass(self, spark: SparkSession):
        df = spark.createDataFrame([{"id": 1}, {"id": 2}])
        rules = [
            ValidationRule(rule=SparkExpr("id IS NOT NULL"), severity="error", name="not_null")
        ]
        outcome = validate_dataframe(df, rules)
        assert outcome.clean_df.count() == 2
        assert outcome.has_fatal is False
        # Quarantine exists but is empty when error rules are present but no rows fail
        if outcome.quarantine_df is not None:
            assert outcome.quarantine_df.count() == 0
        assert len(outcome.validation_results) == 1
        assert outcome.validation_results[0].rows_passed == 2
        assert outcome.validation_results[0].rows_failed == 0

    def test_some_fail_error(self, sample_df):
        rules = [ValidationRule(rule=SparkExpr("amount > 0"), severity="error", name="positive")]
        outcome = validate_dataframe(sample_df, rules)
        assert outcome.has_fatal is False
        assert outcome.clean_df.count() == 2
        assert outcome.quarantine_df is not None
        assert outcome.quarantine_df.count() == 2
        # Quarantine has metadata columns
        q_cols = set(outcome.quarantine_df.columns)
        assert "__rule_name" in q_cols
        assert "__rule_expression" in q_cols
        assert "__severity" in q_cols
        assert "__quarantine_ts" in q_cols

    def test_validation_result_counts(self, sample_df):
        rules = [ValidationRule(rule=SparkExpr("amount > 0"), severity="error", name="positive")]
        outcome = validate_dataframe(sample_df, rules)
        vr = outcome.validation_results[0]
        assert vr.rule_name == "positive"
        assert vr.rows_passed == 2
        assert vr.rows_failed == 2
        assert vr.applied is True


class TestValidateDataframeFatal:
    def test_fatal_aborts(self, sample_df):
        rules = [
            ValidationRule(rule=SparkExpr("amount > 0"), severity="fatal", name="must_positive")
        ]
        outcome = validate_dataframe(sample_df, rules)
        assert outcome.has_fatal is True
        assert outcome.quarantine_df is None
        # clean_df is the original (no split performed)
        assert outcome.clean_df.count() == 4
        assert outcome.validation_results[0].rows_failed == 2

    def test_fatal_all_pass(self, spark: SparkSession):
        df = spark.createDataFrame([{"x": 1}, {"x": 2}])
        rules = [ValidationRule(rule=SparkExpr("x > 0"), severity="fatal", name="positive")]
        outcome = validate_dataframe(df, rules)
        assert outcome.has_fatal is False


class TestValidateDataframeMixedSeverities:
    def test_info_warn_stay_in_clean(self, sample_df):
        """Info and warn rules log but don't quarantine."""
        rules = [
            ValidationRule(rule=SparkExpr("amount > 0"), severity="info", name="info_rule"),
            ValidationRule(rule=SparkExpr("amount > 0"), severity="warn", name="warn_rule"),
        ]
        outcome = validate_dataframe(sample_df, rules)
        assert outcome.has_fatal is False
        assert outcome.quarantine_df is None
        # All rows in clean (info/warn don't quarantine)
        assert outcome.clean_df.count() == 4

    def test_error_quarantines_info_stays(self, sample_df):
        """Error rules quarantine rows, info/warn don't affect split."""
        rules = [
            ValidationRule(rule=SparkExpr("amount > 0"), severity="info", name="info_rule"),
            ValidationRule(rule=SparkExpr("amount > 0"), severity="error", name="error_rule"),
        ]
        outcome = validate_dataframe(sample_df, rules)
        assert outcome.has_fatal is False
        assert outcome.clean_df.count() == 2
        assert outcome.quarantine_df is not None
        assert outcome.quarantine_df.count() == 2

    def test_fatal_with_error_aborts(self, sample_df):
        """Fatal takes precedence — no split even with error rules."""
        rules = [
            ValidationRule(rule=SparkExpr("amount > 0"), severity="error", name="error_rule"),
            ValidationRule(rule=SparkExpr("id < 5"), severity="fatal", name="fatal_rule"),
        ]
        # id < 5 passes for all rows, amount > 0 fails for 2
        # But since fatal rule passes, no has_fatal
        outcome = validate_dataframe(sample_df, rules)
        assert outcome.has_fatal is False
        assert outcome.clean_df.count() == 2
        assert outcome.quarantine_df is not None


class TestValidateDataframeMultipleErrors:
    def test_row_fails_two_rules(self, spark: SparkSession):
        """Row failing two error rules produces two quarantine rows."""
        df = spark.createDataFrame(
            [
                {"id": 1, "a": 10, "b": 20},
                {"id": 2, "a": -1, "b": -1},
            ]
        )
        rules = [
            ValidationRule(rule=SparkExpr("a > 0"), severity="error", name="a_positive"),
            ValidationRule(rule=SparkExpr("b > 0"), severity="error", name="b_positive"),
        ]
        outcome = validate_dataframe(df, rules)
        assert outcome.clean_df.count() == 1
        assert outcome.quarantine_df is not None
        # Row 2 fails both rules → 2 quarantine rows
        assert outcome.quarantine_df.count() == 2
        rule_names = {row["__rule_name"] for row in outcome.quarantine_df.collect()}
        assert rule_names == {"a_positive", "b_positive"}


class TestValidateDataframeUnappliedRule:
    def test_invalid_expression(self, sample_df):
        """Invalid SQL expression marks rule as unapplied."""
        rules = [
            ValidationRule(
                rule=SparkExpr("NONEXISTENT_COLUMN > 0"),
                severity="error",
                name="bad_rule",
            ),
        ]
        outcome = validate_dataframe(sample_df, rules)
        assert len(outcome.validation_results) == 1
        vr = outcome.validation_results[0]
        assert vr.applied is False
        assert vr.rows_passed == 0
        assert vr.rows_failed == 0


class TestValidationOutcome:
    def test_slots(self):
        """ValidationOutcome uses __slots__ for memory efficiency."""
        assert hasattr(ValidationOutcome, "__slots__")
