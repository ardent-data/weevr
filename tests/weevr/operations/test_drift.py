"""Tests for schema drift detection and handling."""

import pytest
from pyspark.sql import SparkSession

from weevr.errors import SchemaDriftError
from weevr.operations.drift import handle_drift

pytestmark = pytest.mark.spark


class TestHandleDrift:
    """Test drift detection and handling."""

    def test_lenient_extra_columns_pass_through(self, spark: SparkSession):
        """lenient + extra columns: pass through, report populated."""
        df = spark.createDataFrame([(1, "a", "extra")], ["id", "name", "new_col"])
        result_df, report = handle_drift(df, ["id", "name"], "lenient", "warn")
        assert "new_col" in result_df.columns
        assert report.has_drift
        assert report.extra_columns == ["new_col"]
        assert report.action_taken == "pass_through"

    def test_strict_error_raises(self, spark: SparkSession):
        """strict + error + extra columns: SchemaDriftError raised."""
        df = spark.createDataFrame([(1, "extra")], ["id", "new_col"])
        with pytest.raises(SchemaDriftError) as exc_info:
            handle_drift(df, ["id"], "strict", "error")
        assert exc_info.value.drift_report is not None
        assert exc_info.value.drift_report.extra_columns == ["new_col"]

    def test_strict_warn_drops_columns(self, spark: SparkSession):
        """strict + warn + extra columns: columns dropped with warning."""
        df = spark.createDataFrame([(1, "extra")], ["id", "new_col"])
        result_df, report = handle_drift(df, ["id"], "strict", "warn")
        assert "new_col" not in result_df.columns
        assert report.action_taken == "drop_warn"

    def test_strict_ignore_drops_silently(self, spark: SparkSession):
        """strict + ignore + extra columns: columns dropped silently."""
        df = spark.createDataFrame([(1, "extra")], ["id", "new_col"])
        result_df, report = handle_drift(df, ["id"], "strict", "ignore")
        assert "new_col" not in result_df.columns
        assert report.action_taken == "drop_silent"

    def test_adaptive_pass_through_evolve(self, spark: SparkSession):
        """adaptive + extra columns: pass through with evolve action."""
        df = spark.createDataFrame([(1, "extra")], ["id", "new_col"])
        result_df, report = handle_drift(df, ["id"], "adaptive", "warn")
        assert "new_col" in result_df.columns
        assert report.action_taken == "pass_through_evolve"

    def test_no_extra_columns_empty_report(self, spark: SparkSession):
        """No extra columns produces empty DriftReport."""
        df = spark.createDataFrame([(1, "Alice")], ["id", "name"])
        result_df, report = handle_drift(df, ["id", "name"], "strict", "error")
        assert not report.has_drift
        assert report.extra_columns == []

    def test_no_baseline_empty_report(self, spark: SparkSession):
        """No baseline (None) produces empty DriftReport."""
        df = spark.createDataFrame([(1, "extra")], ["id", "new_col"])
        result_df, report = handle_drift(df, None, "strict", "error")
        assert not report.has_drift
        assert result_df.columns == ["id", "new_col"]

    def test_engine_columns_excluded(self, spark: SparkSession):
        """Engine columns excluded from extra detection."""
        df = spark.createDataFrame([(1, "val", "audit")], ["id", "name", "_loaded_at"])
        result_df, report = handle_drift(
            df, ["id", "name"], "strict", "error", engine_columns=["_loaded_at"]
        )
        assert not report.has_drift
        assert "_loaded_at" in result_df.columns


class TestHandleDriftMissingColumns:
    """Baseline columns missing from the output are drift (table baseline)."""

    @staticmethod
    def _df(spark: SparkSession):
        return spark.createDataFrame([(1, "a")], ["id", "name"])

    BASELINE = ["id", "name", "email"]

    def test_strict_error_raises_naming_missing(self, spark: SparkSession):
        with pytest.raises(SchemaDriftError, match="missing.*email"):
            handle_drift(self._df(spark), self.BASELINE, "strict", "error", baseline_source="table")

    def test_strict_warn_reports_and_keeps_frame(self, spark: SparkSession, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            df, report = handle_drift(
                self._df(spark), self.BASELINE, "strict", "warn", baseline_source="table"
            )
        assert df.columns == ["id", "name"]
        assert report.missing_columns == ["email"]
        assert report.has_drift
        assert any("email" in m for m in caplog.messages)

    @pytest.mark.parametrize(
        ("mode", "on_drift"), [("strict", "ignore"), ("lenient", "warn"), ("adaptive", "warn")]
    )
    def test_other_modes_report(self, spark: SparkSession, mode: str, on_drift: str):
        df, report = handle_drift(
            self._df(spark), self.BASELINE, mode, on_drift, baseline_source="table"
        )
        assert df.columns == ["id", "name"]
        assert report.missing_columns == ["email"]
        assert report.extra_columns == []

    def test_engine_columns_never_missing(self, spark: SparkSession):
        _, report = handle_drift(
            self._df(spark),
            ["id", "name", "is_deleted"],
            "strict",
            "error",
            engine_columns=["is_deleted"],
            baseline_source="table",
        )
        assert not report.has_drift

    def test_warp_baseline_does_not_report_missing(self, spark: SparkSession):
        # Warp columns absent from the output are warp-only columns, appended later
        _, report = handle_drift(
            self._df(spark), self.BASELINE, "strict", "error", baseline_source="warp"
        )
        assert not report.has_drift

    def test_extra_and_missing_both_reported(self, spark: SparkSession):
        df = spark.createDataFrame([(1, "a", "x")], ["id", "name", "phone"])
        with pytest.raises(SchemaDriftError) as exc:
            handle_drift(df, self.BASELINE, "strict", "error", baseline_source="table")
        assert "phone" in str(exc.value) and "email" in str(exc.value)
