"""Tests for warp and drift error types."""

from weevr.errors import (
    ExecutionError,
    SchemaDriftError,
    WarpEnforcementError,
    WeevError,
)
from weevr.model.warp import DriftReport


class TestWarpEnforcementError:
    """Test WarpEnforcementError."""

    def test_inherits_from_execution_error(self):
        """WarpEnforcementError inherits from ExecutionError."""
        assert issubclass(WarpEnforcementError, ExecutionError)
        assert issubclass(WarpEnforcementError, WeevError)

    def test_basic_construction(self):
        """WarpEnforcementError constructs with message and findings."""
        findings = [
            {"type": "missing_column", "column": "customer_id"},
            {
                "type": "type_mismatch",
                "column": "amount",
                "expected": "decimal(18,2)",
                "actual": "string",
            },
        ]
        err = WarpEnforcementError("Warp contract violation", findings=findings)
        assert err.message == "Warp contract violation"
        assert err.findings == findings
        assert len(err.findings) == 2

    def test_empty_findings(self):
        """WarpEnforcementError accepts empty findings list."""
        err = WarpEnforcementError("No issues", findings=[])
        assert err.findings == []

    def test_str_includes_findings_count(self):
        """String representation includes finding count."""
        findings = [{"type": "missing_column", "column": "id"}]
        err = WarpEnforcementError("Warp violation", findings=findings)
        result = str(err)
        assert "Warp violation" in result
        assert "1" in result

    def test_with_thread_name(self):
        """WarpEnforcementError accepts thread_name context."""
        err = WarpEnforcementError(
            "Violation",
            findings=[{"type": "missing_column", "column": "id"}],
            thread_name="dim_customer",
        )
        assert err.thread_name == "dim_customer"
        assert "dim_customer" in str(err)


class TestSchemaDriftError:
    """Test SchemaDriftError."""

    def test_inherits_from_execution_error(self):
        """SchemaDriftError inherits from ExecutionError."""
        assert issubclass(SchemaDriftError, ExecutionError)
        assert issubclass(SchemaDriftError, WeevError)

    def test_basic_construction(self):
        """SchemaDriftError constructs with message and drift report."""
        report = DriftReport(
            extra_columns=["new_col1", "new_col2"],
            action_taken="error",
            drift_mode="strict",
            baseline_source="warp",
        )
        err = SchemaDriftError("Schema drift detected", drift_report=report)
        assert err.message == "Schema drift detected"
        assert err.drift_report is report
        assert err.drift_report is not None
        assert err.drift_report.extra_columns == ["new_col1", "new_col2"]

    def test_str_includes_extra_columns(self):
        """String representation includes extra column info."""
        report = DriftReport(
            extra_columns=["col_a", "col_b"],
            action_taken="error",
            drift_mode="strict",
        )
        err = SchemaDriftError("Drift detected", drift_report=report)
        result = str(err)
        assert "Drift detected" in result
        assert "2" in result

    def test_with_thread_name(self):
        """SchemaDriftError accepts thread_name context."""
        report = DriftReport(extra_columns=["col_a"])
        err = SchemaDriftError("Drift", drift_report=report, thread_name="fact_orders")
        assert err.thread_name == "fact_orders"
        assert "fact_orders" in str(err)

    def test_with_empty_report(self):
        """SchemaDriftError with empty drift report."""
        report = DriftReport.empty()
        err = SchemaDriftError("No drift", drift_report=report)
        assert err.drift_report is not None
        assert err.drift_report.extra_columns == []
        assert not err.drift_report.has_drift
