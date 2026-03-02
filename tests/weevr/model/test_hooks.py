"""Tests for hook step models."""

import pytest
from pydantic import TypeAdapter, ValidationError

from weevr.model.hooks import (
    HookStep,
    LogMessageStep,
    QualityGateStep,
    SqlStatementStep,
)

hook_step_adapter = TypeAdapter(HookStep)


class TestQualityGateStep:
    """Test QualityGateStep construction and validation."""

    def test_source_freshness(self):
        """Construct a source_freshness gate."""
        step = QualityGateStep(
            type="quality_gate",
            check="source_freshness",
            source="raw_orders",
            max_age="24h",
        )
        assert step.check == "source_freshness"
        assert step.source == "raw_orders"
        assert step.max_age == "24h"

    def test_source_freshness_missing_source(self):
        """source_freshness without source raises ValidationError."""
        with pytest.raises(ValidationError, match="source_freshness requires 'source'"):
            QualityGateStep(
                type="quality_gate",
                check="source_freshness",
                max_age="24h",
            )

    def test_source_freshness_missing_max_age(self):
        """source_freshness without max_age raises ValidationError."""
        with pytest.raises(ValidationError, match="source_freshness requires 'max_age'"):
            QualityGateStep(
                type="quality_gate",
                check="source_freshness",
                source="raw_orders",
            )

    def test_row_count_delta(self):
        """Construct a row_count_delta gate."""
        step = QualityGateStep(
            type="quality_gate",
            check="row_count_delta",
            target="fact_orders",
            max_decrease_pct=10.0,
        )
        assert step.check == "row_count_delta"
        assert step.target == "fact_orders"
        assert step.max_decrease_pct == 10.0

    def test_row_count_delta_missing_target(self):
        """row_count_delta without target raises ValidationError."""
        with pytest.raises(ValidationError, match="row_count_delta requires 'target'"):
            QualityGateStep(
                type="quality_gate",
                check="row_count_delta",
            )

    def test_row_count(self):
        """Construct a row_count gate."""
        step = QualityGateStep(
            type="quality_gate",
            check="row_count",
            target="dim_customer",
            min_count=1,
        )
        assert step.check == "row_count"
        assert step.min_count == 1

    def test_row_count_missing_bounds(self):
        """row_count without min_count or max_count raises ValidationError."""
        with pytest.raises(ValidationError, match="at least 'min_count' or 'max_count'"):
            QualityGateStep(
                type="quality_gate",
                check="row_count",
                target="dim_customer",
            )

    def test_table_exists(self):
        """Construct a table_exists gate."""
        step = QualityGateStep(
            type="quality_gate",
            check="table_exists",
            source="staging_orders",
        )
        assert step.check == "table_exists"
        assert step.source == "staging_orders"

    def test_table_exists_missing_source(self):
        """table_exists without source raises ValidationError."""
        with pytest.raises(ValidationError, match="table_exists requires 'source'"):
            QualityGateStep(
                type="quality_gate",
                check="table_exists",
            )

    def test_expression(self):
        """Construct an expression gate."""
        step = QualityGateStep(
            type="quality_gate",
            check="expression",
            sql="COUNT(*) > 0",
            message="Table must not be empty",
        )
        assert step.check == "expression"
        assert step.sql == "COUNT(*) > 0"
        assert step.message == "Table must not be empty"

    def test_expression_missing_sql(self):
        """expression without sql raises ValidationError."""
        with pytest.raises(ValidationError, match="expression requires 'sql'"):
            QualityGateStep(
                type="quality_gate",
                check="expression",
            )

    def test_on_failure_default_is_none(self):
        """on_failure defaults to None (executor applies phase default)."""
        step = QualityGateStep(
            type="quality_gate",
            check="table_exists",
            source="t",
        )
        assert step.on_failure is None

    def test_on_failure_explicit(self):
        """on_failure can be set to abort or warn."""
        step = QualityGateStep(
            type="quality_gate",
            check="table_exists",
            source="t",
            on_failure="warn",
        )
        assert step.on_failure == "warn"

    def test_name_optional(self):
        """name is optional."""
        step = QualityGateStep(
            type="quality_gate",
            name="freshness_check",
            check="source_freshness",
            source="s",
            max_age="1h",
        )
        assert step.name == "freshness_check"

    def test_frozen(self):
        """QualityGateStep is immutable."""
        step = QualityGateStep(
            type="quality_gate",
            check="table_exists",
            source="t",
        )
        with pytest.raises(ValidationError):
            step.check = "expression"  # type: ignore[misc]


class TestSqlStatementStep:
    """Test SqlStatementStep construction and validation."""

    def test_basic(self):
        """Construct a basic SQL statement step."""
        step = SqlStatementStep(
            type="sql_statement",
            sql="SELECT 1",
        )
        assert step.sql == "SELECT 1"
        assert step.set_var is None

    def test_with_set_var(self):
        """SQL statement with set_var captures result."""
        step = SqlStatementStep(
            type="sql_statement",
            sql="SELECT COUNT(*) FROM target",
            set_var="row_count",
        )
        assert step.set_var == "row_count"

    def test_sql_required(self):
        """sql field is required."""
        with pytest.raises(ValidationError):
            SqlStatementStep(type="sql_statement")  # type: ignore[call-arg]

    def test_on_failure_default_is_none(self):
        """on_failure defaults to None."""
        step = SqlStatementStep(type="sql_statement", sql="SELECT 1")
        assert step.on_failure is None


class TestLogMessageStep:
    """Test LogMessageStep construction and validation."""

    def test_basic(self):
        """Construct a basic log message step."""
        step = LogMessageStep(
            type="log_message",
            message="Starting weave execution",
        )
        assert step.message == "Starting weave execution"
        assert step.level == "info"

    def test_level_warn(self):
        """Log level can be set to warn."""
        step = LogMessageStep(
            type="log_message",
            message="Check this",
            level="warn",
        )
        assert step.level == "warn"

    def test_message_required(self):
        """message field is required."""
        with pytest.raises(ValidationError):
            LogMessageStep(type="log_message")  # type: ignore[call-arg]

    def test_on_failure_default_is_none(self):
        """on_failure defaults to None."""
        step = LogMessageStep(type="log_message", message="hi")
        assert step.on_failure is None


class TestHookStepDiscriminator:
    """Test discriminated union dispatch."""

    def test_quality_gate_from_dict(self):
        """Dict with type=quality_gate dispatches to QualityGateStep."""
        step = hook_step_adapter.validate_python(
            {"type": "quality_gate", "check": "table_exists", "source": "t"}
        )
        assert isinstance(step, QualityGateStep)

    def test_sql_statement_from_dict(self):
        """Dict with type=sql_statement dispatches to SqlStatementStep."""
        step = hook_step_adapter.validate_python({"type": "sql_statement", "sql": "SELECT 1"})
        assert isinstance(step, SqlStatementStep)

    def test_log_message_from_dict(self):
        """Dict with type=log_message dispatches to LogMessageStep."""
        step = hook_step_adapter.validate_python({"type": "log_message", "message": "hello"})
        assert isinstance(step, LogMessageStep)

    def test_unknown_type_rejected(self):
        """Unknown type raises ValidationError."""
        with pytest.raises(ValidationError):
            hook_step_adapter.validate_python({"type": "unknown_step", "foo": "bar"})

    def test_missing_type_rejected(self):
        """Missing type field raises ValidationError."""
        with pytest.raises(ValidationError):
            hook_step_adapter.validate_python({"sql": "SELECT 1"})
