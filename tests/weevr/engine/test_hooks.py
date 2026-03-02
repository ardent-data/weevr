"""Tests for hook step executor."""

import logging
from unittest.mock import MagicMock, patch

import pytest

from weevr.engine.hooks import (
    HookResult,
    _resolve_params,
    _resolve_text,
    run_hook_steps,
)
from weevr.engine.variables import VariableContext
from weevr.errors.exceptions import HookError
from weevr.model.hooks import LogMessageStep, QualityGateStep, SqlStatementStep
from weevr.model.variable import VariableSpec
from weevr.telemetry.collector import SpanCollector
from weevr.telemetry.span import generate_trace_id


class TestHookResult:
    """Test HookResult model."""

    def test_defaults(self):
        """HookResult has sensible defaults."""
        r = HookResult(step_type="sql_statement")
        assert r.step_type == "sql_statement"
        assert r.status == "passed"
        assert r.duration_ms == 0.0
        assert r.gate_result is None
        assert r.error is None

    def test_with_gate_result(self):
        """HookResult can carry a gate result."""
        from weevr.operations.gates import GateResult

        gate = GateResult(check="row_count", passed=True, measured_value=100)
        r = HookResult(step_type="quality_gate", gate_result=gate)
        assert r.gate_result is not None
        assert r.gate_result.passed is True


class TestResolveParams:
    """Test ${param.name} resolution."""

    def test_single_param(self):
        """Single param ref resolved."""
        result = _resolve_params("env is ${param.env}", {"env": "prod"})
        assert result == "env is prod"

    def test_unmatched_left_as_is(self):
        """Unmatched param ref left unchanged."""
        result = _resolve_params("${param.missing}", {})
        assert result == "${param.missing}"

    def test_multiple_params(self):
        """Multiple param refs resolved."""
        result = _resolve_params("${param.a} and ${param.b}", {"a": "x", "b": "y"})
        assert result == "x and y"


class TestResolveText:
    """Test combined var + param resolution."""

    def test_both_namespaces(self):
        """Both ${var.*} and ${param.*} resolved."""
        ctx = VariableContext({"env": VariableSpec(type="string")})
        ctx.set("env", "staging")
        result = _resolve_text("${var.env} in ${param.region}", ctx, {"region": "us-east"})
        assert result == "staging in us-east"


class TestRunHookStepsLogMessage:
    """Test log_message step execution."""

    def test_log_message_emitted(self, caplog):
        """Log message step emits a log entry."""
        spark = MagicMock()
        step = LogMessageStep(type="log_message", message="Hello world")
        ctx = VariableContext()

        with caplog.at_level(logging.INFO, logger="weevr.engine.hooks"):
            results = run_hook_steps(spark, [step], "pre", ctx)

        assert len(results) == 1
        assert results[0].status == "passed"
        assert results[0].step_type == "log_message"
        assert "Hello world" in caplog.text

    def test_log_message_interpolation(self, caplog):
        """Log message resolves variable references."""
        spark = MagicMock()
        specs = {"batch": VariableSpec(type="string")}
        ctx = VariableContext(specs)
        ctx.set("batch", "B001")
        step = LogMessageStep(type="log_message", message="Processing ${var.batch}")

        with caplog.at_level(logging.INFO, logger="weevr.engine.hooks"):
            run_hook_steps(spark, [step], "pre", ctx)

        assert "Processing B001" in caplog.text

    def test_log_warn_level(self, caplog):
        """Log message respects warn level."""
        spark = MagicMock()
        step = LogMessageStep(type="log_message", message="Caution", level="warn")
        ctx = VariableContext()

        with caplog.at_level(logging.WARNING, logger="weevr.engine.hooks"):
            results = run_hook_steps(spark, [step], "pre", ctx)

        assert results[0].status == "passed"
        assert "Caution" in caplog.text


class TestRunHookStepsSqlStatement:
    """Test sql_statement step execution."""

    def test_sql_executed(self):
        """SQL statement is dispatched to spark.sql."""
        spark = MagicMock()
        step = SqlStatementStep(type="sql_statement", sql="CREATE TABLE t (id INT)")
        ctx = VariableContext()

        results = run_hook_steps(spark, [step], "pre", ctx)

        assert len(results) == 1
        assert results[0].status == "passed"
        spark.sql.assert_called_once_with("CREATE TABLE t (id INT)")

    def test_sql_with_set_var(self):
        """SQL statement captures scalar result into variable."""
        spark = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value=42)
        spark.sql.return_value.collect.return_value = [mock_row]

        specs = {"result": VariableSpec(type="int")}
        ctx = VariableContext(specs)
        step = SqlStatementStep(
            type="sql_statement", sql="SELECT COUNT(*) FROM t", set_var="result"
        )

        run_hook_steps(spark, [step], "pre", ctx)

        assert ctx.get("result") == 42

    def test_sql_set_var_empty_result(self):
        """SQL with set_var and empty result sets variable to None."""
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = []

        specs = {"val": VariableSpec(type="string")}
        ctx = VariableContext(specs)
        step = SqlStatementStep(type="sql_statement", sql="SELECT x FROM empty", set_var="val")

        run_hook_steps(spark, [step], "pre", ctx)

        assert ctx.get("val") is None

    def test_sql_resolves_variables(self):
        """SQL statement text resolves ${var.name} refs."""
        spark = MagicMock()
        specs = {"table": VariableSpec(type="string")}
        ctx = VariableContext(specs)
        ctx.set("table", "my_table")
        step = SqlStatementStep(type="sql_statement", sql="SELECT * FROM ${var.table}")

        run_hook_steps(spark, [step], "pre", ctx)

        spark.sql.assert_called_once_with("SELECT * FROM my_table")


class TestRunHookStepsQualityGate:
    """Test quality_gate step execution."""

    def test_passing_gate(self):
        """Passing quality gate returns passed status."""
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        step = QualityGateStep(type="quality_gate", check="table_exists", source="db.table")
        ctx = VariableContext()

        results = run_hook_steps(spark, [step], "pre", ctx)

        assert len(results) == 1
        assert results[0].status == "passed"
        assert results[0].gate_result is not None
        assert results[0].gate_result.passed is True

    def test_failing_gate_abort_pre(self):
        """Failing gate in pre phase aborts by default (R1)."""
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        step = QualityGateStep(type="quality_gate", check="table_exists", source="db.missing")
        ctx = VariableContext()

        with pytest.raises(HookError, match="does not exist"):
            run_hook_steps(spark, [step], "pre", ctx)

    def test_failing_gate_warn_post(self):
        """Failing gate in post phase warns by default (R1)."""
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        step = QualityGateStep(type="quality_gate", check="table_exists", source="db.missing")
        ctx = VariableContext()

        results = run_hook_steps(spark, [step], "post", ctx)

        assert len(results) == 1
        assert results[0].status == "warned"
        assert results[0].error is not None

    def test_explicit_on_failure_warn(self):
        """Explicit on_failure=warn overrides phase default."""
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        step = QualityGateStep(
            type="quality_gate",
            check="table_exists",
            source="db.missing",
            on_failure="warn",
        )
        ctx = VariableContext()

        results = run_hook_steps(spark, [step], "pre", ctx)

        assert results[0].status == "warned"

    def test_explicit_on_failure_abort_in_post(self):
        """Explicit on_failure=abort in post phase raises."""
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        step = QualityGateStep(
            type="quality_gate",
            check="table_exists",
            source="db.missing",
            on_failure="abort",
        )
        ctx = VariableContext()

        with pytest.raises(HookError):
            run_hook_steps(spark, [step], "post", ctx)


class TestRunHookStepsFailureHandling:
    """Test failure handling semantics."""

    def test_abort_on_exception(self):
        """Exception during step with abort raises HookError."""
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("Spark down")
        step = SqlStatementStep(type="sql_statement", sql="SELECT 1", on_failure="abort")
        ctx = VariableContext()

        with pytest.raises(HookError, match="Spark down"):
            run_hook_steps(spark, [step], "pre", ctx)

    def test_warn_on_exception_continues(self):
        """Exception during step with warn logs and continues."""
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("Temporary glitch")
        step = SqlStatementStep(type="sql_statement", sql="SELECT 1", on_failure="warn")
        ctx = VariableContext()

        results = run_hook_steps(spark, [step], "pre", ctx)

        assert len(results) == 1
        assert results[0].status == "warned"
        assert "Temporary glitch" in (results[0].error or "")


class TestRunHookStepsSequential:
    """Test sequential execution order."""

    def test_multiple_steps_in_order(self):
        """Steps execute in declared order."""
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        call_order: list[str] = []

        original_sql = spark.sql

        def track_sql(sql_text):
            call_order.append(f"sql:{sql_text}")
            return original_sql(sql_text)

        spark.sql = track_sql

        steps: list = [
            SqlStatementStep(type="sql_statement", name="step1", sql="SQL1"),
            SqlStatementStep(type="sql_statement", name="step2", sql="SQL2"),
        ]
        ctx = VariableContext()

        results = run_hook_steps(spark, steps, "pre", ctx)

        assert len(results) == 2
        assert results[0].step_name == "step1"
        assert results[1].step_name == "step2"
        assert call_order == ["sql:SQL1", "sql:SQL2"]

    def test_abort_stops_remaining_steps(self):
        """Abort on first step prevents subsequent steps from running."""
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("fail")
        steps: list = [
            SqlStatementStep(type="sql_statement", sql="FAIL", on_failure="abort"),
            SqlStatementStep(type="sql_statement", sql="NEVER"),
        ]
        ctx = VariableContext()

        with pytest.raises(HookError):
            run_hook_steps(spark, steps, "pre", ctx)

        # Only the first SQL was attempted
        assert spark.sql.call_count == 1


class TestRunHookStepsTelemetry:
    """Test telemetry span creation."""

    def test_spans_created(self):
        """Each step produces a telemetry span."""
        spark = MagicMock()
        collector = SpanCollector(generate_trace_id())
        steps: list = [
            LogMessageStep(type="log_message", message="test1"),
            LogMessageStep(type="log_message", message="test2", name="named"),
        ]
        ctx = VariableContext()

        with patch("weevr.engine.hooks.logger"):
            run_hook_steps(
                spark,
                steps,
                "pre",
                ctx,
                collector=collector,
                parent_span_id="parent123",
            )

        spans = collector.get_spans()
        assert len(spans) == 2
        assert spans[0].name == "hook:pre:log_message"
        assert spans[1].name == "hook:pre:log_message:named"
        assert spans[0].parent_span_id == "parent123"

    def test_log_message_span_event(self):
        """Log message step emits a span event with message content."""
        spark = MagicMock()
        collector = SpanCollector(generate_trace_id())
        step = LogMessageStep(type="log_message", message="Pipeline done", level="info")
        ctx = VariableContext()

        with patch("weevr.engine.hooks.logger"):
            run_hook_steps(spark, [step], "post", ctx, collector=collector)

        spans = collector.get_spans()
        assert len(spans) == 1
        assert len(spans[0].events) == 1
        assert spans[0].events[0].name == "log"
        assert spans[0].events[0].attributes["message"] == "Pipeline done"
        assert spans[0].events[0].attributes["level"] == "info"

    def test_error_span_on_warn(self):
        """Warned step creates OK span with warning event."""
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        collector = SpanCollector(generate_trace_id())
        step = QualityGateStep(
            type="quality_gate",
            check="table_exists",
            source="db.missing",
            on_failure="warn",
        )
        ctx = VariableContext()

        run_hook_steps(spark, [step], "post", ctx, collector=collector)

        spans = collector.get_spans()
        assert len(spans) == 1
        assert spans[0].status == "OK"
        assert len(spans[0].events) == 1
        assert spans[0].events[0].name == "warning"

    def test_error_span_on_abort(self):
        """Aborted step creates ERROR span."""
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        collector = SpanCollector(generate_trace_id())
        step = QualityGateStep(
            type="quality_gate",
            check="table_exists",
            source="db.missing",
            on_failure="abort",
        )
        ctx = VariableContext()

        with pytest.raises(HookError):
            run_hook_steps(spark, [step], "pre", ctx, collector=collector)

        spans = collector.get_spans()
        assert len(spans) == 1
        assert spans[0].status == "ERROR"
