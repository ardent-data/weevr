"""Tests for telemetry result models."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from weevr.telemetry.results import (
    AssertionResult,
    LoomTelemetry,
    ThreadTelemetry,
    ValidationResult,
    WeaveTelemetry,
)
from weevr.telemetry.span import ExecutionSpan, SpanStatus


def _make_span(name: str = "test") -> ExecutionSpan:
    return ExecutionSpan(
        trace_id="a" * 32,
        span_id="b" * 16,
        name=name,
        status=SpanStatus.OK,
        start_time=datetime.now(UTC),
        end_time=datetime.now(UTC),
    )


class TestValidationResult:
    def test_create(self) -> None:
        vr = ValidationResult(
            rule_name="null_email",
            expression="email IS NOT NULL",
            severity="error",
            rows_passed=95,
            rows_failed=5,
        )
        assert vr.rule_name == "null_email"
        assert vr.rows_failed == 5
        assert vr.applied is True

    def test_unapplied_rule(self) -> None:
        vr = ValidationResult(
            rule_name="bad_expr",
            expression="INVALID SQL",
            severity="error",
            rows_passed=0,
            rows_failed=0,
            applied=False,
        )
        assert vr.applied is False

    def test_immutable(self) -> None:
        vr = ValidationResult(
            rule_name="test",
            expression="x > 0",
            severity="warn",
            rows_passed=10,
            rows_failed=0,
        )
        with pytest.raises(ValidationError):
            vr.rows_failed = 5  # type: ignore[misc]


class TestAssertionResult:
    def test_create(self) -> None:
        ar = AssertionResult(
            assertion_type="row_count",
            severity="warn",
            passed=True,
            details="Row count 1000 within bounds [100, 10000]",
        )
        assert ar.passed is True
        assert ar.columns is None

    def test_with_columns(self) -> None:
        ar = AssertionResult(
            assertion_type="column_not_null",
            severity="fatal",
            passed=False,
            details="Column 'email' has 3 nulls",
            columns=["email"],
        )
        assert ar.columns == ["email"]
        assert ar.severity == "fatal"


class TestThreadTelemetry:
    def test_create_empty(self) -> None:
        tt = ThreadTelemetry(span=_make_span("thread:customer"))
        assert tt.validation_results == []
        assert tt.assertion_results == []
        assert tt.rows_read == 0
        assert tt.rows_written == 0
        assert tt.rows_quarantined == 0

    def test_create_with_results(self) -> None:
        vr = ValidationResult(
            rule_name="test",
            expression="x > 0",
            severity="error",
            rows_passed=90,
            rows_failed=10,
        )
        ar = AssertionResult(
            assertion_type="row_count",
            severity="warn",
            passed=True,
            details="ok",
        )
        tt = ThreadTelemetry(
            span=_make_span(),
            validation_results=[vr],
            assertion_results=[ar],
            rows_read=100,
            rows_written=90,
            rows_quarantined=10,
        )
        assert len(tt.validation_results) == 1
        assert len(tt.assertion_results) == 1
        assert tt.rows_read == 100


class TestWeaveTelemetry:
    def test_create_empty(self) -> None:
        wt = WeaveTelemetry(span=_make_span("weave:dim"))
        assert wt.thread_telemetry == {}

    def test_with_thread_telemetry(self) -> None:
        tt = ThreadTelemetry(span=_make_span("thread:customer"), rows_read=50)
        wt = WeaveTelemetry(
            span=_make_span("weave:dim"),
            thread_telemetry={"customer": tt},
        )
        assert "customer" in wt.thread_telemetry
        assert wt.thread_telemetry["customer"].rows_read == 50


class TestLoomTelemetry:
    def test_create_empty(self) -> None:
        lt = LoomTelemetry(span=_make_span("loom:nightly"))
        assert lt.weave_telemetry == {}

    def test_full_composition(self) -> None:
        tt = ThreadTelemetry(span=_make_span("thread:customer"), rows_written=100)
        wt = WeaveTelemetry(
            span=_make_span("weave:dim"),
            thread_telemetry={"customer": tt},
        )
        lt = LoomTelemetry(
            span=_make_span("loom:nightly"),
            weave_telemetry={"dim": wt},
        )
        assert lt.weave_telemetry["dim"].thread_telemetry["customer"].rows_written == 100
