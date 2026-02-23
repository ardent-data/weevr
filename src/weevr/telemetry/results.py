"""Telemetry result models — validation, assertion, and execution telemetry."""

from typing import Literal

from weevr.model.base import FrozenBase
from weevr.telemetry.span import ExecutionSpan

# Reuse the severity literal from model/validation.py to avoid circular imports.
# Both modules define severity as the same literal union.
Severity = Literal["info", "warn", "error", "fatal"]


class ValidationResult(FrozenBase):
    """Outcome of a single validation rule evaluation.

    Attributes:
        rule_name: Name of the validation rule.
        expression: The Spark SQL expression that was evaluated.
        severity: Severity level of the rule.
        rows_passed: Number of rows that passed the rule.
        rows_failed: Number of rows that failed the rule.
        applied: False if the rule expression itself errored during evaluation.
    """

    rule_name: str
    expression: str
    severity: Severity
    rows_passed: int
    rows_failed: int
    applied: bool = True


class AssertionResult(FrozenBase):
    """Outcome of a single post-execution assertion.

    Attributes:
        assertion_type: Type of assertion (row_count, column_not_null, unique, expression).
        severity: Configured severity level.
        passed: Whether the assertion passed.
        details: Human-readable outcome description.
        columns: Columns involved, if applicable.
    """

    assertion_type: str
    severity: Severity
    passed: bool
    details: str
    columns: list[str] | None = None


class ThreadTelemetry(FrozenBase):
    """Telemetry data composed into a ThreadResult.

    Attributes:
        span: The execution span for this thread.
        validation_results: Results of pre-write validation rules.
        assertion_results: Results of post-write assertions.
        rows_read: Total rows read from sources.
        rows_written: Rows written to the target.
        rows_quarantined: Rows written to the quarantine table.
    """

    span: ExecutionSpan
    validation_results: list[ValidationResult] = []
    assertion_results: list[AssertionResult] = []
    rows_read: int = 0
    rows_written: int = 0
    rows_quarantined: int = 0


class WeaveTelemetry(FrozenBase):
    """Telemetry data composed into a WeaveResult.

    Attributes:
        span: The execution span for this weave.
        thread_telemetry: Per-thread telemetry keyed by thread name.
    """

    span: ExecutionSpan
    thread_telemetry: dict[str, ThreadTelemetry] = {}


class LoomTelemetry(FrozenBase):
    """Telemetry data composed into a LoomResult.

    Attributes:
        span: The execution span for this loom.
        weave_telemetry: Per-weave telemetry keyed by weave name.
    """

    span: ExecutionSpan
    weave_telemetry: dict[str, WeaveTelemetry] = {}
