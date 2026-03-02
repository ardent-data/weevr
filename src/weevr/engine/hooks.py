"""Hook step executor — dispatches and orchestrates pre/post hook steps."""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from weevr.engine.variables import VariableContext
from weevr.errors.exceptions import HookError
from weevr.model.base import FrozenBase
from weevr.model.hooks import LogMessageStep, QualityGateStep, SqlStatementStep
from weevr.operations.gates import (
    GateResult,
    check_expression,
    check_row_count,
    check_row_count_delta,
    check_source_freshness,
    check_table_exists,
)
from weevr.telemetry.span import SpanStatus

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from weevr.model.hooks import HookStep
    from weevr.telemetry.collector import SpanCollector

logger = logging.getLogger("weevr.engine.hooks")

_PARAM_PATTERN = re.compile(r"\$\{param\.([a-zA-Z_][a-zA-Z0-9_]*)\}")

# Phase-specific defaults for on_failure when not explicitly set (R1)
_DEFAULT_ON_FAILURE: dict[str, str] = {
    "pre": "abort",
    "post": "warn",
}


class HookResult(FrozenBase):
    """Result of a single hook step execution.

    Attributes:
        step_type: Hook step type (quality_gate, sql_statement, log_message).
        step_name: Optional human-readable name for the step.
        phase: Execution phase (pre or post).
        status: Outcome status (passed, warned, aborted, error).
        duration_ms: Execution time in milliseconds.
        gate_result: Quality gate result, if applicable.
        error: Error message, if the step failed.
    """

    step_type: str
    step_name: str | None = None
    phase: str = ""
    status: str = "passed"
    duration_ms: float = 0.0
    gate_result: GateResult | None = None
    error: str | None = None


def _resolve_params(text: str, params: dict[str, Any]) -> str:
    """Replace ``${param.name}`` placeholders in a string.

    Args:
        text: Input string potentially containing ``${param.name}`` refs.
        params: Parameter mapping to resolve against.

    Returns:
        String with resolved parameter references. Unmatched refs left as-is.
    """

    def _replacer(match: re.Match[str]) -> str:
        name = match.group(1)
        if name in params:
            return str(params[name])
        return match.group(0)

    return _PARAM_PATTERN.sub(_replacer, text)


def _resolve_text(text: str, variable_ctx: VariableContext, params: dict[str, Any]) -> str:
    """Resolve both ``${var.name}`` and ``${param.name}`` placeholders.

    Variable refs are resolved first, then param refs.

    Args:
        text: Input string with placeholders.
        variable_ctx: Variable context for ``${var.name}`` resolution.
        params: Parameter mapping for ``${param.name}`` resolution.

    Returns:
        Fully resolved string.
    """
    text = variable_ctx.resolve_refs(text)
    return _resolve_params(text, params)


def _execute_quality_gate(
    spark: SparkSession,
    step: QualityGateStep,
    row_counts: dict[str, int],
) -> GateResult:
    """Dispatch a quality gate check to the appropriate gate function.

    Args:
        spark: Active SparkSession.
        step: Quality gate step configuration.
        row_counts: Pre-snapshot row counts keyed by table alias.

    Returns:
        GateResult from the appropriate check function.

    Raises:
        HookError: If the check type is unknown.
    """
    check = step.check

    if check == "source_freshness":
        assert step.source is not None  # validated by model  # noqa: S101
        assert step.max_age is not None  # noqa: S101
        return check_source_freshness(spark, step.source, step.max_age)

    if check == "row_count_delta":
        assert step.target is not None  # noqa: S101
        before_count = row_counts.get(step.target, 0)
        return check_row_count_delta(
            spark,
            step.target,
            before_count,
            max_decrease_pct=step.max_decrease_pct,
            max_increase_pct=step.max_increase_pct,
            min_delta=step.min_delta,
            max_delta=step.max_delta,
        )

    if check == "row_count":
        assert step.target is not None  # noqa: S101
        return check_row_count(
            spark,
            step.target,
            min_count=step.min_count,
            max_count=step.max_count,
        )

    if check == "table_exists":
        assert step.source is not None  # noqa: S101
        return check_table_exists(spark, step.source)

    if check == "expression":
        assert step.sql is not None  # noqa: S101
        return check_expression(spark, step.sql, step.message)

    msg = f"Unknown quality gate check type: {check}"
    raise HookError(msg, hook_type="quality_gate")


def _execute_sql_statement(
    spark: SparkSession,
    step: SqlStatementStep,
    variable_ctx: VariableContext,
    params: dict[str, Any],
) -> None:
    """Execute a SQL statement step, optionally capturing the result.

    Args:
        spark: Active SparkSession.
        step: SQL statement step configuration.
        variable_ctx: Variable context for interpolation and set_var.
        params: Parameter mapping for interpolation.
    """
    resolved_sql = _resolve_text(step.sql, variable_ctx, params)
    result = spark.sql(resolved_sql)  # noqa: S608

    if step.set_var:
        rows = result.collect()
        value = rows[0][0] if rows else None
        variable_ctx.set(step.set_var, value)


def _execute_log_message(
    step: LogMessageStep,
    variable_ctx: VariableContext,
    params: dict[str, Any],
    span_id: str | None = None,
) -> str:
    """Execute a log message step.

    Args:
        step: Log message step configuration.
        variable_ctx: Variable context for interpolation.
        params: Parameter mapping for interpolation.
        span_id: Optional span ID for correlation.

    Returns:
        The resolved message string (for telemetry span event emission).
    """
    resolved_msg = _resolve_text(step.message, variable_ctx, params)

    extra: dict[str, Any] = {}
    if span_id:
        extra["span_id"] = span_id

    level_map = {"info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}
    logger.log(level_map.get(step.level, logging.INFO), resolved_msg, extra=extra)
    return resolved_msg


def run_hook_steps(
    spark: SparkSession,
    steps: Sequence[HookStep],
    phase: str,
    variable_ctx: VariableContext,
    params: dict[str, Any] | None = None,
    collector: SpanCollector | None = None,
    parent_span_id: str | None = None,
    row_counts: dict[str, int] | None = None,
) -> list[HookResult]:
    """Execute hook steps sequentially in declared order.

    Iterates through each step, dispatches to the appropriate handler,
    records telemetry spans, and handles failure semantics.

    Args:
        spark: Active SparkSession.
        steps: List of hook step configurations.
        phase: Execution phase (``"pre"`` or ``"post"``).
        variable_ctx: Variable context for interpolation and set_var.
        params: Optional parameter mapping for ``${param.name}`` resolution.
        collector: Optional span collector for telemetry.
        parent_span_id: Optional parent span ID for hierarchy.
        row_counts: Pre-snapshot row counts for row_count_delta checks.

    Returns:
        List of HookResult objects, one per step.

    Raises:
        HookError: If a step fails with ``on_failure: abort``.
    """
    import time

    params = params or {}
    row_counts = row_counts or {}
    results: list[HookResult] = []

    for i, step in enumerate(steps):
        step_type = step.type
        step_name = step.name
        on_failure = step.on_failure or _DEFAULT_ON_FAILURE.get(phase, "abort")

        span_label = f"hook:{phase}:{step_type}"
        if step_name:
            span_label += f":{step_name}"

        span = None
        if collector:
            span = collector.start_span(span_label, parent_span_id=parent_span_id)
            span.set_attribute("hook.phase", phase)
            span.set_attribute("hook.step_type", step_type)
            span.set_attribute("hook.step_index", i)
            if step_name:
                span.set_attribute("hook.step_name", step_name)

        start = time.monotonic()
        gate_result: GateResult | None = None
        error_msg: str | None = None
        status = "passed"

        try:
            if isinstance(step, QualityGateStep):
                gate_result = _execute_quality_gate(spark, step, row_counts)
                if not gate_result.passed:
                    error_msg = gate_result.message
                    status = "aborted" if on_failure == "abort" else "warned"

            elif isinstance(step, SqlStatementStep):
                _execute_sql_statement(spark, step, variable_ctx, params)

            elif isinstance(step, LogMessageStep):
                resolved_msg = _execute_log_message(
                    step,
                    variable_ctx,
                    params,
                    span_id=span.span_id if span else None,
                )
                if span:
                    span.add_event("log", {"message": resolved_msg, "level": step.level})

        except HookError:
            raise
        except Exception as exc:
            error_msg = str(exc)
            if on_failure == "abort":
                status = "aborted"
            else:
                status = "warned"
                logger.warning("Hook step %s failed (warn): %s", span_label, exc)

        elapsed_ms = (time.monotonic() - start) * 1000

        if span:
            if status == "passed":
                span_status = SpanStatus.OK
            elif status == "warned":
                span_status = SpanStatus.OK
                span.add_event("warning", {"message": error_msg or ""})
            else:
                span_status = SpanStatus.ERROR
                span.add_event("error", {"message": error_msg or ""})
            collector.add_span(span.finish(span_status))  # type: ignore[union-attr]

        result = HookResult(
            step_type=step_type,
            step_name=step_name,
            phase=phase,
            status=status,
            duration_ms=elapsed_ms,
            gate_result=gate_result,
            error=error_msg,
        )
        results.append(result)

        if status == "aborted":
            raise HookError(
                error_msg or f"Hook step '{step_name or step_type}' failed",
                hook_name=step_name,
                hook_type=step_type,
                phase=phase,
            )

    return results
