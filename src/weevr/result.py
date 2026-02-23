"""Unified result types for the weevr Python API."""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from weevr.engine.planner import ExecutionPlan
    from weevr.engine.result import LoomResult, ThreadResult, WeaveResult
    from weevr.telemetry.results import LoomTelemetry, ThreadTelemetry, WeaveTelemetry


class ExecutionMode(StrEnum):
    """Execution modes for ``Context.run()``.

    Attributes:
        EXECUTE: Full execution — read, transform, write (default).
        VALIDATE: Config + DAG + source existence checks, no execution.
        PLAN: Build and return execution plans without executing.
        PREVIEW: Execute with sampled data, no writes.
    """

    EXECUTE = "execute"
    VALIDATE = "validate"
    PLAN = "plan"
    PREVIEW = "preview"


class RunResult:
    """Unified result returned by ``Context.run()`` for all execution modes.

    Provides a consistent interface regardless of config type (thread, weave,
    loom) or execution mode (execute, validate, plan, preview). Mode-specific
    fields are ``None`` when not applicable.

    Attributes:
        status: Aggregate outcome — ``"success"``, ``"failure"``, or ``"partial"``.
        mode: The execution mode that produced this result.
        config_type: Config kind that was executed (``"thread"``, ``"weave"``, ``"loom"``).
        config_name: Name derived from the config file path.
        duration_ms: Wall-clock duration in milliseconds.
        detail: Underlying engine result (execute mode only).
        telemetry: Structured telemetry data (execute mode only).
        execution_plan: Resolved execution plans (plan mode only).
        preview_data: Output DataFrames keyed by thread name (preview mode only).
        validation_errors: Error messages from validation checks (validate mode only).
        warnings: Non-fatal messages (e.g., zero threads matched a filter).
    """

    __slots__ = (
        "status",
        "mode",
        "config_type",
        "config_name",
        "duration_ms",
        "detail",
        "telemetry",
        "execution_plan",
        "preview_data",
        "validation_errors",
        "warnings",
    )

    def __init__(
        self,
        *,
        status: Literal["success", "failure", "partial"],
        mode: ExecutionMode,
        config_type: str,
        config_name: str,
        duration_ms: int = 0,
        detail: ThreadResult | WeaveResult | LoomResult | None = None,
        telemetry: ThreadTelemetry | WeaveTelemetry | LoomTelemetry | None = None,
        execution_plan: list[ExecutionPlan] | None = None,
        preview_data: dict[str, Any] | None = None,
        validation_errors: list[str] | None = None,
        warnings: list[str] | None = None,
    ) -> None:
        self.status = status
        self.mode = mode
        self.config_type = config_type
        self.config_name = config_name
        self.duration_ms = duration_ms
        self.detail = detail
        self.telemetry = telemetry
        self.execution_plan = execution_plan
        self.preview_data = preview_data
        self.validation_errors = validation_errors
        self.warnings: list[str] = warnings if warnings is not None else []

    def summary(self) -> str:
        """Return a formatted, human-readable execution summary."""
        return f"Status: {self.status}"
