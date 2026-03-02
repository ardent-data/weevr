"""Hook step models for weave execution lifecycle."""

from typing import Annotated, Any, Literal

from pydantic import Discriminator, Tag, model_validator

from weevr.model.base import FrozenBase


class QualityGateStep(FrozenBase):
    """A quality gate check executed as a hook step.

    The ``check`` field determines which check-specific fields are required.

    Attributes:
        type: Step type discriminator, always ``"quality_gate"``.
        name: Optional name for telemetry span naming.
        on_failure: Failure behaviour. ``None`` means the executor applies the
            phase-specific default (pre=abort, post=warn).
        check: Which quality gate check to perform.
        source: Table alias for ``source_freshness`` and ``table_exists`` checks.
        max_age: Duration string for ``source_freshness`` (e.g. ``"24h"``).
        target: Table alias for ``row_count_delta`` and ``row_count`` checks.
        max_decrease_pct: Max allowed decrease percentage for ``row_count_delta``.
        max_increase_pct: Max allowed increase percentage for ``row_count_delta``.
        min_delta: Minimum absolute row change for ``row_count_delta``.
        max_delta: Maximum absolute row change for ``row_count_delta``.
        min_count: Minimum row count for ``row_count``.
        max_count: Maximum row count for ``row_count``.
        sql: Spark SQL boolean expression for ``expression`` check.
        message: Failure message for ``expression`` check diagnostics.
    """

    type: Literal["quality_gate"]
    name: str | None = None
    on_failure: Literal["abort", "warn"] | None = None
    check: Literal["source_freshness", "row_count_delta", "row_count", "table_exists", "expression"]

    # source_freshness / table_exists
    source: str | None = None
    max_age: str | None = None

    # row_count_delta
    target: str | None = None
    max_decrease_pct: float | None = None
    max_increase_pct: float | None = None
    min_delta: int | None = None
    max_delta: int | None = None

    # row_count
    min_count: int | None = None
    max_count: int | None = None

    # expression
    sql: str | None = None
    message: str | None = None

    @model_validator(mode="after")
    def _validate_check_fields(self) -> "QualityGateStep":
        """Validate that required fields are present for each check type."""
        check = self.check

        if check == "source_freshness":
            if not self.source:
                raise ValueError("source_freshness requires 'source'")
            if not self.max_age:
                raise ValueError("source_freshness requires 'max_age'")

        elif check == "row_count_delta":
            if not self.target:
                raise ValueError("row_count_delta requires 'target'")

        elif check == "row_count":
            if not self.target:
                raise ValueError("row_count requires 'target'")
            if self.min_count is None and self.max_count is None:
                raise ValueError("row_count requires at least 'min_count' or 'max_count'")

        elif check == "table_exists":
            if not self.source:
                raise ValueError("table_exists requires 'source'")

        elif check == "expression":
            if not self.sql:
                raise ValueError("expression requires 'sql'")

        return self


class SqlStatementStep(FrozenBase):
    """An arbitrary SQL statement executed as a hook step.

    Optionally captures the scalar result into a weave-scoped variable
    via ``set_var``.

    Attributes:
        type: Step type discriminator, always ``"sql_statement"``.
        name: Optional name for telemetry span naming.
        on_failure: Failure behaviour. ``None`` means the executor applies the
            phase-specific default.
        sql: Spark SQL statement to execute.
        set_var: Optional variable name to capture the scalar result into.
    """

    type: Literal["sql_statement"]
    name: str | None = None
    on_failure: Literal["abort", "warn"] | None = None
    sql: str
    set_var: str | None = None


class LogMessageStep(FrozenBase):
    """A log message emitted as a hook step.

    Attributes:
        type: Step type discriminator, always ``"log_message"``.
        name: Optional name for telemetry span naming.
        on_failure: Failure behaviour. ``None`` means the executor applies the
            phase-specific default.
        message: Message template to log. Supports ``${var.name}`` placeholders.
        level: Log level for the message.
    """

    type: Literal["log_message"]
    name: str | None = None
    on_failure: Literal["abort", "warn"] | None = None
    message: str
    level: Literal["info", "warn", "error"] = "info"


def _hook_step_discriminator(v: Any) -> str:
    """Return the hook step type key for discriminated union dispatch.

    Handles both raw dicts (from YAML) and model instances.
    """
    if isinstance(v, dict):
        return str(v.get("type", "<missing>"))
    if hasattr(v, "type"):
        return str(v.type)
    return f"<unknown:{type(v).__name__}>"


HookStep = Annotated[
    Annotated[QualityGateStep, Tag("quality_gate")]
    | Annotated[SqlStatementStep, Tag("sql_statement")]
    | Annotated[LogMessageStep, Tag("log_message")],
    Discriminator(_hook_step_discriminator),
]
"""Discriminated union of all hook step types.

Dispatches on the ``type`` field: ``quality_gate``, ``sql_statement``, or
``log_message``.
"""
