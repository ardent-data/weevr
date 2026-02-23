"""Structured log event model for weevr telemetry."""

from datetime import UTC, datetime

from weevr.model.base import FrozenBase


class LogEvent(FrozenBase):
    """Frozen Pydantic model for a structured log event.

    Each execution step can produce one or more LogEvents. These are
    serialized to JSON via :class:`StructuredJsonFormatter` and emitted
    through Python's logging module.

    Attributes:
        timestamp: UTC timestamp of the event.
        level: Log level string (INFO, WARNING, ERROR, DEBUG).
        component: Dotted module path (e.g., "engine.executor").
        thread_name: Thread being executed, if applicable.
        weave_name: Weave being executed, if applicable.
        loom_name: Loom being executed, if applicable.
        trace_id: Trace ID correlating this event to an execution trace.
        span_id: Span ID of the active span when this event was emitted.
        message: Human-readable log message.
        attributes: Additional key-value context.
    """

    timestamp: datetime
    level: str
    component: str
    thread_name: str | None = None
    weave_name: str | None = None
    loom_name: str | None = None
    trace_id: str | None = None
    span_id: str | None = None
    message: str
    attributes: dict[str, str | int | float | bool] = {}


def create_log_event(
    level: str,
    component: str,
    message: str,
    *,
    thread_name: str | None = None,
    weave_name: str | None = None,
    loom_name: str | None = None,
    trace_id: str | None = None,
    span_id: str | None = None,
    attributes: dict[str, str | int | float | bool] | None = None,
) -> LogEvent:
    """Create a LogEvent with auto-generated UTC timestamp.

    Args:
        level: Log level (INFO, WARNING, ERROR, DEBUG).
        component: Source component (e.g., "engine.executor").
        message: Human-readable message.
        thread_name: Optional thread context.
        weave_name: Optional weave context.
        loom_name: Optional loom context.
        trace_id: Optional trace correlation ID.
        span_id: Optional span correlation ID.
        attributes: Optional additional key-value attributes.

    Returns:
        Frozen LogEvent instance.
    """
    return LogEvent(
        timestamp=datetime.now(UTC),
        level=level,
        component=component,
        thread_name=thread_name,
        weave_name=weave_name,
        loom_name=loom_name,
        trace_id=trace_id,
        span_id=span_id,
        message=message,
        attributes=attributes or {},
    )
