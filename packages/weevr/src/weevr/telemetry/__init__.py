"""weevr telemetry — structured observability for execution pipelines."""

from weevr.telemetry.collector import SpanBuilder, SpanCollector
from weevr.telemetry.events import LogEvent, create_log_event
from weevr.telemetry.logging import LogLevel, StructuredJsonFormatter, configure_logging
from weevr.telemetry.results import (
    AssertionResult,
    ExportResult,
    LoomTelemetry,
    ThreadTelemetry,
    ValidationResult,
    WeaveTelemetry,
)
from weevr.telemetry.span import (
    ExecutionSpan,
    SpanEvent,
    SpanStatus,
    generate_span_id,
    generate_trace_id,
)
from weevr.telemetry.trace import LoomTrace, ThreadTrace, WeaveTrace

__all__ = [
    # Span model
    "ExecutionSpan",
    "SpanEvent",
    "SpanStatus",
    "generate_trace_id",
    "generate_span_id",
    # Collector
    "SpanCollector",
    "SpanBuilder",
    # Events and logging
    "LogEvent",
    "create_log_event",
    "LogLevel",
    "StructuredJsonFormatter",
    "configure_logging",
    # Telemetry results
    "ValidationResult",
    "AssertionResult",
    "ExportResult",
    "ThreadTelemetry",
    "WeaveTelemetry",
    "LoomTelemetry",
    # Trace
    "ThreadTrace",
    "WeaveTrace",
    "LoomTrace",
]
