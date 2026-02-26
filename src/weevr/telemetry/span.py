"""Execution span models — OTel-compatible span hierarchy for telemetry."""

import secrets
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import Field

from weevr.model.base import FrozenBase


class SpanStatus(StrEnum):
    """Status of an execution span, aligned with OTel conventions."""

    UNSET = "UNSET"
    OK = "OK"
    ERROR = "ERROR"


class SpanEvent(FrozenBase):
    """A timestamped event within a span."""

    name: str
    timestamp: datetime
    attributes: dict[str, str | int | float | bool] = {}


class ExecutionSpan(FrozenBase):
    """OTel-compatible execution span for telemetry.

    Represents a unit of work (thread, weave, or loom execution) with timing,
    status, and key-value metric attributes. Field naming follows OTel Span
    semantic conventions for future export compatibility.

    Attributes:
        trace_id: 32-character hex string identifying the execution trace.
        span_id: 16-character hex string identifying this span.
        parent_span_id: span_id of the parent span, or None for root spans.
        name: Human-readable span name (e.g., "thread:customer_dim").
        status: Outcome status of this span.
        start_time: UTC timestamp when the span started.
        end_time: UTC timestamp when the span ended, or None if still open.
        attributes: Key-value metric attributes (row counts, durations, etc.).
        events: Timestamped events that occurred within this span.
    """

    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    name: str
    status: SpanStatus = SpanStatus.UNSET
    start_time: datetime = Field(default_factory=lambda: datetime.now(UTC))
    end_time: datetime | None = None
    attributes: dict[str, str | int | float | bool] = {}
    events: list[SpanEvent] = []


def generate_trace_id() -> str:
    """Generate a 32-character hex trace ID (OTel convention)."""
    return secrets.token_hex(16)


def generate_span_id() -> str:
    """Generate a 16-character hex span ID (OTel convention)."""
    return secrets.token_hex(8)
