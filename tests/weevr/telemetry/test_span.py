"""Tests for ExecutionSpan, SpanStatus, and SpanEvent models."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from weevr.telemetry.span import (
    ExecutionSpan,
    SpanEvent,
    SpanStatus,
    generate_span_id,
    generate_trace_id,
)


class TestSpanStatus:
    def test_values(self) -> None:
        assert SpanStatus.UNSET == "UNSET"
        assert SpanStatus.OK == "OK"
        assert SpanStatus.ERROR == "ERROR"

    def test_is_str_enum(self) -> None:
        assert isinstance(SpanStatus.OK, str)


class TestSpanEvent:
    def test_create(self) -> None:
        ts = datetime.now(UTC)
        event = SpanEvent(name="validation_complete", timestamp=ts, attributes={"rules": 3})
        assert event.name == "validation_complete"
        assert event.timestamp == ts
        assert event.attributes == {"rules": 3}

    def test_default_attributes(self) -> None:
        event = SpanEvent(name="start", timestamp=datetime.now(UTC))
        assert event.attributes == {}

    def test_immutable(self) -> None:
        event = SpanEvent(name="x", timestamp=datetime.now(UTC))
        with pytest.raises(ValidationError):
            event.name = "y"  # type: ignore[misc]


class TestExecutionSpan:
    def test_create_with_all_fields(self) -> None:
        now = datetime.now(UTC)
        span = ExecutionSpan(
            trace_id="a" * 32,
            span_id="b" * 16,
            parent_span_id="c" * 16,
            name="thread:customer_dim",
            status=SpanStatus.OK,
            start_time=now,
            end_time=now,
            attributes={"rows_read": 100, "duration_ms": 500},
            events=[SpanEvent(name="step", timestamp=now)],
        )
        assert span.trace_id == "a" * 32
        assert span.span_id == "b" * 16
        assert span.parent_span_id == "c" * 16
        assert span.name == "thread:customer_dim"
        assert span.status == SpanStatus.OK
        assert span.attributes["rows_read"] == 100
        assert len(span.events) == 1

    def test_defaults(self) -> None:
        span = ExecutionSpan(
            trace_id="a" * 32,
            span_id="b" * 16,
            name="test",
        )
        assert span.parent_span_id is None
        assert span.status == SpanStatus.UNSET
        assert span.end_time is None
        assert span.attributes == {}
        assert span.events == []
        assert span.start_time is not None

    def test_immutable(self) -> None:
        span = ExecutionSpan(
            trace_id="a" * 32,
            span_id="b" * 16,
            name="test",
        )
        with pytest.raises(ValidationError):
            span.status = SpanStatus.OK  # type: ignore[misc]

    def test_attribute_types(self) -> None:
        span = ExecutionSpan(
            trace_id="a" * 32,
            span_id="b" * 16,
            name="test",
            attributes={
                "str_val": "hello",
                "int_val": 42,
                "float_val": 3.14,
                "bool_val": True,
            },
        )
        assert span.attributes["str_val"] == "hello"
        assert span.attributes["int_val"] == 42
        assert span.attributes["float_val"] == 3.14
        assert span.attributes["bool_val"] is True


class TestIdGeneration:
    def test_trace_id_length(self) -> None:
        tid = generate_trace_id()
        assert len(tid) == 32
        int(tid, 16)  # validates hex

    def test_span_id_length(self) -> None:
        sid = generate_span_id()
        assert len(sid) == 16
        int(sid, 16)  # validates hex

    def test_uniqueness(self) -> None:
        ids = {generate_trace_id() for _ in range(100)}
        assert len(ids) == 100

    def test_span_id_uniqueness(self) -> None:
        ids = {generate_span_id() for _ in range(100)}
        assert len(ids) == 100
