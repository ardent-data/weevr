"""Tests for SpanCollector and SpanBuilder."""

import pytest

from weevr.telemetry.collector import SpanBuilder, SpanCollector
from weevr.telemetry.span import SpanStatus, generate_trace_id


class TestSpanBuilder:
    def test_finish_produces_span(self) -> None:
        builder = SpanBuilder(
            trace_id="a" * 32, span_id="b" * 16, name="test", parent_span_id=None
        )
        span = builder.finish(SpanStatus.OK)
        assert span.trace_id == "a" * 32
        assert span.span_id == "b" * 16
        assert span.name == "test"
        assert span.status == SpanStatus.OK
        assert span.end_time is not None
        assert span.parent_span_id is None

    def test_set_attribute(self) -> None:
        builder = SpanBuilder(
            trace_id="a" * 32, span_id="b" * 16, name="test", parent_span_id=None
        )
        builder.set_attribute("rows_read", 100)
        builder.set_attribute("duration_ms", 42)
        span = builder.finish()
        assert span.attributes["rows_read"] == 100
        assert span.attributes["duration_ms"] == 42

    def test_add_event(self) -> None:
        builder = SpanBuilder(
            trace_id="a" * 32, span_id="b" * 16, name="test", parent_span_id=None
        )
        builder.add_event("validation_start", {"rules": 3})
        builder.add_event("validation_end")
        span = builder.finish()
        assert len(span.events) == 2
        assert span.events[0].name == "validation_start"
        assert span.events[0].attributes == {"rules": 3}
        assert span.events[1].name == "validation_end"

    def test_finish_twice_raises(self) -> None:
        builder = SpanBuilder(
            trace_id="a" * 32, span_id="b" * 16, name="test", parent_span_id=None
        )
        builder.finish()
        with pytest.raises(RuntimeError, match="already finished"):
            builder.finish()

    def test_parent_span_id(self) -> None:
        builder = SpanBuilder(
            trace_id="a" * 32, span_id="b" * 16, name="child", parent_span_id="c" * 16
        )
        span = builder.finish()
        assert span.parent_span_id == "c" * 16

    def test_span_id_property(self) -> None:
        builder = SpanBuilder(
            trace_id="a" * 32, span_id="b" * 16, name="test", parent_span_id=None
        )
        assert builder.span_id == "b" * 16


class TestSpanCollector:
    def test_start_and_finish_span(self) -> None:
        tid = generate_trace_id()
        collector = SpanCollector(tid)
        builder = collector.start_span("thread:customer")
        builder.set_attribute("rows", 50)
        span = builder.finish(SpanStatus.OK)
        collector.add_span(span)

        spans = collector.get_spans()
        assert len(spans) == 1
        assert spans[0].name == "thread:customer"
        assert spans[0].trace_id == tid
        assert spans[0].attributes["rows"] == 50

    def test_multiple_spans(self) -> None:
        tid = generate_trace_id()
        collector = SpanCollector(tid)
        for i in range(3):
            builder = collector.start_span(f"thread:{i}")
            span = builder.finish()
            collector.add_span(span)

        assert len(collector.get_spans()) == 3

    def test_merge_collectors(self) -> None:
        tid = generate_trace_id()
        c1 = SpanCollector(tid)
        c2 = SpanCollector(tid)

        b1 = c1.start_span("thread:a")
        c1.add_span(b1.finish())
        b2 = c2.start_span("thread:b")
        c2.add_span(b2.finish())

        c1.merge(c2)
        spans = c1.get_spans()
        assert len(spans) == 2
        names = {s.name for s in spans}
        assert names == {"thread:a", "thread:b"}

    def test_isolation(self) -> None:
        """Two collectors with the same trace_id don't interfere."""
        tid = generate_trace_id()
        c1 = SpanCollector(tid)
        c2 = SpanCollector(tid)

        b1 = c1.start_span("span_a")
        c1.add_span(b1.finish())

        assert len(c1.get_spans()) == 1
        assert len(c2.get_spans()) == 0

    def test_get_spans_returns_copy(self) -> None:
        tid = generate_trace_id()
        collector = SpanCollector(tid)
        builder = collector.start_span("test")
        collector.add_span(builder.finish())

        spans = collector.get_spans()
        spans.clear()
        assert len(collector.get_spans()) == 1

    def test_trace_id_property(self) -> None:
        tid = generate_trace_id()
        collector = SpanCollector(tid)
        assert collector.trace_id == tid

    def test_start_span_with_parent(self) -> None:
        tid = generate_trace_id()
        collector = SpanCollector(tid)
        parent_builder = collector.start_span("weave:dim")
        parent_span = parent_builder.finish()
        collector.add_span(parent_span)

        child_builder = collector.start_span("thread:customer", parent_span_id=parent_span.span_id)
        child_span = child_builder.finish()
        collector.add_span(child_span)

        spans = collector.get_spans()
        assert spans[1].parent_span_id == spans[0].span_id
