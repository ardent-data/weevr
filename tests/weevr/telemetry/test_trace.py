"""Tests for execution trace tree and flat span serialization."""

from datetime import UTC, datetime

from weevr.telemetry.results import ThreadTelemetry
from weevr.telemetry.span import ExecutionSpan, SpanStatus
from weevr.telemetry.trace import LoomTrace, ThreadTrace, WeaveTrace


def _make_span(name: str, parent: str | None = None) -> ExecutionSpan:
    return ExecutionSpan(
        trace_id="a" * 32,
        span_id=name.replace(":", "_").ljust(16, "0")[:16],
        parent_span_id=parent,
        name=name,
        status=SpanStatus.OK,
        start_time=datetime.now(UTC),
        end_time=datetime.now(UTC),
    )


def _make_thread_trace(name: str, parent: str | None = None) -> ThreadTrace:
    span = _make_span(name, parent)
    return ThreadTrace(
        span=span,
        telemetry=ThreadTelemetry(span=span),
    )


class TestThreadTrace:
    def test_to_spans(self) -> None:
        tt = _make_thread_trace("thread:customer")
        spans = tt.to_spans()
        assert len(spans) == 1
        assert spans[0].name == "thread:customer"


class TestWeaveTrace:
    def test_empty_weave(self) -> None:
        wt = WeaveTrace(span=_make_span("weave:dim"))
        assert wt.threads == {}
        spans = wt.to_spans()
        assert len(spans) == 1

    def test_with_threads(self) -> None:
        weave_span = _make_span("weave:dim")
        wt = WeaveTrace(
            span=weave_span,
            threads={
                "customer": _make_thread_trace("thread:customer", weave_span.span_id),
                "product": _make_thread_trace("thread:product", weave_span.span_id),
            },
        )
        spans = wt.to_spans()
        assert len(spans) == 3
        assert spans[0].name == "weave:dim"
        thread_names = {s.name for s in spans[1:]}
        assert thread_names == {"thread:customer", "thread:product"}


class TestLoomTrace:
    def test_empty_loom(self) -> None:
        lt = LoomTrace(span=_make_span("loom:nightly"))
        assert lt.weaves == {}
        assert len(lt.to_spans()) == 1

    def test_full_tree(self) -> None:
        """Build: 1 loom -> 2 weaves -> 3 threads total."""
        loom_span = _make_span("loom:nightly")
        weave1_span = _make_span("weave:dim", loom_span.span_id)
        weave2_span = _make_span("weave:fact", loom_span.span_id)

        lt = LoomTrace(
            span=loom_span,
            weaves={
                "dim": WeaveTrace(
                    span=weave1_span,
                    threads={
                        "customer": _make_thread_trace("thread:customer", weave1_span.span_id),
                        "product": _make_thread_trace("thread:product", weave1_span.span_id),
                    },
                ),
                "fact": WeaveTrace(
                    span=weave2_span,
                    threads={
                        "sales": _make_thread_trace("thread:sales", weave2_span.span_id),
                    },
                ),
            },
        )

        # Tree navigation
        assert "dim" in lt.weaves
        assert "customer" in lt.weaves["dim"].threads
        assert lt.weaves["fact"].threads["sales"].span.name == "thread:sales"

        # Flat serialization
        spans = lt.to_spans()
        assert len(spans) == 6  # 1 loom + 2 weaves + 3 threads
        assert spans[0].name == "loom:nightly"

    def test_parent_child_relationships(self) -> None:
        loom_span = _make_span("loom:nightly")
        weave_span = _make_span("weave:dim", loom_span.span_id)
        thread_span = _make_span("thread:customer", weave_span.span_id)

        lt = LoomTrace(
            span=loom_span,
            weaves={
                "dim": WeaveTrace(
                    span=weave_span,
                    threads={
                        "customer": ThreadTrace(
                            span=thread_span,
                            telemetry=ThreadTelemetry(span=thread_span),
                        ),
                    },
                ),
            },
        )

        spans = lt.to_spans()
        loom_s = spans[0]
        weave_s = spans[1]
        thread_s = spans[2]
        assert loom_s.parent_span_id is None
        assert weave_s.parent_span_id == loom_s.span_id
        assert thread_s.parent_span_id == weave_s.span_id
