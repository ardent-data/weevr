"""Execution trace — tree-shaped trace with flat span serialization."""

from weevr.model.base import FrozenBase
from weevr.telemetry.results import ThreadTelemetry
from weevr.telemetry.span import ExecutionSpan


class ThreadTrace(FrozenBase):
    """Trace node for a single thread execution.

    Attributes:
        span: The execution span for this thread.
        telemetry: Full telemetry data for the thread.
    """

    span: ExecutionSpan
    telemetry: ThreadTelemetry

    def to_spans(self) -> list[ExecutionSpan]:
        """Return this thread's span as a flat list."""
        return [self.span]


class WeaveTrace(FrozenBase):
    """Trace node for a weave execution.

    Attributes:
        span: The execution span for this weave.
        threads: Per-thread traces keyed by thread name.
    """

    span: ExecutionSpan
    threads: dict[str, ThreadTrace] = {}

    def to_spans(self) -> list[ExecutionSpan]:
        """Flatten weave span and all child thread spans into a list."""
        spans = [self.span]
        for thread_trace in self.threads.values():
            spans.extend(thread_trace.to_spans())
        return spans


class LoomTrace(FrozenBase):
    """Trace node for a loom execution — the root of the trace tree.

    Provides both tree-shaped navigation for programmatic access and
    flat span serialization for OTel export.

    Attributes:
        span: The execution span for this loom.
        weaves: Per-weave traces keyed by weave name.
    """

    span: ExecutionSpan
    weaves: dict[str, WeaveTrace] = {}

    def to_spans(self) -> list[ExecutionSpan]:
        """Recursively flatten the entire trace tree into a span list.

        Returns spans in tree order: loom span, then each weave span followed
        by its thread spans.
        """
        spans = [self.span]
        for weave_trace in self.weaves.values():
            spans.extend(weave_trace.to_spans())
        return spans
