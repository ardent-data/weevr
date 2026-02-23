"""Span collector — mutable accumulator for execution spans."""

from datetime import UTC, datetime

from weevr.telemetry.span import (
    ExecutionSpan,
    SpanEvent,
    SpanStatus,
    generate_span_id,
)


class SpanBuilder:
    """Mutable builder for constructing an ExecutionSpan incrementally.

    Created by :meth:`SpanCollector.start_span`. Accumulate attributes and
    events during execution, then call :meth:`finish` to produce an immutable
    :class:`ExecutionSpan`.
    """

    def __init__(self, trace_id: str, span_id: str, name: str, parent_span_id: str | None) -> None:
        self._trace_id = trace_id
        self._span_id = span_id
        self._name = name
        self._parent_span_id = parent_span_id
        self._start_time = datetime.now(UTC)
        self._attributes: dict[str, str | int | float | bool] = {}
        self._events: list[SpanEvent] = []
        self._finished = False

    @property
    def span_id(self) -> str:
        """The pre-assigned span ID for this builder."""
        return self._span_id

    @property
    def trace_id(self) -> str:
        """The trace ID this span belongs to."""
        return self._trace_id

    def set_attribute(self, key: str, value: str | int | float | bool) -> None:
        """Set a key-value attribute on the span."""
        self._attributes[key] = value

    def add_event(
        self, name: str, attributes: dict[str, str | int | float | bool] | None = None
    ) -> None:
        """Add a timestamped event to the span."""
        self._events.append(
            SpanEvent(
                name=name,
                timestamp=datetime.now(UTC),
                attributes=attributes or {},
            )
        )

    def finish(self, status: SpanStatus = SpanStatus.OK) -> ExecutionSpan:
        """Finalize the span and return an immutable ExecutionSpan.

        Args:
            status: Final status of the span.

        Returns:
            Immutable ExecutionSpan with all accumulated attributes and events.

        Raises:
            RuntimeError: If finish() has already been called on this builder.
        """
        if self._finished:
            raise RuntimeError(f"Span '{self._name}' ({self._span_id}) already finished")
        self._finished = True
        return ExecutionSpan(
            trace_id=self._trace_id,
            span_id=self._span_id,
            parent_span_id=self._parent_span_id,
            name=self._name,
            status=status,
            start_time=self._start_time,
            end_time=datetime.now(UTC),
            attributes=self._attributes,
            events=self._events,
        )


class SpanCollector:
    """Mutable collector that accumulates finished spans during execution.

    Each execution scope (thread, weave, loom) gets its own collector.
    Thread-level collectors are merged into the weave-level collector after
    thread completion, ensuring no contention during concurrent execution.
    """

    def __init__(self, trace_id: str) -> None:
        self._trace_id = trace_id
        self._spans: list[ExecutionSpan] = []

    @property
    def trace_id(self) -> str:
        """The trace ID this collector is bound to."""
        return self._trace_id

    def start_span(self, name: str, parent_span_id: str | None = None) -> SpanBuilder:
        """Start building a new span.

        Args:
            name: Human-readable span name (e.g., "thread:customer_dim").
            parent_span_id: Optional parent span ID for hierarchy.

        Returns:
            A mutable SpanBuilder to accumulate attributes and events.
        """
        return SpanBuilder(
            trace_id=self._trace_id,
            span_id=generate_span_id(),
            name=name,
            parent_span_id=parent_span_id,
        )

    def add_span(self, span: ExecutionSpan) -> None:
        """Add a finished span to the collector."""
        self._spans.append(span)

    def merge(self, other: "SpanCollector") -> None:
        """Merge spans from another collector into this one.

        Used to combine thread-level collectors into the weave-level collector
        after thread completion.
        """
        self._spans.extend(other._spans)

    def get_spans(self) -> list[ExecutionSpan]:
        """Return all collected spans."""
        return list(self._spans)
