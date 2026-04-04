# Telemetry Architecture

This guide explains the internals of weevr's telemetry system: how spans are
created and collected, how the trace tree is composed, and how telemetry
integrates with execution results.

## Introduction

weevr provides OTel-compatible telemetry without depending on the OpenTelemetry
SDK. Spans, trace IDs, and status codes follow OTel conventions, making the
data portable to any tracing backend. The system is designed for concurrent
execution — each thread operates on its own collector, and merging happens
only after thread completion.

## Key concepts

- **ExecutionSpan** — An immutable record of a unit of work. Carries a trace
  ID, span ID, parent span ID, status, timestamps, attributes, and events.
- **SpanBuilder** — A mutable accumulator for constructing a span during
  execution. Attributes and events are added incrementally. Calling `finish()`
  produces an immutable `ExecutionSpan`.
- **SpanCollector** — A mutable container that holds finished spans for a
  given scope (thread, weave, or loom). Collectors merge upward after
  execution completes.
- **Trace tree** — A hierarchy of `LoomTrace` → `WeaveTrace` → `ThreadTrace`
  objects that wrap spans with navigable parent-child relationships.

## How it works

### Span lifecycle

```d2
direction: right

create: collector.start_span() {
  style.fill: "#E3F2FD"
}
accumulate: builder.set_attribute()\nbuilder.add_event() {
  style.fill: "#E8F5E9"
}
finish: builder.finish(status) {
  style.fill: "#FFF3E0"
}
collect: collector.add_span() {
  style.fill: "#F3E5F5"
}
immutable: Immutable ExecutionSpan {
  style.fill: "#E0F2F1"
}

create -> accumulate: SpanBuilder
accumulate -> finish: mutable
finish -> immutable
immutable -> collect: stored in collector
```

1. A `SpanCollector` is created for each execution scope with a shared
   `trace_id`.
2. `collector.start_span("thread:dim_customer", parent_span_id=...)` returns
   a mutable `SpanBuilder` with an auto-generated `span_id`.
3. During execution, the builder accumulates attributes (row counts, durations)
   and events (sources_read, write_complete).
4. `builder.finish(SpanStatus.OK)` stamps the end time and produces an
   immutable `ExecutionSpan`. The builder cannot be used after this call.
5. The finished span is added to the collector.

### Collector isolation during parallel execution

```d2
direction: down

weave_collector: Weave SpanCollector {
  style.fill: "#E3F2FD"
  trace: "trace_id: a1b2c3..."
}

parallel: Parallel Execution Group {
  style.fill: "#FFF3E0"

  t1: Thread: dim_customer {
    style.fill: "#FFE0B2"
    c1: SpanCollector (isolated) {style.fill: "#FFCC80"}
    b1: SpanBuilder → finish() {style.fill: "#FFCC80"}
    c1 -> b1
  }

  t2: Thread: dim_product {
    style.fill: "#FFE0B2"
    c2: SpanCollector (isolated) {style.fill: "#FFCC80"}
    b2: SpanBuilder → finish() {style.fill: "#FFCC80"}
    c2 -> b2
  }

  t3: Thread: dim_store {
    style.fill: "#FFE0B2"
    c3: SpanCollector (isolated) {style.fill: "#FFCC80"}
    b3: SpanBuilder → finish() {style.fill: "#FFCC80"}
    c3 -> b3
  }
}

merge: Merge after completion {
  style.fill: "#E8F5E9"
  op: "weave_collector.merge(\n  thread_collector\n)"
}

parallel.t1 -> merge: complete {style.stroke: "#2E7D32"}
parallel.t2 -> merge: complete {style.stroke: "#2E7D32"}
parallel.t3 -> merge: complete {style.stroke: "#2E7D32"}
merge -> weave_collector: all spans unified {style.stroke: "#1565C0"}
```

When threads run concurrently within a weave, each thread gets its own
`SpanCollector`. This avoids lock contention — no shared mutable state
between threads.

After a thread completes, its collector is merged into the weave-level
collector via `weave_collector.merge(thread_collector)`. This operation
appends all spans from the thread collector into the weave collector's list.

The same pattern repeats at the weave → loom boundary.

### Trace tree composition

The `telemetry.trace` module provides tree-shaped models for navigating
execution results:

| Model | Contains | Used for |
|-------|----------|----------|
| `LoomTrace` | Loom span + `dict[str, WeaveTrace]` | Root of the tree |
| `WeaveTrace` | Weave span + `dict[str, ThreadTrace]` | Intermediate level |
| `ThreadTrace` | Thread span + `ThreadTelemetry` | Leaf nodes with full metrics |

Each trace type provides a `to_spans()` method that recursively flattens the
tree into a list of `ExecutionSpan` objects, suitable for export to OTel-
compatible backends.

### Telemetry result hierarchy

Parallel to the trace tree, the `telemetry.results` module provides data
classes that carry execution metrics:

- **ThreadTelemetry** — Span, row counts (read/written/quarantined), validation
  results, assertion results, watermark state, CDC operation counts.
- **WeaveTelemetry** — Span + `dict[str, ThreadTelemetry]`.
- **LoomTelemetry** — Span + `dict[str, WeaveTelemetry]`.

These are attached to the result objects returned by `ctx.run()`, giving
callers both navigable tree access and flat metric queries.

### Structured logging

The `StructuredJsonFormatter` formats Python log records as single-line JSON
with OTel-compatible fields:

- Standard fields: `timestamp`, `level`, `logger`, `message`
- Correlation fields: `trace_id`, `span_id` (when available)
- Context fields: `weevr_thread`, `weevr_weave`, `weevr_loom`
- Extra: `attributes` dict for custom key-value context

Log level mapping from weevr config values to Python levels:

| weevr level | Python level |
|-------------|-------------|
| `minimal` | `WARNING` |
| `standard` | `INFO` |
| `verbose` | `DEBUG` |
| `debug` | `DEBUG` |

## Module map

| Module | Responsibility |
|--------|----------------|
| `telemetry/span.py` | `ExecutionSpan`, `SpanEvent`, `SpanStatus`, ID generators |
| `telemetry/collector.py` | `SpanBuilder` (mutable), `SpanCollector` (accumulator) |
| `telemetry/trace.py` | `LoomTrace`, `WeaveTrace`, `ThreadTrace` — tree navigation and flattening |
| `telemetry/logging.py` | `StructuredJsonFormatter`, `configure_logging`, `LogLevel` |
| `telemetry/results.py` | `ThreadTelemetry`, `WeaveTelemetry`, `LoomTelemetry`, `ValidationResult`, `AssertionResult` |
| `telemetry/events.py` | `LogEvent` model, `create_log_event` factory |

## Design decisions

- **OTel-compatible but not OTel-dependent** — Span IDs (16-char hex), trace
  IDs (32-char hex), and status codes match OTel conventions. This allows
  export to any OTel backend without requiring the OTel SDK as a runtime
  dependency. The SDK can be added later as an optional integration.
- **Per-thread collectors** — Concurrent threads each get their own collector.
  No locks, no contention. Merging happens only after thread completion, when
  the executor is back in single-threaded control flow.
- **Immutable spans, mutable builders** — `ExecutionSpan` is frozen for safe
  sharing across threads and inclusion in result objects. `SpanBuilder`
  provides a natural accumulation API during execution. The `finish()` method
  enforces the transition from mutable to immutable.
- **Two views of the same data** — The trace tree (`LoomTrace` → `WeaveTrace`
  → `ThreadTrace`) provides hierarchical navigation for programmatic access.
  The `to_spans()` method provides a flat list for serialization and export.

## Further reading

- [Observability](observability.md) — User-facing guide to logging, spans,
  and telemetry access
- [Implement Custom Telemetry Sink](../how-to/implement-custom-telemetry-sink.md)
  — Export telemetry to Delta or external systems
