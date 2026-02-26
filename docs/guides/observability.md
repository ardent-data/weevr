# Observability

weevr provides structured telemetry at every level of execution. This guide
covers logging, execution spans, trace hierarchies, and how to route
telemetry data to external systems.

## Structured JSON logging

All log output from weevr is emitted as structured JSON through Python's
standard `logging` module. The `StructuredJsonFormatter` formats each log
record as a single-line JSON object with OTel-compatible field names:

```json
{
  "timestamp": "2025-06-15T14:30:22.451000+00:00",
  "level": "INFO",
  "logger": "weevr.engine.executor",
  "message": "Thread 'stg_customers' completed: 1204 rows written",
  "thread_name": "stg_customers",
  "weave_name": "staging",
  "trace_id": "a1b2c3d4e5f67890a1b2c3d4e5f67890",
  "span_id": "f1e2d3c4b5a67890"
}
```

Each entry includes:

- **timestamp** -- ISO 8601 UTC timestamp
- **level** -- Python log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
- **logger** -- Dotted module path that emitted the record
- **message** -- Human-readable description
- **thread_name**, **weave_name**, **loom_name** -- Execution context
  (included when available)
- **trace_id**, **span_id** -- Correlation IDs linking the log entry to
  its execution span
- **attributes** -- Optional key-value map with additional context

Because the output is JSON, it integrates directly with log aggregation
systems (Azure Monitor, Elasticsearch, Splunk) without custom parsing.

## Log levels

weevr defines four log levels that map to Python's built-in levels:

| weevr level  | Python level | What it includes |
|--------------|-------------|------------------|
| `minimal`    | `WARNING`   | Warnings and errors only |
| `standard`   | `INFO`      | Execution milestones, row counts, timing |
| `verbose`    | `DEBUG`     | Step-level detail, cache events, DAG decisions |
| `debug`      | `DEBUG`     | Full diagnostic output including config dumps |

The default is `standard`, which provides enough information to confirm
execution success and diagnose common issues without flooding the output.

### Configuring log level

Set the log level when constructing a `Context`:

```python
from weevr import Context

ctx = Context(spark, log_level="verbose")
result = ctx.run("looms/nightly.yaml")
```

You can also set it in YAML execution settings on a loom or weave, and it
cascades through configuration inheritance:

```yaml
# looms/nightly.yaml
defaults:
  execution:
    log_level: standard
```

The `Context` constructor value takes precedence over YAML settings.

## Execution spans

weevr uses an OTel-compatible span model to capture timing, status, and
metrics for every unit of work. Spans form a hierarchy that mirrors the
config structure.

### Span model

Each span is an `ExecutionSpan` with the following fields:

| Field            | Description |
|------------------|-------------|
| `trace_id`       | 32-character hex string identifying the execution trace |
| `span_id`        | 16-character hex string identifying this span |
| `parent_span_id` | Parent span's ID, or `None` for the root span |
| `name`           | Human-readable label (e.g., `"thread:stg_customers"`) |
| `status`         | `OK`, `ERROR`, or `UNSET` |
| `start_time`     | UTC timestamp when the span started |
| `end_time`       | UTC timestamp when the span ended |
| `attributes`     | Key-value metrics (row counts, durations, etc.) |
| `events`         | Timestamped events within the span |

Trace IDs and span IDs follow OTel conventions (hex-encoded random bytes),
making them compatible with tracing backends that accept OTel data.

### Span events

Events mark significant moments within a span. Each `SpanEvent` has a
`name`, `timestamp`, and optional `attributes` dict. Common events include:

- `sources_read` -- All source DataFrames loaded
- `pipeline_complete` -- Transform steps finished
- `validation_complete` -- Pre-write validation done
- `write_complete` -- Target write finished
- `watermark_persisted` -- High-water mark saved

### Span hierarchy

A loom execution produces a three-level span tree:

```text
loom:nightly                        ← root span
  ├── weave:dimensions              ← child of loom
  │   ├── thread:dim_customer       ← child of weave
  │   ├── thread:dim_product        ← child of weave
  │   └── thread:dim_store          ← child of weave
  └── weave:facts
      ├── thread:fact_orders        ← child of weave
      └── thread:fact_returns       ← child of weave
```

The `parent_span_id` on each span links it to its parent, forming a
navigable tree. The `trace_id` is shared across all spans in a single
execution.

### Building spans

weevr uses `SpanCollector` and `SpanBuilder` to construct spans during
execution:

1. A `SpanCollector` is created for the execution scope and holds the
   shared `trace_id`.
2. `collector.start_span("thread:dim_customer", parent_span_id=...)`
   returns a mutable `SpanBuilder`.
3. The builder accumulates attributes and events as the work progresses.
4. `builder.finish(SpanStatus.OK)` produces an immutable `ExecutionSpan`
   that is added to the collector.
5. Thread-level collectors merge into the weave-level collector after
   thread completion.

This design avoids contention during concurrent thread execution -- each
thread operates on its own builder, and merging happens only after the
thread finishes.

## Trace hierarchy and serialization

The `telemetry.trace` module provides tree-shaped trace models that wrap
spans with their child traces:

- `LoomTrace` -- Root of the tree, containing `WeaveTrace` entries
- `WeaveTrace` -- Contains `ThreadTrace` entries
- `ThreadTrace` -- Leaf node wrapping a single thread span and its
  telemetry data

Each trace type exposes a `to_spans()` method that recursively flattens
the tree into a list of `ExecutionSpan` objects, suitable for export to
any OTel-compatible backend:

```python
result = ctx.run("looms/nightly.yaml")

# Navigate the tree
for weave_name, weave_trace in result.detail.trace.weaves.items():
    for thread_name, thread_trace in weave_trace.threads.items():
        span = thread_trace.span
        print(f"{thread_name}: {span.status} in {span.duration_ms}ms")

# Or flatten for export
all_spans = result.detail.trace.to_spans()
```

## Telemetry results

Every `ctx.run()` call in execute mode returns a `RunResult` with a
`telemetry` attribute containing structured telemetry data. The type
depends on what was executed:

| Config type | Telemetry type     | Contents |
|-------------|--------------------|----------|
| Thread      | `ThreadTelemetry`  | Span, validation/assertion results, row counts, watermark state |
| Weave       | `WeaveTelemetry`   | Span, per-thread telemetry keyed by thread name |
| Loom        | `LoomTelemetry`    | Span, per-weave telemetry keyed by weave name |

### Thread telemetry fields

`ThreadTelemetry` is the most detailed telemetry object. It includes:

- **span** -- The thread's execution span
- **rows_read**, **rows_written**, **rows_quarantined** -- Row counts
- **validation_results** -- Per-rule pass/fail counts and severity
- **assertion_results** -- Post-write assertion outcomes
- **load_mode** -- The load mode used (full, incremental, CDC)
- **watermark_column**, **watermark_previous_value**,
  **watermark_new_value** -- Watermark state for incremental loads
- **cdc_inserts**, **cdc_updates**, **cdc_deletes** -- CDC operation
  counts

### Accessing telemetry

```python
result = ctx.run("looms/nightly.yaml")
telemetry = result.telemetry

for weave_name, weave_telem in telemetry.weave_telemetry.items():
    print(f"Weave: {weave_name}")
    print(f"  Duration: {weave_telem.span.end_time - weave_telem.span.start_time}")

    for thread_name, t in weave_telem.thread_telemetry.items():
        print(f"  Thread: {thread_name}")
        print(f"    Read:        {t.rows_read}")
        print(f"    Written:     {t.rows_written}")
        print(f"    Quarantined: {t.rows_quarantined}")
```

## Progress reporting

During execution, weevr emits structured log events at key milestones.
The events you can expect in a typical loom execution include:

| Phase              | Events emitted |
|--------------------|----------------|
| Config loading     | Config parsed, variables resolved, references loaded |
| DAG planning       | Dependency graph built, execution order determined |
| Weave start        | Weave execution started, thread count logged |
| Thread start       | Sources reading, pipeline steps executing |
| Validation         | Per-rule pass/fail counts, quarantine row counts |
| Write              | Write mode, target path, rows written |
| Watermark          | Previous value, new value, persistence status |
| Cache              | Persist/unpersist events for cached DataFrames |
| Thread complete    | Final row counts, duration, status |
| Weave complete     | Aggregate status, thread success/failure counts |
| Loom complete      | Overall status, total duration, summary |

At `standard` log level, you see the start/complete events with row counts
and timing. At `verbose`, you see every step-level event. At `minimal`, you
see only warnings and errors.

## Custom logging handlers

weevr logs through a standard Python logger named `"weevr"`. To route log
output to an external system, attach a custom handler:

```python
import logging

weevr_logger = logging.getLogger("weevr")

# Example: send logs to a file
file_handler = logging.FileHandler("/tmp/weevr-execution.log")
weevr_logger.addHandler(file_handler)

# Example: send logs to Azure Monitor, Datadog, etc.
# weevr_logger.addHandler(your_custom_handler)
```

Because weevr uses `StructuredJsonFormatter`, each record is already a JSON
string. Custom handlers can parse the JSON for routing, filtering, or
enrichment.

## Custom telemetry sinks

For more advanced telemetry export -- writing execution metrics to a Delta
table, pushing spans to a tracing backend, or feeding a monitoring
dashboard -- see the
[Implement a Custom Telemetry Sink](../how-to/implement-custom-telemetry-sink.md)
how-to guide. That guide walks through extracting telemetry from a
`RunResult`, flattening the hierarchy into rows, and writing to Delta.

## Debugging a failing pipeline

When a thread fails, use this progression:

1. **Check `result.status` and `result.summary()`** -- Quick overview of
   what failed.
2. **Set `log_level="verbose"`** -- Rerun with verbose logging to see
   step-by-step execution detail.
3. **Inspect telemetry spans** -- Look at the failing thread's span for
   status, attributes, and events. The span's `attributes` dict often
   contains the error message and the step where failure occurred.
4. **Check validation results** -- If rows were quarantined, review the
   `validation_results` on the thread's telemetry to see which rules
   failed and how many rows were affected.
5. **Use preview mode** -- Run with `mode="preview"` to execute transforms
   against sampled data without writing, isolating whether the issue is
   in the transform logic or the write step.

```python
ctx = Context(spark, log_level="verbose")
result = ctx.run("looms/nightly.yaml")

if result.status != "success":
    print(result.summary())
    # Drill into the failing thread's telemetry
    for weave_name, wt in result.telemetry.weave_telemetry.items():
        for thread_name, tt in wt.thread_telemetry.items():
            if tt.span.status.value == "ERROR":
                print(f"Failed: {thread_name}")
                print(f"  Events: {tt.span.events}")
```

## Next steps

- [Implement Custom Telemetry Sink](../how-to/implement-custom-telemetry-sink.md)
  -- Export telemetry to Delta or external systems
- [Execution Modes](../concepts/execution-modes.md) -- Validate and preview
  without executing
- [Fabric Runtime](fabric-runtime.md) -- Environment setup and
  configuration
