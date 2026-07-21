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
  "weevr_thread": "stg_customers",
  "weevr_weave": "staging",
  "trace_id": "a1b2c3d4e5f67890a1b2c3d4e5f67890",
  "span_id": "f1e2d3c4b5a67890"
}
```

Each entry includes:

- **timestamp** -- ISO 8601 UTC timestamp
- **level** -- Python log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
- **logger** -- Dotted module path that emitted the record
- **message** -- Human-readable description
- **weevr_thread**, **weevr_weave**, **weevr_loom** -- Execution context
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

ctx = Context(spark, "my-project.weevr", log_level="verbose")
result = ctx.run("nightly.loom")
```

You can also set it in the top-level `execution:` block of a loom or
weave:

```yaml
# nightly.loom
execution:
  log_level: standard
```

The `Context` constructor value takes precedence over YAML settings.
Loom and weave blocks merge field-level; `defaults.execution` lands at
thread scope, where execution settings are not applied. See
[Execution Settings](execution-settings.md) for the full precedence and
merge rules.

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

```d2
direction: down

loom: loom:nightly {
  style.font-size: 18

  weave_dim: weave:dimensions {
    t1: thread:dim_customer
    t2: thread:dim_product
    t3: thread:dim_store
  }

  weave_facts: weave:facts {
    t4: thread:fact_orders
    t5: thread:fact_returns
  }

  weave_dim -> weave_facts: sequential
}

legend: {
  style.fill: transparent
  style.stroke: transparent

  note: |md
    Each span carries **trace_id**, **span_id**, **status**,
    **start_time**, **end_time**, and **attributes**.
  |
}
```

Step-level work (read, transform, validate, write, assert) is not
modeled as child spans — that detail lives in the thread span's
attributes and events, and in `step_stats` on `ThreadTelemetry`.
Hooks, lookups, column sets, and sub-pipeline CTEs do get their own
spans, parented to the scope that ran them.

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

Telemetry forms a tree that mirrors the execution hierarchy — the same
objects returned on `RunResult.telemetry`:

- `LoomTelemetry` -- Root of the tree, holding per-weave
  `WeaveTelemetry` entries keyed by weave name
- `WeaveTelemetry` -- Holds per-thread `ThreadTelemetry` entries keyed
  by thread name
- `ThreadTelemetry` -- Leaf carrying the thread's span plus its full
  metrics (row counts, validation results, watermark state)

Every level carries its own `ExecutionSpan`. Walking the tree gives
hierarchical access; collecting the spans level by level yields a flat
list suitable for export to any OTel-compatible backend:

```python
result = ctx.run("nightly.loom")

# Navigate the tree
for weave_name, wt in result.telemetry.weave_telemetry.items():
    for thread_name, tt in wt.thread_telemetry.items():
        span = tt.span
        elapsed = (span.end_time - span.start_time).total_seconds()
        print(f"{thread_name}: {span.status} in {elapsed:.1f}s")

# Flatten the spans for export
spans = [result.telemetry.span]
for wt in result.telemetry.weave_telemetry.values():
    spans.append(wt.span)
    spans.extend(tt.span for tt in wt.thread_telemetry.values())
```

See the [custom telemetry sink how-to](../how-to/implement-custom-telemetry-sink.md)
for a complete export example.

## Telemetry results

```d2
direction: down

run: ctx.run("nightly.loom") {style.fill: "#E3F2FD"}

result: RunResult {
  style.fill: "#E8F5E9"

  telemetry: LoomTelemetry {
    style.fill: "#C8E6C9"

    loom_span: loom span\ntrace_id, start/end time, status

    weave_dim: WeaveTelemetry — dimensions {
      style.fill: "#FFF3E0"
      weave_span: weave span

      t1: ThreadTelemetry\ndim_customer {
        style.fill: "#FFE0B2"
        rows: "rows_read: 5000\nrows_written: 4800\nrows_quarantined: 12"
      }
      t2: ThreadTelemetry\ndim_product {style.fill: "#FFE0B2"}
    }

    weave_facts: WeaveTelemetry — facts {
      style.fill: "#F3E5F5"
      t3: ThreadTelemetry\nfact_orders {style.fill: "#E1BEE7"}
      t4: ThreadTelemetry\nfact_returns {style.fill: "#E1BEE7"}
    }
  }
}

run -> result
```

Every `ctx.run()` call in execute mode returns a `RunResult` with a
`telemetry` attribute containing structured telemetry data. The type
depends on what was executed:

| Config type | Telemetry type     | Contents |
|-------------|--------------------|----------|
| Thread      | `ThreadTelemetry`  | Span, validation/assertion results, row counts, watermark state |
| Weave       | `WeaveTelemetry`   | Span, per-thread telemetry, column set results, lookup results |
| Loom        | `LoomTelemetry`    | Span, per-weave telemetry keyed by weave name |

### Thread telemetry fields

`ThreadTelemetry` is the most detailed telemetry object. It includes:

- **span** -- The thread's execution span
- **rows_read**, **rows_written**, **rows_quarantined** -- Row counts.
  On single-pass write paths, `rows_read` and `rows_written` are
  computed as byproducts of the write itself (query observations and
  Delta commit metrics) rather than by separate Spark jobs; a
  quarantine count runs only when validation actually quarantined
  rows, against the cached quarantine frame. Single-pass commits carry
  a [lineage stamp](#commit-lineage-stamps), so the engine finds its
  own commit exactly even when concurrent writers interleave. In the
  rare case where attribution is truly impossible (the history read
  itself fails), `rows_written` is `None` — unknown, never a false
  zero — and a warning naming the thread and target is surfaced on
  `RunResult.warnings`. Merge-based paths (dimensions, CDC, merge
  write mode) retain direct counts.
- **validation_results** -- Per-rule pass/fail counts and severity
- **assertion_results** -- Post-write assertion outcomes
- **load_mode** -- The load mode used (full, incremental, CDC)
- **watermark_column**, **watermark_previous_value**,
  **watermark_new_value** -- Watermark state for incremental loads
- **cdc_inserts**, **cdc_updates**, **cdc_deletes** -- CDC operation
  counts
- **rows_after_transforms** -- Row count after pipeline transforms,
  before validation
- **audit_columns_applied** -- Names of audit columns injected
  into the output
- **export_results** -- Per-export write results (`ExportResult`
  list with name, type, target, rows_written, duration_ms,
  status, error)
- **watermark_persisted** -- Whether the watermark was
  successfully persisted after write
- **watermark_first_run** -- Whether this is the first run with
  no prior watermark state
- **step_stats** -- Per-step pipeline statistics (resolve match
  rates, validate-mode map counts), keyed `<scope>.<step>.<name>`.
  Collected as a byproduct of the write rather than by extra Spark
  jobs, and harvested best-effort: a failed harvest logs a warning
  and leaves the field absent, never failing the thread. This field
  is engine-internal — its key set and value shapes are unstable
  and may change without notice. In preview mode the values are
  sample-scoped (they reflect the sampled rows, not the full
  source), and are present only when sample capture succeeded.
- **resolved_params** -- Runtime parameter values that drove this
  execution (populated on the outermost telemetry object)
- **warp_name**, **warp_source**, **warp_enforcement** -- Which warp
  applied (description), how it was resolved (`explicit`/`auto`),
  and the enforcement mode in force
- **drift_detected**, **drift_columns**, **drift_mode**,
  **drift_action_taken** -- Schema drift outcome for this run; see
  the [Schema Drift guide](schema-drift.md)

### Dimension merge metrics

Internally, dimension writes derive merge outcomes from the Delta
commit's split update keys — never the conflated
`numTargetRowsUpdated` aggregate, which folds soft-delete stamps into
the same number as version closes:

- `rows_versioned` — current rows closed (a new version was inserted)
- `rows_inserted` — genuinely new entities (new-version inserts are
  excluded arithmetically)
- `rows_deleted` — hard deletes plus soft-delete stamps from
  `on_no_match_source`

`rows_unchanged` is intentionally absent: no Delta metric expresses it
(`numTargetRowsCopied` is a file-rewrite artifact, not a logical
count), and deriving it arithmetically risks a wrong number under
quarantine and seeding edge cases. When merge metrics cannot be read —
or a concurrent writer's commit makes attribution unsafe — the fields
stay zero and a warning is logged. The first write of a dimension is a
plain save, not a merge: its commit carries the
[lineage stamp](#commit-lineage-stamps), and an unattributable
first-write count reports `rows_inserted` as `None` (unknown), never
zero.

Of these, only the first-write insert count currently reaches the
telemetry surface (as the thread's `rows_written`); the per-outcome
split is not yet exposed on `ThreadTelemetry`.

### Weave telemetry fields

`WeaveTelemetry` includes:

- **span** -- The weave's execution span
- **thread_telemetry** -- Per-thread telemetry keyed by thread name
- **hook_results** -- Pre/post hook step outcomes
- **lookup_results** -- Lookup materialization outcomes
- **column_set_results** -- Column set resolution outcomes
  (`ColumnSetResult` list with name, source_type,
  mappings_loaded, skipped)
- **variables** -- Final variable values
- **resolved_params** -- Runtime parameter values

### Accessing telemetry

```python
result = ctx.run("nightly.loom")
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

`rows_written` is `int | None`: `None` means the write succeeded but
its count could not be attributed (unknown). Sinks and dashboards that
aggregate counts should sum the known values and treat `None` as a
data-quality signal, not as zero.

## Commit lineage stamps

Every single-pass engine write — overwrite, append, an incremental
first write, and a dimension's first write — stamps its Delta commit
with a small JSON document in the commit's `userMetadata`, visible in
`DESCRIBE HISTORY`:

```json
{
  "engine": "weevr",
  "version": 1,
  "run_id": "5f0c9c8e-...-b2a1",
  "thread": "dim_customer",
  "loom": "nightly",
  "weave": "dimensions",
  "mode": "append"
}
```

The stamp serves two purposes:

- **Exact attribution.** After a write, the engine finds its own
  commit by its stamp rather than assuming the newest commit is its
  own — so `rows_written` stays exact no matter how many concurrent
  writers (ad-hoc appends, `OPTIMIZE`, other pipelines) interleave.
- **Lineage.** The `run_id` in the stamp is the same run identity
  carried by audit columns (`${run.id}`) and telemetry, so a table's
  Delta history joins back to the run that produced each commit.

The **stable core** of the stamp is the engine marker (`engine`),
`run_id`, and `thread` — attribution matches on exactly these, and
they are safe to build tooling against. The remaining fields (`loom`,
`weave`, `mode`, `version`) are best-effort context and may evolve;
`mode` records the thread's configured write mode, not the physical
branch taken (a first write executes as an overwrite regardless).

Two boundaries to be aware of:

- **Merge commits are not stamped.** Delta only honors per-write
  `userMetadata` on DataFrameWriter paths; merge commits (dimension
  merges, CDC routing, merge write mode) would require session-level
  configuration, which races across parallel threads. Their counts
  come from eager inputs and guarded merge metrics instead.
- **The stamp takes precedence.** If you set a session-level
  `spark.databricks.delta.commitInfo.userMetadata`, engine-stamped
  commits override it with the stamp; commits the engine does not
  stamp (merges, seeds, warp pre-init) still carry your session value.

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
ctx = Context(spark, "my-project.weevr", log_level="verbose")
result = ctx.run("nightly.loom")

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
