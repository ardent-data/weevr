# Implement a Custom Telemetry Sink

**Goal:** Access structured telemetry from a `RunResult` and export it to an
external system such as a Delta table, a monitoring dashboard, or a logging
service.

## Prerequisites

- A working weevr pipeline that produces a `RunResult`
- Target system credentials or write access (e.g., a Lakehouse Delta table)

## Telemetry overview

Every `ctx.run()` call returns a `RunResult` with a `telemetry` attribute. The
telemetry object is structured hierarchically to match the config type that was
executed:

| Config type | Telemetry type | Contents |
|---|---|---|
| Thread | `ThreadTelemetry` | Execution span, validation results, assertion results, row counts, watermark state |
| Weave | `WeaveTelemetry` | Execution span, per-thread telemetry keyed by thread name |
| Loom | `LoomTelemetry` | Execution span, per-weave telemetry keyed by weave name |

## Step 1 -- Extract telemetry from a RunResult

```python
from weevr import Context

ctx = Context(spark, "my-project.weevr")
result = ctx.run("daily.loom")

telemetry = result.telemetry
if telemetry is None:
    print("No telemetry available (non-execute mode)")
```

For a loom execution, drill into per-weave and per-thread telemetry:

```python
for weave_name, weave_telem in telemetry.weave_telemetry.items():
    print(f"Weave: {weave_name}, duration: {weave_telem.span.duration_ms}ms")

    for thread_name, thread_telem in weave_telem.thread_telemetry.items():
        print(f"  Thread: {thread_name}")
        print(f"    Rows read:    {thread_telem.rows_read}")
        print(f"    Rows written: {thread_telem.rows_written}")
        print(f"    Quarantined:  {thread_telem.rows_quarantined}")
```

## Step 2 -- Write telemetry to a Delta table

Flatten the telemetry hierarchy into rows and write to a dedicated telemetry
table:

```python
from datetime import datetime, UTC

rows = []
for weave_name, weave_telem in telemetry.weave_telemetry.items():
    for thread_name, t in weave_telem.thread_telemetry.items():
        rows.append({
            "run_timestamp": datetime.now(UTC).isoformat(),
            "loom": result.config_name,
            "weave": weave_name,
            "thread": thread_name,
            "status": result.status,
            "rows_read": t.rows_read,
            "rows_written": t.rows_written,
            "rows_quarantined": t.rows_quarantined,
            "duration_ms": t.span.duration_ms,
            "load_mode": t.load_mode,
        })

df = spark.createDataFrame(rows)
df.write.format("delta").mode("append").save("Tables/weevr_telemetry")
```

## Step 3 -- Configure structured logging

weevr emits structured JSON log lines during execution. The log level is
controlled via the `Context` constructor:

```python
ctx = Context(spark, "my-project.weevr", log_level="verbose")
```

Available levels: `minimal`, `standard` (default), `verbose`, `debug`. Each log
entry is a JSON object with fields including `timestamp`, `level`, `logger`,
`message`, and optional context attributes (`thread_name`, `weave_name`,
`trace_id`).

To route these logs to an external system, attach a custom Python logging
handler to the `weevr` logger:

```python
import logging

weevr_logger = logging.getLogger("weevr")
weevr_logger.addHandler(your_custom_handler)
```

## Step 4 -- Export validation and assertion details

Validation and assertion results are available per-thread:

```python
for vr in thread_telem.validation_results:
    print(f"Rule: {vr.rule_name}, passed: {vr.rows_passed}, failed: {vr.rows_failed}")

for ar in thread_telem.assertion_results:
    print(f"Assertion: {ar.assertion_type}, passed: {ar.passed}, details: {ar.details}")
```

These objects are Pydantic models, so you can serialize them to JSON with
`vr.model_dump_json()` for ingestion into monitoring tools or alerting systems.
