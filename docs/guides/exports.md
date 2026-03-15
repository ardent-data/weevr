# Exports (Secondary Outputs)

Exports let a thread write its output data to additional locations beyond the primary Delta target. This avoids duplicating thread pipelines for common patterns like Parquet archiving, CSV extracts for non-Spark systems, or compliance copies.

## Overview

Exports write the **same DataFrame** that goes to the primary target — post-mapping and with audit columns applied. They are declared in the `exports:` key at the thread, weave, or loom level and execute sequentially after the primary write, watermark persistence, and post-write assertions.

## Configuration

```yaml
exports:
  - name: parquet_archive
    description: "Daily archive for external consumers"
    type: parquet
    path: /lakehouse/archive/${thread.name}/${run.timestamp}/
    partition_by: [region, date]
    on_failure: warn
    options:
      compression: snappy

  - name: compliance_copy
    type: delta
    alias: compliance.orders_archive
    on_failure: abort
```

### Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | Yes | — | Unique identifier (valid Python identifier). |
| `type` | Yes | — | Output format: `delta`, `parquet`, `csv`, `json`, `orc`. |
| `path` | Conditional | — | OneLake path. Required for non-delta types. Supports context variables. |
| `alias` | Conditional | — | Metastore alias. Delta type only, mutually exclusive with `path`. |
| `description` | No | — | Human-readable label shown in `explain()` output. |
| `mode` | No | `overwrite` | Write mode. Only `overwrite` in v1. |
| `partition_by` | No | — | Partition columns, independent of primary target. |
| `on_failure` | No | `warn` | `abort` fails the thread; `warn` logs and continues. |
| `enabled` | No | `true` | Set to `false` to suppress an inherited export. |
| `options` | No | — | Format-specific Spark DataFrameWriter options. |

## Format Notes

- **Delta**: supports both `path` and `alias`. Auto-creates tables on first write.
- **Parquet**: use `options.compression` for compression codec (snappy, gzip, etc.).
- **CSV**: common options include `header`, `delimiter`, `quote`, `escape`.
- **JSON**: writes one JSON object per line by default.
- **ORC**: Hive-compatible columnar format.

Complex types (arrays, maps, structs) may not be supported by flat-file formats (CSV, JSON). If a format cannot serialize the schema, the export fails and follows the `on_failure` behavior.

## Dynamic Path Naming

Export paths support context variables for archive-style outputs with unique-per-run directories:

- `${run.timestamp}` — ISO 8601 UTC timestamp of execution start
- `${run.id}` — UUID4 unique per execution
- `${thread.name}`, `${weave.name}`, `${loom.name}` — config hierarchy names

```yaml
path: /archive/${loom.name}/${thread.name}/${run.timestamp}/
```

These variables are resolved at execution time, not at config load time.

## Cascade Inheritance

Exports cascade additively through loom → weave → thread:

- Each level can define exports; all are collected by name.
- Same-named exports at a lower level override the higher-level definition.
- `enabled: false` at any level suppresses an inherited export.

```yaml
# loom defaults — applies to all threads
defaults:
  exports:
    - name: audit_archive
      type: parquet
      path: /archive/${thread.name}/${run.timestamp}/

# thread level — suppress the inherited archive
exports:
  - name: audit_archive
    enabled: false
```

## Error Handling

| `on_failure` | Behavior |
|-------------|----------|
| `warn` (default) | Log warning, record error in telemetry, continue to next export. Thread status: success. |
| `abort` | Record error, raise `ExportError`. Thread status: failure. Remaining exports skipped. |

Exports only run after a successful primary write. If the primary write fails, no exports execute.

## Observability

- **explain()**: lists exports with name, type, and target path before execution.
- **summary()**: shows per-export results with row count and status.
- **Flow SVG**: export nodes appear as destinations fanning out from the preparation stage.
- **Sankey waterfall**: export bands shown alongside the target band.
- **Telemetry**: each export produces an `ExportResult` on `ThreadTelemetry`.
