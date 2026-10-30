# Schema Drift

Schema drift occurs when the pipeline output no longer matches the
target: upstream changes add columns, or columns the target has stop
arriving. weevr detects drift and applies configurable handling at
write time.

## What drift detects

Drift detection compares the pipeline output columns against a
baseline, after target column mapping and before the write:

- **Extra columns** — present in the output but not in the baseline.
- **Missing columns** — present in the existing table but not in the
  output. Detected only when the existing table is the baseline; with
  a warp, columns the output lacks are warp-only columns that weevr
  appends with their defaults, and
  [warp enforcement](warps.md#enforcement) reports them.

Engine-managed columns never count as drift: audit columns, SCD and
`_weevr_` columns, soft-delete columns, and CDC operation and
change-feed metadata columns.

### Baseline sources

| Condition | Baseline |
|-----------|----------|
| Warp exists | Warp column names |
| No warp, table exists | Existing Delta table schema |
| No warp, no table | No baseline (drift skipped) |

## Drift modes

The `schema_drift` setting controls the strategy. The `on_drift`
setting controls the severity for strict mode.

### Behavior matrix

| `schema_drift` | `on_drift` | Extra columns | Missing columns |
|-----------------|-----------|---------------|-----------------|
| `lenient` | (ignored) | Reported; not written to an existing table | Reported; written as null |
| `strict` | `error` | Thread fails | Thread fails |
| `strict` | `warn` | Dropped, warning logged | Written as null, warning logged |
| `strict` | `ignore` | Dropped silently | Written as null silently |
| `adaptive` | (ignored) | Added to the Delta table | Reported; written as null |

### lenient (default)

Drift is reported for visibility — in the drift report, telemetry, and
logs — and the write proceeds as configured. With the default
`mapping_mode: auto`, an existing table keeps its columns, so a new
output column is not written until you add it to the table or switch
to `adaptive`.

### strict

Extra and missing columns are handled per `on_drift`:

- **error** — the thread fails with a `SchemaDriftError` naming the
  columns; nothing is written
- **warn** — extra columns are dropped and missing columns are
  written as null, with a logged warning
- **ignore** — the same, silently

### adaptive

New columns are added to the Delta table on append, overwrite, merge,
CDC merge, and dimension merge. Evolution is additive only — weevr
never drops columns or changes column types — and is scoped to the
thread's own write; no session configuration changes. Missing columns
are reported and written as null. When combined with `warp_mode: auto`,
the auto-generated warp file is updated with the new columns marked
`discovered: true`.

## Configuration

Set drift mode on the target or cascade from defaults:

```yaml
target:
  alias: gold.dim_customer
  schema_drift: strict
  on_drift: warn
```

### Cascade via defaults.target

All three drift/warp keys cascade through
`defaults.target.*` at loom, weave, and thread levels:

```yaml
# Loom-level default
defaults:
  target:
    schema_drift: strict
    on_drift: warn
    warp_enforcement: warn
```

Thread-level settings override weave, which overrides loom.

## Drift with warps

When a warp is present, the warp column list is the baseline.
This provides a stricter contract than the existing table schema
because the warp reflects intentional declarations.

## Drift without warps

When no warp exists, the existing Delta table schema is used as
the baseline. This provides value even for threads that have not
adopted warps — any new or missing column from source schema changes
is detected at the target boundary.

On first write (no table exists yet), there is no baseline, so
drift detection is skipped.

## Adaptive + auto interaction

When `schema_drift: adaptive` and `warp_mode: auto` are both
active:

1. New columns are added to the Delta table (adaptive behavior)
2. The auto-generated warp file includes the new columns
3. New columns are marked `discovered: true` in the warp

This creates a living schema document that tracks actual output
evolution. Removing the `discovered` flag from a column signals
intentional curation.

## Observability

Drift events are captured regardless of mode:

- **Telemetry** — `drift_detected`, `drift_columns`,
  `drift_missing_columns`, `drift_mode`, and `drift_action_taken`
  fields on the `ThreadTelemetry` object
- **Drift report** — `ThreadResult.drift_report` lists
  `extra_columns`, `missing_columns`, and the action taken
- **Notebook output** — drift report section in the
  `_repr_html_()` display with extra columns and action taken
- **Logging** — drift findings logged at the appropriate level

## Configuration reference

| Key | Level | Default | Description |
|-----|-------|---------|-------------|
| `target.schema_drift` | Cascadable | `lenient` | Drift mode |
| `target.on_drift` | Cascadable | `warn` | Strict severity |
