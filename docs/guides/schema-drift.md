# Schema Drift

Schema drift occurs when extra columns from upstream source
changes flow through the pipeline to the target boundary. weevr
detects drift and applies configurable handling at write time.

## What drift detects

Drift detection compares the pipeline output columns against a
baseline. Extra columns (present in the output but not in the
baseline) are drift. Missing columns are a separate concern
handled by warp enforcement.

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

| `schema_drift` | `on_drift` | Extra columns | Result |
|-----------------|-----------|---------------|--------|
| `lenient` | (ignored) | Pass through | No change |
| `strict` | `error` | Detected | Thread fails |
| `strict` | `warn` | Dropped | Warning logged |
| `strict` | `ignore` | Dropped | Silent |
| `adaptive` | (ignored) | Pass through | Evolve warp |

### lenient (default)

Extra columns pass through to the target unchanged. A drift
report is populated for visibility but no action is taken.

### strict

Extra columns are detected and handled per `on_drift`:

- **error** — the thread fails with a `SchemaDriftError`
- **warn** — extra columns are dropped with a logged warning
- **ignore** — extra columns are dropped silently

### adaptive

Extra columns pass through to the target. When combined with
`warp_mode: auto`, the auto-generated warp file is updated
with the new columns marked `discovered: true`.

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
adopted warps — any new column from source schema changes is
detected at the target boundary.

On first write (no table exists yet), there is no baseline, so
drift detection is skipped.

## Adaptive + auto interaction

When `schema_drift: adaptive` and `warp_mode: auto` are both
active:

1. Extra columns pass through to Delta (adaptive behavior)
2. The auto-generated warp file includes the new columns
3. New columns are marked `discovered: true` in the warp

This creates a living schema document that tracks actual output
evolution. Removing the `discovered` flag from a column signals
intentional curation.

## Observability

Drift events are captured regardless of mode:

- **Telemetry** — `drift_detected`, `drift_columns`,
  `drift_mode`, and `drift_action_taken` attributes on the
  thread execution span
- **Notebook output** — drift report section in the
  `_repr_html_()` display with extra columns and action taken
- **Logging** — drift findings logged at the appropriate level

## Configuration reference

| Key | Level | Default | Description |
|-----|-------|---------|-------------|
| `target.schema_drift` | Cascadable | `lenient` | Drift mode |
| `target.on_drift` | Cascadable | `warn` | Strict severity |
