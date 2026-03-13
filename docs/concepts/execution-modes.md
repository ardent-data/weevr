# Execution Modes

weevr has two distinct concepts that share the word "mode":

- **Run modes** — the `mode` argument to `ctx.run()`, which controls *what
  the engine does* (execute, validate, plan, preview).
- **Data modes** — the `load` and `write` blocks on a thread, which control
  *how data is read* and *how data is written*.

This page covers both.

## Run modes

The `mode` argument to `Context.run()` controls how far the engine
progresses through the plan → execute → aggregate pipeline.

| Mode | What happens | Result contains |
|------|-------------|-----------------|
| `execute` | Full execution: read, transform, write (default) | Status, row counts, telemetry |
| `validate` | Parse config, resolve references, check DAG — no data touched | `validation_errors` list |
| `plan` | Build the execution plan and return it without running | `execution_plan` list, `summary()` with cache markers, `explain()` for detailed breakdown |
| `preview` | Run transforms against sampled data, skip writes | `preview_data` DataFrames |

```python
# Validate config without touching data
result = ctx.run("nightly.loom", mode="validate")
print(result.validation_errors)

# Inspect the execution plan
result = ctx.run("nightly.loom", mode="plan")
print(result.summary())     # compact: execution groups with cache markers
print(result.explain())     # detailed: dependencies, cache targets, thread detail

# Preview transforms without writing
result = ctx.run("nightly.loom", mode="preview")
```

In notebooks, results render automatically as styled HTML when you
evaluate the result in a cell. Each mode gets a tailored report:

- **execute** — executive summary, per-thread detail with flow diagrams
  and data waterfalls, execution timeline, and annotated DAG
- **validate** — check/error report with color-coded status
- **plan** — summary table with embedded DAG diagram
- **preview** — output shape table (columns × rows per thread)

For plan mode, you can also retrieve the DAG diagram directly:

```python
result = ctx.run("nightly.loom", mode="plan")
dag = result.dag()           # single-weave DAG or loom-level swimlane
dag.save("plan.svg")         # export to file
dag                          # renders inline in a notebook
```

For per-weave access in a loom, use `result.execution_plan[0].dag()`.
Note that `result.dag()` includes full resolved-thread context (sources,
targets, step counts) while the per-plan accessor does not.

## Data modes

weevr separates *how data is read* from *how data is written*. The load
mode controls source reading. The write mode controls target writing.

## Write modes

The `write` block on a thread controls how shaped data reaches the target
Delta table.

### Overwrite

Replaces the entire target table with the new data.

```yaml
write:
  mode: overwrite
```

Use overwrite for full refreshes, snapshots, and targets where you always
want the complete current state. This is the default write mode.

Overwrite is naturally idempotent -- rerunning the same config with the same
source data produces the same target state.

### Append

Inserts new rows into the target without modifying existing rows.

```yaml
write:
  mode: append
```

Use append for event logs, audit trails, and any target that accumulates
rows over time.

!!! warning "Append is not idempotent"

    Rerunning an append produces duplicate rows. Pair append with an
    incremental load mode (watermark or parameter) to prevent reprocessing
    the same source data. See [Idempotency](idempotency.md) for details.

### Merge

Performs an upsert: matching rows are updated, unmatched source rows are
inserted, and unmatched target rows are optionally deleted.

```yaml
write:
  mode: merge
  match_keys: [customer_id, source_system]
  on_match: update
  on_no_match_target: insert
  on_no_match_source: ignore
```

Merge requires `match_keys` -- the columns used to match source rows to
target rows. The behavior for each match outcome is independently
configurable:

| Parameter | Options | Default |
|---|---|---|
| `on_match` | `update`, `ignore` | `update` |
| `on_no_match_target` | `insert`, `ignore` | `insert` |
| `on_no_match_source` | `delete`, `soft_delete`, `ignore` | `ignore` |

For soft deletes, specify the marker column and value:

```yaml
write:
  mode: merge
  match_keys: [customer_id]
  on_no_match_source: soft_delete
  soft_delete_column: is_deleted
  soft_delete_value: "true"
```

Merge is idempotent by match key -- rerunning with the same data produces
the same target state.

!!! tip "SCD patterns"

    SCD Type 1 and SCD Type 2 are not separate write modes. They are
    delivered as reusable stitches that compose on top of the core merge
    mode. See the [YAML Schema Reference](../reference/yaml-schema/thread.md)
    for stitch usage.

## Load modes

```d2
direction: right

run1: Run 1 (first load) {
  source: Source\nall rows
  filter: "watermark: none\n(read everything)"
  target: Target\nfull dataset
  wm: "watermark = 2024-04-05"

  source -> filter -> target
  target -> wm
}

run2: Run 2 (incremental) {
  source: Source\nnew + changed
  filter: "watermark > 2024-04-05"
  target: Target\nmerged result
  wm: "watermark = 2024-05-10"

  source -> filter -> target
  target -> wm
}

run3: Run 3 (incremental) {
  source: Source\nnew + changed
  filter: "watermark > 2024-05-10"
  target: Target\nmerged result
  wm: "watermark = 2024-06-01"

  source -> filter -> target
  target -> wm
}

run1 -> run2: next execution
run2 -> run3: next execution
```

The `load` block on a thread controls how source data is bounded on each
execution.

### Full

Reads all source data on every run.

```yaml
load:
  mode: full
```

This is the default. Pair with `write.mode: overwrite` for a complete
refresh, or with `write.mode: merge` for a full comparison merge.

### Incremental watermark

Reads only source rows that have changed since the last successful run.
weevr persists a high-water mark and filters subsequent reads automatically.

```yaml
load:
  mode: incremental_watermark
  watermark_column: modified_date
  watermark_type: timestamp
```

On the first run, all rows are read (no prior watermark exists). On
subsequent runs, only rows where `modified_date` exceeds the stored
watermark are read.

The watermark is persisted in a configurable state store -- either as a
Delta table property on the target or in a dedicated metadata table.

### Incremental parameter

Incremental boundaries are passed as runtime parameters. weevr does not
manage state -- the caller is responsible for providing the correct range.

```yaml
load:
  mode: incremental_parameter

params:
  start_date:
    type: date
    required: true
  end_date:
    type: date
    required: true
```

Use this mode when the orchestration layer (Fabric pipeline, Airflow)
controls the processing window.

### CDC

The thread understands change data capture patterns. Source rows carry
operation flags (insert, update, delete) that weevr applies as merge
operations on the target.

```yaml
load:
  mode: cdc
  cdc:
    preset: delta_cdf
```

CDC mode supports two configuration styles:

- **Preset** -- Use `delta_cdf` to auto-configure for Delta Change Data
  Feed conventions.
- **Explicit** -- Declare the operation column and flag values directly.

```yaml
load:
  mode: cdc
  cdc:
    operation_column: change_type
    insert_value: "I"
    update_value: "U"
    delete_value: "D"
```

## Choosing the right combination

| Scenario | Load mode | Write mode |
|---|---|---|
| Full snapshot refresh | `full` | `overwrite` |
| Accumulating event log | `incremental_watermark` | `append` |
| Dimension table with updates | `full` or `incremental_watermark` | `merge` |
| CDC from upstream system | `cdc` | `merge` |
| Externally bounded batch | `incremental_parameter` | `append` or `merge` |

## Next steps

- [Idempotency](idempotency.md) -- Understand rerun safety per mode
- [Artifacts Model](artifacts-model.md) -- How targets map to Delta tables
- [YAML Schema: Thread](../reference/yaml-schema/thread.md) -- Full write
  and load configuration reference
