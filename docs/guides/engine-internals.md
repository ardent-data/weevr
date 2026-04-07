# Engine Internals

This guide explains how weevr's execution engine turns configuration into
running Spark pipelines. It covers the planning, execution, and result
aggregation subsystems.

## Introduction

The engine is responsible for a single question: given a set of configured
threads, in what order should they run, and how should each thread's pipeline
execute? The answer involves dependency analysis, parallel scheduling, caching,
conditional execution, and structured result collection.

## Key concepts

- **ExecutionPlan** — An immutable snapshot of thread ordering, dependency
  edges, parallel execution groups, cache targets, lookup dependency
  mappings (`lookup_producers`, `lookup_consumers`), and a
  `lookup_schedule` that assigns each lookup to its materialization point. Produced by the planner
  before any data is read. In notebooks, an `ExecutionPlan` renders
  automatically via `_repr_html_()`. Call `plan.dag()` to get a `DAGDiagram`
  SVG that can be saved with `dag.save("plan.svg")`. For plan mode results,
  `result.dag()` returns either a single-weave DAG or a loom-level swimlane
  diagram depending on how many weaves are present.
- **DAG** — A directed acyclic graph of thread dependencies. Dependencies are
  inferred from source/target path overlap and can also be declared explicitly.
- **Parallel execution group** — A set of threads with no mutual dependencies
  that can run concurrently on the same SparkSession.
- **Cache target** — A thread whose output is read by two or more downstream
  threads. The engine caches the DataFrame in memory to avoid re-reading from
  Delta.

## How it works

Execution follows a three-stage pipeline: **plan → execute → aggregate**.

```d2
direction: right

plan: Plan {
  build_deps: Build dependency graph
  detect: Detect cycles
  topo: Topological sort
  cache: Identify cache targets
  build_deps -> detect -> topo -> cache
}
execute: Execute {
  groups: Process parallel groups
  threads: Run threads concurrently
  persist: Manage cache lifecycle
  groups -> threads -> persist
}
aggregate: Aggregate {
  thread_res: ThreadResult
  weave_res: WeaveResult
  loom_res: LoomResult
  thread_res -> weave_res -> loom_res
}

plan -> execute: ExecutionPlan
execute -> aggregate: results + telemetry
```

### Planning

The planner builds a DAG from thread configurations:

1. **Infer dependencies** — If thread B reads from thread A's target path, B
   depends on A. This happens automatically by matching source paths/aliases
   to target paths/aliases across all threads.
2. **Merge explicit dependencies** — Weave entries can declare additional
   dependencies that are not data-based (e.g., ordering constraints).
3. **Detect cycles** — DFS with three-color marking (white/gray/black) finds
   back edges. If a cycle exists, execution fails immediately with a clear
   error showing the cycle path.
4. **Topological sort** — Kahn's algorithm produces a sequence of parallel
   groups. Threads within a group have no mutual dependencies and can run
   concurrently.
5. **Analyze cache targets** — Threads with two or more downstream dependents
   are flagged for caching, unless the thread explicitly sets `cache: false`.
   Setting `cache: true` is a no-op — it only prevents `cache: false`
   suppression but does not force caching for threads with fewer than two
   dependents.

The result is an immutable `ExecutionPlan` that the executor consumes without
modification.

### Execution

Thread execution is orchestrated at two levels:

**Loom level** (`execute_loom`) — Iterates weaves in declared order. Each weave
gets its own plan and executor pass. Weave-level conditions are evaluated before
planning; a false condition skips the entire weave. If a weave's status is
`"failure"`, the loom stops executing further weaves — remaining weaves are
skipped.

**Weave level** (`execute_weave`) — Orchestrates the full weave lifecycle:

1. **Initialize variables** — A `VariableContext` is created from the
   weave's `variables` block. Variables are available to hook steps via
   `${var.name}` placeholders.
2. **Lookup materialization (external)** — External lookups
   (those not produced by a thread in the weave) are materialized
   before any thread or hook step runs. Internal lookups whose
   source is produced by a thread in group N are deferred and
   materialized at the group N+1 boundary — after their producer
   completes.
3. **Column set materialization** — If the weave (or loom) defines
   `column_sets`, all sets are resolved to from→to mapping dicts.
   Delta/YAML sources are read via `read_source()`; param sources
   resolve from runtime parameters. Results are captured in
   `WeaveTelemetry.column_set_results`.
4. **Pre-steps** — If the weave defines `pre_steps`, hook steps
   run before any thread executes. Failures with
   `on_failure: abort` stop the weave. Pre-steps can read from
   lookups materialized in step 2.
5. **Thread execution** — Parallel groups are processed
   sequentially. Within each group, threads are submitted to a
   `ThreadPoolExecutor` for concurrent execution. Threads
   reference lookups via `source.lookup` and receive the cached
   DataFrame.
6. **Post-steps** — After all threads complete, `post_steps` hook
   steps run. Failures with `on_failure: abort` mark the weave
   as failed.

After each thread completes:

- If it is a cache target, the output DataFrame is persisted.
- Consumer reference counts are decremented; caches are unpersisted when no
  consumers remain.
- Thread-level telemetry collectors are merged into the weave collector.

**Thread level** (`execute_thread`) — A single thread runs through an 18-step
pipeline:

```d2
direction: down

setup: Setup {
  style.fill: "#ECEFF1"
  s0a: "1. Initialize thread resources\n(variables, lookups, column_sets)"
  s0b: "2. Run thread pre_steps"
  s0a -> s0b
}

sources: Sources {
  style.fill: "#E3F2FD"
  s1: "3. Resolve lookups"
  s2: "4. Read sources\n(watermark/CDC filter)"
  s3: "5. Set primary DataFrame"
  s1 -> s2 -> s3
}

transforms: Transforms {
  style.fill: "#FFF3E0"
  s4: "6. Run pipeline steps"
  s5: "7. Validate rules\n(quarantine on failure)"
  s5b: "8. Naming normalization"
  s6: "9. Compute business keys\n+ change hashes"
  s4 -> s5 -> s5b -> s6
}

write: Write {
  style.fill: "#E8F5E9"
  s7: "10. Resolve target path"
  s8: "11. Apply column mapping"
  s8b: "12. Inject audit columns"
  s9: "13. Write to Delta\n(overwrite/append/merge/CDC)"
  s7 -> s8 -> s8b -> s9
}

finalize: Finalize {
  style.fill: "#F3E5F5"
  s10: "14. Persist watermark/CDC state"
  s11: "15. Post-write assertions"
  s11b: "16. Write exports\n(secondary outputs)"
  s11c: "17. Run thread post_steps"
  s12: "18. Build telemetry\n→ ThreadResult"
  s10 -> s11 -> s11b -> s11c -> s12
}

setup -> sources
sources -> transforms
transforms -> write
write -> finalize
```

1. Initialize thread-level resources (variables, lookups, column_sets)
2. Run thread-level `pre_steps`
3. Resolve lookup-based sources from cached or on-demand DataFrames
4. Read all remaining declared sources (with watermark/CDC filtering if
   applicable). For `mode: incremental_watermark`, the primary source
   is filtered against the prior HWM and the new HWM is captured from
   the filtered DataFrame before transforms. For generic `mode: cdc`
   (explicit `cdc.operation_column`) composed with a `watermark_column`,
   the same pattern applies and the HWM aggregate runs *before*
   I/U/D operation routing — delete rows participate in advancing the
   window so append-only CDC history tables (e.g. SAP Open Database
   Mirror) do not reprocess deletes on every run. Empty filtered
   windows yield `new_hwm = None` so step 14 leaves the persisted HWM
   untouched. The Delta CDF preset path (`cdc.preset: delta_cdf`) is
   unchanged: it captures `_commit_version` instead of a column-based
   watermark and rejects `watermark_column` at config-parse time. When
   `load.watermark_format` is set, both reader paths wrap the column
   in `to_timestamp`/`to_date(col, format)` so string-typed watermark
   columns (SAP, mainframe, JSON) can be parsed declaratively.
5. Set the primary (first) source as the working DataFrame
6. Run pipeline steps against the working DataFrame
7. Evaluate validation rules; quarantine or abort on failures
8. Apply column and table naming normalization, including
   reserved word protection (if configured). Seven strategies
   are available: `quote`, `prefix`, `suffix`, `error`,
   `rename`, `revert`, and `drop`. See
   [configuration keys](../reference/configuration-keys.md#reservedwordconfig)
   for details.
9. Compute business keys and change detection hashes
10. Resolve the target write path
11. Apply target column mapping
12. Inject audit columns (bypasses mapping mode; applied for all write modes)
13. Write to the Delta target (standard write or CDC merge routing)
14. Persist watermark or CDC state
15. Run post-write assertions
16. Write exports (secondary outputs, if configured)
17. Run thread-level `post_steps`
18. Build telemetry and return `ThreadResult`

### Failure handling

```d2
direction: down

abort: abort_weave (default) {
  style.fill: "#FFEBEE"

  dim_product_a: dim_product ✓ {style.fill: "#C8E6C9"}
  dim_store_a: dim_store ✗ {style.fill: "#FFCDD2"}
  fact_orders_a: fact_orders ⊘ {style.fill: "#E0E0E0"}
  fact_returns_a: fact_returns ⊘ {style.fill: "#E0E0E0"}
  agg_revenue_a: agg_revenue ⊘ {style.fill: "#E0E0E0"}

  dim_product_a -> fact_orders_a
  dim_store_a -> fact_orders_a
  dim_store_a -> fact_returns_a
  fact_orders_a -> agg_revenue_a
}

skip: skip_downstream {
  style.fill: "#FFF3E0"

  dim_product_s: dim_product ✓ {style.fill: "#C8E6C9"}
  dim_store_s: dim_store ✗ {style.fill: "#FFCDD2"}
  fact_orders_s: fact_orders ⊘ {style.fill: "#E0E0E0"}
  fact_returns_s: fact_returns ⊘ {style.fill: "#E0E0E0"}
  agg_revenue_s: agg_revenue ⊘ {style.fill: "#E0E0E0"}
  fact_shipments_s: fact_shipments ✓ {style.fill: "#C8E6C9"}

  dim_product_s -> fact_orders_s
  dim_store_s -> fact_orders_s
  dim_store_s -> fact_returns_s
  fact_orders_s -> agg_revenue_s
  dim_product_s -> fact_shipments_s
}

legend: {
  style.fill: transparent
  style.stroke: transparent
  note: |md
    ✓ = success, ✗ = failed, ⊘ = skipped
  |
}
```

Each thread has a configurable failure policy:

| Policy | Behavior |
|--------|----------|
| `abort_weave` | All remaining threads in the weave are skipped (default) |
| `skip_downstream` | Only transitive dependents of the failed thread are skipped |
| `continue` | Same as skip_downstream — other independent threads proceed |

Transitive dependents are computed via BFS over the reverse dependency graph.

### Cache lifecycle

```d2
direction: right

produce: dim_product completes {
  style.fill: "#E3F2FD"
}

cache: CacheManager.persist()\nMEMORY_AND_DISK\nconsumers = 2 {
  style.fill: "#FFF3E0"
}

consumer1: fact_orders completes {
  style.fill: "#E8F5E9"
}

decrement1: notify_complete()\nconsumers = 1 {
  style.fill: "#FFF3E0"
}

consumer2: fact_returns completes {
  style.fill: "#E8F5E9"
}

decrement2: notify_complete()\nconsumers = 0 {
  style.fill: "#FFEBEE"
}

unpersist: unpersist()\nmemory released {
  style.fill: "#F3E5F5"
}

produce -> cache: output DataFrame
cache -> consumer1: read from cache
consumer1 -> decrement1
cache -> consumer2: read from cache
consumer2 -> decrement2
decrement2 -> unpersist: count = 0
```

The `CacheManager` uses reference counting to manage in-memory DataFrames:

1. When a cache-target thread completes, its output DataFrame is persisted
   directly at `MEMORY_AND_DISK` level (no re-read from Delta).
2. Each downstream consumer calls `notify_complete()` after finishing, which
   decrements the consumer count.
3. When the count reaches zero, the cached DataFrame is automatically
   unpersisted.
4. A `cleanup()` call in the executor's `finally` block force-unpersists
   anything remaining, preventing memory leaks.

Cache failures are non-fatal — if persistence fails, execution continues
without the cache optimization.

## Module map

| Module | Responsibility |
|--------|----------------|
| `engine/planner.py` | DAG construction, cycle detection, topological sort, cache analysis |
| `engine/executor.py` | Thread-level pipeline: read → transform → validate → write |
| `engine/runner.py` | Weave and loom orchestration, parallel dispatch, failure handling |
| `engine/result.py` | Immutable result models: `ThreadResult`, `WeaveResult`, `LoomResult` |
| `engine/cache_manager.py` | Reference-counted DataFrame caching |
| `engine/conditions.py` | Condition evaluation: parameter resolution, built-in functions, boolean parsing |
| `engine/hooks.py` | Pre/post hook step execution: quality gates, SQL statements, log messages |
| `engine/lookups.py` | Lookup materialization: pre-read, narrow projection, caching/broadcast |
| `engine/variables.py` | Weave-scoped variable binding and resolution |
| `engine/display.py` | SVG visualization (DAG, flow, timeline, waterfall) and HTML result rendering for notebooks |

## Design decisions

- **Immutable plans and results** — `ExecutionPlan`, `ThreadResult`,
  `WeaveResult`, and `LoomResult` are all frozen. This prevents accidental
  mutation during concurrent execution.
- **Per-thread telemetry collectors** — Each thread gets its own `SpanCollector`
  during concurrent execution, avoiding lock contention. Collectors merge into
  the weave-level collector after thread completion.
- **Fail-fast cycle detection** — Cycles are detected before any data is read,
  giving clear errors without wasted compute.
- **Safe condition evaluation** — Conditions are parsed with a recursive
  descent evaluator (no `eval()`). Only whitelisted operators and built-in
  functions are supported.

## Further reading

- [Thread, Weave, Loom](../concepts/thread-weave-loom.md) — The configuration
  hierarchy
- [Add a Thread](../how-to/add-a-thread.md) — Step-by-step thread creation
- [Observability](observability.md) — Telemetry spans and logging
