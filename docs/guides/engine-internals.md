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

1. **Pre-steps** — If the weave defines `pre_steps`, hook steps run before
   any thread executes. Failures with `on_failure: abort` stop the weave.
2. **Lookup materialization** — Named lookups are materialized according to
   the planner's lookup schedule. External lookups (those not produced by a
   thread in the weave) are pre-read before the first group. Internal lookups
   whose source is produced by a thread in group N are deferred and
   materialized at the group N+1 boundary — after their producer completes.
   Threads reference lookups via `source.lookup` and receive the cached
   DataFrame.
3. **Thread execution** — Parallel groups are processed sequentially. Within
   each group, threads are submitted to a `ThreadPoolExecutor` for concurrent
   execution.
4. **Post-steps** — After all threads complete, `post_steps` hook steps run.
   Failures with `on_failure: abort` mark the weave as failed.

After each thread completes:

- If it is a cache target, the output DataFrame is persisted.
- Consumer reference counts are decremented; caches are unpersisted when no
  consumers remain.
- Thread-level telemetry collectors are merged into the weave collector.

**Thread level** (`execute_thread`) — A single thread runs through a 12-step
pipeline:

1. Resolve lookup-based sources from cached or on-demand DataFrames
2. Read all remaining declared sources (with watermark/CDC filtering if applicable)
3. Set the primary (first) source as the working DataFrame
4. Run pipeline steps against the working DataFrame
5. Evaluate validation rules; quarantine or abort on failures
6. Compute business keys and change detection hashes
7. Resolve the target write path
8. Apply target column mapping
9. Write to the Delta target (standard write or CDC merge routing)
10. Persist watermark or CDC state
11. Run post-write assertions
12. Build telemetry and return `ThreadResult`

### Failure handling

Each thread has a configurable failure policy:

| Policy | Behavior |
|--------|----------|
| `abort_weave` | All remaining threads in the weave are skipped (default) |
| `skip_downstream` | Only transitive dependents of the failed thread are skipped |
| `continue` | Same as skip_downstream — other independent threads proceed |

Transitive dependents are computed via BFS over the reverse dependency graph.

### Cache lifecycle

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
