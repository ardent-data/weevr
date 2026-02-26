# Thread, Weave, Loom

weevr organizes work into a three-level hierarchy. Each level has a distinct
responsibility, and configuration flows downward through inheritance.

```text
┌─────────────────────────────────────────────────┐
│  Loom                                           │
│  Deployment unit — orders weaves, sets defaults  │
│                                                 │
│  ┌─────────────────────┐ ┌────────────────────┐ │
│  │  Weave              │ │  Weave             │ │
│  │  DAG of threads     │ │  DAG of threads    │ │
│  │                     │ │                    │ │
│  │  ┌───┐ ┌───┐ ┌───┐ │ │  ┌───┐ ┌───┐      │ │
│  │  │ T │→│ T │→│ T │ │ │  │ T │→│ T │      │ │
│  │  └───┘ └───┘ └───┘ │ │  └───┘ └───┘      │ │
│  │        ↘     ↗      │ │                    │ │
│  │        ┌───┐        │ │                    │ │
│  │        │ T │        │ │                    │ │
│  │        └───┘        │ │                    │ │
│  └─────────────────────┘ └────────────────────┘ │
└─────────────────────────────────────────────────┘
```

## Thread -- the smallest unit

A **thread** is the smallest executable unit in weevr. It defines the flow
of data from one or more sources into a single target.

A thread encapsulates:

- **Sources** -- one or more data inputs (Delta tables, CSV files, etc.)
- **Steps** -- an ordered list of transformations (filter, join, derive, etc.)
- **Target** -- a single write destination, typically a Delta table

```yaml
# dimensions/dim_customer.thread
config_version: "1"
sources:
  customers:
    type: delta
    alias: raw.customers
steps:
  - filter:
      expr: "status = 'active'"
  - derive:
      columns:
        full_name: "concat(first_name, ' ', last_name)"
target:
  alias: silver.dim_customer
write:
  mode: merge
  match_keys: [customer_id]
```

Threads are intentionally narrow. Each thread does one thing: shape data from
sources into a target. Orchestration decisions (ordering, conditional
execution) belong to the weave level.

!!! note "One target per thread"

    A thread always writes to exactly one primary target. If you need the
    same data in multiple locations, use mirror outputs rather than
    duplicating threads.

## Weave -- a dependency graph over threads

A **weave** groups threads into a cohesive processing scope and defines
how they relate to each other.

Key responsibilities:

- **Dependency DAG** -- Defines which threads must complete before others
  can start. Dependencies are auto-inferred from source/target relationships
  (if thread B reads from thread A's target, B depends on A) and can be
  declared explicitly when the relationship is not data-based.
- **Parallel execution** -- Independent threads within a weave run in
  parallel on a shared SparkSession.
- **Conditional execution** -- Weaves can define conditions that control
  which threads run (e.g., run a seed thread only when the target is empty).

```yaml
# dimensions.weave
config_version: "1"
threads:
  - ref: dimensions/dim_customer.thread
  - ref: dimensions/dim_product.thread
  - ref: dimensions/dim_store.thread
    condition:
      when: "table_empty('silver.dim_store')"
```

Weaves are flat -- they are not nested. A weave represents a subject area
or processing stage (e.g., "dimensions", "facts", "aggregates").

## Loom -- the deployment unit

A **loom** packages one or more weaves into a deployable, executable unit.

Key responsibilities:

- **Weave ordering** -- Defines execution order between weaves (e.g.,
  dimensions before facts).
- **Shared defaults** -- Sets configuration inherited by all contained
  weaves and threads.
- **Versioning boundary** -- Acts as the primary unit of versioning and
  release.

```yaml
# nightly.loom
config_version: "1"
weaves:
  - ref: dimensions.weave
  - ref: facts.weave
defaults:
  write:
    mode: merge
  execution:
    log_level: standard
params:
  run_date:
    type: date
    required: true
```

The same weave can appear in multiple looms. A "nightly" loom and a "weekly"
loom might share the dimensions weave but include different fact weaves.

## Configuration inheritance

Configuration values cascade through the hierarchy. The rule is simple:
**most specific wins**.

```text
Loom defaults  →  Weave defaults  →  Thread config
(least specific)                    (most specific)
```

For any property, the lowest-level declaration takes precedence:

| Declared at | Effect |
|---|---|
| Loom only | All weaves and threads in the loom inherit the value |
| Weave only | All threads in the weave inherit the value |
| Thread | That thread uses its own value, ignoring loom/weave defaults |

Scalar values are replaced outright -- a thread value replaces a weave or
loom value. Collection values (lists, maps) are also replaced entirely, not
merged.

!!! tip "Inheritance reduces repetition"

    Define common patterns once at the loom level (write modes, audit
    templates, execution settings) and override only where a thread differs.
    Most threads need very little thread-level configuration when inheritance
    is used effectively.

### Example

A loom sets `write.mode: overwrite` as the default. One weave overrides
it to `merge` for its threads. A single thread within that weave overrides
it again to `append`:

```text
Loom default:   write.mode = overwrite
  └─ Weave:     write.mode = merge       ← overrides loom
       └─ Thread A: (inherits merge)
       └─ Thread B: write.mode = append  ← overrides weave
```

## Project -- the solution boundary

Above the loom sits the **project**, a conceptual boundary that groups
related looms. A project is not a configuration file -- it is the directory
structure that provides the namespace for configuration resolution.

See the [YAML Schema Reference](../reference/yaml-schema/thread.md)
for full details on configuration file structure and reference resolution.

## Next steps

- [Why weevr](why-weevr.md) -- Understand the design principles
- [Execution Modes](execution-modes.md) -- Write and load mode details
- [Artifacts Model](artifacts-model.md) -- How threads map to storage
