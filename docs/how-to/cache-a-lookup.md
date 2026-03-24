# Cache a Lookup

**Goal:** Configure caching on a lookup source so that multiple downstream
threads can read from memory instead of re-scanning the underlying Delta table
on each access.

## Prerequisites

- A weave with two or more threads that share a common lookup source
- The lookup source is a Delta table or other Spark-readable asset

## How auto-cache works

When weevr executes a weave, the `CacheManager` analyzes the thread dependency
DAG to find threads whose output is consumed by multiple downstream threads. It
automatically persists those outputs at `MEMORY_AND_DISK` storage level after
the producing thread completes. When all consumers finish, the cached DataFrame
is unpersisted to release memory.

This means that in many cases, caching happens without any configuration. The
engine handles the lifecycle automatically.

## Step 1 -- Identify the shared lookup

Suppose you have a `dim_product` thread that writes a product dimension table,
and two downstream threads (`fact_orders` and `fact_returns`) that both join
against it:

```text
dim_product  -->  fact_orders
             -->  fact_returns
```

Because `dim_product` feeds two consumers, weevr auto-caches its output after
it finishes writing.

## Step 2 -- Prevent cache suppression

By default, threads with two or more downstream dependents are auto-cached.
Setting `cache: true` on a thread is a no-op in the current engine — it
only prevents `cache: false` suppression. It does **not** force caching for
threads with fewer than two dependents.

If you need a thread's output to be shared across multiple consumers, use
[weave-level lookups](#weave-level-lookups) instead.

## Step 3 -- Disable caching for a thread

In rare cases where auto-caching is undesirable (for instance, a very large
output that should not consume executor memory), disable it explicitly:

```yaml
cache: false
```

This overrides the engine's auto-cache decision for that specific thread.

## Performance considerations

- **Storage level:** Cached DataFrames use `MEMORY_AND_DISK`, which spills to
  local disk if memory is insufficient. This prevents out-of-memory errors at
  the cost of slower disk reads.
- **Lifecycle management:** The `CacheManager` tracks consumer completion and
  automatically unpersists DataFrames when no further consumers remain. A
  `cleanup()` call in a `finally` block ensures caches are released even when
  execution fails partway through.
- **Cache failures are non-fatal:** If a persist or unpersist operation fails,
  the engine logs a warning and continues. Consumers fall back to reading from
  Delta directly. Caching improves performance but never affects correctness.
- **Scope:** Caching operates within a single weave execution. Cached
  DataFrames do not persist across weave boundaries or Spark sessions.

## Step 4 - Use weave-level lookups {#weave-level-lookups}

```d2
direction: down

plan: Execution Plan {
  style.fill: "#E3F2FD"

  group0: "Group 0: dim_product*" {
    style.fill: "#BBDEFB"
    dim_product: dim_product {style.fill: "#90CAF9"}
  }

  materialize: "Lookup materialization point\ndim_product → cache (internal)" {
    style.fill: "#FFF3E0"
  }

  group1: "Group 1: fact_orders, fact_returns" {
    style.fill: "#E8F5E9"
    fact_orders: fact_orders {style.fill: "#A5D6A7"}
    fact_returns: fact_returns {style.fill: "#A5D6A7"}
  }

  group0 -> materialize: "producer completes" {style.stroke: "#E65100"}
  materialize -> group1: "lookup available" {style.stroke: "#2E7D32"}
}

external: External Lookups {
  style.fill: "#F3E5F5"
  ref_regions: ref.regions\n(pre-materialized) {style.fill: "#E1BEE7"}

  note: |md
    External lookups (not produced by any thread
    in the weave) are materialized **before** the
    first execution group.
  |
}

external.ref_regions -> plan.group0: "available from start" {style.stroke-dash: 3}
```

For shared reference datasets, weave-level lookups provide more control than
thread-level caching. Define the lookup in the weave's `lookups` block and
reference it from thread sources:

```yaml
# orders.weave
config_version: "1.0"

lookups:
  dim_product:
    source:
      type: delta
      alias: silver.dim_product
    materialize: true
    strategy: cache

threads:
  - ref: facts/fact_orders.thread
  - ref: facts/fact_returns.thread
```

```yaml
# facts/fact_orders.thread — references the lookup
sources:
  orders:
    type: delta
    alias: raw.orders
  products:
    lookup: dim_product   # resolved from the weave's lookups map

steps:
  - join:
      source: products
      type: left
      on: [product_id]
```

The lookup is read once before threads start and shared across all threads
that reference it. Choose `strategy: broadcast` for small datasets that
benefit from Spark broadcast join hints, or `strategy: cache` (the default)
for larger datasets.

## Step 5 -- Narrow a lookup for efficiency

When a lookup table has many columns but threads only need a few, use narrow
lookup fields to reduce memory:

```yaml
lookups:
  dim_product:
    source:
      type: delta
      alias: silver.dim_product
    materialize: true
    key: [product_id]
    values: [product_name, category]
    filter: "is_active = true"
    unique_key: true
```

| Field | Purpose |
|-------|---------|
| `key` | Join key columns — always retained in the projection |
| `values` | Payload columns to keep. Only `key` + `values` columns are cached. |
| `filter` | SQL WHERE expression applied before projection |
| `unique_key` | Validate that `key` columns are unique after filtering |
| `on_failure` | Behavior on duplicate keys: `"abort"` (default) or `"warn"` |

`key` and `values` must not overlap. If `values` is set, `key` is required.

## Verify caching behavior

Use plan mode to check which threads are cache targets before running:

```python
result = ctx.run("orders.weave", mode="plan")
print(result.summary())      # cache targets are marked with an asterisk (e.g., dim_product*)
print(result.explain())      # "Cache targets" section lists each target and consumer count
```

To observe cache lifecycle events at runtime, use `verbose` or `debug`
log level:

```python
ctx = Context(spark, "my-project.weevr", log_level="verbose")
result = ctx.run("orders.weave")
```

Look for these log entries:

- **Thread auto-caching** — `Cached output of thread` and
  `Unpersisted cached output of thread` confirm the `CacheManager` is
  persisting and releasing thread outputs.
- **Lookup materialization** — `Materialized lookup '<name>'` confirms
  weave-level lookups are being pre-read and cached.
