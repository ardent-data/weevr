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

## Step 2 -- Force-cache a thread

If the engine does not auto-detect a caching opportunity (for example, the
thread is not part of a multi-consumer DAG but you know it will be read
repeatedly), you can force caching with the `cache` flag on the thread:

```yaml
# lookups/dim_product.thread
config_version: "1.0"

sources:
  products:
    type: delta
    alias: bronze.products

steps:
  - select:
      columns:
        - product_id
        - product_name
        - category

target:
  path: Tables/dim_product

write:
  mode: overwrite

cache: true
```

Setting `cache: true` instructs the engine to persist this thread's output
regardless of the DAG analysis.

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

## Verify caching behavior

Use `verbose` or `debug` log level to see cache lifecycle events in the
structured JSON logs:

```python
from weevr import Context

ctx = Context(spark, "my-project.weevr", log_level="verbose")
result = ctx.run("orders.weave")
```

Look for log entries containing `Cached output of thread` and
`Unpersisted cached output of thread` to confirm that caching is active and
cleanup is occurring as expected.
