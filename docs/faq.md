# FAQ

Frequently asked questions about weevr.

---

## What is weevr?

weevr is a configuration-driven execution framework for PySpark. You
declare data shaping intent in YAML -- sources, transforms, targets -- and
weevr interprets that configuration at runtime to execute optimized,
repeatable data transformations using Spark DataFrame APIs. It is designed
for Microsoft Fabric but works in any Spark + Delta Lake environment.

---

## Does weevr generate PySpark code?

No. weevr does not generate code from YAML. Configuration is read at
runtime and drives execution directly through Spark DataFrame operations.
There are no intermediate scripts, notebooks, or code artifacts produced.
This makes execution deterministic and inspectable -- what you see in the
config is what runs.

---

## What Spark and Delta versions are supported?

weevr targets **PySpark 3.5.x** and **Delta Lake 3.2.x**, which align with
Microsoft Fabric Runtime 1.3 and Python 3.11. See the
[Compatibility](reference/compatibility.md) page for the full version
matrix.

---

## Can I use weevr outside Microsoft Fabric?

Yes. weevr runs on any environment that provides PySpark and Delta Lake.
For local development, use a standalone Spark installation with the Delta
extension:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)
```

The same YAML configs work in both local Spark and Fabric. Use parameter
files to swap environment-specific paths. See the
[Fabric Runtime](guides/fabric-runtime.md) guide for details.

---

## How does configuration inheritance work?

Configuration cascades through three levels: **Loom** > **Weave** >
**Thread**. The rule is simple -- most specific wins.

A loom can define default write modes, execution settings, and audit
templates. A weave can override any of those. A thread can override
anything the weave set. Scalar values are replaced outright; collections
(lists, maps) are also replaced entirely, not merged.

```text
Loom default:   write.mode = overwrite
  └─ Weave:     write.mode = merge       ← overrides loom
       └─ Thread A: (inherits merge)
       └─ Thread B: write.mode = append  ← overrides weave
```

See [Thread, Weave, Loom](concepts/thread-weave-loom.md) for the full
explanation.

---

## What write modes are supported?

weevr supports three write modes:

| Mode           | Behavior |
|----------------|----------|
| `overwrite`    | Replace the entire target table (default) |
| `append`       | Insert new rows without modifying existing data |
| `merge`        | Upsert: match on keys, update/insert/delete as configured |

Merge mode is the most configurable, with separate controls for
`on_match`, `on_no_match_target`, and `on_no_match_source` behaviors
including soft delete support. See
[Execution Modes](concepts/execution-modes.md) for details.

---

## How do incremental loads work?

weevr supports watermark-based incremental loading. You configure a
watermark column and type on the thread's load block:

```yaml
load:
  mode: incremental_watermark
  watermark_column: modified_date
  watermark_type: timestamp
```

On the first run, all rows are read. On subsequent runs, weevr reads only
rows where the watermark column exceeds the stored high-water mark. The
watermark state is persisted either as a Delta table property or in a
dedicated metadata table.

Supported watermark types: `timestamp`, `date`, `int`, `long`.

---

## Can I mix Python code with YAML configuration?

Yes. weevr supports two extension points for custom logic:

- **Helper functions** -- Registered Python functions that can be called
  from Spark SQL expressions in your YAML config (e.g., in `derive`,
  `filter`, or `condition` expressions).
- **UDFs** -- User-defined functions registered with Spark that can be
  used in any expression context.

Both are registered through the project-level configuration and are
available across all threads in a loom execution. Custom helpers and UDFs
live in your integration project alongside the YAML configs.

---

## How do I debug a failing thread?

Start by checking the `RunResult`:

```python
result = ctx.run("nightly.loom")
print(result.status)    # "success", "failure", or "partial"
print(result.summary())
```

If that does not pinpoint the issue, increase the log level:

```python
ctx = Context(spark, "my-project.weevr", log_level="verbose")
result = ctx.run("nightly.loom")
```

At `verbose` level, weevr logs every step of execution -- source reads,
transform steps, validation outcomes, and write operations. You can also
inspect the execution span on the thread's telemetry object, which
contains timing, status, and error attributes.

For transform logic issues, use preview mode to run against sampled data
without writing:

```python
result = ctx.run("nightly.loom", mode="preview")
```

See the [Observability](guides/observability.md) guide for the full
debugging workflow.

---

## Is weevr thread-safe?

Yes, within the execution model. Independent threads within a weave execute
concurrently using a `ThreadPoolExecutor` on a shared SparkSession. Each
thread operates on its own `SpanBuilder` and `SpanCollector`, and
collectors are merged only after thread completion. Spark itself handles
concurrent DataFrame operations on a shared session.

Weaves within a loom execute sequentially in the declared order, so there
is no cross-weave concurrency to manage.

---

## What is the difference between a thread and a weave?

A **thread** is the smallest executable unit. It defines a single data
flow: read from one or more sources, apply transforms, write to one
target. A thread is a self-contained transformation with no awareness of
other threads.

A **weave** is a collection of threads organized into a dependency DAG.
The weave is responsible for execution order (which threads depend on
which), parallel execution of independent threads, and conditional
execution logic. A weave represents a subject area or processing stage
(e.g., "dimensions", "facts").

See [Thread, Weave, Loom](concepts/thread-weave-loom.md) for the full
hierarchy.

---

## How do I handle null keys in joins?

By default, weevr uses null-safe join semantics to prevent the common
Spark pitfall where `NULL = NULL` evaluates to `NULL` (not `true`),
silently dropping rows from join results.

If you need to customize this behavior, configure null key handling at the
thread level:

```yaml
keys:
  null_safe: true
  null_key_replacement: "__NULL__"
```

When `null_safe` is enabled, null key values are replaced with a sentinel
before the join and restored afterward. This ensures rows with null keys
participate in joins as expected.

---

## Can I cache lookup tables?

Yes. There are two approaches:

**Thread-level auto-caching** — The `CacheManager` analyzes the thread
dependency DAG within a weave. When it detects that a thread's output
feeds multiple downstream consumers, it persists the output at
`MEMORY_AND_DISK` level automatically. You can force caching on a
specific thread with `cache: true` or disable it with `cache: false`.

**Weave-level lookups** — Define named lookups in the weave's `lookups`
block. Lookups can be pre-materialized (read once, cached or broadcast
before threads run) and shared across threads via the `lookup` source
field:

```yaml
# In the weave
lookups:
  dim_product:
    source:
      type: delta
      alias: silver.dim_product
    materialize: true
    strategy: cache
    key: [product_id]
    values: [product_name, category]

# In a thread's sources
sources:
  products:
    lookup: dim_product
```

Narrow lookups (`key`, `values`, `filter`) reduce memory by retaining
only the columns needed for the join. See
[Cache a Lookup](how-to/cache-a-lookup.md) for the full guide.

---

## What are execution hooks?

Hooks are steps that run before or after thread execution within a weave.
Define them with `pre_steps` (run before any thread) and `post_steps`
(run after all threads complete).

Three hook step types are available:

| Type | Purpose |
|------|---------|
| `quality_gate` | Run predefined quality checks (source freshness, row counts, table existence, expressions) |
| `sql_statement` | Execute arbitrary Spark SQL. Optionally capture a scalar result into a weave variable. |
| `log_message` | Emit a log message with variable placeholders |

Each step has an `on_failure` policy (`abort` or `warn`). Pre-steps
default to `abort`; post-steps default to `warn`.

```yaml
pre_steps:
  - type: quality_gate
    check: source_freshness
    source: raw.orders
    max_age: "24h"

post_steps:
  - type: log_message
    message: "Pipeline complete."
```

See the [Weave YAML Schema](reference/yaml-schema/weave.md) for the full
field reference.

---

---

## How do I validate my config without running a pipeline?

Use validate mode:

```python
result = ctx.run("nightly.loom", mode="validate")
print(result.status)
print(result.validation_errors)
```

Validate mode parses the config, resolves variables and references, checks
the DAG for cycles, and verifies that source paths exist -- all without
reading or writing any data. Use it in CI or as a pre-flight check before
production runs.

You can also use plan mode to see the execution order without running:

```python
result = ctx.run("nightly.loom", mode="plan")
print(result.summary())      # compact view with cache markers (e.g., dim_product*)
print(result.explain())      # detailed breakdown: dependencies, cache targets, thread detail
```

Plan mode builds the full execution plan — thread ordering, dependency
analysis, and cache target selection — without reading or writing any data.
The `summary()` output marks cache targets with an asterisk and includes
footer counts (threads, cached, lookups). `explain()` provides a
section-by-section breakdown including dependency provenance (inferred vs
explicit), cache consumers, lookup schedule, and per-thread source/target
detail. In notebooks, the result renders automatically as styled HTML
with an embedded DAG diagram.

See [Execution Modes](concepts/execution-modes.md) for the full set of
available modes.
