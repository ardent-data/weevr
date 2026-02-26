# weevr

Configuration-driven data shaping for Spark in Microsoft Fabric.

weevr lets you declare data transformation intent in YAML and execute it on PySpark. Define **what** should happen to your data — sources, transforms, joins, validations, write behavior — and let a stable engine handle **how** it runs. No code generation, no abstraction leaks. Just Spark DataFrame operations driven by configuration.

## Installation

```bash
pip install weevr
```

## Quick Start

Define a thread — the smallest unit of work — using a `.thread` file:

```yaml
# dim_customer.thread
config_version: "1.0"
sources:
  raw_customers:
    type: delta
    path: "${lakehouse_path}/raw/customers"
steps:
  - filter:
      expr: "is_active = true"
  - select:
      columns: [customer_id, name, email, region]
target:
  path: "${lakehouse_path}/curated/dim_customer"
write:
  mode: overwrite
```

Run it from a Fabric Notebook or any PySpark environment:

```python
from weevr import Context

ctx = Context(spark, "my-project.weevr")
result = ctx.run("dim_customer.thread")
print(result.status)  # "success"
```

## Features

* **Declarative transforms** — Filter, join, dedup, sort, rename, cast, derive, select, drop, aggregate, window, pivot, and union — all expressed in YAML
* **Flexible write modes** — Overwrite, append, and merge (upsert) with configurable match, update, and delete behavior
* **Validation and data quality** — Pre-write rules with severity routing (info, warn, error, fatal) and automatic row quarantine; post-write assertions for row counts, null checks, uniqueness, and custom expressions
* **DAG orchestration** — Automatic dependency resolution, parallel thread execution within weaves, sequential weave ordering, configurable failure behavior, and auto-cache management
* **Configuration inheritance** — Define patterns once at loom or weave level, cascade to threads with child-wins semantics
* **Variable injection** — Environment-agnostic configs with parameter files and runtime overrides
* **Incremental processing** — Watermark-based incremental loads, CDC merge routing with hard/soft delete support, and Delta Change Data Feed integration
* **Observability** — OTel-compatible execution spans, structured JSON logging, row count reconciliation, and execution trace trees
* **Null-safe defaults** — Opinionated join semantics and key handling that prevent common Spark pitfalls
* **Python API** — `Context` class with `run()` for execution, `load()` for config inspection, and verification modes for dry-run validation

## Core Abstractions

weevr organizes work through four concepts:

* **Thread** — The smallest executable unit. Reads from one or more sources, applies transforms, validates, and writes to a single target.
* **Weave** — A collection of threads forming a dependency DAG. Represents a subject area or processing stage. Independent threads run in parallel.
* **Loom** — A deployable unit packaging one or more weaves with defined execution order. The primary unit of versioning and release.
* **Project** — A logical grouping of looms. Defines the boundary for shared configuration, parameter files, helpers, and UDFs.

## Core Principles

* **Declarative intent, imperative execution** — Configuration is read at runtime and drives execution directly. No code generation from YAML.
* **Spark-native, Fabric-aligned** — All execution uses Spark DataFrame APIs inside Fabric's runtime. No external systems or runtimes.
* **Deterministic and idempotent** — Same configuration and inputs produce consistent behavior. Safe to rerun.
* **Opinionated defaults, configurable overrides** — Safe defaults for null handling, join behavior, and failure semantics. Override when you need to.
* **Configuration reuse through inheritance** — Define patterns once at higher levels and inherit down. Reduces effort, enforces standards.

## Non-goals

weevr is intentionally **not**:

* A low-code or no-code platform
* A visual workflow designer
* A replacement for Spark or re-implementation of data processing primitives
* An abstraction layer that hides the underlying execution engine

The goal is to reduce orchestration friction and enforce repeatable patterns — not to obscure how data is processed.

## Target Audience

* Analysts who know SQL but not Spark
* Data engineers who want config-driven consistency
* Teams building medallion architectures in Fabric
* Anyone seeking repeatable, governed data transformation patterns

## Deployment Model

weevr's engine is a general-purpose library distributed via PyPI. It contains no project-specific configuration.

**Integration projects** are separate repositories containing YAML configs, Fabric Notebooks, and any project-specific UDFs or helpers. This separation lets the engine evolve independently while teams own their configuration.

## Compatibility

| Component | Version |
|-----------|---------|
| Python | 3.11 |
| PySpark | 3.5 |
| Delta Lake | 3.2 |
| Microsoft Fabric Runtime | 1.3 |

## What's Next

* Extensibility — Reusable stitch patterns, project-level UDF and helper registries
* Developer tooling — Test framework, CLI validation, dry-run modes
* Advanced merge patterns — Insert-only mode, complex update strategies

## Documentation

Full documentation is available at [ardent-data.github.io/weevr](https://ardent-data.github.io/weevr/).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, workflow expectations, and pull request conventions. Contributions are welcome.

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
