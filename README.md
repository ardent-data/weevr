# weevr

[![PyPI version](https://img.shields.io/pypi/v/weevr)](https://pypi.org/project/weevr/)
[![Python](https://img.shields.io/pypi/pyversions/weevr)](https://pypi.org/project/weevr/)
[![License](https://img.shields.io/github/license/ardent-data/weevr)](LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/ardent-data/weevr/code-quality.yaml?branch=main&label=CI)](https://github.com/ardent-data/weevr/actions)

Configuration-driven data shaping for Spark in Microsoft Fabric.

**[Documentation](https://ardent-data.github.io/weevr/latest/)** · **[Getting Started](https://ardent-data.github.io/weevr/latest/tutorials/your-first-loom/)** · **[YAML Reference](https://ardent-data.github.io/weevr/latest/reference/yaml-schema/thread/)** · **[weevr-cli](https://ardent-data.github.io/weevr-cli/latest/)**

weevr lets you declare data transformation intent in YAML and execute it on PySpark. Define **what** should happen to your data — sources, transforms, joins, validations, write behavior — and let a stable engine handle **how** it runs. No code generation, no abstraction leaks. Just Spark DataFrame operations driven by configuration.

## Installation

```bash
pip install weevr
```

weevr ships as two wheels sharing the `weevr` namespace. `weevr` is the Spark engine and
depends on `weevr-core`, the Spark-free config model, validation, and planner. Install
`weevr-core` alone to use those without PySpark.

weevr 2 runs on Fabric Runtime 2.0. On Runtime 1.3, use weevr 1.x. See
[Fabric Runtime](https://ardent-data.github.io/weevr/latest/guides/fabric-runtime/#runtime-compatibility).

## Quick Start

Define a thread — the smallest unit of work — using a `.thread` file:

```yaml
# dim_customer.thread
config_version: "1.0"
sources:
  raw_customers:
    type: delta
    alias: raw.customers
steps:
  - filter:
      expr: "is_active = true"
  - select:
      columns: [customer_id, name, email, region]
target:
  alias: curated.dim_customer
write:
  mode: overwrite
```

Run it from a Fabric Notebook, with the file in a `.weevr` project folder:

```python
from weevr import Context

ctx = Context(spark, "my-project.weevr")
result = ctx.run("dim_customer.thread")
print(result.status)  # "success"
```

`ctx.run()` also takes `mode="validate"`, `"plan"`, or `"preview"` to check a config without
writing data.

## Features

* **Declarative transforms** — 23 step types expressed in YAML, covering filtering, joins, dedup, sorting, column shaping, type and null handling, `case_when`, value mapping, string and date operations, aggregation, windows, pivot and unpivot, union, and foreign key resolution
* **Dimensional modeling** — SCD Type 2 dimensions with surrogate keys and system members, and fact tables with foreign key and sentinel handling
* **Flexible write modes** — Overwrite, append, and merge (upsert) with configurable match, update, and delete behavior
* **Validation and data quality** — Pre-write rules with severity routing (info, warn, error, fatal) and automatic row quarantine; post-write assertions for row counts, null checks, uniqueness, and custom expressions
* **DAG orchestration** — Automatic dependency resolution, parallel thread execution within weaves, sequential weave ordering, configurable failure behavior, and auto-cache management
* **Schema contracts** — Warp files declare a target's intended shape, with enforcement, pre-initialization, and strict, lenient, or adaptive schema drift handling
* **Connections and secondary outputs** — Named OneLake connections for cross-lakehouse work, reusable audit column templates, and exports to additional locations
* **Configuration inheritance** — Define patterns once at loom or weave level, cascade to threads with child-wins semantics
* **Variable injection** — Environment-agnostic configs with parameter files and runtime overrides
* **Incremental processing** — Watermark-based incremental loads, CDC merge routing with hard/soft delete support, and Delta Change Data Feed integration
* **Observability** — OTel-compatible execution spans, structured JSON logging, row count reconciliation, and execution trace trees
* **Null-safe defaults** — Opinionated join semantics and key handling that prevent common Spark pitfalls
* **Python API** — `Context` class with `run()` in execute, validate, plan, and preview modes, and `load()` for config inspection

## Core Abstractions

weevr organizes work through four concepts:

* **Thread** — The smallest executable unit. Reads from one or more sources, applies transforms, validates, and writes to a single target.
* **Weave** — A collection of threads forming a dependency DAG. Represents a subject area or processing stage. Independent threads run in parallel.
* **Loom** — A deployable unit packaging one or more weaves with defined execution order. The primary unit of versioning and release.
* **Project** — A logical grouping of looms, stored as a `.weevr` folder. Defines the boundary for shared configuration and parameter files.

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

**Integration projects** are separate repositories containing YAML configs and Fabric Notebooks. This separation lets the engine evolve independently while teams own their configuration.

## Compatibility

| Component | Runtime 2.0 |
|-----------|-------------|
| Python | 3.13 (`weevr-core` alone: 3.12 or later) |
| PySpark | 4.1 |
| Delta Lake | 4.2 |

Fabric Runtime 1.3 (Python 3.11) is supported by weevr 1.x.

## CLI

[weevr-cli](https://github.com/ardent-data/weevr-cli) is a standalone
command-line companion for validating configs, inspecting schemas, and
running dry-run operations outside of a notebook. Install it separately:

```bash
pip install weevr-cli
```

Full CLI documentation: [ardent-data.github.io/weevr-cli](https://ardent-data.github.io/weevr-cli/latest/)

## Documentation

Full documentation is available at [ardent-data.github.io/weevr](https://ardent-data.github.io/weevr/latest/).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, workflow expectations, and pull request conventions. Contributions are welcome.

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
