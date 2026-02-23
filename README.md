# weevr

**weevr** is a configuration-driven data shaping framework for Spark workloads in Microsoft Fabric.

Its purpose is to make PySpark-based data engineering **repeatable, predictable, optimized, and governable** by separating *declarative data shaping intent* from *execution mechanics*.

Users declare **what** should happen to data in YAML configuration. A stable PySpark engine interprets that intent and executes optimized data transformations using Spark's native DataFrame APIs — without exposing implementation complexity to the user.

weevr does not replace Spark. Spark remains the execution engine. weevr provides a disciplined, Fabric-native layer that consistently applies configuration-driven patterns to shape and move data.

## Core principles

### Declarative intent, imperative execution

Configuration is **read at runtime** and drives execution directly. weevr does not generate code from YAML — it interprets configuration and orchestrates Spark DataFrame operations.

This approach emphasizes:

* Predictability over flexibility
* Explicit structure over ad-hoc logic
* Inspectable artifacts over opaque execution

### Spark-native, Fabric-aligned

All execution uses Spark DataFrame APIs inside Fabric's runtime. No external systems, runtimes, or connectivity assumptions are introduced. weevr leverages what Spark already does well.

### Deterministic and idempotent

Given the same configuration and inputs, execution behavior is consistent and safe to rerun. Idempotency guarantees are mode-dependent (overwrite, append, merge).

### Opinionated defaults, configurable overrides

weevr provides safe, well-reasoned defaults for null handling, join behavior, failure semantics, and schema management. Authors can override any default when they have specific requirements.

### Configuration reuse through inheritance

Repeatable patterns are defined once at higher levels (loom, weave) and inherited by threads. This reduces development effort and enforces standards across a project.

## Core abstractions

weevr is built around a small set of core concepts:

* **Thread** — The smallest executable unit. Defines the flow of data from one or more sources into a single authoritative target (Lakehouse table, file output, etc.). Encapsulates sources, transformations, validations, write behavior, and audit columns.

* **Weave** — A flat collection of threads that form a dependency graph (DAG). Represents a subject area or processing stage. Independent threads within a weave execute in parallel on a shared SparkSession.

* **Loom** — A deployable execution unit that packages one or more weaves. Defines execution order between weaves and acts as the primary unit of versioning and release.

* **Project** — The logical solution container that groups related looms. A conceptual boundary for shared configuration (audit templates, parameter files, custom helpers/UDFs).

These concepts provide a shared vocabulary for reasoning about execution without binding users to low-level implementation details.

## Deployment model

weevr's core engine will be distributed as a **public open-source Python library** hosted on GitHub and published to PyPI:

```bash
pip install weevr
```

```python
import weevr

ctx = weevr.Context(
    spark=spark,
    params={"run_date": "2025-01-15"},
    param_file="params/prod.yaml"
)

result = ctx.run("looms/nightly.yaml")
```

The engine contains no project-specific configuration — it is a general-purpose framework.

**Integration projects** are separate Git repositories containing:

* YAML configuration files (threads, weaves, looms, parameters)
* Fabric Notebooks or Spark Job Definitions that import `weevr` and point it at configurations
* Project-specific UDFs, custom helper functions, and Python modules

This separation allows the engine to evolve independently while teams own their integration projects without coupling to engine development.

## Inspectable execution

A key design goal of weevr is that execution leaves behind **structured, inspectable artifacts**:

* Structured telemetry with row counts, durations, and validation outcomes
* Execution traces showing operation order and data flow
* Configuration snapshots with resolved parameters
* Deterministic paths and outputs

This makes it possible to understand *what happened* and *why* without reverse-engineering runtime behavior.

## Status

weevr is in **active development** (Phase 0). The core execution engine is functional — threads, weaves, and looms execute end-to-end from YAML configuration through to Delta table writes with DAG-based orchestration, data validation, and structured telemetry.

### What works today

* **Configuration layer** — YAML parsing, schema validation, variable injection, parameter files, reference resolution, and inheritance cascade (loom → weave → thread)
* **Object model** — Typed domain models for threads, weaves, looms, sources, targets, pipeline steps, keys, write config, and validation rules
* **Thread execution** — Source reading (Delta, CSV, JSON, Parquet), core transforms (filter, derive, select, drop, rename, cast, dedup, sort, join, union), null-safe joins, surrogate key generation, change detection hashing, target column mapping, and Delta writes (overwrite, append, merge)
* **DAG orchestration** — Dependency resolution from source/target path analysis, parallel thread execution within weaves, sequential weave ordering in looms, configurable failure behavior (abort_weave, skip_downstream, continue), and auto-cache management
* **Validation and data quality** — Pre-write validation rules with severity routing (info/warn log only, error quarantines to `{target}_quarantine`, fatal aborts), post-write assertions (row_count, column_not_null, unique, expression)
* **Telemetry** — OTel-compatible execution spans, structured JSON logging, execution trace trees with flat span serialization, row count reconciliation, and telemetry composition on result objects

### Roadmap

* **Phase 0 (current)** — Python API (Context class, run/load/validate), incremental processing (watermark-based loads)
* **Phase 1** — Analytical transforms (aggregate, window, pivot), naming normalization, advanced merge patterns, CDC
* **Phase 2** — Extensibility (stitches, helper/UDF registries), operational features (retry, circuit breaker, mirrors)
* **Phase 3** — Developer tooling (test framework, CLI validation, dry-run modes)

## Non-goals

By design, weevr is intentionally **not**:

* A low-code or no-code platform
* A visual workflow designer
* A replacement for Spark or re-implementation of data processing primitives
* An abstraction layer that hides the underlying execution engine

The intent is to reduce orchestration friction and enforce repeatable patterns, not to obscure how data is actually processed.

## Key capabilities

* **Declarative transformation pipelines** — Filters, joins, dedup, sort, rename, cast, derive, select, drop, and union expressed in YAML. Aggregate, window, and pivot transforms are planned.
* **Expression language** — Spark SQL expressions users already know, plus key generation and change detection hashing
* **Flexible write modes** — Overwrite, append, and merge (upsert) with configurable match/update/delete behavior. Insert-only mode is planned.
* **Validation and data quality** — Pre-write validation rules with configurable severity (info, warn, error, fatal) and automatic row quarantine. Post-write assertions for row counts, null checks, uniqueness, and custom expressions.
* **Configuration inheritance** — Define patterns once at loom or weave level, cascade down to threads with child-wins semantics
* **Variable injection** — Environment-agnostic configs with parameter files and runtime overrides
* **Observability** — OTel-compatible execution spans, structured JSON logging, row count reconciliation, and execution trace trees with flat span serialization
* **DAG orchestration** — Automatic dependency resolution, parallel thread execution, configurable failure behavior, and auto-cache management
* **Null-safe defaults** — Opinionated defaults for join semantics and key handling that prevent common Spark pitfalls
* **Incremental processing** *(planned)* — Watermark-based and parameter-driven incremental loads, plus CDC support
* **Extensibility** *(planned)* — Project-level UDFs, custom helper functions, and reusable stitch patterns

## Target audience

weevr is designed for a broad audience:

* Analysts who know SQL but not Spark
* Data engineers who want config-driven consistency
* Teams building medallion architectures in Fabric
* Anyone seeking repeatable, governed data transformation patterns

The YAML configuration uses familiar terminology and hides Spark complexity, while the expression language leverages Spark SQL so users can apply what they already know.

## Contributing

See `CONTRIBUTING.md` for development setup, workflow expectations, and pull request conventions.

Contributions are welcome.

## License

This project is licensed under the terms specified in the `LICENSE` file in this repository.
