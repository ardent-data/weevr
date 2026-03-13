# Why weevr

## The problem

Data engineering in Microsoft Fabric typically starts with notebooks.
A team writes PySpark code to read from a lakehouse, transform the data,
and write to a Delta table. The first notebook works well. Then comes the
second, and the third, and eventually dozens of notebooks across multiple
lakehouses with shared lookups, incremental loads, and merge logic.

At that point, common pain points emerge:

- **Copy-paste proliferation.** Each notebook implements its own read-transform-write
  pattern. Shared logic is duplicated, not reused. A bug fix in one notebook
  is not automatically propagated to others.

- **No repeatable structure.** Every engineer writes transforms differently.
  Null handling, join semantics, key generation, and write modes vary across
  notebooks with no enforceable standard.

- **Brittle orchestration.** Notebook dependencies are managed through Fabric
  pipelines or manual sequencing. There is no declarative dependency graph,
  so adding or reordering steps requires careful manual coordination.

- **Opaque execution.** When a pipeline fails, diagnosing the root cause means
  reading through notebook cells, inspecting intermediate DataFrames, and
  correlating timestamps across logs. There is no structured telemetry.

- **Environment drift.** Moving from dev to staging to prod requires changing
  hardcoded paths, lakehouse names, and connection strings scattered across
  notebook code.

These are not Spark problems. Spark is the right engine for the work. The
problem is the absence of a disciplined layer between user intent and Spark
execution.

```d2
direction: right

notebooks: Notebooks {
  style.fill: "#FFEBEE"

  nb1: Notebook 1\nread → transform → write {
    style.fill: "#FFCDD2"
  }
  nb2: Notebook 2\nread → transform → write {
    style.fill: "#FFCDD2"
  }
  nb3: Notebook 3\nread → transform → write {
    style.fill: "#FFCDD2"
  }
  nb4: Notebook N\n... {
    style.fill: "#FFCDD2"
  }

  problems: {
    style.fill: transparent
    style.stroke: transparent
    note: |md
      - Duplicated logic
      - No dependency graph
      - Manual orchestration
      - Inconsistent patterns
    |
  }
}

weevr_approach: weevr {
  style.fill: "#E8F5E9"

  yaml: YAML Config {
    style.fill: "#C8E6C9"
    threads: "threads + weaves + looms"
    params: "variables + inheritance"
  }
  engine: weevr Engine {
    style.fill: "#A5D6A7"
    plan: plan
    execute: execute
    telemetry: telemetry
    plan -> execute -> telemetry
  }
  spark: Spark {
    style.fill: "#81C784"
    delta: Delta Lake
  }

  yaml -> engine: interprets
  engine -> spark: executes
}

notebooks -> weevr_approach: replaces {
  style.stroke: "#4A90D9"
  style.stroke-dash: 3
}
```

## The solution

weevr is a **configuration-driven execution framework** that separates
*what* should happen to data from *how* Spark performs that work.

Instead of writing PySpark code, you declare your data shaping intent in YAML:

```yaml
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
```

The weevr engine interprets this configuration at runtime and executes the
corresponding Spark DataFrame operations. No code generation, no magic
notebooks, no intermediate files.

## Design principles

weevr is built on seven foundational principles that guide every design
decision in the framework.

### Declarative intent, imperative execution

Users define *intent* in YAML. The engine translates that intent into
PySpark DataFrame operations. Authors focus on what data should look like;
the engine handles how to get there.

### Single source of truth is configuration

The YAML configuration is canonical. Transformation logic lives in config,
not in notebook cells. This makes pipelines reviewable, diffable, and
version-controllable with standard Git workflows.

### Spark-native, Fabric-aligned

All execution uses Spark DataFrame APIs inside Fabric's runtime. weevr does
not introduce a new processing engine, external runtime, or connectivity
layer. Spark remains the execution engine.

### Deterministic and idempotent

Given the same configuration and inputs, execution behavior is consistent
and safe to rerun. See [Idempotency](idempotency.md) for mode-specific
guarantees.

### Fail-fast validation

Configuration is validated before any data is read. Missing parameters,
invalid references, circular dependencies, and schema mismatches are caught
at startup rather than mid-execution.

### Configuration inheritance

Repeatable patterns are defined once at higher levels and inherited by lower
levels. A loom sets defaults for all its weaves, which set defaults for all
their threads. Only the differences need to be declared. See
[Thread, Weave, Loom](thread-weave-loom.md) for the full inheritance model.

### Opinionated defaults, configurable overrides

weevr provides safe, well-reasoned defaults for null handling, join behavior,
write modes, and failure semantics. Most threads work with zero configuration
for these concerns. When a specific scenario requires different behavior,
every default can be overridden at the thread level.

!!! tip "Who is weevr for?"

    weevr targets a broad audience: from analysts who know SQL but not
    PySpark, to senior data engineers who want config-driven consistency.
    The YAML uses familiar data engineering terminology, and the expression
    language is Spark SQL -- so users apply what they already know.

## What weevr is not

- **Not a replacement for Spark.** Spark is the execution engine. weevr is a
  configuration layer on top of it.
- **Not a scheduler.** Scheduling and pipeline orchestration remain external
  concerns (Fabric pipelines, Apache Airflow, etc.).
- **Not a code generator.** Configuration is interpreted at runtime. No
  PySpark source code is generated from YAML.
- **Not opinionated about data modeling.** Kimball, Data Vault, and other
  modeling patterns are supported through the
  [stitch system](../reference/yaml-schema/thread.md), not baked into the
  engine.

## Next steps

- [Thread, Weave, Loom](thread-weave-loom.md) -- Understand the object model
- [Your First Loom](../tutorials/your-first-loom.md) -- Build a working pipeline
- [Execution Modes](execution-modes.md) -- Learn about write and load modes
