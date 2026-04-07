# weevr

**Configuration-driven execution framework for Spark in Microsoft Fabric.**

weevr lets you declare data shaping intent in YAML and execute it as optimized,
repeatable PySpark transformations. No code generation, no manual notebook
orchestration — just deterministic, metadata-driven data pipelines.

## Key features

- **Declarative YAML** — Define sources, transforms, and targets in configuration
- **Spark-native** — Executes via PySpark DataFrame APIs in Microsoft Fabric
- **Deterministic** — Same config + inputs = same outputs, every time
- **23 transform types** — Filter, derive, join, aggregate, window, pivot, and more
- **DAG orchestration** — Threads form weaves, weaves form looms, with automatic dependency resolution
- **Incremental processing** — Watermark and CDC modes for efficient loads
- **Structured telemetry** — Spans, events, and row counts for full observability

## Quick start

```bash
pip install weevr
```

```python
from weevr import Context

ctx = Context(spark, "my-project.weevr")
result = ctx.run("nightly.loom")
```

See the [Your First Loom](tutorials/your-first-loom.md) tutorial for a complete walkthrough.

## How it works

```d2
direction: right

yaml: YAML Configuration {
  style.fill: "#E3F2FD"
  thread: "Thread\nsources → steps → target" {style.fill: "#BBDEFB"}
  weave: "Weave\nthread DAG + lookups" {style.fill: "#BBDEFB"}
  loom: "Loom\nordered weaves + defaults" {style.fill: "#BBDEFB"}
  thread -> weave -> loom
}

engine: weevr Engine {
  style.fill: "#FFF3E0"
  config_res: Config Resolution\nparse → validate → resolve {style.fill: "#FFE0B2"}
  planner: Planner\nDAG → groups → cache {style.fill: "#FFE0B2"}
  executor: Executor\nread → transform → write {style.fill: "#FFE0B2"}
  telemetry: Telemetry\nspans + metrics {style.fill: "#FFE0B2"}
  config_res -> planner -> executor -> telemetry
}

spark: Spark + Delta Lake {
  style.fill: "#E8F5E9"
  df: DataFrame APIs {style.fill: "#C8E6C9"}
  delta: Delta transactions {style.fill: "#C8E6C9"}
}

fabric: Microsoft Fabric {
  style.fill: "#F3E5F5"
  lakehouse: OneLake / Lakehouse {style.fill: "#E1BEE7"}
  notebook: Notebooks / Pipelines {style.fill: "#E1BEE7"}
}

yaml -> engine: interprets
engine -> spark: executes
spark -> fabric: reads / writes
```

## CLI

[weevr-cli](https://github.com/ardent-data/weevr-cli) is a standalone
command-line companion for validating configs, inspecting schemas, and
running dry-run operations outside of a notebook.

```bash
pip install weevr-cli
```

See the [CLI documentation](https://ardent-data.github.io/weevr-cli/latest/)
for usage and command reference.

## Learn more

| | |
|---|---|
| [**Tutorials**](tutorials/your-first-loom.md) | Step-by-step guides to get started |
| [**How-to Guides**](how-to/add-a-thread.md) | Task-oriented recipes for common scenarios |
| [**Reference**](reference/yaml-schema/thread.md) | YAML schema, API docs, and configuration keys |
| [**Concepts**](concepts/why-weevr.md) | Architecture, design principles, and mental models |
