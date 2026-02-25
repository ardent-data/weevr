# weevr

**Configuration-driven execution framework for Spark in Microsoft Fabric.**

weevr lets you declare data shaping intent in YAML and execute it as optimized,
repeatable PySpark transformations. No code generation, no manual notebook
orchestration — just deterministic, metadata-driven data pipelines.

## Key features

- **Declarative YAML** — Define sources, transforms, and targets in configuration
- **Spark-native** — Executes via PySpark DataFrame APIs in Microsoft Fabric
- **Deterministic** — Same config + inputs = same outputs, every time
- **19 transform types** — Filter, derive, join, aggregate, window, pivot, and more
- **DAG orchestration** — Threads form weaves, weaves form looms, with automatic dependency resolution
- **Incremental processing** — Watermark and CDC modes for efficient loads
- **Structured telemetry** — Spans, events, and row counts for full observability

## Quick start

```bash
pip install weevr
```

```python
from weevr import Context

ctx = Context(spark)
result = ctx.run("loom.yaml")
```

See the [Your First Loom](tutorials/your-first-loom.md) tutorial for a complete walkthrough.

## Learn more

| | |
|---|---|
| [**Tutorials**](tutorials/your-first-loom.md) | Step-by-step guides to get started |
| [**How-to Guides**](how-to/add-a-thread.md) | Task-oriented recipes for common scenarios |
| [**Reference**](reference/yaml-schema/thread.md) | YAML schema, API docs, and configuration keys |
| [**Concepts**](concepts/why-weevr.md) | Architecture, design principles, and mental models |
