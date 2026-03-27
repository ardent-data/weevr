# weevr Examples

Example configurations demonstrating weevr's declarative pipeline model.

Each file uses a typed extension (`.thread`, `.weave`, `.loom`) to indicate its
config type. They are not runnable without a Spark/Fabric environment, but they
serve as reference templates and are validated by CI to ensure structural correctness.

## Examples

| File | Description |
|------|-------------|
| `basic-thread.thread` | Minimal thread: read a Delta source, select columns, write to Delta. |
| `thread-with-transforms.thread` | Thread with multiple pipeline steps: filter, derive, select, cast. |
| `merge-thread.thread` | Thread that merges into a Delta target using match keys. |
| `incremental-thread.thread` | Thread with watermark-based incremental loading. |
| `multi-thread-weave.weave` | Weave orchestrating three threads with explicit dependencies. |
| `full-loom.loom` | Complete loom with two weaves, variables, defaults, and execution settings. |
| `validation-thread.thread` | Thread with data quality validations and post-write assertions. |
| `hooks-quality-gates.weave` | Weave with pre/post quality gates, SQL variables, and log messages. |
| `hooks-lookups.weave` | Weave with materialized (broadcast) and on-demand lookup definitions. |
| `exports-thread.thread` | Thread with secondary exports: Parquet archive and CSV extract. |
| `transform-extensions.thread` | concat, map, fill_null type_defaults, and format in a single pipeline. |

## Fabcon 2026 Demos

The `fabcon/` directory contains a progressive demo sequence for the Fabcon 2026
session. Six demos build from a single thread to a full loom, covering CSV
ingestion, validations, merge with soft delete, incremental watermarks, DAG
orchestration, and multi-weave deployment. See [`fabcon/README.md`](fabcon/README.md)
for the full walkthrough.

## Configuration Hierarchy

weevr uses a layered configuration model:

- **Thread** -- Smallest unit: sources, pipeline steps, and a target.
- **Weave** -- Collection of threads with a dependency DAG.
- **Loom** -- Deployment unit containing weaves with shared defaults.

Configuration cascades from loom to weave to thread (most specific wins).
