# weevr Examples

Example YAML configurations demonstrating weevr's declarative pipeline model.

Each file is a self-contained configuration that illustrates a specific capability.
They are not runnable without a Spark/Fabric environment, but they serve as
reference templates and are validated by CI to ensure structural correctness.

## Examples

| File | Description |
|------|-------------|
| `basic-thread.yml` | Minimal thread: read a Delta source, select columns, write to Delta. |
| `thread-with-transforms.yml` | Thread with multiple pipeline steps: filter, derive, select, cast. |
| `merge-thread.yml` | Thread that merges into a Delta target using match keys. |
| `incremental-thread.yml` | Thread with watermark-based incremental loading. |
| `multi-thread-weave.yml` | Weave orchestrating three threads with explicit dependencies. |
| `full-loom.yml` | Complete loom with two weaves, variables, defaults, and execution settings. |
| `validation-thread.yml` | Thread with data quality validations and post-write assertions. |

## Configuration Hierarchy

weevr uses a layered configuration model:

- **Thread** -- Smallest unit: sources, pipeline steps, and a target.
- **Weave** -- Collection of threads with a dependency DAG.
- **Loom** -- Deployment unit containing weaves with shared defaults.

Configuration cascades from loom to weave to thread (most specific wins).
