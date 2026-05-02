# weevr

Metadata-driven execution engine for Fabric/Spark data pipelines.

This is the engine wheel — see the [project README](../../README.md) and the
[documentation](https://ardent-data.github.io/weevr/) for the full picture.

The pure-Python validation, planner, and telemetry-contract surface lives in
the companion [`weevr-core`](../weevr-core) wheel; `weevr` declares it as a
runtime dependency, so `pip install weevr` continues to install the full
stack unchanged.
