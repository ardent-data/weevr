# weevr-core

Pure-Python validation, planner, and telemetry contracts for weevr.

Install this wheel when you need weevr's Pydantic model layer, configuration
resolver, planner graph, or telemetry schemas without paying for PySpark
— for example, in a CLI that validates `.weevr` projects in CI.

For execution capability (running a loom, weave, or thread end-to-end), see
the companion [`weevr`](../weevr) wheel; it depends on `weevr-core` so a plain
`pip install weevr` installs both transparently.

See the [project README](../../README.md) and the
[documentation](https://ardent-data.github.io/weevr/).
