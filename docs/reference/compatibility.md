# Compatibility

weevr is designed to run within the Microsoft Fabric runtime. The table below
lists the tested and supported versions for each dependency.

Last validated against weevr **2.0.0-alpha.1**. <!-- x-release-please-version -->

## Runtime matrix

weevr 2 supports Fabric Runtime 2.0. Fabric Runtime 1.3 is supported by weevr
1.x (see [Support window for weevr 1.x](#support-window-for-weevr-1x)).

| Component | Runtime 2.0 | Notes |
|-----------|-------------|-------|
| Python | 3.13 | `weevr` accepts `>=3.13,<3.14`; `weevr-core` accepts `>=3.12,<3.14` |
| PySpark | 4.1.1 | Bundled with the runtime |
| Delta Lake | 4.2.0 | Bundled with the runtime |
| Pydantic | 2.x | Configuration validation |

Runtime 1.3 ships Python 3.11, which can't install weevr 2. See
[Fabric Runtime](../guides/fabric-runtime.md#runtime-compatibility) for notes on
Runtime 2.0 and on choosing a runtime per workspace or item, and
[Move to Runtime 2.0](../how-to/move-to-runtime-2.md) to move an existing
workload.

## Support window for weevr 1.x

weevr 1.x is supported for as long as Microsoft supports Fabric Runtime 1.3.
Until then, it receives bug fixes and security fixes as patch releases. New
features go into weevr 2.x.

Microsoft's [runtime lifecycle](https://learn.microsoft.com/fabric/data-engineering/lifecycle)
page puts Runtime 1.3 in Long Term Support from October 1, 2026, through
March 2027. If Microsoft changes that date, the weevr 1.x window changes with
it. After it ends, weevr 1.x gets no further releases.

weevr 1.x also runs on Runtime 2.0 (from 1.21), but the window still follows
Runtime 1.3.
To stay supported on Runtime 2.0, move to weevr 2.x once it is released.

## Python version policy

The `weevr` engine wheel supports Python 3.13, the interpreter of Runtime 2.0.
`weevr-core` supports Python 3.12 and 3.13, so tooling and CI can validate
configs outside Fabric. Development uses Python 3.13, pinned by the
`.python-version` file at the repository root. weevr 1.x supports Python 3.11
through 3.13.

## Spark and Delta compatibility

PySpark and Delta Lake versions are determined by the Fabric Runtime and are
not independently selectable. weevr is tested against the versions Runtime
2.0 bundles: PySpark 4.1.1 with Delta 4.2.0. Running against other Spark or
Delta versions is not officially supported.

## Development dependencies

Development tooling (linting, testing, type checking) is managed via `uv` and
recorded in `pyproject.toml`. These tools are not required at runtime.

| Tool | Purpose |
|------|---------|
| uv | Package management and virtual environments |
| Ruff | Linting and formatting |
| Pyright | Static type checking |
| pytest | Test execution |
| pytest-cov | Coverage reporting |
