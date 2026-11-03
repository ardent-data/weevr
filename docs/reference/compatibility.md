# Compatibility

weevr is designed to run within the Microsoft Fabric runtime. The table below
lists the tested and supported versions for each dependency.

Last validated against weevr **1.20.7**. <!-- x-release-please-version -->

## Runtime matrix

weevr 1.21 and later support both Fabric Runtime 1.3 and Fabric Runtime 2.0.

| Component | Runtime 1.3 | Runtime 2.0 | Notes |
|-----------|-------------|-------------|-------|
| Python | 3.11 | 3.13 | Wheels accept `>=3.11,<3.14` |
| PySpark | 3.5.5 | 4.1.1 | Bundled with the runtime |
| Delta Lake | 3.2.x | 4.2.0 | Bundled with the runtime |
| Pydantic | 2.x | 2.x | Configuration validation |

weevr 1.20 and earlier require Python 3.11 and install only on Runtime 1.3. See
[Fabric Runtime](../guides/fabric-runtime.md#runtime-compatibility) for notes on
Runtime 2.0 and on choosing a runtime per workspace or item.

## Python version policy

The wheels support Python 3.11 through 3.13: the interpreters of Runtime 1.3
(3.11) and Runtime 2.0 (3.13), and 3.12 for `weevr-core` in Python notebooks
and tooling. Development defaults to Python 3.11, pinned by the
`.python-version` file at the repository root.

## Spark and Delta compatibility

PySpark and Delta Lake versions are determined by the Fabric Runtime and are
not independently selectable. weevr is tested against the versions each
supported runtime bundles: PySpark 3.5.5 with Delta 3.2, and PySpark 4.1.1
with Delta 4.2.0. Running against other Spark or Delta versions is not
officially supported.

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
