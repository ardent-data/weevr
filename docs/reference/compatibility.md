# Compatibility

weevr is designed to run within the Microsoft Fabric runtime. The table below
lists the tested and supported versions for each dependency.

Last validated against weevr **1.20.7**. <!-- x-release-please-version -->

## Runtime matrix

| Component | Version | Notes |
|-----------|---------|-------|
| Python | 3.11 | Pinned via `.python-version` |
| PySpark | 3.5.5 | Bundled with Fabric Runtime 1.3 |
| Delta Lake | 3.2.x | Bundled with Fabric Runtime 1.3 |
| Microsoft Fabric Runtime | 1.3 | Target deployment environment |
| Pydantic | 2.x | Configuration validation |

weevr 1.x does not support Fabric Runtime 2.0: Runtime 2.0 runs Python 3.13, so
the install fails there. See [Fabric Runtime](../guides/fabric-runtime.md#runtime-compatibility)
for how to keep a workspace or item on Runtime 1.3.

## Python version policy

weevr targets Python 3.11 exclusively to match the Fabric Runtime 1.3
interpreter. This version is enforced by the `.python-version` file at the
repository root.

## Spark and Delta compatibility

PySpark and Delta Lake versions are determined by the Fabric Runtime and are
not independently selectable. weevr is tested against the versions bundled
with Runtime 1.3. Running against other Spark or Delta versions is not
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
