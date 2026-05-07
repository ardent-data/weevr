# Compatibility

weevr is designed to run within the Microsoft Fabric runtime. The table below
lists the tested and supported versions for each dependency.

Last validated against weevr **1.18.0**.

## Runtime matrix

| Component | Version | Notes |
|-----------|---------|-------|
| Python | 3.11 | Pinned via `.python-version` |
| PySpark | 3.5.x | Bundled with Fabric Runtime 1.3 |
| Delta Lake | 3.2.x | Bundled with Fabric Runtime 1.3 |
| Microsoft Fabric Runtime | 1.3 | Target deployment environment |
| Pydantic | 2.x | Configuration validation |

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
