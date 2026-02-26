# Contributing to weevr

Thanks for contributing to **weevr**. This repository is intentionally scaffold-first and design-driven.

## Development prerequisites

* **Python**: 3.11.x (aligned with Microsoft Fabric Runtime 1.3)
* **uv**: used for Python version pinning, dependency management, and execution
* **Git**: configured with your preferred name and email

This repository pins Python via `.python-version`. If you use uv to manage Python versions:

```bash
uv python install 3.11
uv python pin 3.11
```

## Initial setup

Create or sync the local virtual environment and install development dependencies:

```bash
uv sync --dev
```

This will create a local `.venv` directory (ignored by git) and install all required tooling.

## Running checks locally

Before opening a pull request, ensure the following checks pass locally.

### Linting

```bash
uv run ruff check .
```

### Formatting

```bash
uv run ruff format .
```

### Type checking

```bash
uv run pyright .
```

### Tests

```bash
uv run pytest
```

### Documentation build

```bash
uv run mkdocs build --strict
```

### Docstring coverage

```bash
uv run interrogate src/weevr/ --fail-under 90
```

All checks run in CI use the same commands and are pinned to the same Python version.

## Documentation

Documentation source files live in `docs/` and are built with [MkDocs](https://www.mkdocs.org/). Site configuration is defined in `mkdocs.yml`.

### Preview locally

Start a local dev server that watches for changes:

```bash
uv run mkdocs serve
```

This opens a preview at [http://127.0.0.1:8000](http://127.0.0.1:8000).

### Build and validate

Run a strict build to catch warnings and broken links:

```bash
uv run mkdocs build --strict
```

### Adding a new page

1. Create a new `.md` file under `docs/` (or a subdirectory)
2. Add the file path to the `nav:` section in `mkdocs.yml`
3. Verify the page renders correctly with `uv run mkdocs serve`

### Markdown linting

```bash
npx markdownlint-cli2 "docs/**/*.md"
```

### Spellcheck

```bash
npx cspell "docs/**/*.md"
```

If a term is flagged incorrectly, add it to `.cspell.json`.

## Docstring standards

All public modules, classes, functions, and methods require docstrings following [Google style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings).

### Required structure

* **One-line summary**: imperative mood, ending with a period.
* **Args**: document each parameter with name, type, and description.
* **Returns**: describe the return value and type.
* **Raises**: list exceptions the function may raise.
* **Example**: optional usage snippet for non-trivial APIs.

### Example

```python
def resolve_thread(name: str, config: dict[str, Any]) -> Thread:
    """Resolve a thread definition from raw config.

    Args:
        name: Unique thread identifier within the weave.
        config: Raw YAML-parsed dictionary for this thread.

    Returns:
        A fully resolved Thread with validated source, transforms, and target.

    Raises:
        ConfigError: If required keys are missing or values are invalid.

    Example:
        >>> thread = resolve_thread("orders", raw_config["threads"]["orders"])
        >>> thread.target.mode
        'overwrite'
    """
```

### Enforcement

* **Ruff D-series rules** check docstring format and content at lint time.
* **interrogate** measures docstring coverage across the package. The CI threshold is 90%.

## Adding examples

Example YAML configurations live in `examples/`. Each example should be a valid weevr config that includes at minimum:

```yaml
config_version: "1.0"
name: my-example
```

### Guidelines

* Keep examples focused on a single concept or use case
* Include inline comments explaining non-obvious settings
* Examples are validated in CI via `tests/weevr/test_examples.py`
* When adding a new example, update `examples/README.md` with a short description

## A note on documentation contributions

Documentation updates are welcome but not required from external contributors. Maintainers handle doc maintenance for code changes, so feel free to focus on code and tests.

## Branching model

* Default branch: `main`
* Create feature branches from `main`
* Keep branches short-lived and focused

Recommended branch name patterns:

* `feat/<topic>`
* `fix/<topic>`
* `chore/<topic>`
* `docs/<topic>`
* `ci/<topic>`

## Pull request guidelines

### Scope and size

* One logical change per pull request
* Avoid mixing unrelated concerns (for example, tooling + design + behavior changes)
* If a change affects CI, release configuration, or repository scaffolding, keep the PR strictly scoped to that concern

### Pull request titles (Release Please)

This repository uses **Release Please** for versioning and changelog generation. Release notes are primarily derived from **pull request titles**.

Pull request titles **must** follow Conventional Commit style:

* `feat: <summary>` – new feature
* `fix: <summary>` – bug fix
* `docs: <summary>` – documentation only
* `chore: <summary>` – maintenance or tooling
* `refactor: <summary>` – refactoring without behavior change
* `test: <summary>` – tests only
* `ci: <summary>` – CI or GitHub workflow changes

Examples:

* `feat: add compile command scaffolding`
* `fix: pin python version in CI`
* `chore: add Release Please workflow`

### Breaking changes

If a change introduces a breaking change, indicate it explicitly using one of the following:

* Add `!` after the type: `feat!: <summary>` or `fix!: <summary>`
* Include a `BREAKING CHANGE:` section in the pull request description

### Merge strategy

Preferred merge strategy is **Squash and merge** so the pull request title becomes the authoritative release note entry.

## Commit sign-off (DCO)

This repository **enforces the Developer Certificate of Origin (DCO)**. All commits included in a pull request **must** contain a `Signed-off-by:` line.

The DCO check is enforced via a GitHub App and is a required status check for merging into `main`.

### How to sign off commits

To sign off a commit manually:

```bash
git commit -s -m "chore: update CI"
```

To automatically sign off all future commits:

```bash
git config --global format.signoff true
```

### Fixing a missing sign-off

If a commit was created without a sign-off, you can amend it:

```bash
git commit --amend --signoff
git push --force-with-lease
```

For multiple commits, use an interactive rebase and amend each commit before force-pushing.

Commits created via the GitHub web UI or certain merge strategies may not include a sign-off. Contributors are encouraged to use local commits and the Squash and merge strategy to avoid DCO issues.

## Code of Conduct

By participating in this project, you agree to follow the repository Code of Conduct. See `CODE_OF_CONDUCT.md` for details.
