"""Static layer-direction lock: weevr-core never imports the engine wheel.

The two wheels share the ``weevr`` namespace, and several subpackages
(``weevr.engine``, ``weevr.config``, ``weevr.telemetry``) are split across
both. A module's import path therefore does not say which wheel ships it;
only the source tree holding its file does. This lock walks every module
under the weevr-core source tree with :mod:`ast` and fails on any import —
module-level, function-local, or under ``TYPE_CHECKING`` — whose target
module lives only in the engine wheel's source tree.

The runtime import-isolation locks (``test_no_pyspark_imports.py`` and the
dual-install smoke tests) remain the backstop for imports that execute; this
lock also sees the ones that don't run on the paths those tests exercise.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
CORE_SRC = REPO_ROOT / "packages" / "weevr-core" / "src"
ENGINE_SRC = REPO_ROOT / "packages" / "weevr" / "src"


@dataclass(frozen=True)
class LayerViolation:
    """One core-to-engine-wheel import edge."""

    file: Path
    line: int
    target: str

    def __str__(self) -> str:
        """Render as ``path:line -> module`` for the failure message."""
        return f"{self.file}:{self.line} -> {self.target}"


def _module_index(src: Path) -> dict[str, Path]:
    """Map each dotted module name under ``src`` to its file.

    A package's ``__init__.py`` maps to the package name itself.
    """
    index: dict[str, Path] = {}
    for path in src.rglob("*.py"):
        parts = list(path.relative_to(src).with_suffix("").parts)
        if parts[-1] == "__init__":
            parts.pop()
        if parts:
            index[".".join(parts)] = path
    return index


def _resolve_relative(module: str | None, level: int, current: str, is_package: bool) -> str:
    """Resolve a ``from``-import's base module to an absolute dotted name."""
    if level == 0:
        return module or ""
    package_parts = current.split(".") if is_package else current.split(".")[:-1]
    base_parts = package_parts[: len(package_parts) - (level - 1)]
    if module:
        base_parts.append(module)
    return ".".join(base_parts)


def _import_targets(
    tree: ast.AST, current: str, is_package: bool, known: set[str]
) -> list[tuple[int, str]]:
    """Return ``(line, module)`` for every import in ``tree``, wherever it sits.

    For ``from pkg import name``, the target is ``pkg.name`` when that is a
    known module (a submodule import), otherwise ``pkg``.
    """
    targets: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            targets.extend((node.lineno, alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = _resolve_relative(node.module, node.level, current, is_package)
            for alias in node.names:
                candidate = f"{base}.{alias.name}"
                targets.append((node.lineno, candidate if candidate in known else base))
    return targets


def _owner(target: str, core: dict[str, Path], engine: dict[str, Path]) -> str | None:
    """Return ``"core"``, ``"engine"``, or ``None`` for the module that ships ``target``.

    Walks up to the nearest known module, so an import of a missing submodule
    of an engine-only package still counts as engine.
    """
    parts = target.split(".")
    while parts:
        name = ".".join(parts)
        if name in core:
            return "core"
        if name in engine:
            return "engine"
        parts.pop()
    return None


def find_core_to_engine_imports(core_src: Path, engine_src: Path) -> list[LayerViolation]:
    """Return every import in ``core_src`` whose target ships only in ``engine_src``."""
    core = _module_index(core_src)
    engine = _module_index(engine_src)
    known = set(core) | set(engine)
    violations: list[LayerViolation] = []
    for name, path in sorted(core.items()):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        is_package = path.name == "__init__.py"
        for line, target in _import_targets(tree, name, is_package, known):
            if _owner(target, core, engine) == "engine":
                violations.append(LayerViolation(path.relative_to(core_src), line, target))
    # ``from m import a, b`` yields one target per name; report each edge once.
    return list(dict.fromkeys(violations))


def test_core_never_imports_engine_wheel() -> None:
    """No weevr-core module imports an engine-wheel module in any form."""
    violations = find_core_to_engine_imports(CORE_SRC, ENGINE_SRC)
    assert not violations, "weevr-core imports the engine wheel:\n" + "\n".join(
        str(v) for v in violations
    )


def _write(root: Path, rel: str, text: str = "") -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@pytest.fixture
def synthetic_trees(tmp_path: Path) -> tuple[Path, Path]:
    """Two source trees shaped like the real split: a federated ``weevr.engine``."""
    core = tmp_path / "core"
    engine = tmp_path / "engine"
    _write(core, "weevr/engine/planner.py")
    _write(core, "weevr/engine/formatting.py")
    _write(core, "weevr/model/__init__.py")
    _write(engine, "weevr/__init__.py")
    _write(engine, "weevr/engine/__init__.py")
    _write(engine, "weevr/engine/display.py")
    _write(engine, "weevr/operations/__init__.py")
    _write(engine, "weevr/operations/audit.py")
    return core, engine


@pytest.mark.parametrize(
    ("source", "expected_target"),
    [
        pytest.param("import weevr.operations.audit\n", "weevr.operations.audit", id="import"),
        pytest.param(
            "from weevr.operations.audit import inject\n",
            "weevr.operations.audit",
            id="from-module",
        ),
        pytest.param(
            "from weevr.operations import audit\n", "weevr.operations.audit", id="from-submodule"
        ),
        pytest.param(
            "from weevr.engine import display\n", "weevr.engine.display", id="federated-submodule"
        ),
        pytest.param("from weevr import Context\n", "weevr", id="engine-package-init"),
        pytest.param(
            "def f():\n    from weevr.engine.display import DAG\n",
            "weevr.engine.display",
            id="function-local",
        ),
        pytest.param(
            "from typing import TYPE_CHECKING\n"
            "if TYPE_CHECKING:\n    from weevr.engine.display import DAG\n",
            "weevr.engine.display",
            id="type-checking",
        ),
        pytest.param("from .display import DAG\n", "weevr.engine.display", id="relative"),
        pytest.param(
            "import weevr.operations.missing\n", "weevr.operations.missing", id="missing-submodule"
        ),
    ],
)
def test_lock_detects_each_import_form(
    synthetic_trees: tuple[Path, Path], source: str, expected_target: str
) -> None:
    """Each import form reaching the engine wheel is reported, so the lock can't pass vacuously."""
    core, engine = synthetic_trees
    _write(core, "weevr/engine/planner.py", source)
    violations = find_core_to_engine_imports(core, engine)
    assert [v.target for v in violations] == [expected_target]
    assert violations[0].file == Path("weevr/engine/planner.py")


@pytest.mark.parametrize(
    "source",
    [
        pytest.param("from weevr.engine.formatting import fmt\n", id="core-in-federated"),
        pytest.param("from weevr.engine import formatting\n", id="core-submodule-from-federated"),
        pytest.param("from .formatting import fmt\n", id="relative-core"),
        pytest.param("import weevr.model\nimport pydantic\n", id="core-and-third-party"),
    ],
)
def test_lock_allows_core_and_third_party_imports(
    synthetic_trees: tuple[Path, Path], source: str
) -> None:
    """Imports of core modules and third-party packages are not violations."""
    core, engine = synthetic_trees
    _write(core, "weevr/engine/planner.py", source)
    assert find_core_to_engine_imports(core, engine) == []
