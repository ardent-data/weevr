"""Import-isolation locks for the validation API.

Two regression locks live here, each backed by a ``sys.meta_path`` finder
that rejects a specific import namespace:

- **No-PySpark lock** (Phase A of R-001): rejects every ``pyspark*`` import
  and asserts that the public validation surface remains usable. Lets
  external tooling (CLI, docs generation) consume ``weevr.config`` without
  paying for Spark.
- **No-engine-wheel lock** (Phase B of R-001): rejects every
  ``weevr.operations*`` import and asserts that the core wheel
  (``weevr-core``) validates configuration without the engine wheel
  installed. Locks in the core/engine layer-direction invariant.

Both must remain green for any future change.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.abc import MetaPathFinder
from importlib.machinery import ModuleSpec
from pathlib import Path

import pytest


class _PysparkBlocker(MetaPathFinder):
    """Reject every ``pyspark*`` module name with ImportError."""

    def find_spec(
        self,
        fullname: str,
        _path: object = None,
        _target: object = None,
    ) -> ModuleSpec | None:
        if fullname == "pyspark" or fullname.startswith("pyspark."):
            raise ImportError(f"PySpark import blocked by lock test: {fullname!r}")
        return None


class _OperationsBlocker(MetaPathFinder):
    """Reject every ``weevr.operations*`` module name with ImportError.

    Simulates a ``weevr-core``-only install where the engine wheel
    (which owns ``weevr/operations/``) is absent.
    """

    def find_spec(
        self,
        fullname: str,
        _path: object = None,
        _target: object = None,
    ) -> ModuleSpec | None:
        if fullname == "weevr.operations" or fullname.startswith("weevr.operations."):
            raise ImportError(
                f"weevr.operations import blocked by core-only lock test: {fullname!r}",
            )
        return None


@contextmanager
def _block_pyspark() -> Iterator[None]:
    """Install a PySpark import blocker and clear cached pyspark modules.

    Snapshots ``sys.modules`` and ``sys.meta_path`` on entry, evicts
    every ``pyspark*`` and ``weevr*`` module so reload semantics are
    honored, and restores both on exit. Restoring the full snapshots
    prevents the blocker from leaking into later tests in the same
    session.
    """
    saved_modules = dict(sys.modules)
    saved_meta_path = list(sys.meta_path)

    for name in list(sys.modules):
        if (
            name == "pyspark"
            or name.startswith("pyspark.")
            or name == "weevr"
            or name.startswith("weevr.")
        ):
            del sys.modules[name]

    blocker = _PysparkBlocker()
    sys.meta_path.insert(0, blocker)

    try:
        yield
    finally:
        sys.meta_path[:] = saved_meta_path
        sys.modules.clear()
        sys.modules.update(saved_modules)


@contextmanager
def _block_engine_wheel() -> Iterator[None]:
    """Install a ``weevr.operations`` import blocker and clear cached weevr modules.

    Snapshots ``sys.modules`` and ``sys.meta_path`` on entry, evicts every
    ``weevr*`` module so reload semantics are honored, and restores both on
    exit. Simulates a ``weevr-core``-only install in-process without
    requiring a separate venv.
    """
    saved_modules = dict(sys.modules)
    saved_meta_path = list(sys.meta_path)

    for name in list(sys.modules):
        if name == "weevr" or name.startswith("weevr."):
            del sys.modules[name]

    blocker = _OperationsBlocker()
    sys.meta_path.insert(0, blocker)

    try:
        yield
    finally:
        sys.meta_path[:] = saved_meta_path
        sys.modules.clear()
        sys.modules.update(saved_modules)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "r001_phase_a" / "lock_thread.thread"


def test_import_weevr_without_pyspark() -> None:
    """``import weevr`` must succeed with PySpark blocked."""
    with _block_pyspark():
        weevr = importlib.import_module("weevr")
        assert weevr is not None


def test_import_load_config_without_pyspark() -> None:
    """``from weevr.config import load_config`` must succeed with PySpark blocked."""
    with _block_pyspark():
        config_pkg = importlib.import_module("weevr.config")
        assert hasattr(config_pkg, "load_config")


def test_import_resolved_document_without_pyspark() -> None:
    """The resolved config tree types import with PySpark blocked."""
    with _block_pyspark():
        config_pkg = importlib.import_module("weevr.config")
        assert hasattr(config_pkg, "ResolvedDocument")
        assert hasattr(config_pkg, "EntryMeta")
        document = importlib.import_module("weevr.config.document")
        assert document.ResolvedDocument is config_pkg.ResolvedDocument


def test_import_thread_model_without_pyspark() -> None:
    """``from weevr.model.thread import Thread`` must succeed with PySpark blocked."""
    with _block_pyspark():
        thread_module = importlib.import_module("weevr.model.thread")
        assert hasattr(thread_module, "Thread")


def test_import_reserved_words_without_pyspark() -> None:
    """``weevr.reserved_words`` must resolve without PySpark.

    ``weevr.operations.naming`` (engine-side) imports it at module level.
    """
    with _block_pyspark():
        rw = importlib.import_module("weevr.reserved_words")
        assert hasattr(rw, "resolve_effective_words")


def test_load_config_without_pyspark() -> None:
    """``load_config`` on a thread fixture with rename strategy succeeds.

    Exercises both audit-template merging (via the inheritance cascade)
    and ``column_set._validate_rename_strategy``, which performs a
    function-local import of ``resolve_effective_words`` from
    ``weevr.reserved_words``. Failure here means at least one of the two
    pure-helper paths pulls in PySpark.
    """
    with _block_pyspark():
        config_module = importlib.import_module("weevr.config")
        result = config_module.load_config(FIXTURE_PATH)
        assert result is not None
        assert getattr(result, "name", None) == "lock_thread"


def test_validate_rename_strategy_without_engine_wheel() -> None:
    """``ReservedWordConfig(strategy="rename")`` must validate in a core-only install.

    Regression test for a layer-direction defect where
    ``column_set._validate_rename_strategy`` imported ``resolve_effective_words``
    through the engine wheel instead of the core module ``weevr.reserved_words``.
    In a ``weevr-core``-only install (engine wheel absent), the function-local
    import raised ImportError.
    """
    with _block_engine_wheel():
        column_set = importlib.import_module("weevr.model.column_set")
        config = column_set.ReservedWordConfig(
            strategy="rename",
            rename_map={"order": "order_id"},
        )
        assert config.strategy == "rename"


def test_pyspark_blocker_cleanup() -> None:
    """After the context manager exits, PySpark imports must work again.

    Guards against the blocker leaking into other tests in the same
    pytest session (which would surface as flaky CI elsewhere).
    """
    with _block_pyspark(), pytest.raises(ImportError):
        importlib.import_module("pyspark.sql")

    pyspark_sql = importlib.import_module("pyspark.sql")
    assert pyspark_sql is not None


FORMS_PROJECT = Path(__file__).parent / "weevr" / "fixtures" / "load_parity" / "forms.weevr"


def _tree_summary(root: Path) -> dict[str, object]:
    """Load the forms loom through the loader and reduce it to plain data.

    Uses the loader's own entry point: ``load_resolved`` wraps the result in
    ``LoadedConfig``, whose plan lives in ``weevr.engine``, and in a full
    install the engine wheel's ``weevr.engine`` package needs PySpark. The
    plan side is covered by the core-only install smoke
    (``tests/integration/test_dual_install_modes.py``).
    """
    loader = importlib.import_module("weevr.config.loader")
    locations = importlib.import_module("weevr.config.locations")
    document, warnings = loader.load_resolved_config(
        locations.LocalConfigLocation(root / "forms.loom"),
        project_root=locations.LocalConfigLocation(root),
    )

    def registries(scope: object) -> dict[str, object]:
        return {
            kind: {name: value.model_dump(mode="json") for name, value in registry.items()}
            for kind in ("lookups", "column_sets", "variables")
            if (registry := getattr(scope, kind)) is not None
        }

    def node(doc: object) -> dict[str, object]:
        return {
            "kind": doc.kind,  # type: ignore[attr-defined]
            "name": doc.name,  # type: ignore[attr-defined]
            "model": doc.model.model_dump(mode="json"),  # type: ignore[attr-defined]
            "effective": registries(doc.effective),  # type: ignore[attr-defined]
            "children": [node(c) for c in doc.children.values()],  # type: ignore[attr-defined]
        }

    return {"tree": node(document), "warnings": list(warnings)}


def _copy_forms(tmp_path: Path) -> Path:
    import shutil

    root = tmp_path / "forms.weevr"
    shutil.copytree(FORMS_PROJECT, root)
    return root


def test_full_project_tree_without_pyspark(tmp_path: Path) -> None:
    """A multi-weave loom with templates, lookups and column sets resolves without PySpark.

    The document tree and every node's effective registries match the same
    load with PySpark available.
    """
    root = _copy_forms(tmp_path)
    with_spark = _tree_summary(root)
    with _block_pyspark():
        without_spark = _tree_summary(root)
        assert "pyspark" not in sys.modules
    assert without_spark == with_spark
    assert len(with_spark["tree"]["children"]) == 3  # type: ignore[index]


def test_full_project_tree_without_engine_wheel(tmp_path: Path) -> None:
    """The same load runs with the engine wheel absent (a weevr-core-only install)."""
    root = _copy_forms(tmp_path)
    with_engine = _tree_summary(root)
    with _block_engine_wheel():
        assert _tree_summary(root) == with_engine
