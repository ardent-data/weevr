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


def test_import_thread_model_without_pyspark() -> None:
    """``from weevr.model.thread import Thread`` must succeed with PySpark blocked."""
    with _block_pyspark():
        thread_module = importlib.import_module("weevr.model.thread")
        assert hasattr(thread_module, "Thread")


def test_import_reserved_words_shim_without_pyspark() -> None:
    """The ``operations.reserved_words`` shim path must resolve without PySpark.

    This covers the import path that ``weevr.operations.naming`` (engine-side)
    will continue to use after the canonical module relocates to
    ``weevr.reserved_words`` in Phase B's pure ``git mv``.
    """
    with _block_pyspark():
        rw = importlib.import_module("weevr.operations.reserved_words")
        assert hasattr(rw, "resolve_effective_words")


def test_load_config_without_pyspark() -> None:
    """``load_config`` on a thread fixture with rename strategy succeeds.

    Exercises both audit-template merging (via the inheritance cascade)
    and ``column_set._validate_rename_strategy``, which performs a
    function-local import of ``resolve_effective_words`` through the
    operations.reserved_words shim. Failure here means at least one of
    the two pure-helper extraction paths still pulls in PySpark.
    """
    with _block_pyspark():
        config_module = importlib.import_module("weevr.config")
        result = config_module.load_config(FIXTURE_PATH)
        assert result is not None
        assert getattr(result, "name", None) == "lock_thread"


def test_validate_rename_strategy_without_engine_wheel() -> None:
    """``ReservedWordConfig(strategy="rename")`` must validate in a core-only install.

    Regression test for the layer-direction defect where
    ``column_set._validate_rename_strategy`` chained through the engine-wheel
    shim ``weevr.operations.reserved_words`` instead of the canonical core
    module ``weevr.reserved_words``. In a ``weevr-core``-only install (engine
    wheel absent), the function-local import raised ImportError.
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
