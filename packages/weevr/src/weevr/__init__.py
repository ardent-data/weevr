"""Configuration-driven execution framework for Spark in Microsoft Fabric."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from weevr.context import Context
    from weevr.result import ExecutionMode, LoadedConfig, RunResult

__all__ = ["Context", "ExecutionMode", "LoadedConfig", "RunResult"]

# Map each public attribute to the submodule that defines it. Lazy access
# preserves Phase A's no-pyspark contract: importing weevr does not load
# weevr.context (and therefore PySpark) unless one of these attributes is
# actually accessed. The map is checked against __all__ by
# tests/weevr/test_lazy_init_consistency.py to keep the two in sync.
_LAZY_ATTRS: dict[str, str] = {
    "Context": "weevr.context",
    "ExecutionMode": "weevr.result",
    "LoadedConfig": "weevr.result",
    "RunResult": "weevr.result",
}


def __getattr__(name: str) -> object:
    module_path = _LAZY_ATTRS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, name)


# Federated namespace: weevr ships across two wheels (weevr-core for
# pure-Python validation, planner, and telemetry contracts; weevr for
# the Spark engine). In a normal install both wheels' files merge into
# a single site-packages/weevr/ directory and this loop is a no-op. In
# an editable workspace install each wheel adds its own src/ to sys.path
# separately, so we extend __path__ to pull sibling weevr/ directories
# (notably weevr-core's contributions) into the import-time view. The
# scan counts only src-layout roots that hold a weevr/ package, so
# unrelated sys.path entries (test fixtures, scripts, other editable
# src-layout packages, PYTHONPATH=src) are neither mistaken for weevr
# contributions nor read as a partial install. The
# same pattern is mirrored in every federated sub-package's __init__.py
# (weevr.config, weevr.engine, weevr.telemetry).
def _extend_namespace_path() -> None:
    """Append sibling weevr/ directories on sys.path to ``__path__``."""
    import os.path as osp
    import sys

    components = __name__.split(".")
    src_entries_seen = 0
    candidates_added = 0
    for entry in sys.path:
        # Only weevr's own src-layout roots count: an unrelated src directory
        # on the path (another editable package, PYTHONPATH=src) says nothing
        # about whether this workspace is partially installed.
        own_root = (
            bool(entry) and osp.basename(entry) == "src" and osp.isdir(osp.join(entry, "weevr"))
        )
        if not own_root:
            continue
        src_entries_seen += 1
        candidate = osp.join(entry, *components)
        if osp.isdir(candidate) and candidate not in __path__:
            __path__.append(candidate)
            candidates_added += 1
    # In an editable workspace install (the only mode where src/
    # entries appear on sys.path) we expect at least one sibling
    # contribution beyond the directory Python pre-populates in
    # __path__. If none is found, the install is likely partial and
    # the next cross-wheel submodule import will fail with an
    # unhelpful traceback. Surface a warning instead.
    if src_entries_seen > 0 and candidates_added == 0:
        import warnings

        warnings.warn(
            f"weevr federated namespace {__name__!r}: no sibling "
            "contributor found on sys.path. If this is an editable "
            "workspace install, run `uv sync --dev` from the repo root "
            "to install both wheels.",
            stacklevel=2,
        )


_extend_namespace_path()
del _extend_namespace_path
