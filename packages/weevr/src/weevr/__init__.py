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
# basename(p) == "src" gate restricts the scan to src-layout package
# roots so unrelated sys.path entries (test fixtures, scripts, …) do
# not get mistaken for additional weevr namespace contributions. The
# same pattern is mirrored in every federated sub-package's __init__.py
# (weevr.config, weevr.engine, weevr.telemetry).
def _extend_namespace_path() -> None:
    import os.path as osp
    import sys

    components = __name__.split(".")
    for entry in sys.path:
        if not entry or osp.basename(entry) != "src":
            continue
        candidate = osp.join(entry, *components)
        if osp.isdir(candidate) and candidate not in __path__:
            __path__.append(candidate)


_extend_namespace_path()
del _extend_namespace_path
