"""Configuration-driven execution framework for Spark in Microsoft Fabric."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from weevr.context import Context
    from weevr.result import ExecutionMode, LoadedConfig, RunResult

__all__ = ["Context", "ExecutionMode", "LoadedConfig", "RunResult"]


def __getattr__(name: str) -> object:
    if name == "Context":
        from weevr.context import Context

        return Context
    if name in {"ExecutionMode", "LoadedConfig", "RunResult"}:
        from weevr import result

        return getattr(result, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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
# (weevr.config today; weevr.engine and weevr.telemetry once their
# pure-Python submodules relocate to weevr-core).
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
