"""Configuration-driven execution framework for Spark in Microsoft Fabric."""

import os.path as _osp
import sys as _sys
from typing import TYPE_CHECKING

# Federated namespace: weevr ships across two wheels (weevr-core for
# pure-Python validation, planner, and telemetry contracts; weevr for
# the Spark engine). In a normal install both wheels' files merge into
# a single site-packages/weevr/ directory and this loop is a no-op. In
# an editable workspace install each wheel adds its own src/ to sys.path
# separately, so we extend __path__ to pull sibling weevr/ directories
# (notably weevr-core's contributions) into the import-time view. The
# basename(p) == "src" gate restricts the scan to src-layout package
# roots so unrelated sys.path entries (test fixtures, scripts/, …) do
# not get mistaken for additional weevr namespace contributions.
__path__ = list(__path__) + [
    _osp.join(_p, "weevr")
    for _p in _sys.path
    if _p
    and _osp.basename(_p) == "src"
    and _osp.isdir(_osp.join(_p, "weevr"))
    and _osp.join(_p, "weevr") not in __path__
]

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
