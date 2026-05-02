"""weevr engine — thread, weave, and loom execution orchestration."""


# Federated namespace: weevr.engine spans both wheels — the pure-Python
# planner and result-formatter live in weevr-core; everything that
# touches Spark (executor, runner, cache, display, hooks, result types,
# etc.) and this package's __init__.py stay engine-side. The eager
# imports below pull weevr-core's contributions, so __path__ must
# include the core's engine/ directory before they execute. The same
# helper pattern is mirrored in every federated sub-package's
# __init__.py — see weevr/__init__.py for context.
def _extend_namespace_path() -> None:
    """Append sibling weevr.engine/ directories on sys.path to ``__path__``."""
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

from weevr.engine.cache_manager import CacheManager  # noqa: E402
from weevr.engine.display import DAGDiagram, FlowDiagram  # noqa: E402
from weevr.engine.executor import execute_thread  # noqa: E402
from weevr.engine.planner import ExecutionPlan, build_plan  # noqa: E402
from weevr.engine.result import LoomResult, ThreadResult, WeaveResult  # noqa: E402
from weevr.engine.runner import execute_loom, execute_weave  # noqa: E402

__all__ = [
    # Thread execution
    "execute_thread",
    "ThreadResult",
    # Planner
    "ExecutionPlan",
    "build_plan",
    # Display
    "DAGDiagram",
    "FlowDiagram",
    # Weave execution
    "execute_weave",
    "WeaveResult",
    # Loom execution
    "execute_loom",
    "LoomResult",
    # Cache management
    "CacheManager",
]
