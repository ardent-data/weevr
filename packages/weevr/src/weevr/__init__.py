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
