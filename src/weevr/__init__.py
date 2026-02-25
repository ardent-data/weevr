"""Configuration-driven execution framework for Spark in Microsoft Fabric."""

from weevr.context import Context
from weevr.result import ExecutionMode, LoadedConfig, RunResult

__all__ = ["Context", "ExecutionMode", "LoadedConfig", "RunResult"]


def main() -> None:
    print("Hello from weevr!")
