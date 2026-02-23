"""Weave domain model."""

from typing import Any

from pydantic import model_validator

from weevr.model.base import FrozenBase
from weevr.model.execution import ExecutionConfig
from weevr.model.params import ParamSpec


class ThreadEntry(FrozenBase):
    """A thread reference within a weave, with optional per-thread overrides.

    Attributes:
        name: Thread name as declared in the weave config.
        dependencies: Explicit upstream thread names. When set, these are merged
            with any auto-inferred dependencies from source/target path matching.
    """

    name: str
    dependencies: list[str] | None = None


class Weave(FrozenBase):
    """A collection of thread references with optional shared defaults."""

    name: str = ""
    config_version: str
    threads: list[ThreadEntry]
    defaults: dict[str, Any] | None = None
    params: dict[str, ParamSpec] | None = None
    execution: ExecutionConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_thread_entries(cls, data: Any) -> Any:
        """Normalize thread list to ThreadEntry objects.

        Accepts both plain string entries and dict entries, ensuring backward
        compatibility with existing ``list[str]`` configs (DEC-006).
        """
        if not isinstance(data, dict):
            return data
        threads = data.get("threads")
        if not isinstance(threads, list):
            return data
        normalized = []
        for entry in threads:
            if isinstance(entry, str):
                normalized.append({"name": entry})
            else:
                normalized.append(entry)
        return {**data, "threads": normalized}
