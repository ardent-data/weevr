"""Weave domain model."""

from typing import Any

from pydantic import model_validator

from weevr.model.base import FrozenBase
from weevr.model.execution import ExecutionConfig
from weevr.model.hooks import HookStep
from weevr.model.lookup import Lookup
from weevr.model.naming import NamingConfig
from weevr.model.params import ParamSpec
from weevr.model.variable import VariableSpec


class ConditionSpec(FrozenBase):
    """A condition expression for conditional execution.

    Attributes:
        when: Condition expression string. Supports parameter references
            (``${param.x}``), built-in checks (``table_exists()``,
            ``table_empty()``, ``row_count()``), and simple boolean operators.
    """

    when: str


class ThreadEntry(FrozenBase):
    """A thread reference within a weave, with optional per-thread overrides.

    Attributes:
        name: Thread name. Required for inline definitions; derived from
            filename stem for external references.
        ref: Path to an external ``.thread`` file, relative to project root.
            Mutually exclusive with inline definition (name + body).
        dependencies: Explicit upstream thread names. When set, these are merged
            with any auto-inferred dependencies from source/target path matching.
        condition: Optional condition for conditional execution. When set, the
            thread is only executed if the condition evaluates to True.
    """

    name: str = ""
    ref: str | None = None
    dependencies: list[str] | None = None
    condition: ConditionSpec | None = None


class Weave(FrozenBase):
    """A collection of thread references with optional shared defaults."""

    name: str = ""
    qualified_key: str = ""
    config_version: str
    threads: list[ThreadEntry]
    defaults: dict[str, Any] | None = None
    params: dict[str, ParamSpec] | None = None
    execution: ExecutionConfig | None = None
    naming: NamingConfig | None = None
    lookups: dict[str, Lookup] | None = None
    variables: dict[str, VariableSpec] | None = None
    pre_steps: list[HookStep] | None = None
    post_steps: list[HookStep] | None = None

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
            elif isinstance(entry, dict):
                normalized.append(entry)
            else:
                normalized.append(entry)
        return {**data, "threads": normalized}
