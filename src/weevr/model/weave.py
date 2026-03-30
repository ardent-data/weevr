"""Weave domain model."""

from typing import Any

from pydantic import Field, model_validator

from weevr.model.audit import AuditTemplate
from weevr.model.base import FrozenBase
from weevr.model.column_set import ColumnSet
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

    when: str = Field(
        description=(
            "Condition expression string. Supports parameter references "
            "(${param.x}), built-in checks (table_exists(), table_empty(), "
            "row_count()), and simple boolean operators."
        ),
    )


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

    name: str = Field(
        default="",
        description=(
            "Thread name. Required for inline definitions; derived from "
            "filename stem for external references."
        ),
    )
    ref: str | None = Field(
        default=None,
        description=(
            "Path to an external .thread file, relative to project root. "
            "Mutually exclusive with inline definition."
        ),
    )
    dependencies: list[str] | None = Field(
        default=None,
        description=(
            "Explicit upstream thread names. Merged with auto-inferred "
            "dependencies from source/target path matching."
        ),
    )
    condition: ConditionSpec | None = Field(
        default=None,
        description=(
            "Optional condition for conditional execution. Thread runs "
            "only if the condition evaluates to True."
        ),
    )


class Weave(FrozenBase):
    """A collection of thread references with optional shared defaults."""

    name: str = Field(
        default="",
        description="Weave name, derived from filename stem for external references.",
    )
    qualified_key: str = Field(
        default="",
        description="Dot-separated namespace key, derived from directory path.",
    )
    config_version: str = Field(
        description="Configuration schema version. Must be '1.0'.",
    )
    threads: list[ThreadEntry] = Field(
        description="Ordered list of thread references in this weave.",
    )
    defaults: dict[str, Any] | None = Field(
        default=None,
        description="Default values inherited by all threads in this weave.",
    )
    params: dict[str, ParamSpec] | None = Field(
        default=None,
        description="Typed parameter declarations with defaults and validation.",
    )
    execution: ExecutionConfig | None = Field(
        default=None,
        description="Runtime execution settings (log level, tracing).",
    )
    naming: NamingConfig | None = Field(
        default=None,
        description="Naming normalization rules for columns and tables.",
    )
    lookups: dict[str, Lookup] | None = Field(
        default=None,
        description="Named reference datasets shared across threads in this weave.",
    )
    column_sets: dict[str, ColumnSet] | None = Field(
        default=None,
        description="Named external column mappings for bulk rename operations.",
    )
    variables: dict[str, VariableSpec] | None = Field(
        default=None,
        description="Typed variable declarations set by hook steps via set_var.",
    )
    pre_steps: list[HookStep] | None = Field(
        default=None,
        description="Hook steps executed before threads in this weave run.",
    )
    post_steps: list[HookStep] | None = Field(
        default=None,
        description="Hook steps executed after all threads in this weave complete.",
    )
    audit_templates: dict[str, AuditTemplate] | None = Field(
        default=None,
        description="Named audit column templates inherited by threads.",
    )

    @model_validator(mode="before")
    @classmethod
    def _normalize_thread_entries(cls, data: Any) -> Any:
        """Normalize thread list to ThreadEntry objects.

        Accepts both plain string entries and dict entries, ensuring backward
        compatibility with existing ``list[str]`` configs.
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
