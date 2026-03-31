"""Loom domain model."""

from typing import Any

from pydantic import Field, model_validator

from weevr.model.audit import AuditTemplate
from weevr.model.base import FrozenBase
from weevr.model.column_set import ColumnSet
from weevr.model.connection import OneLakeConnection
from weevr.model.execution import ExecutionConfig
from weevr.model.hooks import HookStep
from weevr.model.lookup import Lookup
from weevr.model.naming import NamingConfig
from weevr.model.params import ParamSpec
from weevr.model.variable import VariableSpec
from weevr.model.weave import ConditionSpec


class WeaveEntry(FrozenBase):
    """A weave reference within a loom, with optional condition.

    Attributes:
        name: Weave name. Required for inline definitions; derived from
            filename stem for external references.
        ref: Path to an external ``.weave`` file, relative to project root.
            Mutually exclusive with inline definition (name + body).
        condition: Optional condition for conditional execution.
    """

    name: str = Field(
        default="",
        description=(
            "Weave name. Required for inline definitions; derived from "
            "filename stem for external references."
        ),
    )
    ref: str | None = Field(
        default=None,
        description=(
            "Path to an external .weave file, relative to project root. "
            "Mutually exclusive with inline definition."
        ),
    )
    condition: ConditionSpec | None = Field(
        default=None,
        description="Optional condition for conditional execution.",
    )


class Loom(FrozenBase):
    """A deployment unit containing weave references with optional shared defaults."""

    name: str = Field(
        default="",
        description="Loom name, derived from filename stem for external references.",
    )
    qualified_key: str = Field(
        default="",
        description="Dot-separated namespace key, derived from directory path.",
    )
    config_version: str = Field(
        description="Configuration schema version. Must be '1.0'.",
    )
    weaves: list[WeaveEntry] = Field(
        description="Ordered list of weave references in this loom.",
    )
    defaults: dict[str, Any] | None = Field(
        default=None,
        description=("Default values inherited by all weaves and threads in this loom."),
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
    column_sets: dict[str, ColumnSet] | None = Field(
        default=None,
        description="Named external column mappings for bulk rename operations.",
    )
    lookups: dict[str, Lookup] | None = Field(
        default=None,
        description="Named reference datasets shared across all weaves.",
    )
    variables: dict[str, VariableSpec] | None = Field(
        default=None,
        description="Typed variable declarations set by hook steps via set_var.",
    )
    pre_steps: list[HookStep] | None = Field(
        default=None,
        description="Hook steps executed before weaves in this loom run.",
    )
    post_steps: list[HookStep] | None = Field(
        default=None,
        description="Hook steps executed after all weaves in this loom complete.",
    )
    audit_templates: dict[str, AuditTemplate] | None = Field(
        default=None,
        description="Named audit column templates inherited by all weaves and threads.",
    )
    connections: dict[str, OneLakeConnection] | None = Field(
        default=None,
        description="Named connection definitions for resolving source and target paths.",
    )

    @model_validator(mode="before")
    @classmethod
    def _normalize_weave_entries(cls, data: Any) -> Any:
        """Normalize weave list to WeaveEntry objects.

        Accepts both plain string entries and dict entries, ensuring backward
        compatibility with existing ``list[str]`` configs.
        """
        if not isinstance(data, dict):
            return data
        weaves = data.get("weaves")
        if not isinstance(weaves, list):
            return data
        normalized = []
        for entry in weaves:
            if isinstance(entry, str):
                normalized.append({"name": entry})
            elif isinstance(entry, dict):
                normalized.append(entry)
            else:
                normalized.append(entry)
        return {**data, "weaves": normalized}
