"""Thread domain model."""

from typing import Any

from pydantic import Field, field_validator

from weevr.model.audit import AuditTemplate
from weevr.model.base import FrozenBase
from weevr.model.column_set import ColumnSet
from weevr.model.connection import OneLakeConnection
from weevr.model.execution import ExecutionConfig
from weevr.model.export import Export
from weevr.model.failure import FailureConfig
from weevr.model.hooks import HookStep
from weevr.model.keys import KeyConfig
from weevr.model.load import LoadConfig
from weevr.model.lookup import Lookup
from weevr.model.params import ParamSpec
from weevr.model.pipeline import Step
from weevr.model.source import Source
from weevr.model.target import Target
from weevr.model.validation import Assertion, ValidationRule
from weevr.model.variable import VariableSpec
from weevr.model.write import WriteConfig


class Thread(FrozenBase):
    """Complete domain model for a thread configuration.

    A thread is the smallest unit of work: one or more sources, a sequence
    of transformation steps, and a single target.
    """

    name: str = Field(
        default="",
        description="Thread name, derived from filename stem for external references.",
    )
    qualified_key: str = Field(
        default="",
        description="Dot-separated namespace key, derived from directory path.",
    )
    config_version: str = Field(
        description="Configuration schema version. Must be '1.0'.",
    )
    sources: dict[str, Source] = Field(
        description="Named data source declarations. At least one entry required.",
    )

    @field_validator("sources")
    @classmethod
    def _sources_non_empty(cls, v: dict[str, Source]) -> dict[str, Source]:
        if not v:
            raise ValueError("sources must contain at least one entry")
        return v

    steps: list[Step] = Field(
        default=[],
        description="Ordered list of transformation steps applied to source data.",
    )
    target: Target = Field(
        description="Write destination with column mapping and partitioning.",
    )
    write: WriteConfig | None = Field(
        default=None,
        description="Write mode and merge parameters for the target.",
    )
    keys: KeyConfig | None = Field(
        default=None,
        description="Business key, surrogate key, and change detection configuration.",
    )
    validations: list[ValidationRule] | None = Field(
        default=None,
        description="Pre-write data quality rules evaluated as Spark SQL expressions.",
    )
    assertions: list[Assertion] | None = Field(
        default=None,
        description="Post-execution assertions on the target dataset.",
    )
    load: LoadConfig | None = Field(
        default=None,
        description="Incremental load mode and watermark parameters.",
    )
    tags: list[str] | None = Field(
        default=None,
        description="User-defined tags for filtering and grouping threads.",
    )
    params: dict[str, ParamSpec] | None = Field(
        default=None,
        description="Typed parameter declarations with defaults and validation.",
    )
    defaults: dict[str, Any] | None = Field(
        default=None,
        description="Default values inherited by nested configuration blocks.",
    )
    failure: FailureConfig | None = Field(
        default=None,
        description="Per-thread failure handling policy.",
    )
    execution: ExecutionConfig | None = Field(
        default=None,
        description="Runtime execution settings (log level, tracing).",
    )
    cache: bool | None = Field(
        default=None,
        description="Whether to cache the thread's DataFrame after transformation.",
    )
    exports: list[Export] | None = Field(
        default=None,
        description="Secondary output destinations for thread data.",
    )
    lookups: dict[str, Lookup] | None = Field(
        default=None,
        description="Named reference datasets shared across this thread.",
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
        description="Hook steps executed before the thread's pipeline runs.",
    )
    post_steps: list[HookStep] | None = Field(
        default=None,
        description="Hook steps executed after the thread's pipeline completes.",
    )
    audit_templates: dict[str, AuditTemplate] | None = Field(
        default=None,
        description="Named audit column templates for reuse across threads.",
    )
    connections: dict[str, OneLakeConnection] | None = Field(
        default=None,
        description="Named connection definitions for resolving source and target paths.",
    )
