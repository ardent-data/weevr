"""Thread domain model."""

import logging
from typing import Any

from pydantic import Field, field_validator, model_validator

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
from weevr.model.pipeline import JoinStep, Step
from weevr.model.source import Source
from weevr.model.sub_pipeline import SubPipeline
from weevr.model.target import Target
from weevr.model.validation import Assertion, ValidationRule
from weevr.model.variable import VariableSpec
from weevr.model.write import WriteConfig

logger = logging.getLogger(__name__)


class Thread(FrozenBase):
    """Complete domain model for a thread configuration.

    A thread is the smallest unit of work: one or more sources, a sequence
    of transformation steps, and a single target.
    """

    model_config = {"frozen": True, "populate_by_name": True}

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

    with_: dict[str, SubPipeline] | None = Field(
        default=None,
        alias="with",
        description=(
            "Named sub-pipelines (CTEs) evaluated before the main pipeline. "
            "Each entry declares an intermediate DataFrame by name."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _remap_with_key(cls, data: Any) -> Any:
        """Remap YAML ``with`` to Python ``with_``."""
        if isinstance(data, dict) and "with" in data and "with_" not in data:
            data = {**data, "with_": data.pop("with")}
        return data

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

    @model_validator(mode="after")
    def _validate_with_block(self) -> "Thread":
        """Validate the with: block for name collisions, forward refs, and alias uniqueness."""
        if self.with_ is None:
            # Still check join alias uniqueness even without a with: block
            self._check_join_alias_uniqueness(set(self.sources.keys()), {})
            return self

        source_names = set(self.sources.keys())
        cte_names = set(self.with_.keys())

        # 1. Name collision: CTE names must not overlap with source names
        collisions = cte_names & source_names
        if collisions:
            collision_list = ", ".join(sorted(collisions))
            raise ValueError(f"with: CTE name(s) collide with source name(s): {collision_list}")

        # 2. Forward reference check: iterate CTEs in declaration order
        available: set[str] = set(source_names)
        for cte_name, sub_pipeline in self.with_.items():
            if sub_pipeline.from_ not in available:
                raise ValueError(
                    f"with: CTE '{cte_name}' references '{sub_pipeline.from_}' which is not "
                    f"declared above it (forward reference or unknown name)"
                )
            available.add(cte_name)

        # 3. Unused CTE warning: collect all source references from join/union steps
        referenced: set[str] = set()
        all_step_lists = list(self.with_.values()) + [self]
        for container in all_step_lists:
            steps_list = container.steps if hasattr(container, "steps") else []
            for step in steps_list:
                if isinstance(step, JoinStep):
                    referenced.add(step.join.source)
                # UnionStep sources field
                step_type = type(step).__name__
                if step_type == "UnionStep":
                    for src in step.union.sources:  # type: ignore[attr-defined]
                        referenced.add(src)

        for cte_name in cte_names:
            if cte_name not in referenced:
                logger.warning("with: CTE '%s' is declared but never referenced", cte_name)

        # 4. Join alias uniqueness across all steps (CTE pipelines + main steps)
        namespace = source_names | cte_names
        self._check_join_alias_uniqueness(namespace, {})

        return self

    def _check_join_alias_uniqueness(self, namespace: set[str], _unused: dict[str, str]) -> None:
        """Check that join aliases are unique and do not collide with the source/CTE namespace."""
        seen_aliases: dict[str, str] = {}

        step_containers: list[list[Any]] = [list(self.steps)]
        if self.with_ is not None:
            for sub in self.with_.values():
                step_containers.append(list(sub.steps))

        for steps in step_containers:
            for step in steps:
                if isinstance(step, JoinStep) and step.join.alias is not None:
                    alias = step.join.alias
                    if alias in namespace:
                        raise ValueError(f"join alias '{alias}' collides with a source or CTE name")
                    if alias in seen_aliases:
                        raise ValueError(
                            f"join alias '{alias}' is used more than once across the thread"
                        )
                    seen_aliases[alias] = alias
