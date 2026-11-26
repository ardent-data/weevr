"""The project document (``weevr.yaml``): identity, engine floor and run-store settings."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from pydantic import ConfigDict, Field, field_validator

from weevr.diagnostics.rules import rule_error
from weevr.model.base import FrozenBase
from weevr.model.params import ParamSpec

SCHEMA_SAFE_NAME = r"^[A-Za-z][A-Za-z0-9_]*$"
"""A name usable as a SQL schema name or part of one: a letter, then letters, digits or ``_``."""

SCHEMA_SAFE_NAME_MAX = 100
"""The longest schema-safe name accepted."""


class RunStore(FrozenBase):
    """Where runs write their output, and in which layout.

    Every field applies to both layouts: ``connection`` moves the store's
    base in either, and ``schema`` names the shared schema or overrides the
    per-project one.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    connection: str | None = Field(
        default=None,
        min_length=1,
        description="Name of the connection whose lakehouse holds the run store.",
    )
    layout: Literal["per_project", "shared"] = Field(
        default="per_project",
        description=(
            "'per_project' gives the project its own tables; 'shared' writes to one schema "
            "that several projects share, each row carrying its project_id."
        ),
    )
    schema_: str | None = Field(
        default=None,
        alias="schema",
        min_length=1,
        max_length=SCHEMA_SAFE_NAME_MAX,
        pattern=SCHEMA_SAFE_NAME,
        description=(
            "Schema name. shared: the shared schema (default 'weevr'); "
            "per_project: overrides the name derived from the project name."
        ),
    )


class Project(FrozenBase):
    """A project's root document, ``weevr.yaml``.

    Identifies the project, optionally pins the weevr versions it runs on,
    says where its run store is, and declares the params every config in the
    project sees. Its ``params`` are the outermost tier of the spec cascade:
    runtime params bind over their defaults, and a loom, weave or thread
    declaring the same name shadows them. The other project-scope blocks
    (``connections``, ``defaults``, ``audit_templates``) are reserved for a
    later release and rejected at load.
    """

    model_config = ConfigDict(extra="forbid")

    kind: Literal["project"] = Field(
        default="project",
        description="Document kind. A project document declares 'project'.",
    )
    config_version: str = Field(description="Schema version of the document (e.g. '2.0').")
    id: UUID = Field(
        description="The project's GUID, generated once and never derived from Fabric IDs.",
    )
    name: str = Field(
        min_length=1,
        max_length=SCHEMA_SAFE_NAME_MAX,
        pattern=SCHEMA_SAFE_NAME,
        description=(
            "Project name; also the default run-store schema suffix, so it must be a "
            "schema-safe identifier."
        ),
    )
    description: str | None = Field(default=None, description="What the project is for.")
    requires_weevr: str | None = Field(
        default=None,
        description="PEP 440 version specifier the installed weevr must satisfy (e.g. '>=2,<3').",
    )
    run_store: RunStore | None = Field(
        default=None,
        description="Run-store settings; omitted means the container's default.",
    )
    params: dict[str, ParamSpec] | None = Field(
        default=None,
        description=(
            "Typed parameter declarations every loom, weave and thread in the project sees; "
            "a declaration of the same name in a document shadows the project's."
        ),
    )
    strict_params: bool | None = Field(
        default=None,
        description=(
            "Enforce the params contract project-wide: a ${param.x} reference must be "
            "declared (P120) and every runtime param declared or referenced (P121). A "
            "document's own strict_params, or the load's, overrides it; unset is lenient."
        ),
    )

    @field_validator("requires_weevr")
    @classmethod
    def _check_specifier(cls, value: str | None) -> str | None:
        """Reject a ``requires_weevr`` that is not a PEP 440 specifier."""
        if value is not None:
            try:
                SpecifierSet(value)
            except InvalidSpecifier:
                raise rule_error(
                    "C103",
                    f"requires_weevr '{value}' is not a PEP 440 version specifier",
                    hint="Write a specifier such as '>=2,<3'.",
                ) from None
        return value
