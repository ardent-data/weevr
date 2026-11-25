"""Parameter declaration models."""

from typing import Any, Literal

from pydantic import BaseModel, Field

from weevr.model.base import FrozenBase

STRICT_PARAMS_DESCRIPTION = (
    "Enforce the params contract in this scope and below: a ${param.x} "
    "reference must be declared (P120) and, at the root, every runtime param "
    "must be declared or referenced (P121). The most specific setting wins; "
    "unset inherits, and is lenient at the root."
)
"""The ``strict_params`` field description shared by the loom, weave and thread models."""


class ParamSpec(FrozenBase):
    """Typed parameter specification for config-level param declarations."""

    name: str = Field(description="Unique name of the parameter as referenced in config.")
    type: Literal["string", "int", "float", "bool", "date", "timestamp", "list[string]"] = Field(
        description="Expected type of the parameter value."
    )
    required: bool = Field(
        default=True,
        description="Whether the parameter must be supplied at runtime.",
    )
    default: Any = Field(
        default=None,
        description=(
            "Default value used when the parameter is not supplied and required is false."
        ),
    )
    description: str = Field(
        default="",
        description="Human-readable description of the parameter's purpose.",
    )


class ParamsConfig(BaseModel):
    """Parameter file schema (flat key-value structure).

    Uses a mutable ``BaseModel`` rather than ``FrozenBase`` because it is a
    validation schema for param files, not a domain object. It relies on
    ``extra="allow"`` for arbitrary param keys and on
    ``model_dump(exclude_unset=True)`` within the config pipeline.
    """

    model_config = {"extra": "allow"}

    kind: Literal["params"] = Field(
        default="params",
        description="Document kind. A params document declares 'params'.",
    )
    config_version: str = Field(description="Schema version of the parameter file.")
