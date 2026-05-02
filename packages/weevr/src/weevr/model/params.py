"""Parameter declaration models."""

from typing import Any, Literal

from pydantic import BaseModel, Field

from weevr.model.base import FrozenBase


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

    config_version: str = Field(description="Schema version of the parameter file.")
