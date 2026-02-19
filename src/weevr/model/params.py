"""Parameter declaration models."""

from typing import Any, Literal

from pydantic import BaseModel

from weevr.model.base import FrozenBase


class ParamSpec(FrozenBase):
    """Typed parameter specification for config-level param declarations."""

    name: str
    type: Literal["string", "int", "float", "bool", "date", "timestamp", "list[string]"]
    required: bool = True
    default: Any = None
    description: str = ""


class ParamsConfig(BaseModel):
    """Parameter file schema (flat key-value structure).

    Uses a mutable ``BaseModel`` rather than ``FrozenBase`` because it is a
    validation schema for param files, not a domain object. It relies on
    ``extra="allow"`` for arbitrary param keys and on
    ``model_dump(exclude_unset=True)`` within the config pipeline.
    """

    model_config = {"extra": "allow"}

    config_version: str
