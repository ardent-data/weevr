"""Variable definition model for weave-scoped variables."""

from typing import Literal

from weevr.model.base import FrozenBase


class VariableSpec(FrozenBase):
    """Declaration of a weave-scoped variable.

    Variables are typed scalar values that can be set by hook steps via
    ``set_var`` and referenced in downstream config as ``${var.name}``.

    Attributes:
        type: Scalar type of the variable value.
        default: Optional default value used when no hook sets the variable.
    """

    type: Literal["string", "int", "long", "float", "double", "boolean", "timestamp", "date"]
    default: str | int | float | bool | None = None
