"""Target mapping models."""

from typing import Any, Literal

from pydantic import model_validator

from weevr.model.base import FrozenBase
from weevr.model.types import SparkExpr


class ColumnMapping(FrozenBase):
    """Mapping specification for a single target column.

    Cross-field validation:
    - ``expr`` and ``drop=True`` are mutually exclusive.
    """

    expr: SparkExpr | None = None
    type: str | None = None
    default: Any = None
    drop: bool = False

    @model_validator(mode="after")
    def _validate_expr_drop_exclusive(self) -> "ColumnMapping":
        if self.expr is not None and self.drop:
            raise ValueError("'expr' and 'drop=True' are mutually exclusive")
        return self


class Target(FrozenBase):
    """Write destination with column mapping and partitioning configuration."""

    alias: str | None = None
    path: str | None = None
    mapping_mode: Literal["auto", "explicit"] = "auto"
    columns: dict[str, ColumnMapping] | None = None
    partition_by: list[str] | None = None
    audit_template: str | None = None
