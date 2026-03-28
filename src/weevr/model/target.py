"""Target mapping models."""

from typing import Any, Literal

from pydantic import model_validator

from weevr.model.base import FrozenBase
from weevr.model.dimension import DimensionConfig
from weevr.model.fact import FactConfig
from weevr.model.naming import NamingConfig
from weevr.model.seed import SeedConfig
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
    audit_columns: dict[str, str] | None = None
    audit_template: list[str] | None = None
    audit_template_inherit: bool = True
    audit_columns_exclude: list[str] | None = None
    naming: NamingConfig | None = None
    dimension: DimensionConfig | None = None
    fact: FactConfig | None = None
    seed: SeedConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_audit_template(cls, data: Any) -> Any:
        if isinstance(data, dict) and isinstance(data.get("audit_template"), str):
            data = {**data, "audit_template": [data["audit_template"]]}
        return data

    @model_validator(mode="after")
    def _validate_exclude_patterns(self) -> "Target":
        """Validate audit_columns_exclude patterns are non-empty strings."""
        if self.audit_columns_exclude:
            for pattern in self.audit_columns_exclude:
                if not pattern:
                    raise ValueError("audit_columns_exclude patterns must be non-empty strings")
        return self

    @model_validator(mode="after")
    def _validate_alias_or_path(self) -> "Target":
        """Validate that at least one of alias or path is provided."""
        if self.alias is None and self.path is None:
            raise ValueError("target requires at least one of 'alias' or 'path'")
        return self

    @model_validator(mode="after")
    def _validate_dimension_fact_exclusive(self) -> "Target":
        """Validate that dimension and fact are mutually exclusive."""
        if self.dimension is not None and self.fact is not None:
            raise ValueError("'dimension' and 'fact' are mutually exclusive on a single target")
        return self
