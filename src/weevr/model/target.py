"""Target mapping models."""

from typing import Any, Literal

from pydantic import Field, model_validator

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

    expr: SparkExpr | None = Field(
        default=None,
        description="Spark SQL expression to compute this column's value.",
    )
    type: str | None = Field(
        default=None,
        description="Target data type to cast to (e.g. 'string', 'decimal(18,2)').",
    )
    default: Any = Field(
        default=None,
        description="Default value when the source column is null.",
    )
    drop: bool = Field(
        default=False,
        description="When True, exclude this column from the output.",
    )

    @model_validator(mode="after")
    def _validate_expr_drop_exclusive(self) -> "ColumnMapping":
        if self.expr is not None and self.drop:
            raise ValueError("'expr' and 'drop=True' are mutually exclusive")
        return self


class Target(FrozenBase):
    """Write destination with column mapping and partitioning configuration."""

    model_config = {"frozen": True, "populate_by_name": True}

    alias: str | None = Field(
        default=None,
        description="Registered table alias for the target. Required for delta targets.",
    )
    path: str | None = Field(
        default=None,
        description="OneLake path for the target. Alternative to alias.",
    )
    connection: str | None = Field(
        default=None,
        description="Reference to a named connection defined at thread, weave, or loom level.",
    )
    schema_override: str | None = Field(
        default=None,
        alias="schema",
        description="Schema override within the connection's lakehouse.",
    )
    table: str | None = Field(
        default=None,
        description="Table name within the connection's lakehouse.",
    )
    mapping_mode: Literal["auto", "explicit"] = Field(
        default="auto",
        description=(
            "Column mapping mode. 'auto' passes all source columns through; "
            "'explicit' requires all output columns to be declared in columns."
        ),
    )
    columns: dict[str, ColumnMapping] | None = Field(
        default=None,
        description="Per-column mapping specifications for the target.",
    )
    partition_by: list[str] | None = Field(
        default=None,
        description="Columns to partition the target table by.",
    )
    audit_columns: dict[str, str] | None = Field(
        default=None,
        description=(
            "Inline audit columns as name-to-expression pairs, appended after user columns."
        ),
    )
    audit_template: list[str] | None = Field(
        default=None,
        description="Named audit templates to apply. Accepts a single string or list.",
    )
    audit_template_inherit: bool = Field(
        default=True,
        description=(
            "Whether to inherit audit templates from parent weave/loom. "
            "Set to False to opt out of cascade."
        ),
    )
    audit_columns_exclude: list[str] | None = Field(
        default=None,
        description="Glob patterns for audit columns to exclude from output.",
    )
    naming: NamingConfig | None = Field(
        default=None,
        description="Naming normalization rules for target columns and table.",
    )
    dimension: DimensionConfig | None = Field(
        default=None,
        description=(
            "Dimension configuration for SCD Type 2 behavior. Mutually exclusive with fact."
        ),
    )
    fact: FactConfig | None = Field(
        default=None,
        description=(
            "Fact table configuration for FK validation. Mutually exclusive with dimension."
        ),
    )
    seed: SeedConfig | None = Field(
        default=None,
        description="Seed rows to insert on first write or when the table is empty.",
    )

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
        """Validate that the target has a write destination.

        Accepted combinations: alias, path, or connection + table.
        """
        has_alias_or_path = self.alias is not None or self.path is not None
        has_connection_target = self.connection is not None and self.table is not None
        if not has_alias_or_path and not has_connection_target:
            raise ValueError(
                "target requires at least one of 'alias', 'path', or 'connection' + 'table'"
            )
        return self

    @model_validator(mode="after")
    def _validate_connection_fields(self) -> "Target":
        """Validate cross-field rules for connection-based targets."""
        if self.connection is not None and self.alias is not None:
            raise ValueError("'connection' and 'alias' are mutually exclusive")
        if self.connection is not None and self.table is None:
            raise ValueError("'connection' requires 'table'")
        if self.table is not None and self.connection is None:
            raise ValueError("'table' requires 'connection'")
        return self

    @model_validator(mode="after")
    def _validate_dimension_fact_exclusive(self) -> "Target":
        """Validate that dimension and fact are mutually exclusive."""
        if self.dimension is not None and self.fact is not None:
            raise ValueError("'dimension' and 'fact' are mutually exclusive on a single target")
        return self
