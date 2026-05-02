"""Warp schema contract models for target table shape declarations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic import Field, model_validator

from weevr.model.base import FrozenBase


class WarpColumn(FrozenBase):
    """A single column declared in a warp schema contract.

    Defines the expected name, type, nullability, and optional default
    for a target table column.
    """

    name: str = Field(
        description="Column name as it appears in the target table.",
    )
    type: str = Field(
        description="Spark SQL type string (e.g. 'bigint', 'string', 'decimal(18,2)').",
    )
    nullable: bool = Field(
        default=True,
        description="Whether the column allows null values.",
    )
    default: Any = Field(
        default=None,
        description="Default value for warp-only column append. None means null.",
    )
    description: str | None = Field(
        default=None,
        description="Human-readable column description for documentation.",
    )
    discovered: bool = Field(
        default=False,
        description=(
            "Marker for columns added by adaptive drift detection. "
            "Remove this flag to signal intentional curation."
        ),
    )


class WarpKeys(FrozenBase):
    """Key declarations for a warp schema contract.

    Documentation metadata identifying surrogate and business keys.
    Not enforced at runtime — used for plan/explain output and
    downstream tooling.
    """

    surrogate: str | None = Field(
        default=None,
        description="Name of the surrogate key column.",
    )
    business: list[str] | None = Field(
        default=None,
        description="Names of the business key columns.",
    )


class WarpConfig(FrozenBase):
    """Typed configuration for a .warp file declaring a target table schema.

    A warp defines the intended shape of a target table: which columns
    should exist, their types, nullability, and optional defaults.
    Cross-field validators ensure column name uniqueness and key
    reference integrity.
    """

    config_version: str = Field(
        description="Schema version for the warp file (e.g. '1.0').",
    )
    description: str | None = Field(
        default=None,
        description="Human-readable description of the warp contract.",
    )
    columns: list[WarpColumn] = Field(
        min_length=1,
        description="Ordered list of column declarations for the target table.",
    )
    keys: WarpKeys | None = Field(
        default=None,
        description="Key declarations (surrogate, business) for documentation.",
    )
    auto_generated: bool = Field(
        default=False,
        description="Marker indicating this warp was auto-generated from pipeline output.",
    )

    @model_validator(mode="after")
    def _validate_unique_column_names(self) -> WarpConfig:
        """Validate that all column names are unique."""
        names = [col.name for col in self.columns]
        seen: set[str] = set()
        duplicates: list[str] = []
        for name in names:
            if name in seen:
                duplicates.append(name)
            seen.add(name)
        if duplicates:
            raise ValueError(f"Duplicate column names: {', '.join(duplicates)}")
        return self

    @model_validator(mode="after")
    def _validate_key_references(self) -> WarpConfig:
        """Validate that key references point to declared columns."""
        if self.keys is None:
            return self
        col_names = {col.name for col in self.columns}
        if self.keys.surrogate and self.keys.surrogate not in col_names:
            raise ValueError(
                f"keys.surrogate '{self.keys.surrogate}' does not reference a declared column"
            )
        if self.keys.business:
            missing = [k for k in self.keys.business if k not in col_names]
            if missing:
                raise ValueError(
                    f"keys.business references undeclared columns: {', '.join(missing)}"
                )
        return self


@dataclass
class EffectiveWarp:
    """Runtime view of a warp merged with engine columns.

    Combines warp-declared columns, warp-only columns (declared in
    the warp but absent from pipeline output), and engine-managed
    columns (audit, SCD, soft-delete).
    """

    declared: list[WarpColumn] = field(default_factory=list)
    warp_only: list[WarpColumn] = field(default_factory=list)
    engine: list[str] = field(default_factory=list)

    @property
    def all_columns(self) -> list[str]:
        """Return all column names: declared + warp-only + engine."""
        declared_names = [col.name for col in self.declared]
        warp_only_names = [col.name for col in self.warp_only]
        return declared_names + warp_only_names + self.engine


@dataclass
class DriftFinding:
    """A single drift finding for a column."""

    column: str
    action: str

    def __repr__(self) -> str:
        """Return string representation."""
        return f"DriftFinding(column={self.column!r}, action={self.action!r})"


@dataclass
class DriftReport:
    """Result of schema drift detection for a single thread execution.

    Captures which extra columns were found and what action was taken
    for each.
    """

    extra_columns: list[str] = field(default_factory=list)
    action_taken: str = ""
    drift_mode: str = ""
    baseline_source: str = ""
    findings: list[DriftFinding] = field(default_factory=list)

    @classmethod
    def empty(cls) -> DriftReport:
        """Create an empty drift report indicating no drift detected."""
        return cls()

    @property
    def has_drift(self) -> bool:
        """Return whether any drift was detected."""
        return len(self.extra_columns) > 0
