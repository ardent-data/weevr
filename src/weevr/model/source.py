"""Source declaration models."""

from typing import Any

from pydantic import Field, model_validator

from weevr.model.base import FrozenBase

_FILE_TYPES = {"csv", "json", "parquet", "excel"}


class DedupConfig(FrozenBase):
    """Deduplication configuration applied immediately after reading a source."""

    keys: list[str] = Field(
        description="Columns to deduplicate on.",
    )
    order_by: str | None = Field(
        default=None,
        description=(
            "Spark SQL ORDER BY expression to determine which row to keep "
            "when duplicates are found."
        ),
    )


class Source(FrozenBase):
    """A data source declaration.

    A source is either a direct data reference (with ``type``) or a lookup
    reference (with ``lookup``). These are mutually exclusive.

    Cross-field validation rules:
    - If ``lookup`` is set: ``type`` must not be set (lookup references are
      resolved at execution time from weave-level lookup definitions).
    - If ``lookup`` is not set: ``type`` is required.
    - ``type == "delta"`` requires ``alias`` to be set.
    - ``type`` in file types (csv, json, parquet, excel) requires ``path`` to be set.
    """

    type: str | None = Field(
        default=None,
        description="Source type: delta, csv, json, parquet, or excel.",
    )
    alias: str | None = Field(
        default=None,
        description="Registered table alias. Required for delta sources.",
    )
    path: str | None = Field(
        default=None,
        description="File path. Required for file-based sources (csv, json, parquet, excel).",
    )
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Format-specific Spark DataFrameReader options.",
    )
    dedup: DedupConfig | None = Field(
        default=None,
        description="Deduplication configuration applied after reading the source.",
    )
    lookup: str | None = Field(
        default=None,
        description=(
            "Reference to a named lookup defined at the weave or loom level. "
            "Mutually exclusive with type."
        ),
    )

    @model_validator(mode="after")
    def _validate_type_fields(self) -> "Source":
        if self.lookup:
            if self.type is not None:
                raise ValueError("lookup sources must not set 'type'")
            return self
        if self.type is None:
            raise ValueError("sources require either 'type' or 'lookup'")
        if self.type == "delta" and not self.alias:
            raise ValueError("delta sources require 'alias' to be set")
        if self.type in _FILE_TYPES and not self.path:
            raise ValueError(f"{self.type} sources require 'path' to be set")
        return self
