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

    A source is either a direct data reference (with ``type``), a connection-based
    reference (with ``connection`` + ``table``), or a lookup reference (with
    ``lookup``). ``lookup`` is mutually exclusive with all other resolution modes.

    Cross-field validation rules:
    - If ``lookup`` is set: ``type`` must not be set.
    - If ``connection`` is set: ``table`` must be set; ``alias`` must not be set;
      ``type`` must be None or ``"delta"``; ``connection`` sources resolve via the
      named connection rather than an inline alias.
    - ``table`` without ``connection`` is rejected.
    - ``connection`` and ``alias`` are mutually exclusive.
    - If neither ``lookup`` nor ``connection`` is set: ``type`` is required.
    - ``type == "delta"`` without ``connection`` requires ``alias`` to be set.
    - ``type`` in file types (csv, json, parquet, excel) requires ``path`` to be set.
    """

    type: str | None = Field(
        default=None,
        description="Source type: delta, csv, json, parquet, or excel.",
    )
    alias: str | None = Field(
        default=None,
        description="Registered table alias. Required for delta sources without a connection.",
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

    model_config = {"frozen": True, "populate_by_name": True}

    @model_validator(mode="after")
    def _validate_type_fields(self) -> "Source":
        if self.lookup:
            if self.type is not None:
                raise ValueError("lookup sources must not set 'type'")
            return self

        if self.connection is not None:
            if self.alias is not None:
                raise ValueError("connection and alias are mutually exclusive")
            if not self.table:
                raise ValueError("connection sources require 'table' to be set")
            if self.type is not None and self.type != "delta":
                raise ValueError(
                    "connection sources only support type 'delta' (or omit type)"
                )
            return self

        if self.table is not None:
            raise ValueError("'table' requires 'connection' to be set")

        if self.type is None:
            raise ValueError("sources require either 'type' or 'lookup'")
        if self.type == "delta" and not self.alias:
            raise ValueError("delta sources require 'alias' to be set")
        if self.type in _FILE_TYPES and not self.path:
            raise ValueError(f"{self.type} sources require 'path' to be set")
        return self
