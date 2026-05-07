"""Source declaration models."""

from typing import Any

from pydantic import Field, model_validator

from weevr.model.base import FrozenBase

_FILE_TYPES = {"csv", "json", "parquet", "excel"}
_GENERATED_TYPES = {"date_sequence", "int_sequence"}
_DATE_SEQUENCE_STEPS = {"day", "week", "month", "year"}


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
    reference (with ``connection`` + ``table``), a lookup reference (with ``lookup``),
    or a generated sequence (with ``type`` set to ``date_sequence`` or
    ``int_sequence``). ``lookup`` is mutually exclusive with all other resolution modes.

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
    - Generated types (``date_sequence``, ``int_sequence``) require ``column``,
      ``start``, and ``end``; ``alias``, ``path``, ``dedup``, ``connection``, and
      non-empty ``options`` are rejected.
    - ``date_sequence`` step must be one of ``day``, ``week``, ``month``, ``year``,
      or omitted.
    - ``int_sequence`` step must be a positive integer or omitted.
    """

    type: str | None = Field(
        default=None,
        description=(
            "Source type: delta, csv, json, parquet, excel, date_sequence, or int_sequence."
        ),
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
    start: str | int | None = Field(
        default=None,
        description=(
            "Sequence start value. Required for generated types. "
            "Accepts a date string (date_sequence) or integer (int_sequence)."
        ),
    )
    end: str | int | None = Field(
        default=None,
        description=(
            "Sequence end value (inclusive). Required for generated types. "
            "Accepts a date string (date_sequence) or integer (int_sequence)."
        ),
    )
    column: str | None = Field(
        default=None,
        description="Output column name. Required for generated types.",
    )
    step: str | int | None = Field(
        default=None,
        description=(
            "Sequence step. For date_sequence: one of day, week, month, year. "
            "For int_sequence: a positive integer. Defaults to 1 (int_sequence) "
            "or day (date_sequence) when omitted."
        ),
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
                raise ValueError("connection sources only support type 'delta' (or omit type)")
            return self

        if self.table is not None:
            raise ValueError("'table' requires 'connection' to be set")

        if self.type in _GENERATED_TYPES:
            if not self.column:
                raise ValueError("generated sources require 'column' to be set")
            if self.start is None:
                raise ValueError("generated sources require 'start' to be set")
            if self.end is None:
                raise ValueError("generated sources require 'end' to be set")
            if self.alias is not None:
                raise ValueError("generated sources must not set 'alias'")
            if self.path is not None:
                raise ValueError("generated sources must not set 'path'")
            if self.options:
                raise ValueError("generated sources must not set 'options'")
            if self.dedup is not None:
                raise ValueError("generated sources must not set 'dedup'")
            if (
                self.type == "date_sequence"
                and self.step is not None
                and self.step not in _DATE_SEQUENCE_STEPS
            ):
                raise ValueError(
                    f"date_sequence step must be one of {sorted(_DATE_SEQUENCE_STEPS)}, "
                    f"got {self.step!r}"
                )
            if (
                self.type == "int_sequence"
                and self.step is not None
                and (not isinstance(self.step, int) or self.step <= 0)
            ):
                raise ValueError(f"int_sequence step must be a positive integer, got {self.step!r}")
            return self

        if self.type is None:
            raise ValueError("sources require either 'type' or 'lookup'")
        if self.type == "delta" and not self.alias:
            raise ValueError("delta sources require 'alias' to be set")
        if self.type in _FILE_TYPES and not self.path:
            raise ValueError(f"{self.type} sources require 'path' to be set")
        return self
