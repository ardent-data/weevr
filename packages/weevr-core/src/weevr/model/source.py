"""Source declaration models."""

from typing import Any

from pydantic import Field, field_validator, model_validator

from weevr.diagnostics.rules import rule_error
from weevr.model.base import FrozenBase

FILE_SOURCE_TYPES = frozenset({"csv", "json", "parquet"})
"""Source types read from files at ``path``."""
_GENERATED_TYPES = {"date_sequence", "int_sequence"}
SOURCE_TYPES = ("delta", "csv", "json", "parquet", "date_sequence", "int_sequence")
"""Every source ``type`` the reader supports; any other value fails config load."""
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
    - ``type``, when set, must be one of ``delta``, ``csv``, ``json``, ``parquet``,
      ``date_sequence``, or ``int_sequence``.
    - If neither ``lookup`` nor ``connection`` is set: ``type`` is required.
    - ``type == "delta"`` without ``connection`` requires ``alias`` to be set.
    - ``type`` in file types (csv, json, parquet) requires ``path`` to be set.
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
            "Source type: delta, csv, json, parquet, date_sequence, or int_sequence. "
            "Any other value fails config load."
        ),
    )
    alias: str | None = Field(
        default=None,
        description="Registered table alias. Required for delta sources without a connection.",
    )
    path: str | None = Field(
        default=None,
        description="File path. Required for file-based sources (csv, json, parquet).",
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

    @field_validator("type")
    @classmethod
    def _validate_type(cls, value: str | None) -> str | None:
        if value is None or value in SOURCE_TYPES:
            return value
        allowed = ", ".join(SOURCE_TYPES)
        hint = None
        if value == "excel":
            hint = "Excel files are not supported; convert them to CSV or Parquet"
        raise rule_error(
            "C103",
            f"unsupported source type {value!r}; expected one of: {allowed}",
            expected=SOURCE_TYPES,
            hint=hint,
        )

    @model_validator(mode="after")
    def _validate_type_fields(self) -> "Source":
        if self.lookup:
            if self.type is not None:
                raise rule_error("C404", "lookup sources must not set 'type'")
            return self

        if self.connection is not None:
            if self.alias is not None:
                raise rule_error("C402", "connection and alias are mutually exclusive")
            if not self.table:
                raise rule_error("C403", "connection sources require 'table' to be set")
            if self.type is not None and self.type != "delta":
                raise rule_error(
                    "C404", "connection sources only support type 'delta' (or omit type)"
                )
            return self

        if self.table is not None:
            raise rule_error("C403", "'table' requires 'connection' to be set")

        if self.type in _GENERATED_TYPES:
            if not self.column:
                raise rule_error("C403", "generated sources require 'column' to be set")
            if self.start is None:
                raise rule_error("C403", "generated sources require 'start' to be set")
            if self.end is None:
                raise rule_error("C403", "generated sources require 'end' to be set")
            if self.alias is not None:
                raise rule_error("C404", "generated sources must not set 'alias'")
            if self.path is not None:
                raise rule_error("C404", "generated sources must not set 'path'")
            if self.options:
                raise rule_error("C404", "generated sources must not set 'options'")
            if self.dedup is not None:
                raise rule_error("C404", "generated sources must not set 'dedup'")
            if (
                self.type == "date_sequence"
                and self.step is not None
                and self.step not in _DATE_SEQUENCE_STEPS
            ):
                raise rule_error(
                    "C103",
                    f"date_sequence step must be one of {sorted(_DATE_SEQUENCE_STEPS)}, "
                    f"got {self.step!r}",
                    expected=sorted(_DATE_SEQUENCE_STEPS),
                )
            if (
                self.type == "int_sequence"
                and self.step is not None
                and (not isinstance(self.step, int) or self.step <= 0)
            ):
                raise rule_error(
                    "C105", f"int_sequence step must be a positive integer, got {self.step!r}"
                )
            return self

        if self.type is None:
            raise rule_error("C403", "sources require either 'type' or 'lookup'")
        if self.type == "delta" and not self.alias:
            raise rule_error("C403", "delta sources require 'alias' to be set")
        if self.type in FILE_SOURCE_TYPES and not self.path:
            raise rule_error("C403", f"{self.type} sources require 'path' to be set")
        return self
