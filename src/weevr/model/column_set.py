"""Column set models for named external column mappings."""

from enum import StrEnum
from typing import Literal

from pydantic import Field, field_validator, model_validator

from weevr.model.base import FrozenBase


class ReservedWordPreset(StrEnum):
    """Built-in reserved word list presets.

    Each preset represents a self-contained set of reserved words for a
    specific query language or engine context. Presets can be combined
    via list composition; the effective word set is the union.

    Attributes:
        ANSI: ANSI SQL reserved keywords (~80 words).
        DAX: DAX reserved words for Power BI semantic models.
        M: M language (Power Query) reserved words.
        POWERBI: Convenience alias expanding to DAX + M union.
        TSQL: T-SQL reserved keywords for Fabric SQL endpoints.
    """

    ANSI = "ansi"
    DAX = "dax"
    M = "m"
    POWERBI = "powerbi"
    TSQL = "tsql"


class ColumnSetSource(FrozenBase):
    """Source definition for a named column set.

    Describes where the column mapping data lives — either a Delta table or a
    YAML file. Delta sources reference a registered table alias; YAML sources
    reference a file path relative to the project root.

    Attributes:
        type: Source kind — ``"delta"`` or ``"yaml"``.
        alias: Registered table alias. Required for ``type="delta"``.
        path: File path to the YAML mapping file. Required for ``type="yaml"``.
        from_column: Column name in the source that holds the incoming column
            names. Defaults to ``"source_name"``.
        to_column: Column name in the source that holds the outgoing (renamed)
            column names. Defaults to ``"target_name"``.
        filter: SQL WHERE expression applied when reading a Delta source.
    """

    type: Literal["delta", "yaml"] = Field(
        description="Source kind — ``delta`` for a registered Delta table or ``yaml`` for a file."
    )
    alias: str | None = Field(
        default=None,
        description="Registered table alias. Required when ``type`` is ``delta``.",
    )
    path: str | None = Field(
        default=None,
        description="File path to the YAML mapping file. Required when ``type`` is ``yaml``.",
    )
    from_column: str = Field(
        default="source_name",
        description="Column name in the source that holds the incoming (original) column names.",
    )
    to_column: str = Field(
        default="target_name",
        description="Column name in the source that holds the outgoing (renamed) column names.",
    )
    filter: str | None = Field(
        default=None,
        description="SQL WHERE expression applied when reading a Delta source.",
    )

    @model_validator(mode="after")
    def _validate_type_specific_fields(self) -> "ColumnSetSource":
        if self.type == "delta" and self.alias is None:
            raise ValueError("'alias' is required when type is 'delta'")
        if self.type == "yaml" and self.path is None:
            raise ValueError("'path' is required when type is 'yaml'")
        return self


class ColumnSet(FrozenBase):
    """A named column set that defines an external column mapping.

    Column sets describe where to find a mapping of incoming column names to
    outgoing column names.  The mapping data can come from a Delta table, a
    YAML file, or a runtime notebook parameter.  Exactly one of ``source`` or
    ``param`` must be provided.

    Attributes:
        source: Source definition pointing to the mapping data.
        param: Name of a runtime notebook parameter that supplies the mapping
            at execution time.
        on_unmapped: Behaviour when an input column has no mapping entry.
            ``"pass_through"`` keeps the column unchanged; ``"error"`` aborts.
        on_extra: Behaviour when the mapping contains entries for columns not
            present in the input. ``"ignore"`` silently skips; ``"warn"`` logs
            a warning; ``"error"`` aborts.
        on_failure: Behaviour when the column set cannot be resolved (e.g.
            source unavailable). ``"abort"`` raises; ``"warn"`` logs and
            continues; ``"skip"`` silently skips the rename step.
    """

    source: ColumnSetSource | None = Field(
        default=None,
        description="Source definition pointing to the column mapping data.",
    )
    param: str | None = Field(
        default=None,
        description=(
            "Name of a runtime notebook parameter that supplies the mapping at execution time."
        ),
    )
    on_unmapped: Literal["pass_through", "error"] = Field(
        default="pass_through",
        description=(
            "Behaviour when an input column has no mapping entry."
            " ``pass_through`` keeps it unchanged; ``error`` aborts."
        ),
    )
    on_extra: Literal["ignore", "warn", "error"] = Field(
        default="ignore",
        description=(
            "Behaviour when the mapping contains entries for columns not present in the input."
            " ``ignore`` skips silently; ``warn`` logs a warning; ``error`` aborts."
        ),
    )
    on_failure: Literal["abort", "warn", "skip"] = Field(
        default="abort",
        description=(
            "Behaviour when the column set cannot be resolved (e.g. source unavailable)."
            " ``abort`` raises; ``warn`` logs and continues; "
            "``skip`` silently skips the rename step."
        ),
    )

    @model_validator(mode="after")
    def _validate_source_xor_param(self) -> "ColumnSet":
        has_source = self.source is not None
        has_param = self.param is not None
        if has_source and has_param:
            raise ValueError("'source' and 'param' are mutually exclusive — provide one, not both")
        if not has_source and not has_param:
            raise ValueError("one of 'source' or 'param' must be provided")
        return self


class ReservedWordConfig(FrozenBase):
    """Configuration for handling reserved word collisions in column names.

    When a column or table name matches a reserved word, the engine can
    handle the collision in one of three ways: quote the name, prefix it,
    or raise an error.

    The ``preset`` field selects one or more built-in word lists. When
    omitted, the ANSI SQL list is used as the default. Specifying any
    preset replaces that default — to include ANSI words alongside other
    presets, list ``"ansi"`` explicitly. A single string is accepted as
    shorthand for a one-element list.

    The ``extend`` and ``exclude`` lists compose on top of the resolved
    preset union, adding or removing individual words.

    Attributes:
        strategy: How to handle reserved word collisions. ``"quote"`` wraps
            the name in back-ticks; ``"prefix"`` prepends ``prefix``;
            ``"error"`` raises a validation error.
        prefix: String prepended to colliding column names when
            ``strategy="prefix"``.
        preset: Built-in word list presets to activate. ``None`` (default)
            uses the ANSI SQL list. Accepts a single string or a list.
        extend: Additional words to treat as reserved beyond the preset.
        exclude: Words to remove from the reserved word check.
    """

    strategy: Literal["prefix", "quote", "error"] = Field(
        default="quote",
        description=(
            "How to handle reserved word collisions. ``quote`` wraps in back-ticks;"
            " ``prefix`` prepends ``prefix``; ``error`` raises a validation error."
        ),
    )
    prefix: str = Field(
        default="_",
        description="String prepended to colliding column names when ``strategy`` is ``prefix``.",
    )
    preset: list[ReservedWordPreset] | None = Field(
        default=None,
        description=(
            "Built-in word list presets to activate. ``None`` uses the ANSI SQL list."
            " Accepts a single string or a list."
        ),
    )
    extend: list[str] = Field(
        default=[],
        description="Additional words to treat as reserved beyond the resolved preset.",
    )
    exclude: list[str] = Field(
        default=[],
        description="Words to remove from the reserved word check.",
    )

    @field_validator("preset", mode="before")
    @classmethod
    def _normalize_preset(cls, v: object) -> object:
        """Accept a single string as sugar for a one-element list."""
        if isinstance(v, str):
            return [v]
        return v
