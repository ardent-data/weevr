"""Column set models for named external column mappings."""

from __future__ import annotations

import logging
from enum import StrEnum
from typing import Literal

from pydantic import Field, field_validator, model_validator

from weevr.model.base import FrozenBase

_log = logging.getLogger(__name__)


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
    def _validate_type_specific_fields(self) -> ColumnSetSource:
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
    def _validate_source_xor_param(self) -> ColumnSet:
        has_source = self.source is not None
        has_param = self.param is not None
        if has_source and has_param:
            raise ValueError("'source' and 'param' are mutually exclusive — provide one, not both")
        if not has_source and not has_param:
            raise ValueError("one of 'source' or 'param' must be provided")
        return self


class ReservedWordConfig(FrozenBase):
    """Configuration for handling reserved word collisions in column names.

    When a column or table name matches a reserved word, the engine applies
    the configured strategy to resolve the collision.

    Strategies:

    - ``"quote"`` — keep the name as-is; rely on backtick-quoting in SQL.
    - ``"prefix"`` — prepend ``prefix`` to colliding names.
    - ``"suffix"`` — append ``suffix`` to colliding names.
    - ``"error"`` — raise a ``ConfigError`` listing all colliding names.
    - ``"rename"`` — apply explicit ``rename_map``; unmapped collisions
      fall through to ``fallback`` strategy.
    - ``"revert"`` — discard the rename for collisions, keeping the
      pre-normalization name.
    - ``"drop"`` — remove colliding columns from the output (columns
      only; not valid for table names).

    The ``preset`` field selects one or more built-in word lists. When
    omitted, the ANSI SQL list is used as the default. Specifying any
    preset replaces that default — to include ANSI words alongside other
    presets, list ``"ansi"`` explicitly. A single string is accepted as
    shorthand for a one-element list.

    The ``extend`` and ``exclude`` lists compose on top of the resolved
    preset union, adding or removing individual words.

    Attributes:
        strategy: How to handle reserved word collisions.
        prefix: String prepended to colliding column names when
            ``strategy="prefix"``.
        suffix: String appended to colliding column names when
            ``strategy="suffix"``.
        rename_map: Explicit mapping of reserved words to replacement
            names. Required when ``strategy="rename"``.
        fallback: Fallback strategy for unmapped collisions when
            ``strategy="rename"``. Cannot be ``"rename"``.
        preset: Built-in word list presets to activate. ``None`` (default)
            uses the ANSI SQL list. Accepts a single string or a list.
        extend: Additional words to treat as reserved beyond the preset.
        exclude: Words to remove from the reserved word check.
    """

    strategy: Literal["prefix", "quote", "error", "suffix", "rename", "revert", "drop"] = Field(
        default="quote",
        description=(
            "How to handle reserved word collisions. ``quote`` wraps in back-ticks;"
            " ``prefix`` prepends ``prefix``; ``suffix`` appends ``suffix``;"
            " ``error`` raises a validation error; ``rename`` applies an explicit"
            " mapping with fallback; ``revert`` keeps the pre-normalization name;"
            " ``drop`` removes colliding columns."
        ),
    )
    prefix: str = Field(
        default="_",
        description="String prepended to colliding column names when ``strategy`` is ``prefix``.",
    )
    suffix: str = Field(
        default="_col",
        description="String appended to colliding column names when ``strategy`` is ``suffix``.",
    )
    rename_map: dict[str, str] | None = Field(
        default=None,
        description=(
            "Explicit mapping of reserved words to replacement names."
            " Required when ``strategy`` is ``rename``. Keys are matched"
            " case-insensitively."
        ),
    )
    fallback: Literal["prefix", "quote", "error", "suffix", "revert", "drop"] | None = Field(
        default="quote",
        description=(
            "Fallback strategy for unmapped collisions when ``strategy`` is ``rename``."
            " Cannot be ``rename`` (no nesting)."
            " ``drop`` is not valid when applied to table names."
        ),
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

    @field_validator("rename_map", mode="before")
    @classmethod
    def _normalize_rename_map_keys(cls, v: object) -> object:
        """Normalize rename_map keys to lowercase for case-insensitive matching."""
        if isinstance(v, dict):
            return {k.lower(): val for k, val in v.items()}
        return v

    @model_validator(mode="after")
    def _validate_rename_strategy(self) -> ReservedWordConfig:
        """Cross-field validation for rename strategy constraints."""
        if self.strategy == "rename" and (self.rename_map is None or len(self.rename_map) == 0):
            raise ValueError("rename_map must be provided and non-empty when strategy is 'rename'")
        # Warn if rename_map values are themselves reserved words
        if self.rename_map and self.strategy == "rename":
            from weevr.operations.reserved_words import resolve_effective_words

            effective = resolve_effective_words(self)
            reserved_values = [v for v in self.rename_map.values() if v.lower() in effective]
            if reserved_values:
                _log.warning(
                    "rename_map values are reserved words: %s",
                    ", ".join(sorted(reserved_values)),
                )
        return self
