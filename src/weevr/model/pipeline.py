"""Pipeline step models with discriminated union dispatch."""

import re
from typing import Annotated, Any, Literal

from pydantic import Discriminator, Tag, field_validator, model_validator

from weevr.model.base import FrozenBase
from weevr.model.types import SparkExpr

# All valid step type keys — used by the discriminator
_STEP_TYPES = frozenset(
    {
        "filter",
        "derive",
        "join",
        "select",
        "drop",
        "rename",
        "cast",
        "dedup",
        "sort",
        "union",
        "aggregate",
        "window",
        "pivot",
        "unpivot",
        "case_when",
        "fill_null",
        "coalesce",
        "string_ops",
        "date_ops",
        "concat",
        "map",
        "format",
        "resolve",
    }
)


def _step_discriminator(v: Any) -> str:
    """Return the step type key for discriminated union dispatch.

    Handles both raw dicts (from YAML parsing) and already-constructed
    step model instances (e.g., after a round-trip).

    Returns an unrecognised sentinel string for invalid inputs so that
    Pydantic emits a ``ValidationError`` rather than a bare ``ValueError``.
    """
    if isinstance(v, dict):
        matched = set(v.keys()) & _STEP_TYPES
        if len(matched) == 1:
            return matched.pop()
        # Return unrecognised tag — Pydantic will raise ValidationError
        return f"<invalid:{','.join(sorted(v.keys()))}>"
    cls_name = type(v).__name__
    if cls_name.endswith("Step"):
        return cls_name.removesuffix("Step").lower()
    return f"<unknown:{cls_name}>"


# ---------------------------------------------------------------------------
# Join key pair
# ---------------------------------------------------------------------------


class JoinKeyPair(FrozenBase):
    """An explicit left/right key pair for join conditions."""

    left: str
    right: str


# ---------------------------------------------------------------------------
# Param models (one per step type)
# ---------------------------------------------------------------------------


class FilterParams(FrozenBase):
    """Parameters for the filter step."""

    expr: SparkExpr


class DeriveParams(FrozenBase):
    """Parameters for the derive step.

    ``columns`` maps output column names to Spark SQL expressions.
    Multiple columns can be derived in a single step.
    """

    columns: dict[str, SparkExpr]


class JoinParams(FrozenBase):
    """Parameters for the join step.

    ``on`` accepts simple string keys (``["id"]``) or explicit pairs
    (``[{"left": "a", "right": "b"}]``). Simple keys are normalised to
    ``JoinKeyPair`` during validation.
    """

    source: str
    type: Literal["inner", "left", "right", "full", "cross", "semi", "anti"] = "inner"
    on: list[JoinKeyPair]
    null_safe: bool = True

    @model_validator(mode="before")
    @classmethod
    def _fix_yaml_on_key(cls, data: Any) -> Any:
        """YAML 1.1 parses bare ``on`` as boolean True. Remap it."""
        if isinstance(data, dict) and True in data and "on" not in data:
            data = {("on" if k is True else k): v for k, v in data.items()}
        return data

    @field_validator("on", mode="before")
    @classmethod
    def _normalize_keys(cls, v: Any) -> list[Any]:
        if not isinstance(v, list):
            raise ValueError("'on' must be a list")
        result = []
        for item in v:
            if isinstance(item, str):
                result.append({"left": item, "right": item})
            else:
                result.append(item)
        return result


class SelectParams(FrozenBase):
    """Parameters for the select step."""

    columns: list[str]


class DropParams(FrozenBase):
    """Parameters for the drop step."""

    columns: list[str]


class RenameParams(FrozenBase):
    """Parameters for the rename step.

    ``columns`` maps old column names to new names for static renames.
    ``column_set`` names a pre-declared column set whose entries are merged
    at execution time, allowing shared rename dictionaries to be reused
    across pipelines.
    """

    columns: dict[str, str] = {}
    column_set: str | None = None


class CastParams(FrozenBase):
    """Parameters for the cast step."""

    columns: dict[str, str]


class DedupParams(FrozenBase):
    """Parameters for the dedup step."""

    keys: list[str]
    order_by: str | None = None
    keep: Literal["first", "last"] = "last"


class SortParams(FrozenBase):
    """Parameters for the sort step."""

    columns: list[str]
    ascending: bool = True


class UnionParams(FrozenBase):
    """Parameters for the union step."""

    sources: list[str]
    mode: Literal["by_name", "by_position"] = "by_name"
    allow_missing: bool = False


# ---------------------------------------------------------------------------
# Step wrapper models (one per step type)
# ---------------------------------------------------------------------------


class FilterStep(FrozenBase):
    """Pipeline step: filter rows using a Spark SQL expression."""

    filter: FilterParams


class DeriveStep(FrozenBase):
    """Pipeline step: derive one or more new columns from Spark SQL expressions."""

    derive: DeriveParams


class JoinStep(FrozenBase):
    """Pipeline step: join with another source."""

    join: JoinParams


class SelectStep(FrozenBase):
    """Pipeline step: select a subset of columns."""

    select: SelectParams


class DropStep(FrozenBase):
    """Pipeline step: drop columns."""

    drop: DropParams


class RenameStep(FrozenBase):
    """Pipeline step: rename columns."""

    rename: RenameParams


class CastStep(FrozenBase):
    """Pipeline step: cast column types."""

    cast: CastParams


class DedupStep(FrozenBase):
    """Pipeline step: deduplicate rows."""

    dedup: DedupParams


class SortStep(FrozenBase):
    """Pipeline step: sort rows."""

    sort: SortParams


class UnionStep(FrozenBase):
    """Pipeline step: union with other sources."""

    union: UnionParams


# ---------------------------------------------------------------------------
# Supporting models for new step types
# ---------------------------------------------------------------------------


class WindowFrame(FrozenBase):
    """Frame specification for window functions."""

    type: Literal["rows", "range"]
    start: int
    end: int

    @field_validator("end")
    @classmethod
    def _start_le_end(cls, v: int, info: Any) -> int:
        start = info.data.get("start")
        if start is not None and start > v:
            raise ValueError(f"frame start ({start}) must be <= end ({v})")
        return v


class CaseWhenBranch(FrozenBase):
    """A single when/then pair for the case_when step."""

    when: SparkExpr
    then: SparkExpr


# ---------------------------------------------------------------------------
# New param models (M08a)
# ---------------------------------------------------------------------------


class AggregateParams(FrozenBase):
    """Parameters for the aggregate step.

    ``measures`` maps output aliases to Spark SQL aggregate expressions.
    ``group_by`` is optional; when omitted, aggregation covers the entire DataFrame.
    """

    group_by: list[str] | None = None
    measures: dict[str, SparkExpr]

    @field_validator("measures")
    @classmethod
    def _measures_non_empty(cls, v: dict[str, SparkExpr]) -> dict[str, SparkExpr]:
        if not v:
            raise ValueError("measures must not be empty")
        return v


class WindowParams(FrozenBase):
    """Parameters for the window step.

    Each step defines a single window specification shared across all functions.
    Multiple window specs require multiple window steps.
    """

    functions: dict[str, SparkExpr]
    partition_by: list[str]
    order_by: list[str] | None = None
    frame: WindowFrame | None = None

    @field_validator("functions")
    @classmethod
    def _functions_non_empty(cls, v: dict[str, SparkExpr]) -> dict[str, SparkExpr]:
        if not v:
            raise ValueError("functions must not be empty")
        return v

    @field_validator("partition_by")
    @classmethod
    def _partition_by_non_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("partition_by must not be empty")
        return v


class PivotParams(FrozenBase):
    """Parameters for the pivot step.

    Explicit values are required for deterministic output columns.
    """

    group_by: list[str]
    pivot_column: str
    values: list[str | int | float | bool]
    aggregate: SparkExpr

    @field_validator("values")
    @classmethod
    def _values_non_empty(cls, v: list[str | int | float | bool]) -> list[str | int | float | bool]:
        if not v:
            raise ValueError("values must not be empty")
        return v


class UnpivotParams(FrozenBase):
    """Parameters for the unpivot step."""

    columns: list[str]
    name_column: str
    value_column: str

    @field_validator("columns")
    @classmethod
    def _columns_non_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("columns must not be empty")
        return v

    @field_validator("value_column")
    @classmethod
    def _name_ne_value(cls, v: str, info: Any) -> str:
        name_col = info.data.get("name_column")
        if name_col is not None and name_col == v:
            raise ValueError("name_column and value_column must be different")
        return v


class CaseWhenParams(FrozenBase):
    """Parameters for the case_when step.

    One output column per step, with a list of when/then conditions.
    """

    column: str
    cases: list[CaseWhenBranch]
    otherwise: SparkExpr | None = None

    @field_validator("cases")
    @classmethod
    def _cases_non_empty(cls, v: list[CaseWhenBranch]) -> list[CaseWhenBranch]:
        if not v:
            raise ValueError("cases must not be empty")
        return v


class FillNullParams(FrozenBase):
    """Parameters for the fill_null step.

    Supports two composable modes: explicit column-to-value mapping
    (``columns``) and schema-driven type defaults (``mode:
    type_defaults``). Both may appear in the same step — type defaults
    apply first, then explicit columns override.
    """

    columns: dict[str, Any] | None = None
    mode: Literal["type_defaults"] | None = None
    code: Literal["unknown", "not_applicable", "invalid"] | None = None
    include: list[str] | None = None
    exclude: list[str] | None = None
    overrides: dict[str, Any] | None = None
    where: str | None = None

    @model_validator(mode="after")
    def _validate_fill_null_config(self) -> "FillNullParams":
        if self.columns is None and self.mode is None:
            raise ValueError("at least one of 'columns' or 'mode' must be set")
        if self.columns is not None and not self.columns and self.mode is None:
            raise ValueError("columns must not be empty when mode is not set")
        if self.mode == "type_defaults" and self.code is None:
            raise ValueError("'code' is required when mode is 'type_defaults'")
        if self.mode is None:
            for field_name in ("code", "include", "exclude", "overrides", "where"):
                if getattr(self, field_name) is not None:
                    raise ValueError(f"'{field_name}' is only valid when mode is 'type_defaults'")
        return self


class CoalesceParams(FrozenBase):
    """Parameters for the coalesce step.

    Maps output column names to ordered lists of source columns.
    """

    columns: dict[str, list[str]]

    @field_validator("columns")
    @classmethod
    def _columns_non_empty(cls, v: dict[str, list[str]]) -> dict[str, list[str]]:
        if not v:
            raise ValueError("columns must not be empty")
        for key, sources in v.items():
            if not sources:
                raise ValueError(f"source list for '{key}' must not be empty")
        return v


class ConcatParams(FrozenBase):
    """Parameters for the concat step.

    Concatenates multiple columns into a single string column with
    configurable null handling, separator, and trimming.
    """

    target: str
    columns: list[str]
    separator: str = ""
    null_mode: Literal["skip", "empty", "literal"] = "skip"
    null_literal: str = "<NULL>"
    trim: bool = False
    collapse_separators: bool = True

    @field_validator("columns")
    @classmethod
    def _columns_non_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("columns must not be empty")
        return v


class ConcatStep(FrozenBase):
    """Pipeline step: concatenate columns into a string."""

    concat: ConcatParams


class MapParams(FrozenBase):
    """Parameters for the map step.

    Maps discrete values in a column to new values using a lookup dict.
    Null handling and unmapped value behavior are independently
    configurable.
    """

    column: str
    target: str | None = None
    values: dict[str, Any]
    default: Any = None
    on_null: Any = None
    unmapped: Literal["keep", "null", "validate"] = "keep"
    case_sensitive: bool = True

    @field_validator("values")
    @classmethod
    def _values_non_empty(cls, v: dict[str, Any]) -> dict[str, Any]:
        if not v:
            raise ValueError("values must not be empty")
        return v

    @model_validator(mode="after")
    def _default_unmapped_exclusive(self) -> "MapParams":
        if self.default is not None and self.unmapped in ("null", "validate"):
            raise ValueError(
                "default and unmapped modes 'null'/'validate' are mutually "
                "exclusive — default already handles unmapped values"
            )
        return self


class MapStep(FrozenBase):
    """Pipeline step: map discrete values."""

    map: MapParams


_FORMAT_PLACEHOLDER_RE = re.compile(r"\{(\d+):(\d+)\}")


class FormatSpec(FrozenBase):
    """Per-column format specification.

    Exactly one of ``pattern``, ``number``, or ``date`` must be set.
    ``source`` defaults to the target column name when omitted.
    """

    source: str | None = None
    pattern: str | None = None
    number: str | None = None
    date: str | None = None
    on_short: Literal["null", "partial"] = "null"
    strict_types: bool = False

    @model_validator(mode="after")
    def _exactly_one_format_type(self) -> "FormatSpec":
        set_count = sum(v is not None for v in (self.pattern, self.number, self.date))
        if set_count != 1:
            raise ValueError("exactly one of 'pattern', 'number', or 'date' must be set")
        return self

    @field_validator("pattern")
    @classmethod
    def _validate_pattern_syntax(cls, v: str | None) -> str | None:
        if v is not None and "{" in v:
            for match in _FORMAT_PLACEHOLDER_RE.finditer(v):
                pos, length = int(match.group(1)), int(match.group(2))
                if pos < 1:
                    raise ValueError(f"pattern position must be >= 1, got {pos}")
                if length < 1:
                    raise ValueError(f"pattern length must be >= 1, got {length}")
        return v

    @field_validator("number")
    @classmethod
    def _validate_number_pattern(cls, v: str | None) -> str | None:
        if v is not None:
            allowed = set("0#.,%-E+();' ")
            invalid = set(v) - allowed
            if invalid:
                raise ValueError(
                    f"number pattern contains invalid characters: "
                    f"{', '.join(sorted(repr(c) for c in invalid))}"
                )
        return v


class FormatParams(FrozenBase):
    """Parameters for the format step.

    Maps target column names to format specifications. Multiple columns
    can be formatted in a single step.
    """

    columns: dict[str, FormatSpec]

    @model_validator(mode="before")
    @classmethod
    def _wrap_root_dict(cls, data: Any) -> Any:
        # If data is a plain dict of column specs (no 'columns' key),
        # wrap it so Pydantic can parse it
        if isinstance(data, dict) and "columns" not in data:
            return {"columns": data}
        return data

    @field_validator("columns")
    @classmethod
    def _columns_non_empty(cls, v: dict[str, FormatSpec]) -> dict[str, FormatSpec]:
        if not v:
            raise ValueError("columns must not be empty")
        return v


class FormatStep(FrozenBase):
    """Pipeline step: format columns using pattern, number, or date rules."""

    format: FormatParams


# ---------------------------------------------------------------------------
# Resolve step models (M114 — FK resolution)
# ---------------------------------------------------------------------------


class CurrentConfig(FrozenBase):
    """Current-flag sub-mode for SCD2 narrowing.

    When the lookup dimension uses a boolean or coded column to mark the
    active record, ``column`` identifies that column and ``value`` is the
    active marker (defaults to ``True``).
    """

    column: str
    value: Any = True


class EffectiveConfig(FrozenBase):
    """SCD2 narrowing configuration for resolve step.

    Supports two mutually exclusive sub-modes:

    * **Date range** — half-open interval ``[from, to)`` checked against
      a fact ``date_column``.
    * **Current flag** — filter lookup rows where a column equals a
      specific value (string sugar or dict form with custom value).
    """

    date_column: str | None = None
    from_: str | None = None
    to: str | None = None
    current: str | CurrentConfig | None = None

    model_config = {"populate_by_name": True}

    @field_validator("from_", mode="before")
    @classmethod
    def _alias_from(cls, v: Any, info: Any) -> Any:
        return v

    @model_validator(mode="before")
    @classmethod
    def _remap_from_key(cls, data: Any) -> Any:
        """Remap YAML ``from`` to Python ``from_``."""
        if isinstance(data, dict) and "from" in data and "from_" not in data:
            data = {**data, "from_": data.pop("from")}
        return data

    @field_validator("current", mode="before")
    @classmethod
    def _current_string_sugar(cls, v: Any) -> Any:
        """Convert string sugar to CurrentConfig."""
        if isinstance(v, str):
            return CurrentConfig(column=v)
        return v

    @model_validator(mode="after")
    def _validate_effective_modes(self) -> "EffectiveConfig":
        date_fields = (self.date_column, self.from_, self.to)
        date_set = sum(f is not None for f in date_fields)
        has_current = self.current is not None

        if date_set > 0 and date_set < 3:
            raise ValueError(
                "date range fields (date_column, from, to) are "
                "all-or-nothing — set all three or none"
            )
        if date_set == 3 and has_current:
            raise ValueError("date range and current flag are mutually exclusive")
        if date_set == 0 and not has_current:
            raise ValueError("effective block requires either date range fields or current flag")
        return self


def _normalize_match(v: Any) -> dict[str, str]:
    """Normalize match sugar: string -> dict, list -> dict, dict passthrough."""
    if isinstance(v, str):
        return {v: v}
    if isinstance(v, list):
        return {item: item for item in v}
    return v


class ResolveBatchItem(FrozenBase):
    """Per-FK configuration within a batch resolve step.

    Every item requires ``name``, ``lookup``, and ``match``.  All other
    fields are optional overrides merged with shared defaults at runtime.
    """

    name: str
    lookup: str
    match: dict[str, str]
    pk: str | None = None
    on_invalid: int | None = None
    on_unknown: int | None = None
    on_duplicate: Literal["error", "warn", "first"] | None = None
    on_failure: Literal["abort", "warn"] | None = None
    normalize: Literal["trim_lower", "trim_upper", "trim", "none"] | None = None
    drop_source_columns: bool | None = None
    include: list[str] | dict[str, str] | None = None
    include_prefix: str | None = None
    effective: EffectiveConfig | None = None
    where: str | None = None

    @field_validator("match", mode="before")
    @classmethod
    def _normalize_match(cls, v: Any) -> dict[str, str]:
        return _normalize_match(v)


class ResolveParams(FrozenBase):
    """Parameters for the resolve step.

    Encapsulates FK resolution: BK completeness check, multi-column
    equi-join against a named lookup, sentinel assignment for invalid
    and unknown BKs, optional SCD2 narrowing, include columns, and
    batch mode for multi-FK fact tables.

    In single mode, ``name``, ``lookup``, ``match``, and ``pk`` are
    required.  In batch mode, ``batch`` contains the per-FK specs and
    the outer-level fields serve as shared defaults.
    """

    name: str | None = None
    lookup: str | None = None
    match: dict[str, str] | None = None
    pk: str | None = None
    on_invalid: int = -4
    on_unknown: int = -1
    on_duplicate: Literal["error", "warn", "first"] = "warn"
    on_failure: Literal["abort", "warn"] = "abort"
    normalize: Literal["trim_lower", "trim_upper", "trim", "none"] | None = None
    drop_source_columns: bool = False
    include: list[str] | dict[str, str] | None = None
    include_prefix: str | None = None
    effective: EffectiveConfig | None = None
    where: str | None = None
    batch: list[ResolveBatchItem] | None = None

    @field_validator("match", mode="before")
    @classmethod
    def _normalize_match(cls, v: Any) -> Any:
        if v is None:
            return v
        return _normalize_match(v)

    @field_validator("include", mode="before")
    @classmethod
    def _include_sugar(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v

    @model_validator(mode="after")
    def _validate_resolve_mode(self) -> "ResolveParams":
        single_fields = (self.name, self.lookup, self.match)
        has_single = any(f is not None for f in single_fields)

        if self.batch is not None and has_single:
            raise ValueError(
                "batch and single-mode fields (name, lookup, match) "
                "are mutually exclusive — use batch items or "
                "single-mode fields, not both"
            )

        if self.batch is None:
            for field_name in ("name", "lookup", "match", "pk"):
                if getattr(self, field_name) is None:
                    raise ValueError(f"'{field_name}' is required in single-mode resolve")

        if self.include_prefix is not None and self.include is None:
            raise ValueError("include_prefix requires include to be set")

        return self


class ResolveStep(FrozenBase):
    """Pipeline step: resolve foreign keys via lookup join."""

    resolve: ResolveParams


class StringOpsParams(FrozenBase):
    """Parameters for the string_ops step.

    Applies a Spark SQL expression template across selected columns.
    Use ``{col}`` as the column placeholder in the expression.
    """

    columns: list[str]
    expr: str
    on_empty: Literal["warn", "error"] = "warn"

    @field_validator("expr")
    @classmethod
    def _expr_has_col_placeholder(cls, v: str) -> str:
        if "{col}" not in v:
            raise ValueError("expr must contain '{col}' placeholder")
        return v


class DateOpsParams(FrozenBase):
    """Parameters for the date_ops step.

    Applies a Spark SQL expression template across selected columns.
    Use ``{col}`` as the column placeholder in the expression.
    """

    columns: list[str]
    expr: str
    on_empty: Literal["warn", "error"] = "warn"

    @field_validator("expr")
    @classmethod
    def _expr_has_col_placeholder(cls, v: str) -> str:
        if "{col}" not in v:
            raise ValueError("expr must contain '{col}' placeholder")
        return v


# ---------------------------------------------------------------------------
# New step wrapper models (M08a)
# ---------------------------------------------------------------------------


class AggregateStep(FrozenBase):
    """Pipeline step: aggregate rows with optional grouping."""

    aggregate: AggregateParams


class WindowStep(FrozenBase):
    """Pipeline step: apply window functions."""

    window: WindowParams


class PivotStep(FrozenBase):
    """Pipeline step: pivot rows to columns."""

    pivot: PivotParams


class UnpivotStep(FrozenBase):
    """Pipeline step: unpivot columns to rows."""

    unpivot: UnpivotParams


class CaseWhenStep(FrozenBase):
    """Pipeline step: conditional column values."""

    case_when: CaseWhenParams


class FillNullStep(FrozenBase):
    """Pipeline step: fill null values with defaults."""

    fill_null: FillNullParams


class CoalesceStep(FrozenBase):
    """Pipeline step: coalesce columns."""

    coalesce: CoalesceParams


class StringOpsStep(FrozenBase):
    """Pipeline step: apply expression template to string columns."""

    string_ops: StringOpsParams


class DateOpsStep(FrozenBase):
    """Pipeline step: apply expression template to date/timestamp columns."""

    date_ops: DateOpsParams


# ---------------------------------------------------------------------------
# Discriminated union type
# ---------------------------------------------------------------------------

Step = Annotated[
    Annotated[FilterStep, Tag("filter")]
    | Annotated[DeriveStep, Tag("derive")]
    | Annotated[JoinStep, Tag("join")]
    | Annotated[SelectStep, Tag("select")]
    | Annotated[DropStep, Tag("drop")]
    | Annotated[RenameStep, Tag("rename")]
    | Annotated[CastStep, Tag("cast")]
    | Annotated[DedupStep, Tag("dedup")]
    | Annotated[SortStep, Tag("sort")]
    | Annotated[UnionStep, Tag("union")]
    | Annotated[AggregateStep, Tag("aggregate")]
    | Annotated[WindowStep, Tag("window")]
    | Annotated[PivotStep, Tag("pivot")]
    | Annotated[UnpivotStep, Tag("unpivot")]
    | Annotated[CaseWhenStep, Tag("case_when")]
    | Annotated[FillNullStep, Tag("fill_null")]
    | Annotated[CoalesceStep, Tag("coalesce")]
    | Annotated[StringOpsStep, Tag("string_ops")]
    | Annotated[DateOpsStep, Tag("date_ops")]
    | Annotated[ConcatStep, Tag("concat")]
    | Annotated[MapStep, Tag("map")]
    | Annotated[FormatStep, Tag("format")]
    | Annotated[ResolveStep, Tag("resolve")],
    Discriminator(_step_discriminator),
]
"""Discriminated union of all pipeline step types.

Accepts a dict with a single key matching the step type name:
``{"filter": {"expr": "amount > 0"}}`` → ``FilterStep``
"""
