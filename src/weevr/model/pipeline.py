"""Pipeline step models with discriminated union dispatch."""

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
    """Parameters for the rename step."""

    columns: dict[str, str]


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
    """Parameters for the fill_null step."""

    columns: dict[str, Any]

    @field_validator("columns")
    @classmethod
    def _columns_non_empty(cls, v: dict[str, Any]) -> dict[str, Any]:
        if not v:
            raise ValueError("columns must not be empty")
        return v


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
    | Annotated[DateOpsStep, Tag("date_ops")],
    Discriminator(_step_discriminator),
]
"""Discriminated union of all pipeline step types.

Accepts a dict with a single key matching the step type name:
``{"filter": {"expr": "amount > 0"}}`` → ``FilterStep``
"""
