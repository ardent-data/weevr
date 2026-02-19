"""Pipeline step models with discriminated union dispatch."""

from typing import Annotated, Any, Literal

from pydantic import Discriminator, Tag, field_validator

from weevr.model.base import FrozenBase
from weevr.model.types import SparkExpr

# All valid step type keys — used by the discriminator
_STEP_TYPES = frozenset(
    {"filter", "derive", "join", "select", "drop", "rename", "cast", "dedup", "sort", "union"}
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
    | Annotated[UnionStep, Tag("union")],
    Discriminator(_step_discriminator),
]
"""Discriminated union of all pipeline step types.

Accepts a dict with a single key matching the step type name:
``{"filter": {"expr": "amount > 0"}}`` → ``FilterStep``
"""
