"""Data quality validation and assertion models."""

from typing import Literal

from weevr.model.base import FrozenBase
from weevr.model.types import SparkExpr


class ValidationRule(FrozenBase):
    """A pre-write data quality rule evaluated as a Spark SQL expression."""

    rule: SparkExpr
    severity: Literal["info", "warn", "error", "fatal"] = "error"
    name: str


class Assertion(FrozenBase):
    """A post-execution assertion on the target dataset."""

    type: Literal["row_count", "column_not_null", "unique"]
    columns: list[str] | None = None
    min: int | None = None
    max: int | None = None
