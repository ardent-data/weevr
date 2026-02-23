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
    """A post-execution assertion on the target dataset.

    Attributes:
        type: Assertion kind — built-in or expression-based.
        severity: Severity level when the assertion fails.
        columns: Columns involved, for column_not_null and unique types.
        min: Minimum bound for row_count assertions.
        max: Maximum bound for row_count assertions.
        expression: Spark SQL expression for expression-type assertions.
    """

    type: Literal["row_count", "column_not_null", "unique", "expression"]
    severity: Literal["info", "warn", "error", "fatal"] = "warn"
    columns: list[str] | None = None
    min: int | None = None
    max: int | None = None
    expression: SparkExpr | None = None
