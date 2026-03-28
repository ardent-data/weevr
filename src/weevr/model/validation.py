"""Data quality validation and assertion models."""

from typing import Any, Literal

from pydantic import field_validator, model_validator

from weevr.model.base import FrozenBase
from weevr.model.types import SparkExpr


class ValidationRule(FrozenBase):
    """A pre-write data quality rule evaluated as a Spark SQL expression."""

    rule: SparkExpr
    severity: Literal["info", "warn", "error", "fatal"] = "error"
    name: str


class SentinelGroup(FrozenBase):
    """A named sentinel with value and optional per-group rate threshold.

    ``value`` is either a raw integer sentinel (e.g. ``-4``) or a system
    member code string (e.g. ``"invalid"``) resolved at evaluation time.
    """

    value: int | str
    max_rate: float | None = None


class Assertion(FrozenBase):
    """A post-execution assertion on the target dataset.

    Supports built-in types (``row_count``, ``column_not_null``,
    ``unique``, ``expression``) and the FK-specific
    ``fk_sentinel_rate`` type for checking sentinel value rates
    across resolved FK columns.
    """

    type: Literal["row_count", "column_not_null", "unique", "expression", "fk_sentinel_rate"]
    severity: Literal["info", "warn", "error", "fatal"] = "warn"
    columns: list[str] | None = None
    min: int | None = None
    max: int | None = None
    expression: SparkExpr | None = None
    # fk_sentinel_rate fields
    column: str | None = None
    sentinel: int | str | None = None
    sentinels: dict[str, SentinelGroup] | None = None
    max_rate: float | None = None
    message: str | None = None

    @field_validator("sentinels", mode="before")
    @classmethod
    def _normalize_sentinel_groups(cls, v: Any) -> Any:
        """Normalize sentinel group sugar.

        Dict-of-int values are wrapped in SentinelGroup(value=v).
        Dict-of-dict values are passed through for Pydantic parsing.
        """
        if v is None or not isinstance(v, dict):
            return v
        normalized: dict[str, Any] = {}
        for name, spec in v.items():
            if isinstance(spec, (int, str)):
                normalized[name] = SentinelGroup(value=spec)
            else:
                normalized[name] = spec
        return normalized

    @model_validator(mode="after")
    def _validate_fk_sentinel_rate(self) -> "Assertion":
        if self.type != "fk_sentinel_rate":
            return self

        if self.column is not None and self.columns is not None:
            raise ValueError("column and columns are mutually exclusive")
        if self.sentinel is not None and self.sentinels is not None:
            raise ValueError("sentinel and sentinels are mutually exclusive")

        return self
