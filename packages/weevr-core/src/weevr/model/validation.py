"""Data quality validation and assertion models."""

from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from weevr.model.base import FrozenBase
from weevr.model.types import SparkExpr


class ValidationRule(FrozenBase):
    """A pre-write data quality rule evaluated as a Spark SQL expression."""

    rule: SparkExpr = Field(description="Spark SQL boolean expression that must evaluate to true.")
    severity: Literal["info", "warn", "error", "fatal"] = Field(
        default="error",
        description="Severity level when the rule fails.",
    )
    name: str = Field(description="Unique identifier for the validation rule.")


class SentinelGroup(FrozenBase):
    """A named sentinel with value and optional per-group rate threshold.

    ``value`` is either a raw integer sentinel (e.g. ``-4``) or a system
    member code string (e.g. ``"invalid"``) resolved at evaluation time.
    """

    value: int | str = Field(
        description=(
            "Raw integer sentinel (e.g. ``-4``) or system member code string "
            '(e.g. ``"invalid"``) resolved at evaluation time.'
        ),
    )
    max_rate: float | None = Field(
        default=None,
        description="Maximum allowed rate (0.0–1.0) for this sentinel within the group.",
    )


class Assertion(FrozenBase):
    """A post-execution assertion on the target dataset.

    Supports built-in types (``row_count``, ``column_not_null``,
    ``unique``, ``expression``) and the FK-specific
    ``fk_sentinel_rate`` type for checking sentinel value rates
    across resolved FK columns.
    """

    type: Literal["row_count", "column_not_null", "unique", "expression", "fk_sentinel_rate"] = (
        Field(description="Assertion type that determines which fields are required.")
    )
    severity: Literal["info", "warn", "error", "fatal"] = Field(
        default="warn",
        description="Severity level when the assertion fails.",
    )
    columns: list[str] | None = Field(
        default=None,
        description="Columns to assert against, used by ``column_not_null`` and ``unique``.",
    )
    min: int | None = Field(
        default=None,
        description="Minimum row count threshold for the ``row_count`` assertion type.",
    )
    max: int | None = Field(
        default=None,
        description="Maximum row count threshold for the ``row_count`` assertion type.",
    )
    expression: SparkExpr | None = Field(
        default=None,
        description="Spark SQL boolean expression for the ``expression`` assertion type.",
    )
    # fk_sentinel_rate fields
    column: str | None = Field(
        default=None,
        description="Single FK column to check; mutually exclusive with ``columns``.",
    )
    sentinel: int | str | None = Field(
        default=None,
        description=(
            "Single sentinel value to check rate for; mutually exclusive with ``sentinels``."
        ),
    )
    sentinels: dict[str, SentinelGroup] | None = Field(
        default=None,
        description=(
            "Named map of sentinel groups to check rates for; mutually exclusive with ``sentinel``."
        ),
    )
    max_rate: float | None = Field(
        default=None,
        description="Maximum allowed sentinel rate (0.0–1.0) across all sentinels.",
    )
    message: str | None = Field(
        default=None,
        description="Optional failure message appended to the assertion error output.",
    )

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

        if self.column is None and self.columns is None:
            raise ValueError("fk_sentinel_rate requires 'column' or 'columns'")
        if self.sentinel is None and self.sentinels is None:
            raise ValueError("fk_sentinel_rate requires 'sentinel' or 'sentinels'")

        if self.column is not None and self.columns is not None:
            raise ValueError("column and columns are mutually exclusive")
        if self.sentinel is not None and self.sentinels is not None:
            raise ValueError("sentinel and sentinels are mutually exclusive")

        return self
