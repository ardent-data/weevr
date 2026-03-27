"""Type-aware fill-value resolver for schema-driven null replacement.

Provides a shared function that maps DataFrame column types to
semantically meaningful fill values based on a code (unknown,
not_applicable, invalid). Used by the fill_null step and designed
for future consumption by system member seeding.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from fnmatch import fnmatch
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructType,
    TimestampType,
)

# Mapping: code → {DataType class → fill value}
_TYPE_DEFAULTS: dict[str, dict[type, Any]] = {
    "unknown": {
        StringType: "Unknown",
        BooleanType: False,
        IntegerType: 0,
        LongType: 0,
        FloatType: 0.0,
        DoubleType: 0.0,
        DecimalType: Decimal(0),
        DateType: date(1970, 1, 1),
        TimestampType: datetime(1970, 1, 1),
    },
    "not_applicable": {
        StringType: "Not Applicable",
        BooleanType: False,
        IntegerType: 0,
        LongType: 0,
        FloatType: 0.0,
        DoubleType: 0.0,
        DecimalType: Decimal(0),
        DateType: date(1970, 1, 1),
        TimestampType: datetime(1970, 1, 1),
    },
    "invalid": {
        StringType: "Invalid",
        BooleanType: False,
        IntegerType: 0,
        LongType: 0,
        FloatType: 0.0,
        DoubleType: 0.0,
        DecimalType: Decimal(0),
        DateType: date(1970, 1, 1),
        TimestampType: datetime(1970, 1, 1),
    },
}

_COMPLEX_TYPES = (ArrayType, MapType, StructType)


def resolve_type_defaults(
    df: DataFrame,
    code: str,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve type-aware fill values for DataFrame columns.

    Inspects the DataFrame schema and returns a ``{col_name: fill_value}``
    dict based on the semantic code and each column's Spark DataType.
    Complex types (array, map, struct) are skipped.

    Args:
        df: Source DataFrame whose schema determines column types.
        code: Semantic code — ``"unknown"``, ``"not_applicable"``, or
            ``"invalid"``.
        include: Optional glob patterns to restrict which columns are
            included. When set, only matching columns are processed.
        exclude: Optional glob patterns to remove columns from the result.
            Applied after include filtering.
        overrides: Per-column fill values that replace the type-based
            default for specific columns.

    Returns:
        Dict mapping column names to their type-appropriate fill values.

    Raises:
        ValueError: If ``code`` is not a recognised fill code.
    """
    if code not in _TYPE_DEFAULTS:
        raise ValueError(f"unrecognized fill code: {code!r}")

    type_map = _TYPE_DEFAULTS[code]
    result: dict[str, Any] = {}

    for field in df.schema.fields:
        col_name = field.name

        # Filter by include globs
        if include is not None and not any(fnmatch(col_name, pat) for pat in include):
            continue

        # Filter by exclude globs
        if exclude is not None and any(fnmatch(col_name, pat) for pat in exclude):
            continue

        # Skip complex types
        if isinstance(field.dataType, _COMPLEX_TYPES):
            continue

        # Look up fill value by DataType class
        fill_value = type_map.get(type(field.dataType))
        if fill_value is not None:
            result[col_name] = fill_value

    # Apply overrides
    if overrides:
        result.update(overrides)

    return result
