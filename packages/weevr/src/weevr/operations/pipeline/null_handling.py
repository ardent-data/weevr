"""Pipeline null-handling step handlers — fill_null, coalesce."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, StructField, TimestampType

from weevr.model.pipeline import CoalesceParams, FillNullParams
from weevr.operations.pipeline._result import StepResult
from weevr.operations.pipeline.type_defaults import resolve_type_defaults


def _fill_literal(field: StructField, value: Any) -> Column:
    """Build a Column expression for a fill value matching the field's type.

    ``df.fillna()`` rejects ``date``, ``datetime``, and ``Decimal``
    values, so the no-``where`` branch of :func:`apply_fill_null`
    falls back to per-column ``withColumn`` for those types. This
    helper constructs the right Spark column expression per type:
    parsed ISO literal for Date/Timestamp, scale-matching cast for
    Decimal, plain ``F.lit`` for everything else.

    Temporal and Decimal values must match the target field type;
    mismatches raise ``TypeError`` rather than silently coercing —
    for example, a ``datetime`` override against a ``DateType``
    column would otherwise truncate the time component invisibly.
    ``datetime`` is checked before ``date`` because ``datetime`` is
    a ``date`` subclass.
    """
    dt = field.dataType
    if isinstance(value, datetime):
        if not isinstance(dt, TimestampType):
            raise TypeError(
                f"datetime fill value for field {field.name!r} of type "
                f"{type(dt).__name__} requires TimestampType"
            )
        return F.to_timestamp(F.lit(value.isoformat()))
    if isinstance(value, date):
        if not isinstance(dt, DateType):
            raise TypeError(
                f"date fill value for field {field.name!r} of type "
                f"{type(dt).__name__} requires DateType"
            )
        return F.to_date(F.lit(value.isoformat()))
    if isinstance(value, Decimal):
        if not isinstance(dt, DecimalType):
            raise TypeError(
                f"Decimal fill value for field {field.name!r} of type "
                f"{type(dt).__name__} requires DecimalType"
            )
        return F.lit(str(value)).cast(dt)
    return F.lit(value)


def apply_fill_null(df: DataFrame, params: FillNullParams) -> StepResult:
    """Fill null values with specified defaults per column.

    Supports two composable modes:

    1. **Explicit columns** — ``params.columns`` maps column names to
       fill values, applied via ``df.fillna()``.
    2. **Type defaults** — ``params.mode == "type_defaults"`` resolves
       fill values from the DataFrame schema using a semantic code.
       Optional ``where`` predicate applies fills conditionally.

    When both are present, type defaults apply first, then explicit
    columns override on top.

    Args:
        df: Input DataFrame.
        params: Fill-null parameters.

    Returns:
        StepResult with nulls replaced and metadata listing filled
        columns when using type_defaults mode.
    """
    result = df
    metadata: dict[str, Any] = {}

    # Phase 1: type_defaults mode
    if params.mode == "type_defaults":
        fill_dict: dict[str, Any] = resolve_type_defaults(
            result,
            params.code,  # type: ignore[arg-type]  # validated non-None by model
            include=params.include,
            exclude=params.exclude,
            overrides=params.overrides,
        )
        # Explicit columns take priority — exclude them from type defaults so
        # they are not pre-filled before phase 2 runs.
        if params.columns:
            for col_name in params.columns:
                fill_dict.pop(col_name, None)
        if fill_dict:
            if params.where is not None:
                # Conditional fill: per-column when(condition & isNull, fill)
                condition = F.expr(params.where)
                for col_name, fill_value in fill_dict.items():
                    result = result.withColumn(
                        col_name,
                        F.when(
                            condition & F.col(col_name).isNull(),
                            F.lit(fill_value),
                        ).otherwise(F.col(col_name)),
                    )
            else:
                # df.fillna() only accepts str/int/float/bool.
                schema_by_name = {f.name: f for f in result.schema.fields}
                simple: dict[str, Any] = {}
                column_wise: dict[str, Any] = {}
                for col_name, fill_value in fill_dict.items():
                    if isinstance(fill_value, (date, datetime, Decimal)):
                        column_wise[col_name] = fill_value
                    else:
                        simple[col_name] = fill_value
                if simple:
                    result = result.fillna(simple)
                for col_name, fill_value in column_wise.items():
                    result = result.withColumn(
                        col_name,
                        F.when(
                            F.col(col_name).isNull(),
                            _fill_literal(schema_by_name[col_name], fill_value),
                        ).otherwise(F.col(col_name)),
                    )
            metadata["columns_filled"] = list(fill_dict.keys())

    # Phase 2: explicit columns (applied on top of type_defaults)
    if params.columns is not None and params.columns:
        result = result.fillna(params.columns)

    return StepResult(result, metadata)


def apply_coalesce(df: DataFrame, params: CoalesceParams) -> StepResult:
    """Coalesce multiple source columns into output columns.

    For each output column, selects the first non-null value from the
    ordered list of source columns.

    Args:
        df: Input DataFrame.
        params: Coalesce parameters — output column to source columns map.

    Returns:
        StepResult with coalesced output columns.
    """
    result = df
    for output_col, source_cols in params.columns.items():
        result = result.withColumn(output_col, F.coalesce(*[F.col(c) for c in source_cols]))
    return StepResult(result)
