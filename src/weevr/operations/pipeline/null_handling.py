"""Pipeline null-handling step handlers — fill_null, coalesce."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from weevr.model.pipeline import CoalesceParams, FillNullParams
from weevr.operations.pipeline.type_defaults import resolve_type_defaults


def apply_fill_null(df: DataFrame, params: FillNullParams) -> DataFrame:
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
        DataFrame with nulls replaced.
    """
    result = df

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
                result = result.fillna(fill_dict)

    # Phase 2: explicit columns (applied on top of type_defaults)
    if params.columns is not None and params.columns:
        result = result.fillna(params.columns)

    return result


def apply_coalesce(df: DataFrame, params: CoalesceParams) -> DataFrame:
    """Coalesce multiple source columns into output columns.

    For each output column, selects the first non-null value from the
    ordered list of source columns.

    Args:
        df: Input DataFrame.
        params: Coalesce parameters — output column to source columns map.

    Returns:
        DataFrame with coalesced output columns.
    """
    result = df
    for output_col, source_cols in params.columns.items():
        result = result.withColumn(output_col, F.coalesce(*[F.col(c) for c in source_cols]))
    return result
