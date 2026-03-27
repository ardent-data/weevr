"""Map step handler — discrete value mapping with null and unmapped control."""

from __future__ import annotations

from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

from weevr.model.pipeline import MapParams
from weevr.operations.pipeline import StepResult


def apply_map(df: DataFrame, params: MapParams) -> StepResult:
    """Map discrete values in a column using a lookup dict.

    Null handling cascade: on_null (if set) → default (if set) → NULL
    passthrough. Unmapped modes: keep original, set NULL, or flag for
    validation.

    Args:
        df: Input DataFrame.
        params: Map parameters.

    Returns:
        StepResult with mapped column (and optional flag column for
        validate mode).
    """
    source_col = params.column
    target_col = params.target or params.column
    col_ref = F.col(source_col)
    metadata: dict[str, Any] = {}

    # For case-insensitive matching, compare lowered values
    compare_col: Column = col_ref if params.case_sensitive else F.lower(col_ref)

    # Build CASE WHEN chain — NULL handled first, then each mapped key
    expr: Column | None = None

    # Null handling cascade: on_null → default → passthrough (None)
    null_value = params.on_null if params.on_null is not None else params.default
    if null_value is not None:
        expr = F.when(col_ref.isNull(), F.lit(null_value))

    for key, mapped_value in params.values.items():
        compare_key = key if params.case_sensitive else key.lower()
        condition = compare_col == F.lit(compare_key)
        if expr is None:
            expr = F.when(condition, F.lit(mapped_value))
        else:
            expr = expr.when(condition, F.lit(mapped_value))

    # Resolve otherwise clause based on default / unmapped mode
    if params.default is not None:
        otherwise: Column = F.lit(params.default)
    elif params.unmapped == "null":
        otherwise = F.lit(None)
    else:
        # keep or validate — retain original value
        otherwise = col_ref

    expr = otherwise if expr is None else expr.otherwise(otherwise)

    # validate mode: compute the unmapped flag against the original column
    # before the mapping overwrites it (when target_col == source_col)
    if params.unmapped == "validate":
        mapped_keys = list(params.values.keys())
        if params.case_sensitive:
            is_mapped = col_ref.isin(mapped_keys)
        else:
            is_mapped = F.lower(col_ref).isin([k.lower() for k in mapped_keys])

        flag_expr = F.when(col_ref.isNull(), F.lit(False)).otherwise(~is_mapped)
        df = df.withColumn("__map_unmapped", flag_expr)

    result = df.withColumn(target_col, expr)

    if params.unmapped == "validate":
        unmapped_count = result.filter(F.col("__map_unmapped")).count()
        metadata["unmapped_count"] = unmapped_count

    return StepResult(result, metadata)
