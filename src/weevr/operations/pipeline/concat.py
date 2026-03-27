"""Concat step handler — null-aware string concatenation."""

from __future__ import annotations

import re

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from weevr.model.pipeline import ConcatParams
from weevr.operations.pipeline import StepResult


def apply_concat(df: DataFrame, params: ConcatParams) -> StepResult:
    """Concatenate columns into a single string column.

    Processing pipeline:
    1. Cast non-string columns to string
    2. Trim inputs (if trim=True)
    3. Evaluate null_mode per column
    4. Join with separator
    5. Collapse adjacent separators (if enabled)
    6. Trim result → empty string to NULL

    Args:
        df: Input DataFrame.
        params: Concat parameters.

    Returns:
        StepResult with the target column added.
    """
    cols = []
    for col_name in params.columns:
        col = F.col(col_name)

        # Step 1: auto-cast non-string columns
        field = next((f for f in df.schema.fields if f.name == col_name), None)
        if field is not None and not isinstance(field.dataType, StringType):
            col = col.cast("string")

        # Step 2: trim inputs
        if params.trim:
            col = F.trim(col)

        # Step 3: evaluate null_mode
        if params.null_mode == "skip":
            # After trim, whitespace-only values become empty strings; convert to null
            # so concat_ws naturally skips them. Without trim, only nulls are skipped.
            if params.trim:
                col = F.when(
                    col.isNull() | (col == F.lit("")), F.lit(None).cast("string")
                ).otherwise(col)
        elif params.null_mode == "empty":
            col = F.coalesce(col, F.lit(""))
        elif params.null_mode == "literal":
            col = F.coalesce(col, F.lit(params.null_literal))

        cols.append(col)

    # Step 4: join with separator (concat_ws skips NULLs naturally)
    expr = F.concat_ws(params.separator, *cols)

    # Step 5: collapse adjacent separators and strip leading/trailing separators.
    # Only applicable in skip mode where nulls leave gaps between separators.
    if params.collapse_separators and params.separator and params.null_mode == "skip":
        escaped_sep = re.escape(params.separator)
        expr = F.regexp_replace(expr, f"({escaped_sep}){{2,}}", params.separator)
        expr = F.regexp_replace(expr, f"^{escaped_sep}|{escaped_sep}$", "")

    # Step 6: trim result, then convert empty string to NULL
    if params.trim:
        expr = F.trim(expr)
    expr = F.when(expr.isNull() | (expr == F.lit("")), F.lit(None).cast("string")).otherwise(expr)

    result = df.withColumn(params.target, expr)
    return StepResult(result)
