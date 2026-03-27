"""Format step handler — pattern, number, and date column formatting."""

from __future__ import annotations

import re
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import (
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from weevr.model.pipeline import FormatParams
from weevr.operations.pipeline import StepResult

_PLACEHOLDER_RE = re.compile(r"\{(\d+):(\d+)\}")


def _build_pattern_expr(col: Column, pattern: str, on_short: str) -> Column:
    """Build a Spark Column expression for pattern formatting.

    Pattern uses 1-indexed {pos:len} placeholders that map directly to
    Spark substring(). Literal text between placeholders is preserved.
    """
    parts: list[Column] = []
    last_end = 0
    max_pos = 0

    for match in _PLACEHOLDER_RE.finditer(pattern):
        if match.start() > last_end:
            parts.append(F.lit(pattern[last_end : match.start()]))

        pos = int(match.group(1))
        length = int(match.group(2))
        parts.append(F.substring(col, pos, length))
        max_pos = max(max_pos, pos + length - 1)
        last_end = match.end()

    if last_end < len(pattern):
        parts.append(F.lit(pattern[last_end:]))

    expr = parts[0] if len(parts) == 1 else F.concat(*parts)

    # NULL source → NULL output always applies
    expr = F.when(col.isNull(), F.lit(None).cast("string")).otherwise(expr)

    # on_short=null: guard against strings shorter than required
    if on_short == "null" and max_pos > 0:
        expr = F.when(
            col.isNotNull() & (F.length(col) >= F.lit(max_pos)),
            expr,
        ).otherwise(F.lit(None).cast("string"))

    return expr


def _build_date_expr(col: Column, pattern: str, source_type: Any, strict_types: bool) -> Column:
    """Build a Spark Column expression for date formatting.

    Uses Spark ``date_format()`` which accepts Java SimpleDateFormat patterns.
    Auto-casts non-date/timestamp sources to timestamp when ``strict_types`` is False.
    """
    if not isinstance(source_type, (DateType, TimestampType)):
        if strict_types:
            raise ValueError(
                f"strict_types=True but source column is {source_type}, expected date/timestamp"
            )
        col = col.cast("timestamp")

    return F.when(col.isNull(), F.lit(None).cast("string")).otherwise(F.date_format(col, pattern))


def apply_format(df: DataFrame, params: FormatParams) -> StepResult:
    """Format columns using pattern, number, or date rules.

    All columns are formatted in a single ``withColumns()`` call for
    efficiency. Each target column resolves its source (defaults to
    target name) and applies the appropriate format expression.

    Args:
        df: Input DataFrame.
        params: Format parameters mapping target names to format specs.

    Returns:
        StepResult with formatted columns.
    """
    col_exprs: dict[str, Column] = {}
    # Track temp cast columns needed for number formatting via SQL expr
    cast_cols: dict[str, Column] = {}

    for target_name, spec in params.columns.items():
        source_name = spec.source or target_name
        source_col = F.col(source_name)

        source_field = next((f for f in df.schema.fields if f.name == source_name), None)
        source_type = source_field.dataType if source_field else StringType()

        if spec.pattern is not None:
            if not isinstance(source_type, StringType):
                source_col = source_col.cast("string")
            col_exprs[target_name] = _build_pattern_expr(source_col, spec.pattern, spec.on_short)

        elif spec.number is not None:
            numeric_types = (IntegerType, LongType, FloatType, DoubleType)
            if not isinstance(source_type, numeric_types):
                if spec.strict_types:
                    raise ValueError(
                        f"strict_types=True but source column '{source_name}' is "
                        f"{source_type}, expected numeric"
                    )
                cast_col_name = f"_fmt_cast_{target_name}"
                cast_cols[cast_col_name] = source_col.cast("double")
            else:
                cast_col_name = f"_fmt_cast_{target_name}"
                cast_cols[cast_col_name] = source_col

            col_exprs[target_name] = F.when(
                F.col(cast_col_name).isNull(), F.lit(None).cast("string")
            ).otherwise(F.expr(f"format_number(`{cast_col_name}`, '{spec.number}')"))

        elif spec.date is not None:
            col_exprs[target_name] = _build_date_expr(
                source_col, spec.date, source_type, spec.strict_types
            )

    # Add cast columns first (needed by number exprs), then target columns
    result = df.withColumns(cast_cols).withColumns(col_exprs)

    # Drop the temporary cast columns
    if cast_cols:
        result = result.drop(*cast_cols.keys())

    return StepResult(result)
