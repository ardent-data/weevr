"""Pipeline analytical step handlers — aggregate, window, pivot, unpivot."""

from pyspark.sql import Column, DataFrame, Window, WindowSpec
from pyspark.sql import functions as F

from weevr.model.pipeline import AggregateParams, PivotParams, UnpivotParams, WindowParams


def _parse_order_by(columns: list[str]) -> list[Column]:
    """Parse SQL-style order-by strings into PySpark Column expressions.

    Each string is either ``"column_name"`` (ascending) or
    ``"column_name desc"``/``"column_name asc"`` with an explicit direction suffix.

    Args:
        columns: List of order-by strings.

    Returns:
        List of PySpark Column objects with the correct sort direction.
    """
    result: list[Column] = []
    for col_str in columns:
        parts = col_str.strip().rsplit(maxsplit=1)
        if len(parts) == 2 and parts[1].lower() in ("asc", "desc"):
            col_name, direction = parts
            if direction.lower() == "desc":
                col_expr = F.col(col_name).desc()
            else:
                col_expr = F.col(col_name).asc()
        else:
            col_expr = F.col(col_str.strip()).asc()
        result.append(col_expr)
    return result


def _build_window_spec(params: WindowParams) -> WindowSpec:
    """Build a PySpark WindowSpec from window parameters."""
    spec: WindowSpec = Window.partitionBy(*params.partition_by)

    if params.order_by:
        spec = spec.orderBy(*_parse_order_by(params.order_by))

    if params.frame is not None:
        if params.frame.type == "rows":
            spec = spec.rowsBetween(params.frame.start, params.frame.end)
        else:
            spec = spec.rangeBetween(params.frame.start, params.frame.end)

    return spec


def apply_aggregate(df: DataFrame, params: AggregateParams) -> DataFrame:
    """Aggregate rows with optional grouping.

    Args:
        df: Input DataFrame.
        params: Aggregate parameters — optional group_by and measures map.

    Returns:
        Aggregated DataFrame with one row per group (or one row total).
    """
    agg_exprs = [F.expr(expr).alias(alias) for alias, expr in params.measures.items()]
    if params.group_by:
        return df.groupBy(*params.group_by).agg(*agg_exprs)
    return df.agg(*agg_exprs)


def apply_window(df: DataFrame, params: WindowParams) -> DataFrame:
    """Apply window functions over a partition specification.

    Args:
        df: Input DataFrame.
        params: Window parameters — functions, partition_by, optional order_by and frame.

    Returns:
        DataFrame with new columns added for each window function.
    """
    spec = _build_window_spec(params)
    result = df
    for alias, expr in params.functions.items():
        result = result.withColumn(alias, F.expr(expr).over(spec))
    return result


def apply_pivot(df: DataFrame, params: PivotParams) -> DataFrame:
    """Pivot rows to columns using explicit values for deterministic output.

    Args:
        df: Input DataFrame.
        params: Pivot parameters — group_by, pivot_column, values, aggregate.

    Returns:
        Pivoted DataFrame with one column per pivot value.
    """
    return (
        df.groupBy(*params.group_by)
        .pivot(params.pivot_column, params.values)
        .agg(F.expr(params.aggregate))
    )


def apply_unpivot(df: DataFrame, params: UnpivotParams) -> DataFrame:
    """Unpivot columns to rows.

    Uses the ``stack()`` SQL expression for broad PySpark version compatibility.

    Args:
        df: Input DataFrame.
        params: Unpivot parameters — columns to unpivot, name and value column names.

    Returns:
        Unpivoted DataFrame with id columns preserved and unpivoted data in two new columns.
    """
    id_cols = [c for c in df.columns if c not in params.columns]
    n = len(params.columns)
    stack_args = ", ".join(f"'{c}', `{c}`" for c in params.columns)
    stack_expr = f"stack({n}, {stack_args}) as ({params.name_column}, {params.value_column})"
    return df.select([F.col(c) for c in id_cols] + [F.expr(stack_expr)])
