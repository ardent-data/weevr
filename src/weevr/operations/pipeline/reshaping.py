"""Pipeline reshaping step handlers — dedup and sort."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from weevr.model.pipeline import DedupParams, SortParams


def apply_dedup(df: DataFrame, params: DedupParams) -> DataFrame:
    """Deduplicate rows, keeping one row per unique key combination.

    When ``params.order_by`` is set, rows are ranked by that expression.
    ``params.keep`` controls direction: ``"first"`` keeps the lowest-ranked
    row (ascending), ``"last"`` keeps the highest-ranked row (descending).

    When ``order_by`` is absent, ``dropDuplicates`` is used, which retains
    an arbitrary representative row per key group.

    Args:
        df: Input DataFrame.
        params: Dedup parameters — keys, optional sort expression, keep mode.

    Returns:
        DataFrame with at most one row per unique key combination.
    """
    if params.order_by is None:
        return df.dropDuplicates(params.keys)

    base_col = F.expr(params.order_by)
    order_col = base_col.asc() if params.keep == "first" else base_col.desc()
    window = Window.partitionBy(params.keys).orderBy(order_col)
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def apply_sort(df: DataFrame, params: SortParams) -> DataFrame:
    """Sort the DataFrame by one or more columns.

    Args:
        df: Input DataFrame.
        params: Sort parameters — column names and ascending flag.

    Returns:
        DataFrame sorted by the specified columns in the given direction.
    """
    return df.orderBy(*params.columns, ascending=params.ascending)
