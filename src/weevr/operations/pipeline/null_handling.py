"""Pipeline null-handling step handlers — fill_null, coalesce."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from weevr.model.pipeline import CoalesceParams, FillNullParams


def apply_fill_null(df: DataFrame, params: FillNullParams) -> DataFrame:
    """Fill null values with specified defaults per column.

    Args:
        df: Input DataFrame.
        params: Fill-null parameters — column-to-default map.

    Returns:
        DataFrame with nulls replaced by specified defaults.
    """
    return df.fillna(params.columns)


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
        result = result.withColumn(
            output_col, F.coalesce(*[F.col(c) for c in source_cols])
        )
    return result
