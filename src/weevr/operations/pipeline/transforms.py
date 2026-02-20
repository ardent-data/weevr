"""Pipeline transform step handlers — filter, derive, select, drop, rename, cast."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from weevr.model.pipeline import (
    CastParams,
    DeriveParams,
    DropParams,
    FilterParams,
    RenameParams,
    SelectParams,
)


def apply_filter(df: DataFrame, params: FilterParams) -> DataFrame:
    """Filter rows using a Spark SQL expression.

    Args:
        df: Input DataFrame.
        params: Filter parameters containing a Spark SQL predicate expression.

    Returns:
        DataFrame with only rows satisfying the filter expression.
    """
    return df.filter(F.expr(params.expr))


def apply_derive(df: DataFrame, params: DeriveParams) -> DataFrame:
    """Add or replace columns using Spark SQL expressions.

    Args:
        df: Input DataFrame.
        params: Derive parameters mapping output column names to SQL expressions.

    Returns:
        DataFrame with new or replaced columns appended.
    """
    return df.withColumns({name: F.expr(expr) for name, expr in params.columns.items()})


def apply_select(df: DataFrame, params: SelectParams) -> DataFrame:
    """Select a subset of columns, discarding the rest.

    Args:
        df: Input DataFrame.
        params: Select parameters containing the ordered list of columns to keep.

    Returns:
        DataFrame with only the specified columns in the given order.
    """
    return df.select(params.columns)


def apply_drop(df: DataFrame, params: DropParams) -> DataFrame:
    """Drop one or more columns from the DataFrame.

    Args:
        df: Input DataFrame.
        params: Drop parameters containing the list of column names to remove.

    Returns:
        DataFrame with the specified columns removed.
    """
    return df.drop(*params.columns)


def apply_rename(df: DataFrame, params: RenameParams) -> DataFrame:
    """Rename columns using a mapping of old name -> new name.

    Args:
        df: Input DataFrame.
        params: Rename parameters mapping existing column names to new names.

    Returns:
        DataFrame with columns renamed.
    """
    result = df
    for old_name, new_name in params.columns.items():
        result = result.withColumnRenamed(old_name, new_name)
    return result


def apply_cast(df: DataFrame, params: CastParams) -> DataFrame:
    """Cast columns to new data types.

    Args:
        df: Input DataFrame.
        params: Cast parameters mapping column names to Spark SQL type strings.

    Returns:
        DataFrame with the specified columns cast to their new types.
    """
    return df.withColumns({col: F.col(col).cast(dtype) for col, dtype in params.columns.items()})
