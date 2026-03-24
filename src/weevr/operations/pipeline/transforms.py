"""Pipeline transform step handlers — filter, derive, select, drop, rename, cast."""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from weevr.errors.exceptions import ConfigError
from weevr.model.pipeline import (
    CastParams,
    DeriveParams,
    DropParams,
    FilterParams,
    RenameParams,
    SelectParams,
)

_log = logging.getLogger(__name__)


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


def apply_rename(
    df: DataFrame,
    params: RenameParams,
    *,
    column_set_mapping: dict[str, str] | None = None,
    on_unmapped: str = "pass_through",
    on_extra: str = "ignore",
) -> DataFrame:
    """Rename columns using a mapping of old name -> new name.

    Accepts an optional resolved column set mapping that is merged with the static
    ``params.columns`` mapping. Static entries win when both define the same key.

    Args:
        df: Input DataFrame.
        params: Rename parameters mapping existing column names to new names.
        column_set_mapping: Optional resolved column set dict of old->new names.
            Merged with ``params.columns``; static entries take precedence.
        on_unmapped: Behaviour when a DataFrame column has no rename entry.
            ``"pass_through"`` (default) keeps the column unchanged; ``"error"``
            raises :class:`~weevr.errors.exceptions.ConfigError`.
        on_extra: Behaviour when a mapping key references a column that does not
            exist in the DataFrame. ``"ignore"`` (default) silently skips the
            entry; ``"warn"`` logs a warning; ``"error"`` raises
            :class:`~weevr.errors.exceptions.ConfigError`.

    Returns:
        DataFrame with columns renamed according to the merged mapping.

    Raises:
        ConfigError: If ``on_unmapped="error"`` and unmapped columns exist, or if
            ``on_extra="error"`` and extra mapping keys exist.
    """
    merged: dict[str, str] = {**(column_set_mapping or {}), **params.columns}

    if column_set_mapping is not None:
        df_cols = set(df.columns)
        mapping_keys = set(merged.keys())
        loaded = len(merged)
        applied = len(mapping_keys & df_cols)
        unmapped = len(df_cols - mapping_keys)
        extra = len(mapping_keys - df_cols)
        _log.info(
            "Column set rename: %d loaded, %d applied, %d unmapped, %d extra",
            loaded,
            applied,
            unmapped,
            extra,
        )

    if merged:
        df_cols = set(df.columns)
        mapping_keys = set(merged.keys())

        extra = mapping_keys - df_cols
        if extra:
            extra_list = sorted(extra)
            if on_extra == "error":
                raise ConfigError(
                    f"rename mapping references columns not in DataFrame: {extra_list}"
                )
            if on_extra == "warn":
                _log.warning("rename mapping references columns not in DataFrame: %s", extra_list)

        unmapped = df_cols - mapping_keys
        if unmapped and on_unmapped == "error":
            raise ConfigError(f"rename step has unmapped columns: {sorted(unmapped)}")

        # Only apply entries for columns that exist in the DataFrame
        active_mapping = {old: new for old, new in merged.items() if old in df_cols}
        if active_mapping:
            return df.withColumnsRenamed(active_mapping)

    return df


def apply_cast(df: DataFrame, params: CastParams) -> DataFrame:
    """Cast columns to new data types.

    Args:
        df: Input DataFrame.
        params: Cast parameters mapping column names to Spark SQL type strings.

    Returns:
        DataFrame with the specified columns cast to their new types.
    """
    return df.withColumns({col: F.col(col).cast(dtype) for col, dtype in params.columns.items()})
