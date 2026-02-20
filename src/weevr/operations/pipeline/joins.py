"""Pipeline join and union step handlers."""

from functools import reduce

from pyspark.sql import DataFrame

from weevr.errors.exceptions import ExecutionError
from weevr.model.pipeline import JoinParams, UnionParams


def apply_join(
    df: DataFrame,
    params: JoinParams,
    sources: dict[str, DataFrame],
) -> DataFrame:
    """Join the working DataFrame with another source.

    Args:
        df: Left-side DataFrame (the current working frame).
        params: Join parameters — source alias, join type, key pairs, null-safe flag.
        sources: All loaded source DataFrames, keyed by alias.

    Returns:
        Joined DataFrame.

    Raises:
        ExecutionError: If the join source alias is not found in ``sources``.
    """
    if params.source not in sources:
        raise ExecutionError(
            f"Join source '{params.source}' not found in loaded sources. "
            f"Available: {sorted(sources)}"
        )
    right_df = sources[params.source]

    if params.type == "cross":
        return df.crossJoin(right_df)

    condition = _build_join_condition(df, right_df, params)
    return df.join(right_df, on=condition, how=params.type)


def apply_union(
    df: DataFrame,
    params: UnionParams,
    sources: dict[str, DataFrame],
) -> DataFrame:
    """Union the working DataFrame with one or more other sources.

    Args:
        df: Initial DataFrame.
        params: Union parameters — list of source aliases, mode, allow_missing flag.
        sources: All loaded source DataFrames, keyed by alias.

    Returns:
        DataFrame with all rows from ``df`` and every listed source unioned together.

    Raises:
        ExecutionError: If any source alias is not found in ``sources``.
    """
    result = df
    for alias in params.sources:
        if alias not in sources:
            raise ExecutionError(
                f"Union source '{alias}' not found in loaded sources. "
                f"Available: {sorted(sources)}"
            )
        other = sources[alias]
        if params.mode == "by_name":
            result = result.unionByName(other, allowMissingColumns=params.allow_missing)
        else:
            result = result.union(other)
    return result


def _build_join_condition(df: DataFrame, right_df: DataFrame, params: JoinParams):  # type: ignore[return]
    """Build a compound join condition from key pairs.

    Uses ``eqNullSafe`` when ``params.null_safe`` is True (the default), which
    treats NULL == NULL as True. Falls back to standard equality otherwise.
    """
    conditions = []
    for pair in params.on:
        left_col = df[pair.left]
        right_col = right_df[pair.right]
        if params.null_safe:
            conditions.append(left_col.eqNullSafe(right_col))
        else:
            conditions.append(left_col == right_col)
    return reduce(lambda a, b: a & b, conditions)
