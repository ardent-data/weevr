"""Pipeline join and union step handlers."""

import logging
from fnmatch import fnmatch
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from weevr.errors.exceptions import ExecutionError
from weevr.model.pipeline import JoinParams, UnionParams

logger = logging.getLogger(__name__)


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
        ExecutionError: If the join source alias is not found in ``sources``,
            or if column control validation fails.
    """
    if params.source not in sources:
        raise ExecutionError(
            f"Join source '{params.source}' not found in loaded sources. "
            f"Available: {sorted(sources)}"
        )
    right_df = sources[params.source]

    if params.filter:
        right_df = right_df.where(expr(params.filter))
    if params.alias:
        right_df = right_df.alias(params.alias)

    # Capture left-side columns before join so source columns can be identified afterward.
    left_cols: set[str] = set(df.columns)

    if params.type == "cross":
        result = df.crossJoin(right_df)
    else:
        condition = _build_join_condition(df, right_df, params)
        result = df.join(right_df, on=condition, how=params.type)

        # Column-expression joins keep both sides of matching key columns,
        # causing ambiguous references downstream. Drop the right-side
        # duplicates when the left and right key names are the same.
        for pair in params.on:
            if pair.left == pair.right:
                result = result.drop(right_df[pair.right])

    result = _apply_column_control(result, params, left_cols)
    return result


def _apply_column_control(
    result: DataFrame,
    params: JoinParams,
    left_cols: set[str],
) -> DataFrame:
    """Apply include/exclude/prefix/rename column control to joined source columns.

    Processing order (per DEC-017): dup-key-drop → include → exclude → prefix → rename.

    Note: when a non-key column name exists on both sides of the join, the
    source-column detection cannot distinguish left from right copies. Use
    ``alias`` + ``prefix`` to disambiguate before column control in that case.
    """
    no_control = (
        params.include is None
        and params.exclude is None
        and params.prefix is None
        and params.rename is None
    )
    if no_control:
        return result

    # Identify source (right-side) columns as those not present in the left DataFrame.
    source_cols = [c for c in result.columns if c not in left_cols]

    # --- include ---
    if params.include is not None:
        if len(params.include) == 0:
            logger.warning(
                "join column control: include is an empty list — all source columns will be dropped"
            )
            surviving = []
        else:
            surviving = [c for c in source_cols if any(fnmatch(c, pat) for pat in params.include)]
        to_drop = [c for c in source_cols if c not in surviving]
        for col in to_drop:
            result = result.drop(col)
        source_cols = surviving
    else:
        surviving = list(source_cols)

    # --- exclude ---
    if params.exclude:
        excluded = [c for c in source_cols if any(fnmatch(c, pat) for pat in params.exclude)]
        for col in excluded:
            result = result.drop(col)
        source_cols = [c for c in source_cols if c not in excluded]

    # --- prefix ---
    if params.prefix:
        rename_map: dict[str, str] = {c: f"{params.prefix}{c}" for c in source_cols}
        for old, new in rename_map.items():
            result = result.withColumnRenamed(old, new)
        source_cols = [rename_map[c] for c in source_cols]

    # --- rename ---
    if params.rename:
        surviving_set = set(source_cols)
        for old_name in params.rename:
            if old_name not in surviving_set:
                raise ExecutionError(
                    f"Join rename: column '{old_name}' not found in surviving source columns. "
                    f"Surviving: {sorted(surviving_set)}"
                )
        for new_name in params.rename.values():
            if new_name in left_cols:
                raise ExecutionError(
                    f"Join rename: target name '{new_name}' collides with a left-side column."
                )
        for old_name, new_name in params.rename.items():
            result = result.withColumnRenamed(old_name, new_name)

    return result


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
                f"Union source '{alias}' not found in loaded sources. Available: {sorted(sources)}"
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
