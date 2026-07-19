"""Resolve step handler — FK resolution via lookup join.

Encapsulates the complete FK resolution pattern: BK completeness check,
optional normalization, left join against a named lookup, sentinel
assignment for invalid and unknown BKs, on_duplicate handling, include
extra columns, and source column drop.
"""

from __future__ import annotations

import logging
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.window import Window

from weevr.errors.exceptions import ExecutionError
from weevr.model.pipeline import ResolveParams
from weevr.operations.pipeline._lookup_meta import LookupMeta
from weevr.operations.pipeline._observations import ObservationRegistry
from weevr.operations.pipeline._result import StepResult

logger = logging.getLogger(__name__)


def _normalize_col(col: Column, mode: str) -> Column:
    """Apply a normalization preset to a column expression."""
    if mode == "trim_lower":
        return F.lower(F.trim(col))
    if mode == "trim_upper":
        return F.upper(F.trim(col))
    if mode == "trim":
        return F.trim(col)
    return col


def _is_blank(col: Column) -> Column:
    """Return a boolean column that is True when the value is null or blank."""
    return col.isNull() | (F.trim(col) == F.lit(""))


def apply_resolve(
    df: DataFrame,
    params: ResolveParams,
    lookup_df: DataFrame,
    *,
    _drop_source_columns: bool | None = None,
    observations: ObservationRegistry | None = None,
    lookup_meta: LookupMeta | None = None,
) -> StepResult:
    """Resolve foreign keys by joining fact rows to a lookup dimension.

    Args:
        df: Fact DataFrame.
        params: Resolve step parameters.
        lookup_df: Lookup (dimension) DataFrame.
        _drop_source_columns: Override for drop behavior (used by batch
            mode to defer drops until after all FKs complete).
        observations: When provided, resolution statistics are attached
            as an Observation (computed by the terminal action, no extra
            Spark jobs) instead of an eager aggregation, and the eager
            stats disappear from ``StepResult.metadata``.
        lookup_meta: Materialization-time facts about the lookup. When
            the lookup's unique-key check already proved the join keys
            duplicate-free (and the reuse is valid for this resolve),
            the duplicate gate skips even its lookup-side pre-check.

    Returns:
        StepResult with the resolved FK column added and metadata
        containing per-FK resolution statistics (eager mode only).
    """
    if params.match is None:
        raise ExecutionError("match is required for resolve")
    if params.pk is None:
        raise ExecutionError("pk is required for resolve")
    if params.name is None:
        raise ExecutionError("name is required for resolve")

    source_cols = list(params.match.keys())
    lookup_bk_cols = list(params.match.values())
    should_drop = (
        _drop_source_columns if _drop_source_columns is not None else params.drop_source_columns
    )

    # ---------------------------------------------------------------
    # Step 1: Flag invalid rows (any null/blank source column)
    # ---------------------------------------------------------------
    invalid_flag = "__resolve_invalid__"
    invalid_expr = F.lit(False)
    for sc in source_cols:
        invalid_expr = invalid_expr | _is_blank(F.col(sc))
    df = df.withColumn(invalid_flag, invalid_expr)

    # ---------------------------------------------------------------
    # Step 2: Normalize source columns and lookup BK columns
    # ---------------------------------------------------------------
    norm_source_map: dict[str, str] = {}
    norm_lookup_map: dict[str, str] = {}
    if params.normalize and params.normalize != "none":
        for sc, lc in params.match.items():
            ns = f"__norm_src_{sc}__"
            nl = f"__norm_lkp_{lc}__"
            df = df.withColumn(ns, _normalize_col(F.col(sc), params.normalize))
            lookup_df = lookup_df.withColumn(nl, _normalize_col(F.col(lc), params.normalize))
            norm_source_map[sc] = ns
            norm_lookup_map[lc] = nl

    # ---------------------------------------------------------------
    # Step 2a: Apply effective block filter to lookup
    # ---------------------------------------------------------------
    if params.effective is not None:
        eff = params.effective
        if eff.current is not None and not isinstance(eff.current, str):
            # Current flag mode — filter lookup to active records
            current_col = eff.current.column
            current_val = eff.current.value
            lookup_df = lookup_df.filter(F.col(current_col) == F.lit(current_val))
        # Date range mode is handled in the join condition (step 3)

    # ---------------------------------------------------------------
    # Step 2b: Apply where predicate to lookup
    # ---------------------------------------------------------------
    if params.where is not None:
        # Simple where without ${} interpolation at this stage
        where_expr = params.where
        if "${" not in where_expr:
            lookup_df = lookup_df.filter(F.expr(where_expr))

    # ---------------------------------------------------------------
    # Step 3: Build join condition
    # ---------------------------------------------------------------
    join_cols_src = [norm_source_map.get(sc, sc) for sc in source_cols]
    join_cols_lkp: list[str] = [norm_lookup_map.get(lc) or lc for lc in lookup_bk_cols]
    join_cond: Column = F.lit(True)
    for src_c, lkp_c in zip(join_cols_src, join_cols_lkp, strict=True):
        join_cond = join_cond & (F.col(f"__fact__.{src_c}") == F.col(f"__lkp__.{lkp_c}"))

    # Add effective date range condition to join
    if params.effective is not None and params.effective.date_column is not None:
        eff = params.effective
        date_col = f"__fact__.{eff.date_column}"
        from_col = f"__lkp__.{eff.from_}"
        to_col = f"__lkp__.{eff.to}"
        # Half-open interval: [from, to) with null to = current record
        join_cond = join_cond & (F.col(date_col) >= F.col(from_col))
        join_cond = join_cond & (F.col(to_col).isNull() | (F.col(date_col) < F.col(to_col)))

    # ---------------------------------------------------------------
    # Step 4: Duplicate gate. For plain business-key resolves, decide
    # lookup-side whether duplicates are possible at all: a dim-sized
    # pre-check (or the reused materialization-time unique-key outcome)
    # proves the common case clean and skips the row-id column, the
    # window, and the fact-side duplicate count entirely. Any duplicate
    # keys fall back to the exact fact-side path so on_duplicate
    # semantics — error only when a fact row actually matches a
    # duplicated key, before the write — are byte-identical. Resolves
    # with an ``effective`` block keep the fact-side path
    # unconditionally (a BK legitimately carries multiple versions).
    # ---------------------------------------------------------------
    pk_col = f"__lkp__.{params.pk}"
    if _should_broadcast(lookup_meta, df.sparkSession):
        lookup_df = F.broadcast(lookup_df)
    lkp_aliased = lookup_df.alias("__lkp__")

    plain_bk = params.effective is None
    if plain_bk:
        if _uniqueness_reusable(lookup_meta, lookup_bk_cols, params.normalize):
            need_dup_window = False
        else:
            need_dup_window = _has_duplicate_join_keys(lookup_df, join_cols_lkp)
    else:
        need_dup_window = True

    if need_dup_window:
        dup_marker = "__resolve_dup_rn__"
        row_id = "__resolve_row_id__"
        fact_base = df.withColumn(row_id, F.monotonically_increasing_id())
        fact_aliased = fact_base.alias("__fact__")
        joined = fact_aliased.join(lkp_aliased, join_cond, "left")

        window = Window.partitionBy(F.col(f"__fact__.{row_id}")).orderBy(
            F.col(pk_col).asc_nulls_last()
        )
        joined = joined.withColumn(dup_marker, F.row_number().over(window))

        # Count duplicates before filtering
        dup_count = joined.filter(F.col(dup_marker) > 1).count()

        if dup_count > 0:
            if params.on_duplicate == "error":
                raise ExecutionError(
                    f"Duplicate matches found during resolve of '{params.name}': "
                    f"{dup_count} extra match(es). Use on_duplicate='warn' or "
                    f"'first' to handle duplicates."
                )
            if params.on_duplicate == "warn":
                logger.warning(
                    "Resolve '%s': %d duplicate match(es) found; taking first match per row.",
                    params.name,
                    dup_count,
                )

        # Keep only first match per fact row
        joined = joined.filter(F.col(dup_marker) == 1)
    else:
        # Duplicates provably impossible: a plain left join yields at most
        # one match per fact row — no row id, no window, no dup count.
        fact_base = df
        fact_aliased = fact_base.alias("__fact__")
        joined = fact_aliased.join(lkp_aliased, join_cond, "left")
        dup_count = 0

    # ---------------------------------------------------------------
    # Step 6: Resolve FK value — match, unknown, invalid
    # ---------------------------------------------------------------
    resolved_col = (
        F.when(
            F.col(f"__fact__.{invalid_flag}"),
            F.lit(params.on_invalid),
        )
        .when(
            F.col(pk_col).isNull(),
            F.lit(params.on_unknown),
        )
        .otherwise(F.col(pk_col))
    )
    joined = joined.withColumn(params.name, resolved_col)

    # ---------------------------------------------------------------
    # Step 7: Include columns from lookup
    # ---------------------------------------------------------------
    include_renames: dict[str, str] = {}
    if params.include is not None:
        if isinstance(params.include, dict):
            include_renames = params.include
        else:
            include_renames = {c: c for c in params.include}

        prefix = params.include_prefix or ""
        existing_cols = set(fact_base.columns)
        for src_name, tgt_name in include_renames.items():
            final_name = f"{prefix}{tgt_name}"
            if final_name in existing_cols:
                raise ExecutionError(
                    f"Include column '{final_name}' collides with an "
                    f"existing fact column. Use include_prefix or a "
                    f"dict rename to avoid the collision."
                )
            joined = joined.withColumn(
                final_name,
                F.when(
                    F.col(f"__fact__.{invalid_flag}"),
                    F.lit(None),
                ).otherwise(F.col(f"__lkp__.{src_name}")),
            )

    # ---------------------------------------------------------------
    # Step 8: Select final columns
    # ---------------------------------------------------------------
    # Keep all fact columns (minus internal ones) + resolved + includes
    fact_cols = [c for c in fact_base.columns if not c.startswith("__resolve_")]
    # Remove the invalid flag column
    fact_cols = [c for c in fact_cols if c != invalid_flag]

    select_exprs = [F.col(f"__fact__.{c}") for c in fact_cols]
    select_exprs.append(F.col(params.name))

    # Add include columns
    if params.include is not None:
        prefix = params.include_prefix or ""
        for _, tgt_name in include_renames.items():
            final_name = f"{prefix}{tgt_name}"
            select_exprs.append(F.col(final_name))

    result_df = joined.select(*select_exprs)

    # Drop internal row_id (window path only; the name never survives
    # the clean path)
    if "__resolve_row_id__" in result_df.columns:
        result_df = result_df.drop("__resolve_row_id__")

    # ---------------------------------------------------------------
    # Step 9: Drop source columns if requested
    # ---------------------------------------------------------------
    if should_drop:
        result_df = result_df.drop(*source_cols)

    # ---------------------------------------------------------------
    # Step 10: Resolution stats — observed lazily when a registry is
    # provided (fulfilled by the terminal action, zero extra jobs);
    # computed eagerly otherwise so the function stays independently
    # usable outside the engine.
    # ---------------------------------------------------------------
    fk_col = F.col(params.name)
    stat_exprs = (
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(fk_col == F.lit(params.on_invalid), 1).otherwise(0)).alias("invalid"),
        F.sum(F.when(fk_col == F.lit(params.on_unknown), 1).otherwise(0)).alias("unknown"),
    )

    if observations is not None:
        obs = observations.create(
            step="resolve",
            name=params.name,
            extras={"duplicates": dup_count},
            derive=_finalize_resolve_stats,
        )
        result_df = result_df.observe(obs, *stat_exprs)
        return StepResult(result_df, metadata={})

    agg_row = result_df.agg(*stat_exprs).collect()[0]
    stats = _finalize_resolve_stats({**agg_row.asDict(), "duplicates": dup_count})
    return StepResult(
        result_df,
        metadata={"resolve_stats": {params.name: stats}},
    )


def _parse_size_bytes(value: str) -> int | None:
    """Parse Spark's byte-valued threshold strings (``10485760b``, ``10m``)."""
    v = value.strip().lower()
    multipliers = {"k": 1024, "m": 1024**2, "g": 1024**3}
    try:
        if v.endswith("b"):
            v = v[:-1]
        if v and v[-1] in multipliers:
            return int(v[:-1]) * multipliers[v[-1]]
        return int(v)
    except ValueError:
        return None


def _should_broadcast(meta: LookupMeta | None, spark: Any) -> bool:
    """Broadcast policy: declared strategy, or size confidently known small.

    The size clause compares the source table's Delta snapshot bytes — a
    conservative upper bound for the narrowed lookup — against
    ``spark.sql.autoBroadcastJoinThreshold``. No evidence, disabled
    threshold, or unparsable configuration means no hint: AQE decides.
    Never forces an unknown-size lookup onto the driver.
    """
    if meta is None:
        return False
    if meta.broadcast_declared:
        return True
    if meta.size_in_bytes is None:
        return False
    try:
        raw = spark.conf.get("spark.sql.autoBroadcastJoinThreshold", "10485760b")
    except Exception:
        return False
    threshold = _parse_size_bytes(raw or "")
    if threshold is None or threshold <= 0:
        return False
    return meta.size_in_bytes <= threshold


def _uniqueness_reusable(
    meta: LookupMeta | None,
    join_key_columns: list[str],
    normalize: str | None,
) -> bool:
    """Whether a materialization-time unique-key outcome proves this join clean.

    Valid only when the declared unique-key columns equal this resolve's
    match-target columns AND no normalization applies — normalization can
    merge two distinct keys (``trim_lower`` collapses ``"A"``/``"a"``)
    after the check ran, so a passed check proves nothing post-normalize.
    Row-removing filters (``where``, current-flag) cannot create
    duplicates and never invalidate the reuse.
    """
    if meta is None or not meta.unique_key_checked or meta.unique_key_passed is not True:
        return False
    if normalize is not None and normalize != "none":
        return False
    if meta.key_columns is None:
        return False
    # Order-insensitive: uniqueness over a column set is independent of
    # declaration order, and the join condition is a conjunction.
    return set(meta.key_columns) == set(join_key_columns)


def _has_duplicate_join_keys(lookup_df: DataFrame, join_columns: list[str]) -> bool:
    """Dim-sized pre-check: does any join-key group hold more than one row?"""
    counted = lookup_df.groupBy(*[F.col(c) for c in join_columns]).agg(
        F.count(F.lit(1)).alias("__resolve_precheck_cnt__")
    )
    return counted.filter(F.col("__resolve_precheck_cnt__") > 1).limit(1).count() > 0


def _finalize_resolve_stats(values: dict[str, Any]) -> dict[str, Any]:
    """Derive the published resolve stats from raw aggregate values."""
    total = int(values.get("total") or 0)
    invalid_count = int(values.get("invalid") or 0)
    unknown_count = int(values.get("unknown") or 0)
    matched_count = total - invalid_count - unknown_count
    return {
        "total": total,
        "matched": matched_count,
        "unknown": unknown_count,
        "invalid": invalid_count,
        "duplicates": int(values.get("duplicates") or 0),
        "match_rate": round(matched_count / total * 100, 2) if total > 0 else 0.0,
    }


def apply_resolve_batch(
    df: DataFrame,
    params: ResolveParams,
    lookups: dict[str, DataFrame],
    *,
    observations: ObservationRegistry | None = None,
    lookup_meta: dict[str, LookupMeta] | None = None,
) -> StepResult:
    """Resolve multiple FKs in batch mode.

    Iterates through merged batch items, applying single FK resolution
    per item without dropping source columns until all FKs are
    complete. Aggregates per-item stats in metadata.

    Args:
        df: Fact DataFrame.
        params: Resolve parameters with batch items.
        lookups: Dict mapping lookup names to DataFrames.
        observations: Passed through to each item's resolve; see
            :func:`apply_resolve`.
        lookup_meta: Materialization facts keyed by lookup name; each
            item receives its own lookup's entry.

    Returns:
        StepResult with all FK columns added and aggregated metadata.
    """
    items = params.resolve_batch_items()
    all_stats: dict[str, Any] = {}
    all_source_cols: set[str] = set()
    should_drop = params.drop_source_columns
    result_df = df

    for item in items:
        lookup_name = item.lookup
        lookup_df = lookups.get(lookup_name)
        if lookup_df is None:
            raise ExecutionError(f"Lookup '{lookup_name}' not found for batch item '{item.name}'")

        # Track source columns for deferred drop
        if item.match is None:
            raise ExecutionError(f"Batch resolve item '{item.name}' requires 'match'")
        item_source_cols = set(item.match.keys())
        if item.drop_source_columns or should_drop:
            all_source_cols.update(item_source_cols)

        # Build a temporary ResolveParams for single FK resolution
        item_params = ResolveParams(
            name=item.name,
            lookup=item.lookup,
            match=item.match,
            pk=item.pk,  # type: ignore[arg-type]
            on_invalid=item.on_invalid if item.on_invalid is not None else params.on_invalid,
            on_unknown=item.on_unknown if item.on_unknown is not None else params.on_unknown,
            on_duplicate=(
                item.on_duplicate if item.on_duplicate is not None else params.on_duplicate
            ),
            on_failure=item.on_failure if item.on_failure is not None else params.on_failure,
            normalize=item.normalize if item.normalize is not None else params.normalize,
            drop_source_columns=False,  # Defer drop
            include=item.include,
            include_prefix=item.include_prefix,
            effective=item.effective,
            where=item.where,
        )

        step_result = apply_resolve(
            result_df,
            item_params,
            lookup_df,
            _drop_source_columns=False,
            observations=observations,
            lookup_meta=(lookup_meta or {}).get(lookup_name),
        )
        result_df = step_result.df

        # Collect per-item stats
        item_stats = step_result.metadata.get("resolve_stats", {})
        all_stats.update(item_stats)

    # Deferred source column drop
    if all_source_cols:
        result_df = result_df.drop(*all_source_cols)

    return StepResult(result_df, metadata={"resolve_stats": all_stats})
