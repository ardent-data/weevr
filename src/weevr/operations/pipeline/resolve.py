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

from weevr.model.pipeline import ResolveParams
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
) -> StepResult:
    """Resolve foreign keys by joining fact rows to a lookup dimension.

    Args:
        df: Fact DataFrame.
        params: Resolve step parameters.
        lookup_df: Lookup (dimension) DataFrame.
        _drop_source_columns: Override for drop behavior (used by batch
            mode to defer drops until after all FKs complete).

    Returns:
        StepResult with the resolved FK column added and metadata
        containing per-FK resolution statistics.
    """
    assert params.match is not None, "match is required"
    assert params.pk is not None, "pk is required"
    assert params.name is not None, "name is required"

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
    # Step 3: Build join condition
    # ---------------------------------------------------------------
    join_cols_src = [norm_source_map.get(sc, sc) for sc in source_cols]
    join_cols_lkp = [norm_lookup_map.get(lc, lc) for lc in lookup_bk_cols]
    join_cond: Column = F.lit(True)
    for src_c, lkp_c in zip(join_cols_src, join_cols_lkp, strict=True):
        join_cond = join_cond & (F.col(f"__fact__.{src_c}") == F.col(f"__lkp__.{lkp_c}"))

    # Alias DataFrames to avoid column ambiguity
    fact_aliased = df.alias("__fact__")
    lkp_aliased = lookup_df.alias("__lkp__")

    # ---------------------------------------------------------------
    # Step 4: Left join
    # ---------------------------------------------------------------
    joined = fact_aliased.join(lkp_aliased, join_cond, "left")

    # ---------------------------------------------------------------
    # Step 5: on_duplicate handling via window ROW_NUMBER
    # ---------------------------------------------------------------
    pk_col = f"__lkp__.{params.pk}"
    dup_marker = "__resolve_dup_rn__"

    # Add row number partitioned by fact row key columns
    # Use monotonically_increasing_id as a unique row identifier for
    # the fact side before the join
    row_id = "__resolve_row_id__"
    df_with_id = df.withColumn(row_id, F.monotonically_increasing_id())
    fact_aliased = df_with_id.alias("__fact__")
    lkp_aliased = lookup_df.alias("__lkp__")
    joined = fact_aliased.join(lkp_aliased, join_cond, "left")

    window = Window.partitionBy(F.col(f"__fact__.{row_id}")).orderBy(F.col(pk_col).asc_nulls_last())
    joined = joined.withColumn(dup_marker, F.row_number().over(window))

    # Count duplicates before filtering
    dup_count = joined.filter(F.col(dup_marker) > 1).count()

    if dup_count > 0:
        if params.on_duplicate == "error":
            raise ValueError(
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
        for src_name, tgt_name in include_renames.items():
            final_name = f"{prefix}{tgt_name}"
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
    fact_cols = [c for c in df_with_id.columns if not c.startswith("__resolve_")]
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

    # Drop internal row_id
    if row_id in result_df.columns:
        result_df = result_df.drop(row_id)

    # ---------------------------------------------------------------
    # Step 9: Drop source columns if requested
    # ---------------------------------------------------------------
    if should_drop:
        result_df = result_df.drop(*source_cols)

    # ---------------------------------------------------------------
    # Step 10: Compute resolution stats
    # ---------------------------------------------------------------
    total = result_df.count()
    invalid_count = result_df.filter(F.col(params.name) == F.lit(params.on_invalid)).count()
    unknown_count = result_df.filter(F.col(params.name) == F.lit(params.on_unknown)).count()
    matched_count = total - invalid_count - unknown_count

    stats: dict[str, Any] = {
        "total": total,
        "matched": matched_count,
        "unknown": unknown_count,
        "invalid": invalid_count,
        "duplicates": dup_count,
        "match_rate": round(matched_count / total * 100, 2) if total > 0 else 0.0,
    }

    return StepResult(
        result_df,
        metadata={"resolve_stats": {params.name: stats}},
    )
