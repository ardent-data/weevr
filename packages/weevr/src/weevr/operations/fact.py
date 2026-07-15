"""Fact target validation operations."""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from weevr.model.fact import FactConfig

logger = logging.getLogger(__name__)


def validate_fact_schema(df: DataFrame, fact_config: FactConfig) -> list[str]:
    """Validate FK column existence against the output DataFrame.

    Driver-side only — inspects the schema and triggers no Spark actions.

    Args:
        df: Output DataFrame after transforms.
        fact_config: Fact target configuration with FK column names.

    Returns:
        List of ``ERROR:`` prefixed diagnostics, one per missing FK column.
    """
    diagnostics: list[str] = []
    df_columns = set(df.columns)
    for fk_col in fact_config.foreign_keys:
        if fk_col not in df_columns:
            diagnostics.append(f"ERROR: FK column '{fk_col}' not found in output DataFrame")
    return diagnostics


def check_fact_sentinels(df: DataFrame, fact_config: FactConfig) -> list[str]:
    """Warn when FK columns contain no sentinel values.

    Evaluates all present FK columns in a single aggregation pass, so the
    cost does not scale with FK count. The check is exact — every row is
    considered, not a sample. FK columns absent from the DataFrame are
    skipped (existence is ``validate_fact_schema``'s job). Advisory only:
    failures are swallowed and logged at debug level.

    Args:
        df: DataFrame to check — the pipeline output or the written table.
        fact_config: Fact target configuration with FK column names and
            sentinel value conventions.

    Returns:
        List of ``WARN:`` prefixed advisory diagnostics.
    """
    diagnostics: list[str] = []
    df_columns = set(df.columns)
    present_fks = [c for c in fact_config.foreign_keys if c in df_columns]
    if not present_fks:
        return diagnostics

    sentinel_vals = {
        fact_config.sentinel_values.invalid,
        fact_config.sentinel_values.missing,
    }
    logger.debug(
        "Fact target validation: %d FK columns present, %d missing",
        len(present_fks),
        len(fact_config.foreign_keys) - len(present_fks),
    )
    try:
        # max(isin) yields True when any sentinel exists, False when none
        # do, and NULL for all-null or empty columns — only True clears
        # the advisory.
        exprs = [
            F.max(F.col(fk).isin(*sentinel_vals)).alias(fk) for fk in dict.fromkeys(present_fks)
        ]
        sentinel_row = df.agg(*exprs).collect()[0]
        for fk_col in present_fks:
            if not sentinel_row[fk_col]:
                msg = f"WARN: FK column '{fk_col}' contains no sentinel values ({sentinel_vals})"
                diagnostics.append(msg)
                logger.warning(msg)
    except Exception:
        logger.debug("FK sentinel advisory check failed", exc_info=True)

    return diagnostics


def validate_fact_target(df: DataFrame, fact_config: FactConfig) -> list[str]:
    """Validate fact target constraints against the output DataFrame.

    Combines the schema-only FK existence check with the sentinel
    advisory check.

    Args:
        df: Output DataFrame after transforms.
        fact_config: Fact target configuration with FK column names.

    Returns:
        List of diagnostic messages. ``ERROR:`` prefixed messages are fatal;
        ``WARN:`` prefixed messages are advisory.
    """
    return validate_fact_schema(df, fact_config) + check_fact_sentinels(df, fact_config)
