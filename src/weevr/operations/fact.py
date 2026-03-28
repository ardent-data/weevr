"""Fact target validation operations."""

import logging

from pyspark.sql import DataFrame

from weevr.model.fact import FactConfig

logger = logging.getLogger(__name__)


def validate_fact_target(df: DataFrame, fact_config: FactConfig) -> list[str]:
    """Validate fact target constraints against the output DataFrame.

    Checks FK column existence and emits advisory warnings when FK
    columns contain no sentinel values matching the declared conventions.

    Args:
        df: Output DataFrame after transforms.
        fact_config: Fact target configuration with FK column names.

    Returns:
        List of diagnostic messages. ``ERROR:`` prefixed messages are fatal;
        ``WARN:`` prefixed messages are advisory.
    """
    diagnostics: list[str] = []
    df_columns = set(df.columns)
    for fk_col in fact_config.foreign_keys:
        if fk_col not in df_columns:
            diagnostics.append(f"ERROR: FK column '{fk_col}' not found in output DataFrame")

    # Advisory: warn if present FK columns contain no sentinel values.
    present_fks = [c for c in fact_config.foreign_keys if c in df_columns]
    if present_fks:
        sentinel_vals = {
            fact_config.sentinel_values.invalid,
            fact_config.sentinel_values.missing,
        }
        logger.debug(
            "Fact target validation: %d FK columns present, %d missing",
            len(present_fks),
            len(fact_config.foreign_keys) - len(present_fks),
        )
        for fk_col in present_fks:
            try:
                distinct_vals = {
                    row[0] for row in df.select(fk_col).distinct().limit(1000).collect()
                }
                if not distinct_vals & sentinel_vals:
                    msg = (
                        f"WARN: FK column '{fk_col}' contains no sentinel values ({sentinel_vals})"
                    )
                    diagnostics.append(msg)
                    logger.warning(msg)
            except Exception:
                logger.debug("FK sentinel advisory check failed", exc_info=True)

    return diagnostics
