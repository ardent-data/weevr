"""Fact target validation operations."""

import logging

from pyspark.sql import DataFrame

from weevr.model.fact import FactConfig

logger = logging.getLogger(__name__)


def validate_fact_target(df: DataFrame, fact_config: FactConfig) -> list[str]:
    """Validate fact target constraints against the output DataFrame.

    Args:
        df: Output DataFrame after transforms.
        fact_config: Fact target configuration with FK column names.

    Returns:
        List of diagnostic messages. ``ERROR:`` prefixed messages are fatal.
    """
    diagnostics: list[str] = []
    df_columns = set(df.columns)
    for fk_col in fact_config.foreign_keys:
        if fk_col not in df_columns:
            diagnostics.append(f"ERROR: FK column '{fk_col}' not found in output DataFrame")
    return diagnostics
