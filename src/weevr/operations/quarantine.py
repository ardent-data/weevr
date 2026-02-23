"""Quarantine table writer for validation failures."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def write_quarantine(
    spark: SparkSession,
    quarantine_df: DataFrame | None,
    target_path: str,
) -> int:
    """Write quarantine rows to a Delta table at {target_path}_quarantine.

    Writes using overwrite mode for idempotent re-execution. Skips the
    write entirely if quarantine_df is None or empty.

    Args:
        spark: Active SparkSession.
        quarantine_df: DataFrame with quarantine metadata columns
            (__rule_name, __rule_expression, __severity, __quarantine_ts).
            May be None if no rows need quarantining.
        target_path: Path of the original target Delta table. The quarantine
            table is written to ``{target_path}_quarantine``.

    Returns:
        Number of rows written, or 0 if no write occurred.
    """
    if quarantine_df is None:
        return 0

    row_count = quarantine_df.count()
    if row_count == 0:
        return 0

    quarantine_path = f"{target_path}_quarantine"
    quarantine_df.write.format("delta").mode("overwrite").save(quarantine_path)

    return row_count
