"""Seed row operations for initial table population."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructType

from weevr.delta import delta_table_exists
from weevr.errors.exceptions import ExecutionError
from weevr.model.dimension import DimensionConfig, SystemMemberConfig
from weevr.model.seed import SeedConfig
from weevr.operations.pipeline.type_defaults import resolve_type_defaults

logger = logging.getLogger(__name__)


@dataclass
class SeedResult:
    """Result of a seed execution attempt.

    Attributes:
        triggered: Whether the seed condition evaluated to True.
        rows_inserted: Number of rows written; 0 when not triggered.
        trigger_condition: The ``on`` value from the seed config.
        skipped_reason: Human-readable explanation when ``triggered`` is False.
    """

    triggered: bool
    rows_inserted: int
    trigger_condition: str
    skipped_reason: str | None = field(default=None)


def _table_row_count(spark: SparkSession, path: str) -> int:
    """Return the number of rows in an existing Delta table.

    Args:
        spark: Active SparkSession.
        path: File path to the Delta table.

    Returns:
        Row count as an integer.
    """
    return spark.read.format("delta").load(path).count()


def check_seed_trigger(spark: SparkSession, table_path: str, seed_config: SeedConfig) -> bool:
    """Evaluate whether the seed trigger condition is satisfied.

    For ``first_write``, the trigger fires when no Delta table exists at
    *table_path*. For ``empty``, the trigger fires when the table does not
    exist or contains zero rows.

    Args:
        spark: Active SparkSession.
        table_path: Filesystem path to the target Delta table.
        seed_config: Seed configuration carrying the ``on`` condition.

    Returns:
        ``True`` when seeds should be inserted, ``False`` otherwise.
    """
    exists = delta_table_exists(spark, table_path)

    if seed_config.on == "first_write":
        return not exists

    # on == "empty"
    if not exists:
        return True
    return _table_row_count(spark, table_path) == 0


def build_seed_dataframe(
    spark: SparkSession,
    seed_rows: list[dict[str, Any]],
    target_schema: StructType,
) -> DataFrame:
    """Build a DataFrame from seed rows cast to the target schema.

    Columns present in *target_schema* but absent from a seed row are filled
    with ``null``. Columns in the seed rows that are not in *target_schema* are
    silently dropped. Each column is cast to its declared schema type; a cast
    that produces all-null values for a non-null seed value raises
    :class:`~weevr.errors.exceptions.ExecutionError`.

    Args:
        spark: Active SparkSession.
        seed_rows: List of row dicts (column name → value).
        target_schema: Target table schema to conform to.

    Returns:
        DataFrame with the schema matching *target_schema*.

    Raises:
        ExecutionError: When a seed value cannot be cast to the target type.
    """
    schema_field_names = {f.name for f in target_schema.fields}

    # Normalise rows: keep only schema columns, fill missing ones with None,
    # and use the target schema directly so Spark handles null-only columns.
    normalised: list[dict[str, Any]] = []
    for row in seed_rows:
        normalised.append({f.name: row.get(f.name) for f in target_schema.fields})

    # Use the target schema for creation so null columns have the correct type.
    # This avoids Spark's schema inference failing on all-null columns.
    # Spark raises PySparkTypeError when a value is incompatible with the declared type.
    try:
        df = spark.createDataFrame(normalised, schema=target_schema)
    except Exception as exc:
        raise ExecutionError(
            f"Seed row contains a value incompatible with the target schema: {exc}",
            cause=exc,
        ) from exc

    # Reorder columns to match target schema order.
    df = df.select([f.name for f in target_schema.fields if f.name in schema_field_names])

    return df


def execute_seeds(
    spark: SparkSession,
    seed_config: SeedConfig,
    table_path: str,
    target_schema: StructType,
) -> SeedResult:
    """Check the seed trigger and insert rows when the condition is satisfied.

    When triggered and the target table already exists, rows are appended using
    Delta ``append`` mode. When the table does not yet exist, it is created.

    Args:
        spark: Active SparkSession.
        seed_config: Seed configuration with trigger condition and row data.
        table_path: Filesystem path to the target Delta table.
        target_schema: Schema that seed rows should conform to.

    Returns:
        :class:`SeedResult` describing the outcome.
    """
    triggered = check_seed_trigger(spark, table_path, seed_config)

    if not triggered:
        return SeedResult(
            triggered=False,
            rows_inserted=0,
            trigger_condition=seed_config.on,
            skipped_reason=f"Trigger condition '{seed_config.on}' was not met",
        )

    seed_df = build_seed_dataframe(spark, seed_config.rows, target_schema)
    rows_inserted = len(seed_config.rows)

    table_exists = delta_table_exists(spark, table_path)
    if table_exists:
        seed_df.write.format("delta").mode("append").save(table_path)
    else:
        seed_df.write.format("delta").save(table_path)

    return SeedResult(
        triggered=True,
        rows_inserted=rows_inserted,
        trigger_condition=seed_config.on,
    )


# ---------------------------------------------------------------------------
# Default system members
# ---------------------------------------------------------------------------

_DEFAULT_SYSTEM_MEMBERS: list[SystemMemberConfig] = [
    SystemMemberConfig(sk=-1, code="unknown", label="Unknown"),
    SystemMemberConfig(sk=-2, code="not_applicable", label="Not Applicable"),
]


def build_system_member_rows(
    dimension_config: DimensionConfig,
    target_df: DataFrame,
) -> list[dict[str, Any]]:
    """Build seed row dicts for dimension system member sentinel rows.

    Each system member produces one row where:

    - The surrogate key column receives the configured negative SK value.
    - String columns receive the semantic code label (via
      :func:`resolve_type_defaults`).
    - Numeric, boolean, and date columns receive type-appropriate zero/false
      defaults.
    - The label column (if configured) receives the member's ``label``.

    Args:
        dimension_config: Dimension configuration with system member
            definitions and SK column name.
        target_df: DataFrame whose schema determines column types for
            default value resolution.

    Returns:
        List of row dicts ready for :func:`build_seed_dataframe`.
    """
    members = (
        dimension_config.system_members
        if dimension_config.system_members
        else _DEFAULT_SYSTEM_MEMBERS
    )

    sk_col = dimension_config.surrogate_key.name
    rows: list[dict[str, Any]] = []

    for member in members:
        # Resolve type-aware defaults using the member's code
        code = member.code.lower()
        # Map code to known fill codes; fall back to "unknown"
        fill_code = code if code in ("unknown", "not_applicable", "invalid") else "unknown"
        defaults = resolve_type_defaults(target_df, fill_code)

        row: dict[str, Any] = {**defaults}
        row[sk_col] = member.sk

        # Assign the member's code to string BK columns. For non-string
        # BK columns the type default set by resolve_type_defaults
        # (e.g. 0 for Integer, date(1970,1,1) for Date) is kept — the
        # string code would fail Spark's schema coercion downstream.
        schema_by_name = {f.name: f for f in target_df.schema.fields}
        for bk_col in dimension_config.business_key:
            field = schema_by_name.get(bk_col)
            if field is not None and isinstance(field.dataType, StringType):
                row[bk_col] = member.code

        # Set label column if configured
        if dimension_config.label_column is not None:
            row[dimension_config.label_column] = member.label

        # Set SCD columns for system member rows
        if dimension_config.track_history:
            scd = dimension_config.columns
            row[scd.valid_from] = dimension_config.dates.min
            row[scd.valid_to] = dimension_config.dates.max
            row[scd.is_current] = True

        rows.append(row)

    return rows
