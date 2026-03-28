"""Dimension merge builder — translates DimensionConfig into merge operations.

Provides the ``DimensionMergeBuilder`` class that generates write config,
key config, and SCD column injection logic from a composable dimension
target configuration.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from weevr.delta import delta_table_exists, resolve_delta_table
from weevr.model.dimension import DimensionConfig
from weevr.model.write import WriteConfig
from weevr.operations.writers import _quote_identifier

logger = logging.getLogger(__name__)


@dataclass
class DimensionMergeResult:
    """Metrics from a dimension merge operation.

    Attributes:
        rows_versioned: Rows closed and re-inserted as new versions.
        rows_overwritten: Rows updated in place.
        rows_inserted: New rows inserted (unmatched in target).
        rows_unchanged: Matched rows with no hash change.
        rows_deleted: Rows deleted or soft-deleted (unmatched by source).
    """

    rows_versioned: int = 0
    rows_overwritten: int = 0
    rows_inserted: int = 0
    rows_unchanged: int = 0
    rows_deleted: int = 0


class DimensionMergeBuilder:
    """Translates a DimensionConfig into merge operations for the engine.

    The builder generates:
    - A WriteConfig for the underlying merge (mode=merge, match on BK)
    - SCD column injection logic
    - System member SK values for auto-exclusion

    Args:
        dimension_config: The dimension configuration block.
        write_overrides: Optional user write config for non-dimension fields
            (on_no_match_source, soft_delete_column, etc.).
    """

    def __init__(
        self,
        dimension_config: DimensionConfig,
        write_overrides: WriteConfig | None = None,
    ) -> None:
        """Initialize the builder with dimension config and optional overrides."""
        self._config = dimension_config
        self._overrides = write_overrides

    @property
    def config(self) -> DimensionConfig:
        """The underlying dimension configuration."""
        return self._config

    def build_write_config(self) -> WriteConfig:
        """Generate a WriteConfig for the dimension merge.

        The engine always uses merge mode with business key as match keys.
        User overrides for ``on_no_match_source``, ``on_no_match_target``,
        ``soft_delete_column``, and ``soft_delete_value`` are applied.

        Returns:
            WriteConfig configured for dimension merge semantics.
        """
        on_no_match_target = "insert"
        on_no_match_source = "ignore"
        soft_delete_column = None
        soft_delete_value = True

        if self._overrides:
            on_no_match_target = self._overrides.on_no_match_target
            on_no_match_source = self._overrides.on_no_match_source
            soft_delete_column = self._overrides.soft_delete_column
            soft_delete_value = self._overrides.soft_delete_value

        return WriteConfig(
            mode="merge",
            match_keys=list(self._config.business_key),
            on_match="update",
            on_no_match_target=on_no_match_target,
            on_no_match_source=on_no_match_source,
            soft_delete_column=soft_delete_column,
            soft_delete_value=soft_delete_value,
        )

    def inject_scd_columns(
        self,
        df: DataFrame,
        run_timestamp: str,
    ) -> DataFrame:
        """Add SCD tracking columns to the DataFrame.

        Injects ``valid_from``, ``valid_to``, and ``is_current`` columns
        with initial values appropriate for new rows.

        Args:
            df: Input DataFrame.
            run_timestamp: ISO timestamp for ``valid_from``.

        Returns:
            DataFrame with SCD columns appended.
        """
        if not self._config.track_history:
            return df

        scd = self._config.columns
        dates = self._config.dates

        df = df.withColumn(scd.valid_from, F.lit(run_timestamp))
        df = df.withColumn(scd.valid_to, F.lit(dates.max))
        df = df.withColumn(scd.is_current, F.lit(True))

        return df

    def get_system_member_sk_values(self) -> set[int]:
        """Return the set of SK values used by system members.

        Used for auto-exclusion during delete/soft_delete operations
        (DEC-003). Only negative integers are returned.

        Returns:
            Set of negative SK values.
        """
        if not self._config.seed_system_members:
            return set()

        if self._config.system_members:
            return {m.sk for m in self._config.system_members}
        # Default system members: unknown (-1), not_applicable (-2)
        return {-1, -2}

    def get_scd_column_names(self) -> set[str]:
        """Return the set of SCD column names managed by the engine.

        Returns:
            Set containing valid_from, valid_to, and is_current names.
        """
        scd = self._config.columns
        return {scd.valid_from, scd.valid_to, scd.is_current}

    def has_version_groups(self) -> bool:
        """Check if any change detection group uses on_change: version.

        Returns:
            True if at least one group versions on change.
        """
        if not self._config.change_detection:
            return False
        return any(g.on_change == "version" for g in self._config.change_detection.values())

    def get_overwrite_columns(self) -> list[str]:
        """Return columns from groups with on_change: overwrite.

        Returns:
            Flat list of column names in overwrite groups.
        """
        if not self._config.change_detection:
            return []
        cols: list[str] = []
        for group in self._config.change_detection.values():
            if group.on_change == "overwrite" and isinstance(group.columns, list):
                cols.extend(group.columns)
        return cols

    def get_static_columns(self) -> list[str]:
        """Return columns from groups with on_change: static.

        Returns:
            Flat list of column names in static groups.
        """
        if not self._config.change_detection:
            return []
        cols: list[str] = []
        for group in self._config.change_detection.values():
            if group.on_change == "static" and isinstance(group.columns, list):
                cols.extend(group.columns)
        return cols


def execute_dimension_merge(
    spark: SparkSession,
    source_df: DataFrame,
    builder: DimensionMergeBuilder,
    target_path: str,
    run_timestamp: str,
) -> DimensionMergeResult:
    """Execute the staged dimension merge against a Delta target.

    For non-versioned dimensions (track_history=False), delegates to a
    standard Delta MERGE with business key matching and hash-based change
    detection.

    For versioned dimensions (track_history=True), executes a staged
    close-and-insert pattern:

    1. Read current target rows (filtered by ``is_current`` when
       ``history_filter`` is True).
    2. Join source to target on business key.
    3. Compare change detection hashes per group.
    4. Build MERGE clauses: close changed rows (set valid_to, is_current),
       insert new versions, insert unmatched source rows.
    5. Handle ``on_no_match_source`` with system member auto-exclusion.

    Args:
        spark: Active SparkSession.
        source_df: Source DataFrame with keys and hashes already computed.
        builder: Configured DimensionMergeBuilder.
        target_path: Path to the Delta target table.
        run_timestamp: ISO timestamp for SCD valid_from values.

    Returns:
        DimensionMergeResult with row-level metrics.
    """
    config = builder.config
    result = DimensionMergeResult()

    if not delta_table_exists(spark, target_path):
        # First write — insert all rows directly
        result.rows_inserted = source_df.count()
        source_df.write.format("delta").save(target_path)
        return result

    if config.track_history and builder.has_version_groups():
        return _execute_versioned_merge(spark, source_df, builder, target_path, run_timestamp)

    return _execute_standard_merge(spark, source_df, builder, target_path, run_timestamp)


def _execute_versioned_merge(
    spark: SparkSession,
    source_df: DataFrame,
    builder: DimensionMergeBuilder,
    target_path: str,
    run_timestamp: str,
) -> DimensionMergeResult:
    """SCD Type 2 staged merge: pre-join to identify changes, then MERGE.

    The approach:
    1. Read current target rows.
    2. Join source to target on BK to identify changed/new rows.
    3. Build new-version rows for changed entities.
    4. Union new-version rows (with a marker) into the source.
    5. Single MERGE: close changed rows, insert new versions + new entities.
    """
    config = builder.config
    result = DimensionMergeResult()
    scd = config.columns
    bk_cols = config.business_key

    # Identify version-group hash columns
    version_hash_cols: list[str] = []
    if config.change_detection:
        for key, group in config.change_detection.items():
            col_name = group.name or key
            if group.on_change == "version":
                version_hash_cols.append(col_name)

    # Read current target rows
    target_snap = spark.read.format("delta").load(target_path)
    if config.history_filter:
        target_snap = target_snap.filter(
            F.col(scd.is_current) == True  # noqa: E712
        )

    # Join source to target on BK to detect changes
    join_cond = [source_df[c].eqNullSafe(target_snap[c]) for c in bk_cols]
    joined = source_df.alias("src").join(target_snap.alias("tgt"), join_cond, "left")

    # Build version change condition
    if version_hash_cols:
        version_changed = F.lit(False)
        for hc in version_hash_cols:
            version_changed = version_changed | (F.col(f"src.{hc}") != F.col(f"tgt.{hc}"))
    else:
        version_changed = F.lit(False)

    # New-version rows: source rows where BK matched but hash changed.
    # Check ALL target-side BK columns to handle composite keys correctly.
    is_matched = reduce(
        lambda a, c: a & F.col(f"tgt.{c}").isNotNull(),
        bk_cols[1:],
        F.col(f"tgt.{bk_cols[0]}").isNotNull(),
    )
    new_versions = joined.filter(is_matched & version_changed)

    # Select only source columns for new version rows
    src_cols = [F.col(f"src.{c}") for c in source_df.columns]
    new_version_df = new_versions.select(src_cols)

    # Tag the new-version rows with a unique marker column so the MERGE
    # sees them as "not matched" (different BK value via the marker).
    marker = "__dim_new_version__"
    source_tagged = source_df.withColumn(marker, F.lit(False))
    new_version_tagged = new_version_df.withColumn(marker, F.lit(True))
    merged_source = source_tagged.unionByName(new_version_tagged)

    # Build merge condition: match on BK AND marker=False
    # (so new-version rows never match existing target rows)
    merge_parts = [
        f"target.{_quote_identifier(c)} <=> source.{_quote_identifier(c)}" for c in bk_cols
    ]
    merge_parts.append(f"source.{_quote_identifier(marker)} = false")
    merge_cond = " AND ".join(merge_parts)

    delta_table = resolve_delta_table(spark, target_path)
    merger = delta_table.alias("target").merge(merged_source.alias("source"), merge_cond)

    # Matched + version hash changed → close old row
    if version_hash_cols:
        version_change_sql = " OR ".join(
            f"source.{_quote_identifier(c)} != target.{_quote_identifier(c)}"
            for c in version_hash_cols
        )
        close_set: dict[str, Column] = {
            scd.valid_to: F.lit(run_timestamp),
            scd.is_current: F.lit(False),
        }
        # Apply overwrite columns during close
        for col_name in builder.get_overwrite_columns():
            close_set[col_name] = F.col(f"source.{_quote_identifier(col_name)}")
        # Apply previous_columns
        if config.previous_columns:
            for prev_col, src_col in config.previous_columns.items():
                close_set[prev_col] = F.col(f"target.{_quote_identifier(src_col)}")
        merger = merger.whenMatchedUpdate(condition=version_change_sql, set=close_set)

    # Not matched → insert (includes new entities AND new version rows).
    # IMPORTANT: use source_df.columns (not merged_source.columns) to
    # exclude the __dim_new_version__ marker from the insert values.
    # The marker must never be written to the target schema.
    insert_cols = {c: F.col(f"source.{_quote_identifier(c)}") for c in source_df.columns}
    merger = merger.whenNotMatchedInsert(values=insert_cols)

    # Handle on_no_match_source
    _apply_not_matched_by_source(merger, builder)

    merger.execute()
    return result


def _execute_standard_merge(
    spark: SparkSession,
    source_df: DataFrame,
    builder: DimensionMergeBuilder,
    target_path: str,
    run_timestamp: str,
) -> DimensionMergeResult:
    """Standard merge for non-versioned dimensions (Type 1)."""
    config = builder.config
    result = DimensionMergeResult()
    bk_cols = config.business_key

    merge_cond = " AND ".join(
        f"target.{_quote_identifier(c)} <=> source.{_quote_identifier(c)}" for c in bk_cols
    )

    delta_table = resolve_delta_table(spark, target_path)

    # Build change condition from all non-static hash cols
    change_hash_cols: list[str] = []
    if config.change_detection:
        for key, group in config.change_detection.items():
            col_name = group.name or key
            if group.on_change != "static":
                change_hash_cols.append(col_name)

    if change_hash_cols:
        change_cond = " OR ".join(
            f"source.{_quote_identifier(c)} != target.{_quote_identifier(c)}"
            for c in change_hash_cols
        )
    else:
        change_cond = "1 = 0"

    # Build update set (exclude BK, static, SCD, SK)
    static_cols = set(builder.get_static_columns())
    scd_col_names = builder.get_scd_column_names()
    sk_col = config.surrogate_key.name

    update_set: dict[str, Column] = {}
    for col in source_df.columns:
        if col in bk_cols or col in static_cols:
            continue
        if col in scd_col_names or col == sk_col:
            continue
        update_set[col] = F.col(f"source.{_quote_identifier(col)}")

    # Handle previous_columns
    if config.previous_columns:
        for prev_col, src_col in config.previous_columns.items():
            update_set[prev_col] = F.col(f"target.{_quote_identifier(src_col)}")

    merger = delta_table.alias("target").merge(source_df.alias("source"), merge_cond)

    if update_set:
        merger = merger.whenMatchedUpdate(condition=change_cond, set=update_set)

    merger = merger.whenNotMatchedInsertAll()
    _apply_not_matched_by_source(merger, builder)
    merger.execute()

    return result


def _apply_not_matched_by_source(
    merger: object,  # DeltaMergeBuilder — avoid import
    builder: DimensionMergeBuilder,
) -> None:
    """Apply on_no_match_source clauses with system member exclusion."""
    write_config = builder.build_write_config()
    sk_col = builder.config.surrogate_key.name
    system_member_sks = builder.get_system_member_sk_values()

    if write_config.on_no_match_source == "delete":
        if system_member_sks:
            exclusion = " AND ".join(
                f"target.{_quote_identifier(sk_col)} != {v}" for v in system_member_sks
            )
            merger.whenNotMatchedBySourceDelete(condition=exclusion)  # type: ignore[union-attr]
        else:
            merger.whenNotMatchedBySourceDelete()  # type: ignore[union-attr]
    elif write_config.on_no_match_source == "soft_delete":
        sd_col = write_config.soft_delete_column
        sd_val = write_config.soft_delete_value
        if sd_col:
            condition = None
            if system_member_sks:
                condition = " AND ".join(
                    f"target.{_quote_identifier(sk_col)} != {v}" for v in system_member_sks
                )
            merger.whenNotMatchedBySourceUpdate(  # type: ignore[union-attr]
                condition=condition, set={sd_col: F.lit(sd_val)}
            )
