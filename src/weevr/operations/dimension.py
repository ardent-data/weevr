"""Dimension merge builder — translates DimensionConfig into merge operations.

Provides the ``DimensionMergeBuilder`` class that generates write config,
key config, and SCD column injection logic from a composable dimension
target configuration.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from weevr.model.dimension import DimensionConfig
from weevr.model.write import WriteConfig

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
