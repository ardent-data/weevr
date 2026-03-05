"""TablePropertiesStore — watermark persistence via Delta table properties."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast

from weevr.errors.exceptions import StateError
from weevr.state.watermark import WatermarkState, WatermarkStore

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


_WatermarkType: TypeAlias = Literal["timestamp", "date", "int", "long"]
_PROP_PREFIX = "weevr.watermark"


class TablePropertiesStore(WatermarkStore):
    """Persists watermarks as Delta table properties on the target table.

    Properties are stored under the ``weevr.watermark.{thread_name}.*``
    namespace within the target table's tblproperties.
    """

    def __init__(self, target_path: str) -> None:
        """Initialize with the path to the Delta target table."""
        if "`" in target_path:
            raise StateError(
                "Target path must not contain backtick characters",
                store_type="table_properties",
            )
        self._target_path = target_path

    @property
    def target_path(self) -> str:
        """Path to the Delta target table."""
        return self._target_path

    @staticmethod
    def _is_table_alias(path: str) -> bool:
        """Return True if *path* looks like a metastore table alias."""
        return "://" not in path and "/" not in path

    def _resolve_delta_table(self, spark: SparkSession) -> Any:
        """Resolve the target as a DeltaTable, handling both paths and aliases."""
        from delta.tables import DeltaTable

        if self._is_table_alias(self._target_path):
            return DeltaTable.forName(spark, self._target_path)
        return DeltaTable.forPath(spark, self._target_path)

    def _table_exists(self, spark: SparkSession) -> bool:
        """Return True if the target Delta table exists."""
        try:
            if self._is_table_alias(self._target_path):
                spark.read.format("delta").table(self._target_path).limit(0).collect()
                return True
            from delta.tables import DeltaTable

            return DeltaTable.isDeltaTable(spark, self._target_path)
        except Exception:
            return False

    def read(self, spark: SparkSession, thread_name: str) -> WatermarkState | None:
        """Load watermark state from target table properties."""
        try:
            if not self._table_exists(spark):
                return None

            detail = self._resolve_delta_table(spark).detail()
            props_row = detail.select("properties").collect()

            if not props_row:
                return None

            props: dict[str, str] = props_row[0]["properties"] or {}
            prefix = f"{_PROP_PREFIX}.{thread_name}."

            wm_props = {k.removeprefix(prefix): v for k, v in props.items() if k.startswith(prefix)}

            if not wm_props or "last_value" not in wm_props:
                return None

            wm_type = cast(_WatermarkType, wm_props["watermark_type"])
            return WatermarkState(
                thread_name=thread_name,
                watermark_column=wm_props["watermark_column"],
                watermark_type=wm_type,
                last_value=wm_props["last_value"],
                last_updated=datetime.fromisoformat(wm_props["last_updated"]),
                run_id=wm_props.get("run_id") or None,
            )
        except Exception as e:
            if isinstance(e, StateError):
                raise
            raise StateError(
                f"Failed to read watermark state for thread '{thread_name}'",
                cause=e,
                thread_name=thread_name,
                store_type="table_properties",
            ) from e

    def write(self, spark: SparkSession, state: WatermarkState) -> None:
        """Persist watermark state as table properties on the target."""
        try:
            prefix = f"{_PROP_PREFIX}.{state.thread_name}"
            props = {
                f"{prefix}.last_value": state.last_value,
                f"{prefix}.watermark_column": state.watermark_column,
                f"{prefix}.watermark_type": state.watermark_type,
                f"{prefix}.last_updated": state.last_updated.isoformat(),
                f"{prefix}.run_id": state.run_id or "",
            }

            props_sql = ", ".join(
                f"'{k}' = '{v.replace(chr(39), chr(39) + chr(39))}'" for k, v in props.items()
            )
            if self._is_table_alias(self._target_path):
                table_ref = self._target_path
            else:
                table_ref = f"delta.`{self._target_path}`"
            spark.sql(f"ALTER TABLE {table_ref} SET TBLPROPERTIES ({props_sql})")
        except Exception as e:
            if isinstance(e, StateError):
                raise
            raise StateError(
                f"Failed to write watermark state for thread '{state.thread_name}'",
                cause=e,
                thread_name=state.thread_name,
                store_type="table_properties",
            ) from e
