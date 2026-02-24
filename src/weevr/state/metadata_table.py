"""MetadataTableStore — watermark persistence via dedicated Delta table."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from weevr.errors.exceptions import StateError
from weevr.state.watermark import WatermarkState, WatermarkStore

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_WATERMARK_SCHEMA = StructType(
    [
        StructField("thread_name", StringType(), False),
        StructField("watermark_column", StringType(), False),
        StructField("watermark_type", StringType(), False),
        StructField("last_value", StringType(), False),
        StructField("last_updated", TimestampType(), True),
        StructField("run_id", StringType(), True),
    ]
)


class MetadataTableStore(WatermarkStore):
    """Persists watermarks in a dedicated ``weevr_watermarks`` Delta table.

    The table is auto-created on first write if it does not exist.
    """

    def __init__(self, table_path: str) -> None:
        self._table_path = table_path

    @property
    def table_path(self) -> str:
        """Path to the watermarks metadata table."""
        return self._table_path

    def _ensure_table_exists(self, spark: SparkSession) -> None:
        """Create the watermarks table if it does not exist."""
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS delta.`{self._table_path}` (
                thread_name STRING,
                watermark_column STRING,
                watermark_type STRING,
                last_value STRING,
                last_updated TIMESTAMP,
                run_id STRING
            )
            USING DELTA
        """)

    def read(self, spark: SparkSession, thread_name: str) -> WatermarkState | None:
        """Load watermark state for a thread from the metadata table."""
        try:
            self._ensure_table_exists(spark)

            rows = (
                spark.read.format("delta")
                .load(self._table_path)
                .filter(f"thread_name = '{thread_name}'")
                .collect()
            )

            if not rows:
                return None

            row = rows[0]
            return WatermarkState(
                thread_name=row["thread_name"],
                watermark_column=row["watermark_column"],
                watermark_type=row["watermark_type"],
                last_value=row["last_value"],
                last_updated=row["last_updated"],
                run_id=row["run_id"],
            )
        except Exception as e:
            if isinstance(e, StateError):
                raise
            raise StateError(
                f"Failed to read watermark state for thread '{thread_name}'",
                cause=e,
                thread_name=thread_name,
                store_type="metadata_table",
            ) from e

    def write(self, spark: SparkSession, state: WatermarkState) -> None:
        """Upsert watermark state into the metadata table."""
        try:
            self._ensure_table_exists(spark)

            from delta.tables import DeltaTable

            state_df = spark.createDataFrame(
                [
                    (
                        state.thread_name,
                        state.watermark_column,
                        state.watermark_type,
                        state.last_value,
                        state.last_updated,
                        state.run_id,
                    )
                ],
                schema=_WATERMARK_SCHEMA,
            )

            target = DeltaTable.forPath(spark, self._table_path)
            (
                target.alias("target")
                .merge(state_df.alias("source"), "target.thread_name = source.thread_name")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        except Exception as e:
            if isinstance(e, StateError):
                raise
            raise StateError(
                f"Failed to write watermark state for thread '{state.thread_name}'",
                cause=e,
                thread_name=state.thread_name,
                store_type="metadata_table",
            ) from e
