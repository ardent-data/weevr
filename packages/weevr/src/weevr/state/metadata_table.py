"""MetadataTableStore — watermark persistence via dedicated Delta table."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from weevr.delta import is_table_alias, read_delta, resolve_delta_table
from weevr.errors.exceptions import StateError
from weevr.state.watermark import WatermarkState, WatermarkStore

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Read from the exception text, where Spark renders the error class. If it's ever
# missing, a lost race just falls back to the single check other failures get.
_CREATE_RACE_ERROR = "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION"
_CREATE_RACE_WAIT_SECONDS = 10.0
_CREATE_RACE_POLL_SECONDS = 0.1

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
        """Initialize with the path to the watermarks metadata table."""
        if "`" in table_path:
            raise StateError(
                "Table path must not contain backtick characters",
                store_type="metadata_table",
            )
        self._table_path = table_path
        self._table_ensured = False

    @property
    def table_path(self) -> str:
        """Path to the watermarks metadata table."""
        return self._table_path

    def _ensure_table_exists(self, spark: SparkSession) -> None:
        """Create the watermarks table if it does not exist.

        Runs the DDL at most once per store instance — table existence
        is monotonic within a run, so later reads and writes skip it.
        """
        if self._table_ensured:
            return
        if is_table_alias(self._table_path):
            table_ref = self._table_path
        else:
            table_ref = f"delta.`{self._table_path}`"
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_ref} (
                    thread_name STRING,
                    watermark_column STRING,
                    watermark_type STRING,
                    last_value STRING,
                    last_updated TIMESTAMP,
                    run_id STRING
                )
                USING DELTA
            """)
        except Exception as exc:
            lost_race = _CREATE_RACE_ERROR in str(exc)
            if not self._table_appears(spark, wait=lost_race):
                detail = "; the location is not empty and isn't a Delta table" if lost_race else ""
                raise StateError(
                    f"Failed to create the watermark table at '{self._table_path}'{detail}",
                    cause=exc,
                ) from exc
        self._table_ensured = True

    def _table_appears(self, spark: SparkSession, *, wait: bool) -> bool:
        """Whether the table appears after a failed create.

        Threads sharing a store race on first use: IF NOT EXISTS isn't
        atomic, so a loser can fail on the winner's uncommitted files, and
        the table only reads as Delta once the winner commits. Any other
        create failure gets one check, so a real fault doesn't wait.
        """
        if wait:
            logger.info("Waiting for a concurrent create of '%s' to commit", self._table_path)
        deadline = time.monotonic() + (_CREATE_RACE_WAIT_SECONDS if wait else 0.0)
        while not self._table_exists(spark):
            if time.monotonic() >= deadline:
                return False
            time.sleep(_CREATE_RACE_POLL_SECONDS)
        return True

    def _table_exists(self, spark: SparkSession) -> bool:
        """Whether the watermark table exists and reads as a watermark table."""
        try:
            columns = read_delta(spark, self._table_path).columns
        except Exception:
            return False
        return "thread_name" in columns

    def read(self, spark: SparkSession, thread_name: str) -> WatermarkState | None:
        """Load watermark state for a thread from the metadata table."""
        try:
            self._ensure_table_exists(spark)

            rows = (
                read_delta(spark, self._table_path)
                .filter(F.col("thread_name") == thread_name)
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

            target = resolve_delta_table(spark, self._table_path)
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
