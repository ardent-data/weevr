"""Spark integration tests for watermark incremental execution end-to-end."""

from typing import Literal
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from spark_helpers import create_delta_table
from weevr.engine.executor import execute_thread
from weevr.errors.exceptions import StateError
from weevr.model.load import LoadConfig, WatermarkStoreConfig
from weevr.model.source import Source
from weevr.model.target import Target
from weevr.model.thread import Thread
from weevr.model.write import WriteConfig
from weevr.state.metadata_table import MetadataTableStore

pytestmark = pytest.mark.spark


def _make_watermark_thread(
    name: str,
    source_path: str,
    target_path: str,
    wm_store_path: str,
    *,
    watermark_column: str = "ts",
    watermark_type: str = "int",
    watermark_inclusive: bool = False,
    write_mode: Literal["overwrite", "append", "merge"] = "overwrite",
) -> Thread:
    """Build a thread with incremental_watermark load config."""
    return Thread(
        name=name,
        config_version="1",
        sources={"main": Source(type="delta", alias=source_path)},
        steps=[],
        target=Target(path=target_path),
        write=WriteConfig(mode=write_mode),
        load=LoadConfig(
            mode="incremental_watermark",
            watermark_column=watermark_column,
            watermark_type=watermark_type,  # type: ignore[arg-type]
            watermark_inclusive=watermark_inclusive,
            watermark_store=WatermarkStoreConfig(
                type="metadata_table",
                table_path=wm_store_path,
            ),
        ),
    )


class TestWatermarkFirstRun:
    """First-run behavior: no prior state → full load → persist HWM."""

    def test_first_run_reads_all_rows(self, spark: SparkSession, tmp_delta_path) -> None:
        """First run with no prior state reads all source rows."""
        src = tmp_delta_path("wm_first_src")
        tgt = tmp_delta_path("wm_first_tgt")
        wm = tmp_delta_path("wm_first_store")
        create_delta_table(spark, src, [{"id": 1, "ts": 10}, {"id": 2, "ts": 20}])

        thread = _make_watermark_thread("t1", src, tgt, wm)
        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.rows_written == 2

    def test_first_run_persists_hwm(self, spark: SparkSession, tmp_delta_path) -> None:
        """First run persists the HWM from source data."""
        src = tmp_delta_path("wm_hwm_src")
        tgt = tmp_delta_path("wm_hwm_tgt")
        wm = tmp_delta_path("wm_hwm_store")
        create_delta_table(spark, src, [{"id": 1, "ts": 10}, {"id": 2, "ts": 20}])

        thread = _make_watermark_thread("t1", src, tgt, wm)
        execute_thread(spark, thread)

        store = MetadataTableStore(wm)
        state = store.read(spark, "t1")
        assert state is not None
        assert state.last_value == "20"

    def test_first_run_telemetry_shows_first_run(self, spark: SparkSession, tmp_delta_path) -> None:
        """First run telemetry reports watermark_first_run=True."""
        src = tmp_delta_path("wm_tel_src")
        tgt = tmp_delta_path("wm_tel_tgt")
        wm = tmp_delta_path("wm_tel_store")
        create_delta_table(spark, src, [{"id": 1, "ts": 10}])

        thread = _make_watermark_thread("t1", src, tgt, wm)
        result = execute_thread(spark, thread)

        assert result.telemetry is not None
        assert result.telemetry.watermark_first_run is True
        assert result.telemetry.watermark_persisted is True
        assert result.telemetry.watermark_previous_value is None
        assert result.telemetry.watermark_new_value == "10"


class TestWatermarkSecondRun:
    """Second-run behavior: prior state → filtered read."""

    def test_second_run_reads_only_new_rows(self, spark: SparkSession, tmp_delta_path) -> None:
        """Second run with HWM=20 only reads rows where ts > 20."""
        src = tmp_delta_path("wm_inc_src")
        tgt = tmp_delta_path("wm_inc_tgt")
        wm = tmp_delta_path("wm_inc_store")

        # First run: seed with initial data
        create_delta_table(spark, src, [{"id": 1, "ts": 10}, {"id": 2, "ts": 20}])
        thread = _make_watermark_thread("t1", src, tgt, wm)
        execute_thread(spark, thread)

        # Add new data above HWM
        new_data = spark.createDataFrame([{"id": 3, "ts": 30}, {"id": 4, "ts": 40}])
        new_data.write.format("delta").mode("append").save(src)

        # Second run: should only process ts > 20
        result = execute_thread(spark, thread)

        assert result.rows_written == 2
        written = spark.read.format("delta").load(tgt)
        ids = sorted([r["id"] for r in written.collect()])
        assert ids == [3, 4]

    def test_second_run_telemetry_not_first_run(self, spark: SparkSession, tmp_delta_path) -> None:
        """Second run telemetry reports watermark_first_run=False."""
        src = tmp_delta_path("wm_tel2_src")
        tgt = tmp_delta_path("wm_tel2_tgt")
        wm = tmp_delta_path("wm_tel2_store")

        create_delta_table(spark, src, [{"id": 1, "ts": 10}])
        thread = _make_watermark_thread("t1", src, tgt, wm)
        execute_thread(spark, thread)

        # Add new row and re-run
        spark.createDataFrame([{"id": 2, "ts": 20}]).write.format("delta").mode("append").save(src)
        result = execute_thread(spark, thread)

        assert result.telemetry is not None
        assert result.telemetry.watermark_first_run is False
        assert result.telemetry.watermark_previous_value == "10"
        assert result.telemetry.watermark_new_value == "20"

    def test_hwm_updates_after_second_run(self, spark: SparkSession, tmp_delta_path) -> None:
        """HWM is updated after second run to reflect new max."""
        src = tmp_delta_path("wm_upd_src")
        tgt = tmp_delta_path("wm_upd_tgt")
        wm = tmp_delta_path("wm_upd_store")

        create_delta_table(spark, src, [{"id": 1, "ts": 100}])
        thread = _make_watermark_thread("t1", src, tgt, wm)
        execute_thread(spark, thread)

        spark.createDataFrame([{"id": 2, "ts": 200}]).write.format("delta").mode("append").save(src)
        execute_thread(spark, thread)

        store = MetadataTableStore(wm)
        state = store.read(spark, "t1")
        assert state is not None
        assert state.last_value == "200"


class TestWatermarkInclusive:
    """Watermark inclusive vs exclusive filtering."""

    def test_exclusive_filter_skips_boundary_row(self, spark: SparkSession, tmp_delta_path) -> None:
        """Default exclusive filter (>) does not re-read the boundary row."""
        src = tmp_delta_path("wm_excl_src")
        tgt = tmp_delta_path("wm_excl_tgt")
        wm = tmp_delta_path("wm_excl_store")

        create_delta_table(spark, src, [{"id": 1, "ts": 10}, {"id": 2, "ts": 20}])
        thread = _make_watermark_thread("t1", src, tgt, wm, watermark_inclusive=False)
        execute_thread(spark, thread)

        # Re-run without adding new data — HWM=20, filter ts > 20 → 0 rows
        result = execute_thread(spark, thread)
        assert result.rows_written == 0

    def test_inclusive_filter_rereads_boundary_row(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Inclusive filter (>=) re-reads the boundary row."""
        src = tmp_delta_path("wm_incl_src")
        tgt = tmp_delta_path("wm_incl_tgt")
        wm = tmp_delta_path("wm_incl_store")

        create_delta_table(spark, src, [{"id": 1, "ts": 10}, {"id": 2, "ts": 20}])
        thread = _make_watermark_thread("t1", src, tgt, wm, watermark_inclusive=True)
        execute_thread(spark, thread)

        # Re-run without adding new data — HWM=20, filter ts >= 20 → 1 row
        result = execute_thread(spark, thread)
        assert result.rows_written == 1
        written = spark.read.format("delta").load(tgt)
        assert written.collect()[0]["ts"] == 20


class TestWatermarkHwmFromSource:
    """HWM is captured from source data, not from transformed output (DEC-003)."""

    def test_hwm_reflects_source_max_even_when_filtered(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """If transforms filter some rows, HWM still reflects source MAX."""
        from weevr.model.pipeline import FilterParams, FilterStep
        from weevr.model.types import SparkExpr

        src = tmp_delta_path("wm_dec3_src")
        tgt = tmp_delta_path("wm_dec3_tgt")
        wm = tmp_delta_path("wm_dec3_store")

        create_delta_table(
            spark,
            src,
            [
                {"id": 1, "ts": 10, "keep": True},
                {"id": 2, "ts": 20, "keep": False},
                {"id": 3, "ts": 30, "keep": True},
            ],
        )

        # The filter step will drop the ts=20 row, but HWM should still be 30
        thread = Thread(
            name="t1",
            config_version="1",
            sources={"main": Source(type="delta", alias=src)},
            steps=[FilterStep(filter=FilterParams(expr=SparkExpr("keep = true")))],
            target=Target(path=tgt),
            write=WriteConfig(mode="overwrite"),
            load=LoadConfig(
                mode="incremental_watermark",
                watermark_column="ts",
                watermark_type="int",
                watermark_store=WatermarkStoreConfig(
                    type="metadata_table",
                    table_path=wm,
                ),
            ),
        )
        result = execute_thread(spark, thread)

        # Only 2 rows written (keep=true), but HWM captured before transforms
        assert result.rows_written == 2
        assert result.telemetry is not None
        assert result.telemetry.watermark_new_value == "30"

        store = MetadataTableStore(wm)
        state = store.read(spark, "t1")
        assert state is not None
        assert state.last_value == "30"


class TestWatermarkPersistenceFailure:
    """Watermark persistence failure fails the thread (DEC-007)."""

    def test_persistence_failure_raises_state_error(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """If watermark store.write() fails, thread raises StateError."""
        src = tmp_delta_path("wm_fail_src")
        tgt = tmp_delta_path("wm_fail_tgt")
        wm = tmp_delta_path("wm_fail_store")

        create_delta_table(spark, src, [{"id": 1, "ts": 10}])
        thread = _make_watermark_thread("t1", src, tgt, wm)

        with patch("weevr.engine.executor.resolve_store") as mock_resolve:
            mock_store = MagicMock()
            mock_store.read.return_value = None  # first run
            mock_store.write.side_effect = StateError("Simulated write failure", thread_name="t1")
            mock_resolve.return_value = mock_store

            with pytest.raises(StateError, match="Simulated write failure"):
                execute_thread(spark, thread)


class TestWatermarkTimestamp:
    """Watermark with timestamp type — verifies non-int watermark types work end-to-end."""

    def test_timestamp_watermark_filters_correctly(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Timestamp watermark persists ISO value and filters on second run."""
        from pyspark.sql import functions as F

        src = tmp_delta_path("wm_ts_src")
        tgt = tmp_delta_path("wm_ts_tgt")
        wm = tmp_delta_path("wm_ts_store")

        initial = spark.createDataFrame(
            [
                (1, "2024-01-01T00:00:00", "a"),
                (2, "2024-06-15T12:00:00", "b"),
            ],
            ["id", "updated_at", "val"],
        ).withColumn("updated_at", F.col("updated_at").cast("timestamp"))
        initial.write.format("delta").mode("overwrite").save(src)

        thread = _make_watermark_thread(
            "ts_thread",
            src,
            tgt,
            wm,
            watermark_column="updated_at",
            watermark_type="timestamp",
        )

        # First run — reads all rows
        result1 = execute_thread(spark, thread)
        assert result1.rows_written == 2
        assert result1.telemetry is not None
        assert result1.telemetry.watermark_first_run is True
        assert result1.telemetry.watermark_new_value is not None
        assert "2024-06-15" in result1.telemetry.watermark_new_value

        # Verify HWM persisted
        store = MetadataTableStore(wm)
        state = store.read(spark, "ts_thread")
        assert state is not None
        assert state.watermark_type == "timestamp"
        assert "2024-06-15" in state.last_value

        # Add new data after the HWM
        new_rows = spark.createDataFrame(
            [(3, "2024-12-01T00:00:00", "c")],
            ["id", "updated_at", "val"],
        ).withColumn("updated_at", F.col("updated_at").cast("timestamp"))
        new_rows.write.format("delta").mode("append").save(src)

        # Second run — should only read the new row
        result2 = execute_thread(spark, thread)
        assert result2.rows_written == 1
        assert result2.telemetry is not None
        assert result2.telemetry.watermark_first_run is False
        assert "2024-12-01" in (result2.telemetry.watermark_new_value or "")
