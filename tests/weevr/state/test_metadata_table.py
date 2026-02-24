"""Spark integration tests for MetadataTableStore."""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from weevr.state.metadata_table import MetadataTableStore
from weevr.state.watermark import WatermarkState

pytestmark = pytest.mark.spark


class TestMetadataTableStore:
    """Integration tests for MetadataTableStore with real Delta tables."""

    def test_auto_creates_table(self, spark: SparkSession, tmp_delta_path) -> None:
        """Writing to a non-existent table path auto-creates the table."""
        path = tmp_delta_path("wm_autocreate")
        store = MetadataTableStore(path)

        state = WatermarkState(
            thread_name="t1",
            watermark_column="updated_at",
            watermark_type="timestamp",
            last_value="2024-01-01T00:00:00",
            last_updated=datetime(2024, 6, 1, tzinfo=UTC),
        )
        store.write(spark, state)

        # Table should now exist and be readable
        df = spark.read.format("delta").load(path)
        assert df.count() == 1

    def test_write_read_roundtrip(self, spark: SparkSession, tmp_delta_path) -> None:
        """Write state then read it back — values must match."""
        path = tmp_delta_path("wm_roundtrip")
        store = MetadataTableStore(path)

        written = WatermarkState(
            thread_name="thread_a",
            watermark_column="ts",
            watermark_type="timestamp",
            last_value="2024-06-15T12:00:00",
            last_updated=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
            run_id="run-001",
        )
        store.write(spark, written)

        result = store.read(spark, "thread_a")
        assert result is not None
        assert result.thread_name == "thread_a"
        assert result.watermark_column == "ts"
        assert result.watermark_type == "timestamp"
        assert result.last_value == "2024-06-15T12:00:00"
        assert result.run_id == "run-001"

    def test_update_existing(self, spark: SparkSession, tmp_delta_path) -> None:
        """Writing a second state for the same thread updates the existing row."""
        path = tmp_delta_path("wm_update")
        store = MetadataTableStore(path)

        first = WatermarkState(
            thread_name="t1",
            watermark_column="id",
            watermark_type="int",
            last_value="100",
            last_updated=datetime(2024, 1, 1, tzinfo=UTC),
        )
        store.write(spark, first)

        second = WatermarkState(
            thread_name="t1",
            watermark_column="id",
            watermark_type="int",
            last_value="200",
            last_updated=datetime(2024, 2, 1, tzinfo=UTC),
        )
        store.write(spark, second)

        result = store.read(spark, "t1")
        assert result is not None
        assert result.last_value == "200"

        # Should still be exactly one row for this thread
        df = spark.read.format("delta").load(path)
        assert df.filter("thread_name = 't1'").count() == 1

    def test_read_nonexistent_thread(self, spark: SparkSession, tmp_delta_path) -> None:
        """Reading state for a thread that was never written returns None."""
        path = tmp_delta_path("wm_nonexistent")
        store = MetadataTableStore(path)

        # Write one thread so the table exists
        state = WatermarkState(
            thread_name="exists",
            watermark_column="col",
            watermark_type="date",
            last_value="2024-01-01",
            last_updated=datetime(2024, 1, 1, tzinfo=UTC),
        )
        store.write(spark, state)

        result = store.read(spark, "does_not_exist")
        assert result is None

    def test_multiple_threads_isolated(self, spark: SparkSession, tmp_delta_path) -> None:
        """Multiple threads in the same table are isolated from each other."""
        path = tmp_delta_path("wm_multi")
        store = MetadataTableStore(path)

        for name, val in [("thread_x", "10"), ("thread_y", "20")]:
            store.write(
                spark,
                WatermarkState(
                    thread_name=name,
                    watermark_column="seq",
                    watermark_type="long",
                    last_value=val,
                    last_updated=datetime(2024, 1, 1, tzinfo=UTC),
                ),
            )

        x = store.read(spark, "thread_x")
        y = store.read(spark, "thread_y")
        assert x is not None and x.last_value == "10"
        assert y is not None and y.last_value == "20"
