"""Spark integration tests for TablePropertiesStore."""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from spark_helpers import create_delta_table
from weevr.errors.exceptions import StateError
from weevr.state.table_properties import TablePropertiesStore
from weevr.state.watermark import WatermarkState


class TestTablePropertiesStoreInit:
    """Unit tests for TablePropertiesStore constructor validation."""

    def test_rejects_backtick_in_path(self) -> None:
        with pytest.raises(StateError, match="backtick"):
            TablePropertiesStore("/path/with`backtick/table")

    def test_accepts_normal_path(self) -> None:
        store = TablePropertiesStore("/lakehouse/tables/my_table")
        assert store.target_path == "/lakehouse/tables/my_table"


@pytest.mark.spark
class TestTablePropertiesStore:
    """Integration tests for TablePropertiesStore with real Delta tables."""

    def test_write_read_roundtrip(self, spark: SparkSession, tmp_delta_path) -> None:
        """Write watermark state then read it back from tblproperties."""
        path = tmp_delta_path("tbl_roundtrip")
        create_delta_table(spark, path, [{"id": 1, "val": "a"}])

        store = TablePropertiesStore(path)
        written = WatermarkState(
            thread_name="t1",
            watermark_column="updated_at",
            watermark_type="timestamp",
            last_value="2024-06-15T12:00:00",
            last_updated=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
            run_id="run-abc",
        )
        store.write(spark, written)

        result = store.read(spark, "t1")
        assert result is not None
        assert result.thread_name == "t1"
        assert result.watermark_column == "updated_at"
        assert result.watermark_type == "timestamp"
        assert result.last_value == "2024-06-15T12:00:00"
        assert result.run_id == "run-abc"

    def test_read_no_watermark_properties(self, spark: SparkSession, tmp_delta_path) -> None:
        """Target table with no watermark tblproperties returns None."""
        path = tmp_delta_path("tbl_no_props")
        create_delta_table(spark, path, [{"id": 1}])

        store = TablePropertiesStore(path)
        result = store.read(spark, "some_thread")
        assert result is None

    def test_update_existing(self, spark: SparkSession, tmp_delta_path) -> None:
        """Writing a second state overwrites the existing tblproperties."""
        path = tmp_delta_path("tbl_update")
        create_delta_table(spark, path, [{"id": 1}])

        store = TablePropertiesStore(path)

        first = WatermarkState(
            thread_name="t1",
            watermark_column="ts",
            watermark_type="timestamp",
            last_value="2024-01-01T00:00:00",
            last_updated=datetime(2024, 1, 1, tzinfo=UTC),
        )
        store.write(spark, first)

        second = WatermarkState(
            thread_name="t1",
            watermark_column="ts",
            watermark_type="timestamp",
            last_value="2024-06-01T00:00:00",
            last_updated=datetime(2024, 6, 1, tzinfo=UTC),
        )
        store.write(spark, second)

        result = store.read(spark, "t1")
        assert result is not None
        assert result.last_value == "2024-06-01T00:00:00"

    def test_target_table_must_exist(self, spark: SparkSession, tmp_delta_path) -> None:
        """Writing to a non-existent target table raises StateError."""
        path = tmp_delta_path("tbl_not_exists") + "/nope"
        store = TablePropertiesStore(path)

        state = WatermarkState(
            thread_name="t1",
            watermark_column="col",
            watermark_type="int",
            last_value="42",
            last_updated=datetime(2024, 1, 1, tzinfo=UTC),
        )
        with pytest.raises(StateError, match="Failed to write watermark"):
            store.write(spark, state)

    def test_read_nonexistent_target_raises_state_error(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Reading from a non-existent target table raises StateError."""
        path = tmp_delta_path("tbl_read_missing") + "/nope"
        store = TablePropertiesStore(path)

        with pytest.raises(StateError, match="Failed to read watermark"):
            store.read(spark, "t1")

    def test_multiple_threads_on_same_table(self, spark: SparkSession, tmp_delta_path) -> None:
        """Multiple threads can store watermarks on the same target table."""
        path = tmp_delta_path("tbl_multi_thread")
        create_delta_table(spark, path, [{"id": 1}])

        store = TablePropertiesStore(path)

        for name, val in [("thread_a", "100"), ("thread_b", "200")]:
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

        a = store.read(spark, "thread_a")
        b = store.read(spark, "thread_b")
        assert a is not None and a.last_value == "100"
        assert b is not None and b.last_value == "200"
