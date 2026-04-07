"""Spark integration tests for CDC merge operations."""

from datetime import datetime
from typing import Literal
from unittest.mock import MagicMock, patch

import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from spark_helpers import create_delta_table
from weevr.engine.executor import execute_thread
from weevr.errors.exceptions import StateError
from weevr.model.load import CdcConfig, LoadConfig, WatermarkStoreConfig
from weevr.model.source import Source
from weevr.model.target import Target
from weevr.model.thread import Thread
from weevr.model.write import WriteConfig
from weevr.operations.readers import read_cdc_source
from weevr.operations.writers import execute_cdc_merge
from weevr.state.metadata_table import MetadataTableStore

pytestmark = pytest.mark.spark


def _make_cdc_watermark_thread(
    name: str,
    source_path: str,
    target_path: str,
    *,
    wm_store_path: str | None = None,
    watermark_inclusive: bool = False,
    store_type: Literal["table_properties", "metadata_table"] = "metadata_table",
) -> Thread:
    """Build a generic-CDC thread that composes with a watermark column.

    Uses ``mode=cdc`` with an explicit operation_column mapping and an
    AEDATTM-style timestamp watermark column.
    """
    if store_type == "metadata_table":
        assert wm_store_path is not None
        wm_store = WatermarkStoreConfig(type="metadata_table", table_path=wm_store_path)
    else:
        wm_store = WatermarkStoreConfig(type="table_properties")

    return Thread(
        name=name,
        config_version="1",
        sources={"main": Source(type="delta", alias=source_path)},
        steps=[],
        target=Target(path=target_path),
        write=WriteConfig(mode="merge", match_keys=["id"]),
        load=LoadConfig(
            mode="cdc",
            cdc=CdcConfig(
                operation_column="OPFLAG",
                insert_value="I",
                update_value="U",
                delete_value="D",
                on_delete="hard_delete",
            ),
            watermark_column="AEDATTM",
            watermark_type="timestamp",
            watermark_inclusive=watermark_inclusive,
            watermark_store=wm_store,
        ),
    )


class TestCdcGenericMerge:
    """CDC merge with generic (explicit) operation column."""

    def test_insert_update_delete(self, spark: SparkSession, tmp_delta_path) -> None:
        """Generic CDC with all three operation types applied correctly."""
        tgt = tmp_delta_path("cdc_iud_tgt")
        create_delta_table(
            spark,
            tgt,
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"},
            ],
        )

        cdc_df = spark.createDataFrame(
            [
                {"id": 4, "name": "Dave", "op": "I"},
                {"id": 2, "name": "Bobby", "op": "U"},
                {"id": 3, "name": "Charlie", "op": "D"},
            ]
        )

        cdc_config = CdcConfig(
            operation_column="op",
            insert_value="I",
            update_value="U",
            delete_value="D",
        )
        write_config = WriteConfig(mode="merge", match_keys=["id"])

        counts = execute_cdc_merge(spark, cdc_df, tgt, write_config, cdc_config)

        assert counts["inserts"] == 1
        assert counts["updates"] == 1
        assert counts["deletes"] == 1

        result = spark.read.format("delta").load(tgt)
        rows = {r["id"]: r["name"] for r in result.collect()}
        assert rows[1] == "Alice"  # unchanged
        assert rows[4] == "Dave"  # inserted
        assert rows[2] == "Bobby"  # updated
        assert 3 not in rows  # hard deleted

    def test_first_cdc_write_creates_table(self, spark: SparkSession, tmp_delta_path) -> None:
        """First CDC write to non-existent target creates the table (excluding deletes)."""
        tgt = tmp_delta_path("cdc_first_tgt")

        cdc_df = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice", "op": "I"},
                {"id": 2, "name": "Bob", "op": "D"},
            ]
        )

        cdc_config = CdcConfig(
            operation_column="op",
            insert_value="I",
            update_value="U",
            delete_value="D",
        )
        write_config = WriteConfig(mode="merge", match_keys=["id"])

        counts = execute_cdc_merge(spark, cdc_df, tgt, write_config, cdc_config)

        assert counts["inserts"] == 1
        assert counts["deletes"] == 1

        result = spark.read.format("delta").load(tgt)
        assert result.count() == 1
        assert result.collect()[0]["name"] == "Alice"


class TestCdcSoftDelete:
    """CDC with soft delete — delete rows get a flag column set."""

    def test_soft_delete_sets_flag(self, spark: SparkSession, tmp_delta_path) -> None:
        """on_delete=soft_delete sets the flag column instead of removing the row."""
        tgt = tmp_delta_path("cdc_soft_tgt")
        create_delta_table(
            spark,
            tgt,
            [
                {"id": 1, "name": "Alice", "is_deleted": False},
                {"id": 2, "name": "Bob", "is_deleted": False},
            ],
        )

        cdc_df = spark.createDataFrame([{"id": 2, "name": "Bob", "op": "D", "is_deleted": False}])

        cdc_config = CdcConfig(
            operation_column="op",
            insert_value="I",
            update_value="U",
            delete_value="D",
            on_delete="soft_delete",
        )
        write_config = WriteConfig(
            mode="merge",
            match_keys=["id"],
            soft_delete_column="is_deleted",
        )

        execute_cdc_merge(spark, cdc_df, tgt, write_config, cdc_config)

        result = spark.read.format("delta").load(tgt)
        rows = {r["id"]: r for r in result.collect()}
        assert result.count() == 2  # Row not physically removed
        assert rows[1]["is_deleted"] is False  # unchanged
        assert rows[2]["is_deleted"] is True  # soft deleted

    def test_hard_delete_removes_row(self, spark: SparkSession, tmp_delta_path) -> None:
        """on_delete=hard_delete physically removes the matching row."""
        tgt = tmp_delta_path("cdc_hard_tgt")
        create_delta_table(
            spark,
            tgt,
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        )

        cdc_df = spark.createDataFrame([{"id": 2, "name": "Bob", "op": "D"}])

        cdc_config = CdcConfig(
            operation_column="op",
            insert_value="I",
            update_value="U",
            delete_value="D",
            on_delete="hard_delete",
        )
        write_config = WriteConfig(mode="merge", match_keys=["id"])

        execute_cdc_merge(spark, cdc_df, tgt, write_config, cdc_config)

        result = spark.read.format("delta").load(tgt)
        assert result.count() == 1
        assert result.collect()[0]["id"] == 1

    def test_soft_delete_active_value_on_insert_and_update(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """soft_delete_active_value is written to inserted/updated rows;
        delete op still sets soft_delete_value."""
        cdc_df = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice", "op": "U", "is_deleted": False},  # re-appears via update
                {"id": 2, "name": "Bobby", "op": "U", "is_deleted": False},  # normal update
                {"id": 3, "name": "Charlie", "op": "I", "is_deleted": False},  # new insert
                {"id": 4, "name": "Dave", "op": "D", "is_deleted": False},  # soft-delete
            ]
        )

        cdc_config = CdcConfig(
            operation_column="op",
            insert_value="I",
            update_value="U",
            delete_value="D",
            on_delete="soft_delete",
        )
        write_config = WriteConfig(
            mode="merge",
            match_keys=["id"],
            soft_delete_column="is_deleted",
            soft_delete_value=True,
            soft_delete_active_value=False,
        )

        # id=4 doesn't exist in target — create it first so the merge sees it as a target row
        from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("is_deleted", BooleanType(), nullable=True),
            ]
        )
        tgt2 = tmp_delta_path("cdc_sd_default_tgt2")
        create_delta_table(
            spark,
            tgt2,
            [
                {"id": 1, "name": "Alice", "is_deleted": True},
                {"id": 2, "name": "Bob", "is_deleted": False},
                {"id": 4, "name": "Dave", "is_deleted": False},
            ],
            schema=schema,
        )

        execute_cdc_merge(spark, cdc_df, tgt2, write_config, cdc_config)

        result = spark.read.format("delta").load(tgt2)
        rows = {r["id"]: r for r in result.collect()}
        # id=1 updated: soft_delete_active_value resets the flag to False
        assert rows[1]["is_deleted"] is False
        # id=2 updated normally: soft_delete_active_value written
        assert rows[2]["is_deleted"] is False
        # id=3 inserted: soft_delete_active_value written
        assert rows[3]["is_deleted"] is False
        # id=4 soft-deleted: soft_delete_value (True) written
        assert rows[4]["is_deleted"] is True


class TestCdcInsertOnly:
    """CDC with insert-only operations (no update/delete values)."""

    def test_insert_only_cdc(self, spark: SparkSession, tmp_delta_path) -> None:
        """CDC with only insert_value inserts new rows, ignores existing."""
        tgt = tmp_delta_path("cdc_ins_tgt")
        create_delta_table(spark, tgt, [{"id": 1, "val": "a"}])

        cdc_df = spark.createDataFrame(
            [
                {"id": 1, "val": "a_updated", "op": "I"},
                {"id": 2, "val": "b", "op": "I"},
            ]
        )

        cdc_config = CdcConfig(
            operation_column="op",
            insert_value="I",
            update_value="U",
        )
        write_config = WriteConfig(mode="merge", match_keys=["id"])

        counts = execute_cdc_merge(spark, cdc_df, tgt, write_config, cdc_config)

        assert counts["inserts"] == 2
        result = spark.read.format("delta").load(tgt)
        assert result.count() == 2


class TestCdcDeltaCdf:
    """CDC with Delta Change Data Feed preset."""

    @staticmethod
    def _enable_cdf(spark: SparkSession, path: str) -> None:
        """Enable Change Data Feed on an existing Delta table."""
        spark.sql(
            f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
        )

    def test_cdf_reads_changes(self, spark: SparkSession, tmp_delta_path) -> None:
        """Delta CDF preset reads insert/update/delete changes from CDF."""
        from weevr.model.source import Source

        src = tmp_delta_path("cdf_src")

        # Create source table and enable CDF
        create_delta_table(spark, src, [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        self._enable_cdf(spark, src)

        # Record the version after enabling CDF
        initial_version = (
            DeltaTable.forPath(spark, src).history(1).select("version").collect()[0]["version"]
        )

        # Write changes — these will be captured in CDF
        dt = DeltaTable.forPath(spark, src)
        # Update id=2
        dt.update(condition="id = 2", set={"name": "'Bobby'"})
        # Delete id=1
        dt.delete(condition="id = 1")
        # Insert id=3
        spark.createDataFrame([{"id": 3, "name": "Charlie"}]).write.format("delta").mode(
            "append"
        ).save(src)

        # Read CDF changes starting after our initial version
        source = Source(type="delta", alias=src)
        cdc_config = CdcConfig(preset="delta_cdf")
        cdf_df, _ = read_cdc_source(spark, source, cdc_config, last_version=initial_version)

        # Verify CDF columns present
        assert "_change_type" in cdf_df.columns
        assert "_commit_version" in cdf_df.columns

        # Verify we got change rows
        change_types = {r["_change_type"] for r in cdf_df.collect()}
        assert "update_postimage" in change_types
        assert "delete" in change_types
        assert "insert" in change_types

    def test_cdf_merge_end_to_end(self, spark: SparkSession, tmp_delta_path) -> None:
        """Delta CDF preset end-to-end: read CDF changes and merge to target."""
        from weevr.model.source import Source

        src = tmp_delta_path("cdf_e2e_src")
        tgt = tmp_delta_path("cdf_e2e_tgt")

        # Create source and target
        create_delta_table(spark, src, [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        create_delta_table(spark, tgt, [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        self._enable_cdf(spark, src)

        initial_version = (
            DeltaTable.forPath(spark, src).history(1).select("version").collect()[0]["version"]
        )

        # Make changes in source
        dt = DeltaTable.forPath(spark, src)
        dt.update(condition="id = 2", set={"name": "'Bobby'"})

        # Read CDF
        source = Source(type="delta", alias=src)
        cdc_config = CdcConfig(preset="delta_cdf", on_delete="hard_delete")
        cdf_df, _ = read_cdc_source(spark, source, cdc_config, last_version=initial_version)

        # Merge CDF changes to target
        write_config = WriteConfig(mode="merge", match_keys=["id"])
        counts = execute_cdc_merge(spark, cdf_df, tgt, write_config, cdc_config)

        assert counts["updates"] == 1

        result = spark.read.format("delta").load(tgt)
        rows = {r["id"]: r["name"] for r in result.collect()}
        assert rows[2] == "Bobby"
        assert rows[1] == "Alice"  # unchanged


class TestCdcWatermarkComposition:
    """End-to-end tests for generic CDC composed with watermark filter.

    Exercises the executor wiring: read_cdc_source returns the new HWM,
    the executor routes it through the existing save_watermark plumbing,
    and the next run reads only rows past the persisted HWM. Mirrors the
    pattern in test_watermark_integration.py.
    """

    def _seed_cdc_source(
        self, spark: SparkSession, path: str, rows: list[tuple[int, str, str]]
    ) -> None:
        df = spark.createDataFrame(
            [(rid, op, datetime.fromisoformat(ts)) for rid, op, ts in rows],
            "id INT, OPFLAG STRING, AEDATTM TIMESTAMP",
        )
        df.write.format("delta").mode("overwrite").save(path)

    def _append_cdc_rows(
        self, spark: SparkSession, path: str, rows: list[tuple[int, str, str]]
    ) -> None:
        df = spark.createDataFrame(
            [(rid, op, datetime.fromisoformat(ts)) for rid, op, ts in rows],
            "id INT, OPFLAG STRING, AEDATTM TIMESTAMP",
        )
        df.write.format("delta").mode("append").save(path)

    def test_steady_state_two_iterations_metadata_table(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Two-iteration steady state: second run reads strictly fewer rows."""
        src = tmp_delta_path("cdc_wm_src")
        tgt = tmp_delta_path("cdc_wm_tgt")
        wm = tmp_delta_path("cdc_wm_store")

        self._seed_cdc_source(
            spark,
            src,
            [
                (1, "I", "2026-01-01T10:00:00"),
                (2, "I", "2026-01-02T10:00:00"),
                (3, "U", "2026-01-03T10:00:00"),
            ],
        )

        thread = _make_cdc_watermark_thread("cdc_wm_t1", src, tgt, wm_store_path=wm)

        # First run reads all 3 rows
        result1 = execute_thread(spark, thread)
        assert result1.status == "success"

        store = MetadataTableStore(wm)
        state1 = store.read(spark, "cdc_wm_t1")
        assert state1 is not None
        assert "2026-01-03" in state1.last_value

        # Append new rows past the HWM
        self._append_cdc_rows(
            spark,
            src,
            [
                (4, "I", "2026-01-05T10:00:00"),
                (3, "D", "2026-01-06T10:00:00"),  # delete row carries timestamp
            ],
        )

        # Second run should narrow to only the appended rows
        result2 = execute_thread(spark, thread)
        assert result2.status == "success"
        assert result2.telemetry is not None
        assert result2.telemetry.rows_read == 2

        # HWM advanced past the delete row's timestamp (DEC-003)
        state2 = store.read(spark, "cdc_wm_t1")
        assert state2 is not None
        assert "2026-01-06" in state2.last_value

        # Target reflects the cumulative state: id=4 inserted, id=3 deleted.
        # The CDC merge strips OPFLAG from the written rows, so we only read ids.
        target_ids = {r["id"] for r in spark.read.format("delta").load(tgt).collect()}
        assert 4 in target_ids
        assert 3 not in target_ids  # hard-deleted by the D row in iteration 2

    def test_table_properties_backend(self, spark: SparkSession, tmp_delta_path) -> None:
        """table_properties watermark store works for CDC + watermark threads."""
        src = tmp_delta_path("cdc_wm_tp_src")
        tgt = tmp_delta_path("cdc_wm_tp_tgt")

        self._seed_cdc_source(
            spark,
            src,
            [
                (1, "I", "2026-01-01T10:00:00"),
                (2, "I", "2026-01-02T10:00:00"),
            ],
        )

        thread = _make_cdc_watermark_thread("cdc_wm_tp_t1", src, tgt, store_type="table_properties")

        # First run: reads both rows, persists HWM in target table properties
        result1 = execute_thread(spark, thread)
        assert result1.status == "success"

        # Append rows past the HWM
        self._append_cdc_rows(spark, src, [(3, "I", "2026-01-05T10:00:00")])

        # Second run: HWM read from target table properties, only the new row
        result2 = execute_thread(spark, thread)
        assert result2.status == "success"
        assert result2.telemetry is not None
        assert result2.telemetry.rows_read == 1

        target_rows = {r["id"] for r in spark.read.format("delta").load(tgt).collect()}
        assert target_rows == {1, 2, 3}

    def test_executor_unpacks_hwm_single_run(self, spark: SparkSession, tmp_delta_path) -> None:
        """Lightweight executor smoke test: single run persists CDC HWM.

        Asserts the tuple-unpack contract from read_cdc_source is wired
        through the executor and reaches save_watermark. Heavier
        steady-state and recovery scenarios live in the other tests in
        this class.
        """
        src = tmp_delta_path("cdc_wm_smoke_src")
        tgt = tmp_delta_path("cdc_wm_smoke_tgt")
        wm = tmp_delta_path("cdc_wm_smoke_store")

        self._seed_cdc_source(
            spark,
            src,
            [(1, "I", "2026-01-01T10:00:00")],
        )

        thread = _make_cdc_watermark_thread("cdc_wm_smoke_t1", src, tgt, wm_store_path=wm)
        result = execute_thread(spark, thread)
        assert result.status == "success"

        store = MetadataTableStore(wm)
        state = store.read(spark, "cdc_wm_smoke_t1")
        assert state is not None
        assert state.last_value is not None

    def test_inclusive_filter_rereads_boundary_row(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """watermark_inclusive=True re-reads the boundary row on the second run."""
        src = tmp_delta_path("cdc_wm_incl_src")
        tgt = tmp_delta_path("cdc_wm_incl_tgt")
        wm = tmp_delta_path("cdc_wm_incl_store")

        self._seed_cdc_source(
            spark,
            src,
            [
                (1, "I", "2026-01-01T10:00:00"),
                (2, "I", "2026-01-02T10:00:00"),
            ],
        )

        thread = _make_cdc_watermark_thread(
            "cdc_wm_incl_t1", src, tgt, wm_store_path=wm, watermark_inclusive=True
        )

        # First run consumes both rows; HWM = max(AEDATTM) = 2026-01-02
        execute_thread(spark, thread)

        # Re-run with no new rows: inclusive >= filter re-reads the boundary
        result = execute_thread(spark, thread)
        assert result.status == "success"
        assert result.telemetry is not None
        assert result.telemetry.rows_read == 1

    def test_empty_window_leaves_prior_hwm_untouched(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """DEC-001: empty subsequent window does not overwrite the persisted HWM."""
        src = tmp_delta_path("cdc_wm_empty_src")
        tgt = tmp_delta_path("cdc_wm_empty_tgt")
        wm = tmp_delta_path("cdc_wm_empty_store")

        self._seed_cdc_source(
            spark,
            src,
            [
                (1, "I", "2026-01-01T10:00:00"),
                (2, "I", "2026-01-02T10:00:00"),
            ],
        )

        thread = _make_cdc_watermark_thread("cdc_wm_empty_t1", src, tgt, wm_store_path=wm)

        # First run captures HWM = 2026-01-02
        execute_thread(spark, thread)
        store = MetadataTableStore(wm)
        state_before = store.read(spark, "cdc_wm_empty_t1")
        assert state_before is not None
        prior_hwm = state_before.last_value

        # Re-run with no new rows: filtered window is empty, save_watermark
        # is not called, prior HWM stays in place.
        result = execute_thread(spark, thread)
        assert result.status == "success"
        assert result.telemetry is not None
        assert result.telemetry.rows_read == 0

        state_after = store.read(spark, "cdc_wm_empty_t1")
        assert state_after is not None
        assert state_after.last_value == prior_hwm

    def test_state_write_failure_leaves_prior_hwm_untouched(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """A watermark-store write failure does not advance the persisted HWM
        and the next run reprocesses the same window idempotently.

        Two phases:
        1. First run succeeds and persists HWM for row 1.
        2. Second run adds row 2 to the source but the state-store write
           is patched to raise StateError. The data-layer merge still
           runs (that's the failure mode being exercised), but the HWM
           is not advanced.
        3. Third run (no patch) re-reads the same window, remerges row 2
           idempotently via match keys, and advances the HWM.
        """
        src = tmp_delta_path("cdc_wm_fail_src")
        tgt = tmp_delta_path("cdc_wm_fail_tgt")
        wm = tmp_delta_path("cdc_wm_fail_store")

        self._seed_cdc_source(
            spark,
            src,
            [(1, "I", "2026-01-01T10:00:00")],
        )

        thread = _make_cdc_watermark_thread("cdc_wm_fail_t1", src, tgt, wm_store_path=wm)

        # Phase 1: successful first run establishes prior HWM.
        execute_thread(spark, thread)
        store = MetadataTableStore(wm)
        prior_state = store.read(spark, "cdc_wm_fail_t1")
        assert prior_state is not None
        prior_hwm = prior_state.last_value

        # Append a second row so the next run has new work to do.
        self._append_cdc_rows(spark, src, [(2, "I", "2026-01-02T10:00:00")])

        # Phase 2: patch the state store write to simulate a failure after
        # the data-write succeeds. Preserve the real read so prior_state is
        # still loaded.
        real_store = MetadataTableStore(wm)
        with patch("weevr.engine.executor.resolve_store") as mock_resolve:
            mock_store = MagicMock()
            mock_store.read.side_effect = lambda s, name: real_store.read(s, name)
            mock_store.write.side_effect = StateError(
                "Simulated state write failure", thread_name="cdc_wm_fail_t1"
            )
            mock_resolve.return_value = mock_store

            with pytest.raises(StateError, match="Simulated state write failure"):
                execute_thread(spark, thread)

        # Persisted HWM unchanged.
        state_after_failure = store.read(spark, "cdc_wm_fail_t1")
        assert state_after_failure is not None
        assert state_after_failure.last_value == prior_hwm

        # Phase 3: rerun without the patch. The same filtered window is
        # reprocessed; CDC merge is idempotent on match keys so row 2 ends
        # up present exactly once, and the HWM advances.
        result = execute_thread(spark, thread)
        assert result.status == "success"

        state_after_recovery = store.read(spark, "cdc_wm_fail_t1")
        assert state_after_recovery is not None
        assert state_after_recovery.last_value != prior_hwm

        target_df = spark.read.format("delta").load(tgt).orderBy("id")
        ids = [row["id"] for row in target_df.collect()]
        assert ids == [1, 2]
