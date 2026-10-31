"""Watermarks never skip or duplicate rows, end to end through ``execute_thread``."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession

from weevr.engine.executor import execute_thread
from weevr.model.thread import Thread
from weevr.state.metadata_table import MetadataTableStore
from weevr.state.table_properties import TablePropertiesStore
from weevr.state.watermark import WatermarkState

pytestmark = pytest.mark.spark


@pytest.fixture()
def new_york_session(spark: SparkSession) -> Iterator[SparkSession]:
    """Session zone America/New_York while the driver stays on its own zone (UTC in CI)."""
    previous = spark.conf.get("spark.sql.session.timeZone") or "UTC"
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    yield spark
    spark.conf.set("spark.sql.session.timeZone", previous)


def _thread(
    src: str,
    tgt: str,
    store: str,
    *,
    wm_type: str = "timestamp",
    wm_col: str = "ts",
    load_extra: dict[str, Any] | None = None,
    name: str = "t",
) -> Thread:
    return Thread.model_validate(
        {
            "name": name,
            "config_version": "1.0",
            "sources": {"main": {"type": "delta", "alias": src}},
            "target": {"path": tgt},
            "write": {"mode": "append"},
            "load": {
                "mode": "incremental_watermark",
                "watermark_column": wm_col,
                "watermark_type": wm_type,
                "watermark_store": {"type": "metadata_table", "table_path": store},
                **(load_extra or {}),
            },
        }
    )


def _append(spark: SparkSession, path: str, sql: str) -> None:
    spark.sql(sql).write.format("delta").mode("append").save(path)


class TestTimestampWatermarkZones:
    """A stored timestamp HWM denotes the same instant in every zone."""

    def test_new_row_after_run_is_read_when_zones_differ(self, new_york_session, tmp_path):
        spark = new_york_session
        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(
            spark,
            src,
            "select 1 as id, timestamp'2024-01-01 10:00:00' as ts "
            "union all select 2, timestamp'2024-01-01 12:00:00'",
        )
        thread = _thread(src, tgt, store)
        assert execute_thread(spark, thread).status == "success"
        _append(spark, src, "select 3 as id, timestamp'2024-01-01 14:00:00' as ts")

        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert spark.read.format("delta").load(tgt).count() == 3
        state = MetadataTableStore(store).read(spark, "t")
        assert state is not None
        # 14:00 New York = 19:00 UTC
        assert state.last_value == "2024-01-01T19:00:00.000000Z"

    def test_legacy_value_read_in_driver_zone_and_rewritten(self, new_york_session, tmp_path):
        spark = new_york_session
        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(
            spark,
            src,
            "select 1 as id, timestamp'2024-01-01 12:00:00' as ts "
            "union all select 2, timestamp'2024-01-01 14:00:00'",
        )
        # A pre-fix value: str() of the collected 12:00 NY row on a UTC driver
        MetadataTableStore(store).write(
            spark,
            WatermarkState(
                thread_name="t",
                watermark_column="ts",
                watermark_type="timestamp",
                last_value="2024-01-01 17:00:00",
                last_updated=datetime.now(UTC),
            ),
        )
        with patch("weevr.state.hwm._driver_tz", return_value=UTC):
            result = execute_thread(spark, _thread(src, tgt, store))

        assert result.status == "success"
        rows = spark.read.format("delta").load(tgt).collect()
        assert [r.id for r in rows] == [2]
        state = MetadataTableStore(store).read(spark, "t")
        assert state is not None and state.last_value.endswith("Z")

    def test_legacy_value_honors_a_non_utc_driver(self, spark: SparkSession, tmp_path):
        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(
            spark,
            src,
            "select 1 as id, timestamp'2024-01-01 17:00:00' as ts "
            "union all select 2, timestamp'2024-01-01 18:00:00'",
        )
        MetadataTableStore(store).write(
            spark,
            WatermarkState(
                thread_name="t",
                watermark_column="ts",
                watermark_type="timestamp",
                last_value="2024-01-01 12:00:00",  # 12:00 on a UTC-5 driver = 17:00 UTC
                last_updated=datetime.now(UTC),
            ),
        )
        with patch("weevr.state.hwm._driver_tz", return_value=timezone(timedelta(hours=-5))):
            result = execute_thread(spark, _thread(src, tgt, store))
        assert result.status == "success"
        assert [r.id for r in spark.read.format("delta").load(tgt).collect()] == [2]


class TestOtherWatermarkTypes:
    """date, numeric, NTZ and formatted-string watermarks keep working."""

    def test_date_watermark_text_unchanged(self, spark: SparkSession, tmp_path):
        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(spark, src, "select 1 as id, date'2024-01-01' as d")
        thread = _thread(src, tgt, store, wm_type="date", wm_col="d")
        assert execute_thread(spark, thread).status == "success"
        state = MetadataTableStore(store).read(spark, "t")
        assert state is not None and state.last_value == "2024-01-01"

    def test_ntz_watermark_round_trip(self, new_york_session, tmp_path):
        spark = new_york_session
        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(spark, src, "select 1 as id, timestamp_ntz'2024-01-01 12:00:00' as ts")
        thread = _thread(src, tgt, store)
        assert execute_thread(spark, thread).status == "success"
        _append(spark, src, "select 2 as id, timestamp_ntz'2024-01-01 14:00:00' as ts")
        assert execute_thread(spark, thread).status == "success"
        assert spark.read.format("delta").load(tgt).count() == 2
        state = MetadataTableStore(store).read(spark, "t")
        assert state is not None and not state.last_value.endswith("Z")

    def test_formatted_string_watermark_round_trip(self, new_york_session, tmp_path):
        spark = new_york_session
        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(spark, src, "select 1 as id, '20240101120000' as ts")
        thread = _thread(src, tgt, store, load_extra={"watermark_format": "yyyyMMddHHmmss"})
        assert execute_thread(spark, thread).status == "success"
        _append(spark, src, "select 2 as id, '20240101140000' as ts")
        assert execute_thread(spark, thread).status == "success"
        assert spark.read.format("delta").load(tgt).count() == 2
        state = MetadataTableStore(store).read(spark, "t")
        assert state is not None and state.last_value == "2024-01-01T19:00:00.000000Z"


class TestGenericCdcWatermark:
    """The generic-CDC HWM uses the same canonical form."""

    def test_cdc_hwm_is_canonical(self, new_york_session, tmp_path):
        spark = new_york_session
        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(spark, tgt, "select 1 as id, 'a' as name, timestamp'2024-01-01 09:00:00' as ts")
        _append(
            spark,
            src,
            "select 1 as id, 'a2' as name, 'U' as op, timestamp'2024-01-01 12:00:00' as ts",
        )
        thread = Thread.model_validate(
            {
                "name": "cdc",
                "config_version": "1.0",
                "sources": {"main": {"type": "delta", "alias": src}},
                "target": {"path": tgt},
                "write": {"mode": "merge", "match_keys": ["id"]},
                "load": {
                    "mode": "cdc",
                    "cdc": {
                        "operation_column": "op",
                        "insert_value": "I",
                        "update_value": "U",
                        "delete_value": "D",
                    },
                    "watermark_column": "ts",
                    "watermark_type": "timestamp",
                    "watermark_store": {"type": "metadata_table", "table_path": store},
                },
            }
        )
        assert execute_thread(spark, thread).status == "success"
        state = MetadataTableStore(store).read(spark, "cdc")
        assert state is not None and state.last_value == "2024-01-01T17:00:00.000000Z"


def _int_thread(src: str, tgt: str, store: str, *, write: dict[str, Any] | None = None) -> Thread:
    return Thread.model_validate(
        {
            "name": "t",
            "config_version": "1.0",
            "sources": {"main": {"type": "delta", "alias": src}},
            "target": {"path": tgt},
            "write": write or {"mode": "append"},
            "load": {
                "mode": "incremental_watermark",
                "watermark_column": "seq",
                "watermark_type": "long",
                "watermark_store": {"type": "metadata_table", "table_path": store},
            },
        }
    )


class TestFailedSaveRecovery:
    """A failed watermark save never makes the next run re-append."""

    def test_next_run_recovers_from_the_commit_stamp(self, spark: SparkSession, tmp_path, caplog):
        import logging

        from weevr.errors import StateError

        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(spark, src, "select 1 as id, 10L as seq union all select 2, 20L")
        thread = _int_thread(src, tgt, store)
        with (
            patch.object(MetadataTableStore, "write", side_effect=RuntimeError("store down")),
            pytest.raises(StateError),
        ):
            execute_thread(spark, thread)
        assert spark.read.format("delta").load(tgt).count() == 2

        with caplog.at_level(logging.INFO, logger="weevr.engine.executor"):
            result = execute_thread(spark, thread)

        assert result.status == "success"
        assert spark.read.format("delta").load(tgt).count() == 2  # no duplicates
        assert any("commit" in m and "20" in m for m in caplog.messages)
        state = MetadataTableStore(store).read(spark, "t")
        assert state is None or state.last_value == "20"

    @pytest.mark.parametrize(
        "store_cls, store_config",
        [
            (MetadataTableStore, None),
            (TablePropertiesStore, {"watermark_store": {"type": "table_properties"}}),
        ],
        ids=["metadata_table", "table_properties"],
    )
    def test_failed_save_on_a_later_run_recovers(
        self, new_york_session, tmp_path, store_cls, store_config
    ):
        from weevr.errors import StateError

        spark = new_york_session
        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        thread = _thread(src, tgt, store, load_extra=store_config)

        def add(row_id: int, hour: int) -> None:
            _append(spark, src, f"select {row_id} as id, timestamp'2024-01-01 {hour}:00:00' as ts")

        def target_rows() -> int:
            return spark.read.format("delta").load(tgt).count()

        add(1, 10)
        assert execute_thread(spark, thread).status == "success"

        add(2, 12)
        with (
            patch.object(store_cls, "write", side_effect=RuntimeError("store down")),
            pytest.raises(StateError),
        ):
            execute_thread(spark, thread)
        assert target_rows() == 2

        assert execute_thread(spark, thread).status == "success"
        assert target_rows() == 2  # no duplicates

        add(3, 14)
        assert execute_thread(spark, thread).status == "success"
        assert target_rows() == 3  # only the new row

    def test_append_commit_carries_the_hwm(self, spark: SparkSession, tmp_path):
        import json

        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(spark, src, "select 1 as id, 10L as seq")
        assert execute_thread(spark, _int_thread(src, tgt, store)).status == "success"
        latest = spark.sql(f"DESCRIBE HISTORY delta.`{tgt}` LIMIT 1").first()
        assert latest is not None
        meta = json.loads(latest["userMetadata"])
        assert meta["engine"] == "weevr" and meta["thread"] == "t" and meta["hwm"] == "10"

    def test_manual_reset_after_the_commit_wins(self, spark: SparkSession, tmp_path):
        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(spark, src, "select 1 as id, 10L as seq union all select 2, 20L")
        thread = _int_thread(src, tgt, store)
        assert execute_thread(spark, thread).status == "success"
        # Reset back to 10 to reprocess seq 20 — made after the stamped commit
        MetadataTableStore(store).write(
            spark,
            WatermarkState(
                thread_name="t",
                watermark_column="seq",
                watermark_type="long",
                last_value="10",
                last_updated=datetime.now(UTC),
            ),
        )
        result = execute_thread(spark, thread)
        assert result.status == "success"
        assert spark.read.format("delta").load(tgt).filter("seq = 20").count() == 2


class TestHistoryReadFailure:
    """An unreadable commit history is reported as that, not as a missing stamp."""

    def test_first_run_guard_names_the_unreadable_history(self, spark: SparkSession, tmp_path):
        from delta.tables import DeltaTable

        from weevr.errors import StateError

        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(spark, src, "select 1 as id, 10L as seq")
        _append(spark, tgt, "select 9 as id, 90L as seq")
        with (
            patch.object(DeltaTable, "history", side_effect=RuntimeError("log unreadable")),
            pytest.raises(StateError, match="commit history") as exc,
        ):
            execute_thread(spark, _int_thread(src, tgt, store))
        assert "seed-or-reset-a-watermark" in str(exc.value)
        assert spark.read.format("delta").load(tgt).count() == 1

    def test_stored_watermark_run_logs_the_cause(self, spark: SparkSession, tmp_path, caplog):
        import logging

        from delta.tables import DeltaTable

        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(spark, src, "select 1 as id, 10L as seq")
        thread = _int_thread(src, tgt, store)
        assert execute_thread(spark, thread).status == "success"

        _append(spark, src, "select 2 as id, 20L as seq")
        with (
            patch.object(DeltaTable, "history", side_effect=RuntimeError("log unreadable")),
            caplog.at_level(logging.WARNING, logger="weevr.operations.target_handle"),
        ):
            assert execute_thread(spark, thread).status == "success"

        assert spark.read.format("delta").load(tgt).count() == 2
        warned = [r for r in caplog.records if "can't be recovered" in r.getMessage()]
        assert warned and warned[0].exc_info is not None


class TestCorruptStoredWatermark:
    """Unparseable stored text fails naming the thread and the value."""

    @pytest.mark.parametrize(
        "wm_type, wm_col, column_sql, stored",
        [
            ("timestamp", "ts", "timestamp'2024-01-01 10:00:00'", "not-a-timestamp"),
            ("long", "seq", "10L", "not-a-number"),
            ("int", "seq", "10", "not-a-number"),
        ],
        ids=["timestamp", "long", "int"],
    )
    def test_fails_with_a_state_error(
        self, spark: SparkSession, tmp_path, wm_type, wm_col, column_sql, stored
    ):
        from weevr.errors import StateError

        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(spark, src, f"select 1 as id, {column_sql} as {wm_col}")
        MetadataTableStore(store).write(
            spark,
            WatermarkState(
                thread_name="t",
                watermark_column=wm_col,
                watermark_type=wm_type,
                last_value=stored,
                last_updated=datetime.now(UTC),
            ),
        )
        with pytest.raises(StateError, match=stored) as exc:
            execute_thread(spark, _thread(src, tgt, store, wm_type=wm_type, wm_col=wm_col))
        assert exc.value.thread_name == "t"


class TestFirstRunGuard:
    """A first run never re-appends onto an already-populated target."""

    def test_non_empty_target_without_state_fails_before_writing(
        self, spark: SparkSession, tmp_path
    ):
        from weevr.errors import StateError

        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(spark, src, "select 1 as id, 10L as seq")
        _append(spark, tgt, "select 99 as id, 5L as seq")
        with pytest.raises(StateError, match="seed-or-reset-a-watermark") as exc:
            execute_thread(spark, _int_thread(src, tgt, store))
        assert "'t'" in str(exc.value) and tgt in str(exc.value)
        assert spark.read.format("delta").load(tgt).count() == 1

    def test_empty_target_proceeds(self, spark: SparkSession, tmp_path):
        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(spark, src, "select 1 as id, 10L as seq")
        spark.sql("select 1 as id, 1L as seq").limit(0).write.format("delta").save(tgt)
        assert execute_thread(spark, _int_thread(src, tgt, store)).status == "success"
        assert spark.read.format("delta").load(tgt).count() == 1

    def test_merge_mode_is_not_guarded(self, spark: SparkSession, tmp_path):
        src, tgt, store = (str(tmp_path / n) for n in ("src", "tgt", "wm"))
        _append(spark, src, "select 1 as id, 10L as seq")
        _append(spark, tgt, "select 1 as id, 5L as seq")
        thread = _int_thread(src, tgt, store, write={"mode": "merge", "match_keys": ["id"]})
        assert execute_thread(spark, thread).status == "success"
        row = spark.read.format("delta").load(tgt).first()
        assert row is not None and row.seq == 10
