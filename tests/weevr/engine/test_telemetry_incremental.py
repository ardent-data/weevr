"""Tests for incremental telemetry fields and parameter mode behavior."""

import pytest
from pyspark.sql import SparkSession

from spark_helpers import create_delta_table
from weevr.engine.executor import execute_thread
from weevr.model.load import CdcConfig, LoadConfig, WatermarkStoreConfig
from weevr.model.source import Source
from weevr.model.target import Target
from weevr.model.thread import Thread
from weevr.model.write import WriteConfig

pytestmark = pytest.mark.spark


class TestCdcTelemetry:
    """CDC telemetry fields populated in ThreadResult."""

    def test_cdc_telemetry_counts(self, spark: SparkSession, tmp_delta_path) -> None:
        """CDC execution populates cdc_inserts, cdc_updates, cdc_deletes."""
        src = tmp_delta_path("cdc_tel_src")
        tgt = tmp_delta_path("cdc_tel_tgt")
        wm = tmp_delta_path("cdc_tel_store")

        # Create source with CDC change flags
        create_delta_table(
            spark,
            src,
            [
                {"id": 1, "name": "Alice", "op": "I"},
                {"id": 2, "name": "Bobby", "op": "U"},
            ],
        )
        # Pre-create target so merge works
        create_delta_table(spark, tgt, [{"id": 2, "name": "Bob"}])

        thread = Thread(
            name="cdc_tel",
            config_version="1",
            sources={"main": Source(type="delta", alias=src)},
            steps=[],
            target=Target(path=tgt),
            write=WriteConfig(mode="merge", match_keys=["id"]),
            load=LoadConfig(
                mode="cdc",
                cdc=CdcConfig(
                    operation_column="op",
                    insert_value="I",
                    update_value="U",
                ),
                watermark_store=WatermarkStoreConfig(
                    type="metadata_table",
                    table_path=wm,
                ),
            ),
        )
        result = execute_thread(spark, thread)

        assert result.telemetry is not None
        assert result.telemetry.cdc_inserts == 1
        assert result.telemetry.cdc_updates == 1
        assert result.telemetry.cdc_deletes == 0
        assert result.telemetry.load_mode == "cdc"


class TestIncrementalParameterTelemetry:
    """incremental_parameter mode telemetry — semantic marker (DEC-008)."""

    def test_parameter_mode_records_load_mode(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Thread with incremental_parameter records load_mode in telemetry."""
        src = tmp_delta_path("param_tel_src")
        tgt = tmp_delta_path("param_tel_tgt")
        create_delta_table(spark, src, [{"id": 1, "val": "a"}])

        thread = Thread(
            name="param_thread",
            config_version="1",
            sources={"main": Source(type="delta", alias=src)},
            steps=[],
            target=Target(path=tgt),
            load=LoadConfig(mode="incremental_parameter"),
        )
        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.telemetry is not None
        assert result.telemetry.load_mode == "incremental_parameter"
        # No watermark fields populated for parameter mode
        assert result.telemetry.watermark_column is None
        assert result.telemetry.watermark_persisted is False


class TestFullModeTelemetryUnaffected:
    """Full-mode thread — new telemetry fields are None/default (regression check)."""

    def test_full_mode_no_incremental_fields(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Full-mode execution has incremental telemetry fields at defaults."""
        src = tmp_delta_path("full_tel_src")
        tgt = tmp_delta_path("full_tel_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        thread = Thread(
            name="full_thread",
            config_version="1",
            sources={"main": Source(type="delta", alias=src)},
            steps=[],
            target=Target(path=tgt),
        )
        result = execute_thread(spark, thread)

        assert result.telemetry is not None
        assert result.telemetry.load_mode == "full"
        assert result.telemetry.watermark_column is None
        assert result.telemetry.watermark_previous_value is None
        assert result.telemetry.watermark_new_value is None
        assert result.telemetry.watermark_persisted is False
        assert result.telemetry.watermark_first_run is False
        assert result.telemetry.cdc_inserts is None
        assert result.telemetry.cdc_updates is None
        assert result.telemetry.cdc_deletes is None
