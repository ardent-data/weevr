"""Schema drift on existing targets, end to end through ``execute_thread``."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from pyspark.sql import DataFrame, SparkSession

from weevr.engine.executor import execute_thread
from weevr.errors import SchemaDriftError
from weevr.model.thread import Thread

pytestmark = pytest.mark.spark

DIMENSION = {
    "business_key": ["id"],
    "surrogate_key": {"name": "sk", "columns": ["id"]},
    "change_detection": {"attrs": {"columns": ["name"], "on_change": "overwrite"}},
}


def _thread(target: dict[str, Any], write: dict[str, Any] | None = None, **extra: Any) -> Thread:
    body: dict[str, Any] = {
        "name": "t",
        "config_version": "1.0",
        "sources": {"src": {"type": "delta", "alias": "src_table"}},
        "target": target,
        **extra,
    }
    if write is not None:
        body["write"] = write
    return Thread.model_validate(body)


def _run(spark: SparkSession, thread: Thread, df: DataFrame):
    with patch("weevr.engine.executor.read_source", side_effect=lambda *a, **k: df):
        return execute_thread(spark, thread)


WRITES = {
    "append": {"mode": "append"},
    "overwrite": {"mode": "overwrite"},
    "merge": {"mode": "merge", "match_keys": ["id"]},
}


class TestNewColumnOnExistingTable:
    """A new output column on an existing table is drift, for every write mode."""

    @pytest.fixture()
    def target(self, spark: SparkSession, tmp_path: Path) -> str:
        path = str(tmp_path / "tgt")
        spark.createDataFrame([(1, "a")], ["id", "name"]).write.format("delta").save(path)
        return path

    @pytest.fixture()
    def output(self, spark: SparkSession) -> DataFrame:
        return spark.createDataFrame([(2, "b", "e@x")], ["id", "name", "email"])

    @pytest.mark.parametrize("write", list(WRITES))
    def test_strict_error_raises(self, spark, target, output, write):
        thread = _thread(
            {"path": target, "schema_drift": "strict", "on_drift": "error"}, WRITES[write]
        )
        with pytest.raises(SchemaDriftError, match="email"):
            _run(spark, thread, output)
        assert spark.read.format("delta").load(target).count() == 1

    @pytest.mark.parametrize("write", list(WRITES))
    def test_strict_warn_drops_with_report(self, spark, target, output, write):
        thread = _thread(
            {"path": target, "schema_drift": "strict", "on_drift": "warn"}, WRITES[write]
        )
        result = _run(spark, thread, output)
        assert result.status == "success"
        assert result.drift_report is not None
        assert result.drift_report["extra_columns"] == ["email"]
        assert result.drift_report["action_taken"] == "drop_warn"
        assert spark.read.format("delta").load(target).columns == ["id", "name"]

    @pytest.mark.parametrize("write", list(WRITES))
    def test_lenient_reports_and_writes_target_columns(self, spark, target, output, write):
        thread = _thread({"path": target, "schema_drift": "lenient"}, WRITES[write])
        result = _run(spark, thread, output)
        assert result.status == "success"
        assert result.drift_report is not None
        assert result.drift_report["extra_columns"] == ["email"]
        assert spark.read.format("delta").load(target).columns == ["id", "name"]

    def test_no_drift_has_empty_report(self, spark, target):
        df = spark.createDataFrame([(2, "b")], ["id", "name"])
        result = _run(
            spark, _thread({"path": target, "schema_drift": "strict"}, WRITES["append"]), df
        )
        assert result.status == "success"
        assert result.drift_report is None

    def test_column_order_follows_the_table(self, spark, target):
        df = spark.createDataFrame([("b", 2, "e@x")], ["name", "id", "email"])
        result = _run(spark, _thread({"path": target}, WRITES["append"]), df)
        assert result.status == "success"
        rows = spark.read.format("delta").load(target).orderBy("id").collect()
        assert [tuple(r) for r in rows] == [(1, "a"), (2, "b")]

    def test_audit_columns_are_not_drift(self, spark, tmp_path: Path):
        path = str(tmp_path / "audited")
        spark.createDataFrame([(1, "a", "x")], ["id", "name", "loaded_by"]).write.format(
            "delta"
        ).save(path)
        thread = _thread(
            {
                "path": path,
                "schema_drift": "strict",
                "on_drift": "error",
                "audit_columns": {"loaded_by": "'weevr'"},
            },
            WRITES["append"],
        )
        result = _run(spark, thread, spark.createDataFrame([(2, "b")], ["id", "name"]))
        assert result.status == "success", result.error
        assert result.drift_report is None


class TestDimensionDrift:
    """Dimension targets see drift on existing tables too."""

    @pytest.fixture()
    def target(self, spark: SparkSession, tmp_path: Path) -> str:
        path = str(tmp_path / "dim")
        first = _thread({"path": path, "dimension": DIMENSION})
        result = _run(spark, first, spark.createDataFrame([(1, "a")], ["id", "name"]))
        assert result.status == "success", result.error
        return path

    def test_strict_error_raises(self, spark, target):
        thread = _thread(
            {"path": target, "dimension": DIMENSION, "schema_drift": "strict", "on_drift": "error"}
        )
        with pytest.raises(SchemaDriftError, match="email"):
            _run(spark, thread, spark.createDataFrame([(2, "b", "e")], ["id", "name", "email"]))

    def test_lenient_reports(self, spark, target):
        before = spark.read.format("delta").load(target).columns
        thread = _thread({"path": target, "dimension": DIMENSION, "schema_drift": "lenient"})
        result = _run(
            spark, thread, spark.createDataFrame([(2, "b", "e")], ["id", "name", "email"])
        )
        assert result.status == "success", result.error
        assert result.drift_report is not None
        assert result.drift_report["extra_columns"] == ["email"]
        assert spark.read.format("delta").load(target).columns == before

    def test_no_drift_on_rerun(self, spark, target):
        thread = _thread(
            {"path": target, "dimension": DIMENSION, "schema_drift": "strict", "on_drift": "error"}
        )
        result = _run(spark, thread, spark.createDataFrame([(1, "a2")], ["id", "name"]))
        assert result.status == "success", result.error
        assert result.drift_report is None
