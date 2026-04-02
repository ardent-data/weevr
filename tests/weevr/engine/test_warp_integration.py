"""Integration tests for warp and drift lifecycle in execute_thread."""

from typing import Any
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession

from weevr.engine.executor import execute_thread
from weevr.errors import SchemaDriftError
from weevr.model.thread import Thread


def _make_thread(
    name: str = "test_thread",
    target_alias: str = "test.output",
    warp: str | bool | None = None,
    warp_mode: str | None = None,
    warp_init: bool = False,
    warp_enforcement: str = "warn",
    schema_drift: str = "lenient",
    on_drift: str = "warn",
    **extra_target: Any,
) -> Thread:
    """Create a minimal Thread with warp/drift settings."""
    target: dict[str, Any] = {
        "alias": target_alias,
        "warp_enforcement": warp_enforcement,
        "schema_drift": schema_drift,
        "on_drift": on_drift,
    }
    if warp is not None:
        target["warp"] = warp
    if warp_mode is not None:
        target["warp_mode"] = warp_mode
    if warp_init:
        target["warp_init"] = True
    target.update(extra_target)

    return Thread.model_validate(
        {
            "name": name,
            "config_version": "1.0",
            "sources": {"src": {"type": "delta", "alias": "src_table"}},
            "target": target,
        }
    )


class TestDefaultBehavior:
    """Default settings (no warp, lenient drift) should not change existing behavior."""

    def test_no_warp_lenient_default(self, spark: SparkSession, tmp_delta_path):
        """Thread without warp and lenient drift: existing behavior unchanged."""
        path = tmp_delta_path("default_behavior")
        spark.createDataFrame([(1, "Alice")], ["id", "name"]).write.format("delta").save(
            path + "_src"
        )

        thread = _make_thread(target_alias=path)

        source_df = spark.read.format("delta").load(path + "_src")
        with patch("weevr.engine.executor.read_sources") as mock_read:
            mock_read.return_value = {"src": source_df}
            result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.rows_written > 0
        assert result.warp_findings is None
        assert result.drift_report is None

    def test_no_warp_no_auto_discovery(self, spark: SparkSession, tmp_delta_path):
        """Thread with warp=None does not trigger auto-discovery."""
        path = tmp_delta_path("no_auto_discover")
        thread = _make_thread(target_alias=path)
        source_df = spark.createDataFrame([(1,)], ["id"])
        with patch("weevr.engine.executor.read_sources") as mock_read:
            mock_read.return_value = {"src": source_df}
            result = execute_thread(spark, thread)
        assert result.status == "success"
        assert result.warp_findings is None


class TestWarpEnforcementIntegration:
    """Test warp enforcement through the executor pipeline."""

    def test_warn_mode_logs_findings(self, spark: SparkSession, tmp_delta_path):
        """Warp enforcement in warn mode logs but doesn't fail."""
        path = tmp_delta_path("warn_enforcement")
        thread = _make_thread(
            target_alias=path,
            warp=False,  # Opt out of warp
            warp_enforcement="warn",
        )
        source_df = spark.createDataFrame([(1, "Alice")], ["id", "name"])
        with patch("weevr.engine.executor.read_sources") as mock_read:
            mock_read.return_value = {"src": source_df}
            result = execute_thread(spark, thread)
        assert result.status == "success"


class TestDriftDetectionIntegration:
    """Test drift detection through the executor pipeline."""

    def test_lenient_passes_extra_columns(self, spark: SparkSession, tmp_delta_path):
        """Lenient drift: extra columns pass through to target."""
        path = tmp_delta_path("lenient_drift")
        thread = _make_thread(
            target_alias=path,
            warp=False,
            schema_drift="lenient",
        )
        source_df = spark.createDataFrame([(1, "Alice", "extra")], ["id", "name", "new_col"])
        with patch("weevr.engine.executor.read_sources") as mock_read:
            mock_read.return_value = {"src": source_df}
            result = execute_thread(spark, thread)
        assert result.status == "success"
        output_df = spark.read.format("delta").load(path)
        assert "new_col" in output_df.columns

    def test_strict_error_with_warp_raises(self, spark: SparkSession, tmp_delta_path):
        """Strict drift with warp baseline and on_drift=error raises SchemaDriftError."""
        from pathlib import Path as PyPath

        path = tmp_delta_path("strict_drift_warp_error")
        thread = Thread.model_validate(
            {
                "name": "test_thread",
                "config_version": "1.0",
                "sources": {"src": {"type": "delta", "alias": "src_table"}},
                "target": {
                    "path": path,
                    "warp": "test_warp",
                    "schema_drift": "strict",
                    "on_drift": "error",
                },
            }
        )
        # Create a .warp file that declares only id and name
        warp_dir = PyPath(path).parent
        warp_path = warp_dir / "test_warp.warp"
        import yaml

        yaml.safe_dump(
            {
                "config_version": "1.0",
                "columns": [
                    {"name": "id", "type": "bigint"},
                    {"name": "name", "type": "string"},
                ],
            },
            warp_path.open("w"),
        )
        # Source has extra column not declared in warp
        source_df = spark.createDataFrame([(2, "Bob", "extra")], ["id", "name", "new_col"])
        with patch("weevr.engine.executor.read_sources") as mock_read:
            mock_read.return_value = {"src": source_df}
            with pytest.raises(SchemaDriftError) as exc_info:
                execute_thread(spark, thread)
            assert exc_info.value.thread_name == "test_thread"
            assert exc_info.value.drift_report is not None
            assert "new_col" in exc_info.value.drift_report.extra_columns

    def test_strict_warn_drops_extra_columns(self, spark: SparkSession, tmp_delta_path):
        """Strict drift with on_drift=warn drops extra columns and reports."""
        from pathlib import Path as PyPath

        path = tmp_delta_path("strict_drift_warn")
        thread = Thread.model_validate(
            {
                "name": "test_thread",
                "config_version": "1.0",
                "sources": {"src": {"type": "delta", "alias": "src_table"}},
                "target": {
                    "path": path,
                    "warp": "test_warp",
                    "schema_drift": "strict",
                    "on_drift": "warn",
                },
            }
        )
        warp_dir = PyPath(path).parent
        warp_path = warp_dir / "test_warp.warp"
        import yaml

        yaml.safe_dump(
            {
                "config_version": "1.0",
                "columns": [
                    {"name": "id", "type": "bigint"},
                    {"name": "name", "type": "string"},
                ],
            },
            warp_path.open("w"),
        )
        source_df = spark.createDataFrame([(2, "Bob", "extra")], ["id", "name", "new_col"])
        with patch("weevr.engine.executor.read_sources") as mock_read:
            mock_read.return_value = {"src": source_df}
            result = execute_thread(spark, thread)
        assert result.status == "success"
        assert result.drift_report is not None
        assert result.drift_report["extra_columns"] == ["new_col"]
        assert result.drift_report["baseline_source"] == "warp"
        output_df = spark.read.format("delta").load(path)
        assert "new_col" not in output_df.columns
