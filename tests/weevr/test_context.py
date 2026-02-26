"""Tests for Context construction, load(), run() execute mode, and filtering."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from weevr.context import Context
from weevr.errors import ExecutionError
from weevr.model.source import Source
from weevr.model.target import Target
from weevr.model.thread import Thread
from weevr.result import ExecutionMode, RunResult

# ---------------------------------------------------------------------------
# Unit tests — no Spark required
# ---------------------------------------------------------------------------


class TestContextValidation:
    def test_invalid_spark_type(self) -> None:
        with pytest.raises(TypeError, match="SparkSession"):
            Context(spark="not a session", project="dummy")  # type: ignore[arg-type]

    def test_invalid_log_level(self) -> None:
        mock_spark = MagicMock(spec=SparkSession)
        with pytest.raises(ValueError, match="log_level"):
            Context(spark=mock_spark, project="dummy", log_level="nope")

    def test_valid_construction(self, tmp_path: Path) -> None:
        mock_spark = MagicMock(spec=SparkSession)
        project_dir = tmp_path / "test.weevr"
        project_dir.mkdir()
        ctx = Context(
            spark=mock_spark, project=str(project_dir), params={"a": "1"}, log_level="minimal"
        )
        assert ctx.spark is mock_spark
        assert ctx.params == {"a": "1"}
        assert ctx.log_level.value == "minimal"


class TestRunValidation:
    def test_invalid_mode(self, tmp_path: Path) -> None:
        mock_spark = MagicMock(spec=SparkSession)
        project_dir = tmp_path / "test.weevr"
        project_dir.mkdir()
        ctx = Context(spark=mock_spark, project=str(project_dir))
        with pytest.raises(ValueError, match="Invalid mode"):
            ctx.run("some/path.yaml", mode="bogus")

    def test_tags_and_threads_exclusive(self, tmp_path: Path) -> None:
        mock_spark = MagicMock(spec=SparkSession)
        project_dir = tmp_path / "test.weevr"
        project_dir.mkdir()
        ctx = Context(spark=mock_spark, project=str(project_dir))
        with pytest.raises(ValueError, match="mutually exclusive"):
            ctx.run("some/path.yaml", tags=["t1"], threads=["t2"])


class TestRunErrorHandling:
    def test_execution_error_returns_failure_result(self, tmp_path: Path) -> None:
        mock_spark = MagicMock(spec=SparkSession)
        project_dir = tmp_path / "test.weevr"
        project_dir.mkdir()
        ctx = Context(spark=mock_spark, project=str(project_dir))

        with (
            patch.object(ctx, "_load_resolved") as mock_load,
            patch.object(ctx, "_run_execute", side_effect=ExecutionError("Spark write failed")),
        ):
            mock_load.return_value = MagicMock(config_type="thread", config_name="dim_test")
            result = ctx.run(str(tmp_path / "threads" / "dim_test.yaml"))

        assert isinstance(result, RunResult)
        assert result.status == "failure"
        assert result.mode == ExecutionMode.EXECUTE
        assert result.config_type == "thread"
        assert result.config_name == "dim_test"
        assert any("Spark write failed" in w for w in result.warnings)
        assert result.duration_ms >= 0


class TestFilterThreads:
    @pytest.fixture()
    def thread_map(self) -> dict[str, Thread]:
        return {
            "dim_customer": Thread(
                name="dim_customer",
                config_version="1.0",
                sources={"main": Source(type="delta", alias="raw_customers")},
                target=Target(path="/data/dim_customer"),
                tags=["dimension", "core"],
            ),
            "dim_product": Thread(
                name="dim_product",
                config_version="1.0",
                sources={"main": Source(type="delta", alias="raw_products")},
                target=Target(path="/data/dim_product"),
                tags=["dimension"],
            ),
            "fact_sales": Thread(
                name="fact_sales",
                config_version="1.0",
                sources={"main": Source(type="delta", alias="raw_sales")},
                target=Target(path="/data/fact_sales"),
                tags=["fact"],
            ),
        }

    def test_no_filter_returns_all(self, thread_map: dict[str, Thread]) -> None:
        filtered, warnings = Context._filter_threads(thread_map)
        assert filtered == thread_map
        assert warnings == []

    def test_filter_by_thread_names(self, thread_map: dict[str, Thread]) -> None:
        filtered, warnings = Context._filter_threads(
            thread_map, thread_names=["dim_customer", "fact_sales"]
        )
        assert set(filtered.keys()) == {"dim_customer", "fact_sales"}
        assert warnings == []

    def test_filter_by_tags(self, thread_map: dict[str, Thread]) -> None:
        filtered, warnings = Context._filter_threads(thread_map, tags=["dimension"])
        assert set(filtered.keys()) == {"dim_customer", "dim_product"}
        assert warnings == []

    def test_filter_by_tags_intersection(self, thread_map: dict[str, Thread]) -> None:
        filtered, warnings = Context._filter_threads(thread_map, tags=["core"])
        assert set(filtered.keys()) == {"dim_customer"}
        assert warnings == []

    def test_filter_no_match_returns_warning(self, thread_map: dict[str, Thread]) -> None:
        filtered, warnings = Context._filter_threads(thread_map, thread_names=["nonexistent"])
        assert filtered == {}
        assert len(warnings) == 1
        assert "No threads matched" in warnings[0]

    def test_filter_by_tags_no_match(self, thread_map: dict[str, Thread]) -> None:
        filtered, warnings = Context._filter_threads(thread_map, tags=["nonexistent_tag"])
        assert filtered == {}
        assert "No threads matched" in warnings[0]


# ---------------------------------------------------------------------------
# Integration tests — require Spark
# ---------------------------------------------------------------------------


def _create_thread_yaml(base: Path, name: str, src_alias: str, tgt_path: str) -> Path:
    """Create a minimal thread YAML config at base/threads/<name>.yaml."""
    thread_dir = base / "threads"
    thread_dir.mkdir(parents=True, exist_ok=True)
    yaml_path = thread_dir / f"{name}.yaml"
    yaml_path.write_text(
        f"""\
config_version: "1.0"
sources:
  main:
    type: delta
    alias: "{src_alias}"
target:
  path: "{tgt_path}"
write:
  mode: overwrite
"""
    )
    return yaml_path


def _create_weave_yaml(base: Path, name: str, thread_names: list[str]) -> Path:
    """Create a minimal weave YAML config referencing threads by name."""
    weave_dir = base / "weaves"
    weave_dir.mkdir(parents=True, exist_ok=True)
    yaml_path = weave_dir / f"{name}.yaml"
    threads_yaml = "\n".join(f"  - {t}" for t in thread_names)
    yaml_path.write_text(
        f"""\
config_version: "1.0"
threads:
{threads_yaml}
"""
    )
    return yaml_path


def _create_loom_yaml(base: Path, name: str, weave_names: list[str]) -> Path:
    """Create a minimal loom YAML config referencing weaves."""
    loom_dir = base / "looms"
    loom_dir.mkdir(parents=True, exist_ok=True)
    yaml_path = loom_dir / f"{name}.yaml"
    weaves_yaml = "\n".join(f"  - {w}" for w in weave_names)
    yaml_path.write_text(
        f"""\
config_version: "1.0"
weaves:
{weaves_yaml}
"""
    )
    return yaml_path


@pytest.mark.spark
class TestContextLoad:
    def test_load_thread(self, spark: SparkSession, tmp_path: Path) -> None:
        src_path = str(tmp_path / "src_data")
        tgt_path = str(tmp_path / "tgt_data")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src_path)

        thread_yaml = _create_thread_yaml(tmp_path, "dim_test", src_path, tgt_path)
        ctx = Context(spark=spark)
        loaded = ctx.load(thread_yaml)

        assert loaded.config_type == "thread"
        assert loaded.name == "dim_test"
        assert loaded.execution_plan is None

    def test_load_weave(self, spark: SparkSession, tmp_path: Path) -> None:
        src = str(tmp_path / "src_w")
        tgt = str(tmp_path / "tgt_w")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src)

        _create_thread_yaml(tmp_path, "t1", src, tgt)
        weave_yaml = _create_weave_yaml(tmp_path, "test_weave", ["t1"])
        ctx = Context(spark=spark)
        loaded = ctx.load(weave_yaml)

        assert loaded.config_type == "weave"
        assert loaded.config_name == "test_weave"
        assert loaded.execution_plan is not None

    def test_load_loom(self, spark: SparkSession, tmp_path: Path) -> None:
        src = str(tmp_path / "src_l")
        tgt = str(tmp_path / "tgt_l")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src)

        _create_thread_yaml(tmp_path, "t1", src, tgt)
        _create_weave_yaml(tmp_path, "w1", ["t1"])
        loom_yaml = _create_loom_yaml(tmp_path, "test_loom", ["w1"])
        ctx = Context(spark=spark)
        loaded = ctx.load(loom_yaml)

        assert loaded.config_type == "loom"
        assert loaded.config_name == "test_loom"
        assert loaded.execution_plan is not None


@pytest.mark.spark
class TestContextRunExecute:
    def test_run_execute_thread(self, spark: SparkSession, tmp_path: Path) -> None:
        src = str(tmp_path / "src_exec")
        tgt = str(tmp_path / "tgt_exec")
        spark.createDataFrame([{"id": 1, "val": "a"}]).write.format("delta").save(src)

        thread_yaml = _create_thread_yaml(tmp_path, "t_exec", src, tgt)
        ctx = Context(spark=spark)
        result = ctx.run(thread_yaml)

        assert isinstance(result, RunResult)
        assert result.status == "success"
        assert result.mode == ExecutionMode.EXECUTE
        assert result.config_type == "thread"
        assert result.detail is not None
        assert result.duration_ms >= 0

    def test_run_execute_weave(self, spark: SparkSession, tmp_path: Path) -> None:
        src = str(tmp_path / "src_wexec")
        tgt = str(tmp_path / "tgt_wexec")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src)

        _create_thread_yaml(tmp_path, "tw1", src, tgt)
        weave_yaml = _create_weave_yaml(tmp_path, "wexec", ["tw1"])
        ctx = Context(spark=spark)
        result = ctx.run(weave_yaml)

        assert result.status == "success"
        assert result.mode == ExecutionMode.EXECUTE
        assert result.config_type == "weave"
        assert result.detail is not None

    def test_run_execute_loom(self, spark: SparkSession, tmp_path: Path) -> None:
        src = str(tmp_path / "src_lexec")
        tgt = str(tmp_path / "tgt_lexec")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src)

        _create_thread_yaml(tmp_path, "tl1", src, tgt)
        _create_weave_yaml(tmp_path, "wl1", ["tl1"])
        loom_yaml = _create_loom_yaml(tmp_path, "lexec", ["wl1"])
        ctx = Context(spark=spark)
        result = ctx.run(loom_yaml)

        assert result.status == "success"
        assert result.mode == ExecutionMode.EXECUTE
        assert result.config_type == "loom"
        assert result.detail is not None

    def test_context_reusable(self, spark: SparkSession, tmp_path: Path) -> None:
        src = str(tmp_path / "src_reuse")
        tgt1 = str(tmp_path / "tgt_reuse1")
        tgt2 = str(tmp_path / "tgt_reuse2")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src)

        yaml1 = _create_thread_yaml(tmp_path / "proj1", "r1", src, tgt1)
        yaml2 = _create_thread_yaml(tmp_path / "proj2", "r2", src, tgt2)
        ctx = Context(spark=spark)

        r1 = ctx.run(yaml1)
        r2 = ctx.run(yaml2)

        assert r1.status == "success"
        assert r2.status == "success"
        assert r1.config_name != r2.config_name

    def test_filter_zero_matches_returns_success_with_warning(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        src = str(tmp_path / "src_filt")
        tgt = str(tmp_path / "tgt_filt")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src)

        _create_thread_yaml(tmp_path, "tf1", src, tgt)
        weave_yaml = _create_weave_yaml(tmp_path, "wfilt", ["tf1"])
        ctx = Context(spark=spark)
        result = ctx.run(weave_yaml, threads=["nonexistent"])

        assert result.status == "success"
        assert any("No threads matched" in w for w in result.warnings)


@pytest.mark.spark
class TestValidateMode:
    def test_validate_valid_thread(self, spark: SparkSession, tmp_path: Path) -> None:
        src = str(tmp_path / "src_val")
        tgt = str(tmp_path / "tgt_val")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src)

        yaml_path = _create_thread_yaml(tmp_path, "vt1", src, tgt)
        ctx = Context(spark=spark)
        result = ctx.run(yaml_path, mode="validate")

        assert result.status == "success"
        assert result.mode == ExecutionMode.VALIDATE
        assert result.validation_errors is None

    def test_validate_valid_weave(self, spark: SparkSession, tmp_path: Path) -> None:
        src = str(tmp_path / "src_vw")
        tgt = str(tmp_path / "tgt_vw")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src)

        _create_thread_yaml(tmp_path, "vt2", src, tgt)
        weave_yaml = _create_weave_yaml(tmp_path, "vw1", ["vt2"])
        ctx = Context(spark=spark)
        result = ctx.run(weave_yaml, mode="validate")

        assert result.status == "success"
        assert result.validation_errors is None

    def test_validate_missing_source(self, spark: SparkSession, tmp_path: Path) -> None:
        tgt = str(tmp_path / "tgt_miss")
        yaml_path = _create_thread_yaml(tmp_path, "vmiss", "/nonexistent/path", tgt)
        ctx = Context(spark=spark)
        result = ctx.run(yaml_path, mode="validate")

        assert result.status == "failure"
        assert result.validation_errors is not None
        assert any("not found" in e for e in result.validation_errors)


@pytest.mark.spark
class TestPlanMode:
    def test_plan_thread(self, spark: SparkSession, tmp_path: Path) -> None:
        src = str(tmp_path / "src_pt")
        tgt = str(tmp_path / "tgt_pt")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src)

        yaml_path = _create_thread_yaml(tmp_path, "pt1", src, tgt)
        ctx = Context(spark=spark)
        result = ctx.run(yaml_path, mode="plan")

        assert result.status == "success"
        assert result.mode == ExecutionMode.PLAN
        assert result.execution_plan is None  # threads have no plan

    def test_plan_weave(self, spark: SparkSession, tmp_path: Path) -> None:
        src = str(tmp_path / "src_pw")
        tgt = str(tmp_path / "tgt_pw")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src)

        _create_thread_yaml(tmp_path, "pw1", src, tgt)
        weave_yaml = _create_weave_yaml(tmp_path, "wpln", ["pw1"])
        ctx = Context(spark=spark)
        result = ctx.run(weave_yaml, mode="plan")

        assert result.status == "success"
        assert result.execution_plan is not None
        assert len(result.execution_plan) == 1
        assert result.execution_plan[0].weave_name == "wpln"

    def test_plan_loom(self, spark: SparkSession, tmp_path: Path) -> None:
        src = str(tmp_path / "src_pl")
        tgt = str(tmp_path / "tgt_pl")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src)

        _create_thread_yaml(tmp_path, "pl1", src, tgt)
        _create_weave_yaml(tmp_path, "wpl1", ["pl1"])
        loom_yaml = _create_loom_yaml(tmp_path, "lpln", ["wpl1"])
        ctx = Context(spark=spark)
        result = ctx.run(loom_yaml, mode="plan")

        assert result.status == "success"
        assert result.execution_plan is not None
        assert len(result.execution_plan) == 1


@pytest.mark.spark
class TestPreviewMode:
    def test_preview_limits_rows(self, spark: SparkSession, tmp_path: Path) -> None:
        src = str(tmp_path / "src_prev")
        tgt = str(tmp_path / "tgt_prev")
        data = [{"id": i, "val": f"v{i}"} for i in range(100)]
        spark.createDataFrame(data).write.format("delta").save(src)

        yaml_path = _create_thread_yaml(tmp_path, "prev1", src, tgt)
        ctx = Context(spark=spark)
        result = ctx.run(yaml_path, mode="preview", sample_rows=10)

        assert result.status == "success"
        assert result.mode == ExecutionMode.PREVIEW
        assert result.preview_data is not None
        assert "prev1" in result.preview_data
        assert result.preview_data["prev1"].count() <= 10

    def test_preview_no_writes(self, spark: SparkSession, tmp_path: Path) -> None:
        src = str(tmp_path / "src_nw")
        tgt = str(tmp_path / "tgt_nw")
        spark.createDataFrame([{"id": 1}]).write.format("delta").save(src)

        yaml_path = _create_thread_yaml(tmp_path, "prev2", src, tgt)
        ctx = Context(spark=spark)
        ctx.run(yaml_path, mode="preview")

        # Target should NOT exist after preview
        assert not Path(tgt).exists() or not any(Path(tgt).iterdir())

    def test_preview_custom_sample_rows(self, spark: SparkSession, tmp_path: Path) -> None:
        src = str(tmp_path / "src_csr")
        tgt = str(tmp_path / "tgt_csr")
        data = [{"id": i} for i in range(200)]
        spark.createDataFrame(data).write.format("delta").save(src)

        yaml_path = _create_thread_yaml(tmp_path, "prev3", src, tgt)
        ctx = Context(spark=spark)
        result = ctx.run(yaml_path, mode="preview", sample_rows=50)

        assert result.preview_data is not None
        assert result.preview_data["prev3"].count() <= 50
