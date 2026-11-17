"""Validate mode returns every config problem and never raises for one."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from pyspark.sql import SparkSession

from weevr.context import Context
from weevr.errors import ConfigError
from weevr.result import ExecutionMode

pytestmark = pytest.mark.spark


def _write(path: Path, data: dict[str, Any] | str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data if isinstance(data, str) else yaml.safe_dump(data, sort_keys=False))
    return path


def _thread(source: str, target: str, **extra: Any) -> dict[str, Any]:
    return {
        "config_version": "1.0",
        "sources": {"src": {"type": "delta", "alias": source}},
        "target": {"path": target},
        **extra,
    }


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """A weave with a schema error, a dependency cycle and a missing source."""
    root = tmp_path / "p.weevr"
    out = tmp_path / "out"
    _write(root / "broken.thread", _thread("raw.a", str(out / "a"), write={"mode": "bogus"}))
    _write(root / "one.thread", _thread("raw.b", str(out / "one")))
    _write(root / "two.thread", _thread("raw.c", str(out / "two")))
    _write(root / "lonely.thread", _thread("missing.nowhere", str(out / "lonely")))
    _write(
        root / "w.weave",
        {
            "config_version": "1.0",
            "threads": [
                {"ref": "broken.thread"},
                {"ref": "one.thread", "dependencies": ["two"]},
                {"ref": "two.thread", "dependencies": ["one"]},
                {"ref": "lonely.thread"},
            ],
        },
    )
    return root


def _codes(result: Any) -> set[str]:
    return {d.code for d in result.diagnostics}


class TestValidateReports:
    """Validate returns load, plan and existence problems together."""

    def test_schema_cycle_and_missing_source_in_one_result(
        self, spark: SparkSession, project: Path
    ) -> None:
        result = Context(spark, project).run("w.weave", mode="validate")
        assert result.status == "failure"
        assert result.mode is ExecutionMode.VALIDATE
        assert {"C103", "D101", "S101"} <= _codes(result)
        (schema,) = [d for d in result.diagnostics if d.code == "C103"]
        assert schema.file == str(project / "broken.thread")
        assert schema.line is not None
        (cycle,) = [d for d in result.diagnostics if d.code == "D101"]
        assert cycle.file == str(project / "w.weave")

    def test_a_file_that_fails_to_parse_is_reported(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        root = tmp_path / "p.weevr"
        _write(root / "t.thread", "config_version: '1.0'\nsources: [1, 2\n")
        result = Context(spark, root).run("t.thread", mode="validate")
        assert result.status == "failure"
        assert _codes(result) == {"C005"}
        assert (result.config_type, result.config_name) == ("thread", "t")

    def test_standalone_column_set_error_is_reported(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        root = tmp_path / "p.weevr"
        thread = _thread("raw.a", str(tmp_path / "out"))
        thread["steps"] = [{"rename": {"column_set": "nope"}}]
        _write(root / "t.thread", thread)
        result = Context(spark, root).run("t.thread", mode="validate")
        assert "R124" in _codes(result)

    def test_load_warnings_appear(self, spark: SparkSession, tmp_path: Path) -> None:
        root = tmp_path / "p.weevr"
        _write(
            root / "t.thread",
            _thread("raw.a", str(tmp_path / "out"), execution={"log_level": "minimal"}),
        )
        _write(root / "w.weave", {"config_version": "1.0", "threads": [{"ref": "t.thread"}]})
        result = Context(spark, root).run("w.weave", mode="validate")
        assert "C491" in _codes(result)

    def test_deprecated_string_view_keeps_todays_content(
        self, spark: SparkSession, project: Path
    ) -> None:
        result = Context(spark, project).run("w.weave", mode="validate")
        with pytest.deprecated_call():
            errors = result.validation_errors
        assert errors is not None
        assert any(e.startswith("DAG validation failed: ") for e in errors)
        assert any("not found: missing.nowhere" in e for e in errors)

    def test_a_spark_failure_still_raises(self, spark: SparkSession, project: Path) -> None:
        from unittest.mock import patch

        lost = RuntimeError("Spark session is gone")
        with (
            patch("weevr.config.fabric.build_fabric_context", side_effect=lost),
            pytest.raises(RuntimeError, match="session is gone"),
        ):
            Context(spark, project).run("w.weave", mode="validate")


class TestRunModesStillRaise:
    """Execute, plan and preview raise for config problems, as before."""

    @pytest.mark.parametrize("mode", ["execute", "plan", "preview"])
    def test_raises_with_load_and_plan_problems(
        self, spark: SparkSession, project: Path, mode: str
    ) -> None:
        with pytest.raises(ConfigError) as exc_info:
            Context(spark, project).run("w.weave", mode=mode)
        exc = exc_info.value
        assert {d.code for d in exc.diagnostics} == {"C103", "D101"}
        lines = str(exc).splitlines()
        assert lines[0] == "2 config problems:"
        assert any(line.startswith("  - C103 invalid-value: ") for line in lines)
        assert any(line.startswith("  - D101 circular-dependency: ") for line in lines)

    def test_context_load_raises_load_problems_only(
        self, spark: SparkSession, project: Path
    ) -> None:
        with pytest.raises(ConfigError) as exc_info:
            Context(spark, project).load("w.weave")
        assert {d.code for d in exc_info.value.diagnostics} == {"C103"}
