"""Unit tests for RunResult, LoadedConfig, and ExecutionMode."""

from __future__ import annotations

import pytest

from weevr.model.loom import Loom, WeaveEntry
from weevr.model.source import Source
from weevr.model.target import Target
from weevr.model.thread import Thread
from weevr.model.weave import ThreadEntry, Weave
from weevr.result import ExecutionMode, LoadedConfig, RunResult


class TestExecutionMode:
    def test_values(self) -> None:
        assert ExecutionMode.EXECUTE == "execute"
        assert ExecutionMode.VALIDATE == "validate"
        assert ExecutionMode.PLAN == "plan"
        assert ExecutionMode.PREVIEW == "preview"

    def test_string_coercion(self) -> None:
        assert str(ExecutionMode.EXECUTE) == "execute"
        assert str(ExecutionMode.PREVIEW) == "preview"

    def test_membership(self) -> None:
        assert "execute" in ExecutionMode.__members__.values()
        assert "plan" in ExecutionMode.__members__.values()

    def test_from_string(self) -> None:
        assert ExecutionMode("execute") is ExecutionMode.EXECUTE
        assert ExecutionMode("preview") is ExecutionMode.PREVIEW

    def test_invalid_raises(self) -> None:
        with pytest.raises(ValueError):
            ExecutionMode("bogus")


class TestRunResult:
    def test_execute_mode(self) -> None:
        result = RunResult(
            status="success",
            mode=ExecutionMode.EXECUTE,
            config_type="thread",
            config_name="dim_customer",
            duration_ms=123,
        )
        assert result.status == "success"
        assert result.mode == ExecutionMode.EXECUTE
        assert result.config_type == "thread"
        assert result.config_name == "dim_customer"
        assert result.duration_ms == 123
        assert result.detail is None
        assert result.telemetry is None
        assert result.execution_plan is None
        assert result.preview_data is None
        assert result.validation_errors is None
        assert result.warnings == []

    def test_validate_mode(self) -> None:
        result = RunResult(
            status="failure",
            mode=ExecutionMode.VALIDATE,
            config_type="weave",
            config_name="dimensions",
            validation_errors=["Source 'raw' not found"],
        )
        assert result.status == "failure"
        assert result.mode == ExecutionMode.VALIDATE
        assert result.validation_errors == ["Source 'raw' not found"]
        assert result.detail is None

    def test_plan_mode(self) -> None:
        result = RunResult(
            status="success",
            mode=ExecutionMode.PLAN,
            config_type="loom",
            config_name="nightly",
            execution_plan=[],
        )
        assert result.execution_plan == []
        assert result.detail is None
        assert result.preview_data is None

    def test_preview_mode(self) -> None:
        mock_data = {"dim_customer": "mock_dataframe"}
        result = RunResult(
            status="success",
            mode=ExecutionMode.PREVIEW,
            config_type="thread",
            config_name="dim_customer",
            preview_data=mock_data,
        )
        assert result.preview_data == mock_data

    def test_warnings_default_empty(self) -> None:
        result = RunResult(
            status="success",
            mode=ExecutionMode.EXECUTE,
            config_type="thread",
            config_name="t",
        )
        assert result.warnings == []

    def test_warnings_provided(self) -> None:
        result = RunResult(
            status="success",
            mode=ExecutionMode.EXECUTE,
            config_type="thread",
            config_name="t",
            warnings=["No threads matched filter"],
        )
        assert result.warnings == ["No threads matched filter"]

    def test_warnings_mutable(self) -> None:
        result = RunResult(
            status="success",
            mode=ExecutionMode.EXECUTE,
            config_type="thread",
            config_name="t",
        )
        result.warnings.append("warning 1")
        assert result.warnings == ["warning 1"]

    def test_summary_execute_thread(self) -> None:
        result = RunResult(
            status="success",
            mode=ExecutionMode.EXECUTE,
            config_type="thread",
            config_name="dim_customer",
            duration_ms=1200,
        )
        s = result.summary()
        assert "Status: success" in s
        assert "thread:dim_customer" in s
        assert "1.2s" in s

    def test_summary_execute_thread_with_detail(self) -> None:
        from weevr.engine.result import ThreadResult

        detail = ThreadResult(
            status="success",
            thread_name="dim_customer",
            rows_written=1234,
            write_mode="overwrite",
            target_path="/data/dim_customer",
        )
        result = RunResult(
            status="success",
            mode=ExecutionMode.EXECUTE,
            config_type="thread",
            config_name="dim_customer",
            duration_ms=800,
            detail=detail,
        )
        s = result.summary()
        assert "1,234 written" in s

    def test_summary_validate_success(self) -> None:
        result = RunResult(
            status="success",
            mode=ExecutionMode.VALIDATE,
            config_type="weave",
            config_name="dimensions",
        )
        s = result.summary()
        assert "validate" in s
        assert "\u2713" in s  # checkmark

    def test_summary_validate_failure(self) -> None:
        result = RunResult(
            status="failure",
            mode=ExecutionMode.VALIDATE,
            config_type="thread",
            config_name="dim_customer",
            validation_errors=["Source 'raw' not found: /data/raw"],
        )
        s = result.summary()
        assert "Errors:" in s
        assert "Source 'raw' not found" in s

    def test_summary_plan(self) -> None:
        from weevr.engine.planner import ExecutionPlan

        plan = ExecutionPlan(
            weave_name="dimensions",
            threads=["dim_product", "dim_customer"],
            dependencies={"dim_customer": ["dim_product"]},
            dependents={"dim_product": ["dim_customer"]},
            execution_order=[["dim_product"], ["dim_customer"]],
            cache_targets=[],
            inferred_dependencies={},
            explicit_dependencies={"dim_customer": ["dim_product"]},
        )
        result = RunResult(
            status="success",
            mode=ExecutionMode.PLAN,
            config_type="weave",
            config_name="dimensions",
            execution_plan=[plan],
        )
        s = result.summary()
        assert "plan" in s
        assert "Execution order:" in s
        assert "dim_product" in s
        assert "dim_customer" in s

    def test_summary_preview(self) -> None:
        result = RunResult(
            status="success",
            mode=ExecutionMode.PREVIEW,
            config_type="thread",
            config_name="dim_customer",
        )
        s = result.summary()
        assert "preview" in s
        assert "thread:dim_customer" in s

    def test_summary_with_warnings(self) -> None:
        result = RunResult(
            status="success",
            mode=ExecutionMode.EXECUTE,
            config_type="weave",
            config_name="dims",
            warnings=["No threads matched filter"],
        )
        s = result.summary()
        assert "Warnings:" in s
        assert "No threads matched filter" in s

    def test_summary_duration_formatting(self) -> None:
        # Milliseconds
        r1 = RunResult(
            status="success",
            mode=ExecutionMode.EXECUTE,
            config_type="thread",
            config_name="t",
            duration_ms=500,
        )
        assert "500ms" in r1.summary()

        # Seconds
        r2 = RunResult(
            status="success",
            mode=ExecutionMode.EXECUTE,
            config_type="thread",
            config_name="t",
            duration_ms=2100,
        )
        assert "2.1s" in r2.summary()

        # Minutes
        r3 = RunResult(
            status="success",
            mode=ExecutionMode.EXECUTE,
            config_type="thread",
            config_name="t",
            duration_ms=135000,
        )
        assert "2m" in r3.summary()

    def test_partial_status(self) -> None:
        result = RunResult(
            status="partial",
            mode=ExecutionMode.EXECUTE,
            config_type="weave",
            config_name="dims",
        )
        assert result.status == "partial"


class TestLoadedConfig:
    @pytest.fixture()
    def sample_thread(self) -> Thread:
        return Thread(
            name="dim_customer",
            config_version="1.0",
            sources={"main": Source(type="delta", alias="raw_customers")},
            target=Target(path="/data/curated/dim_customer"),
        )

    @pytest.fixture()
    def sample_weave(self) -> Weave:
        return Weave(
            name="dimensions",
            config_version="1.0",
            threads=[ThreadEntry(name="dim_customer")],
        )

    @pytest.fixture()
    def sample_loom(self) -> Loom:
        return Loom.model_validate(
            {
                "name": "nightly",
                "config_version": "1.0",
                "weaves": ["dimensions"],
            }
        )

    def test_model_property(self, sample_thread: Thread) -> None:
        loaded = LoadedConfig(sample_thread, "thread", "dim_customer")
        assert loaded.model is sample_thread

    def test_config_type(self, sample_thread: Thread) -> None:
        loaded = LoadedConfig(sample_thread, "thread", "dim_customer")
        assert loaded.config_type == "thread"

    def test_config_name(self, sample_thread: Thread) -> None:
        loaded = LoadedConfig(sample_thread, "thread", "dim_customer")
        assert loaded.config_name == "dim_customer"

    def test_getattr_proxies_name(self, sample_thread: Thread) -> None:
        loaded = LoadedConfig(sample_thread, "thread", "dim_customer")
        assert loaded.name == "dim_customer"

    def test_getattr_proxies_config_version(self, sample_thread: Thread) -> None:
        loaded = LoadedConfig(sample_thread, "thread", "dim_customer")
        assert loaded.config_version == "1.0"

    def test_getattr_proxies_sources(self, sample_thread: Thread) -> None:
        loaded = LoadedConfig(sample_thread, "thread", "dim_customer")
        assert "main" in loaded.sources

    def test_getattr_proxies_weaves(self, sample_loom: Loom) -> None:
        loaded = LoadedConfig(sample_loom, "loom", "nightly")
        assert loaded.weaves == [WeaveEntry(name="dimensions")]

    def test_getattr_proxies_threads(self, sample_weave: Weave) -> None:
        loaded = LoadedConfig(sample_weave, "weave", "dimensions")
        assert len(loaded.threads) == 1

    def test_getattr_unknown_raises(self, sample_thread: Thread) -> None:
        loaded = LoadedConfig(sample_thread, "thread", "dim_customer")
        with pytest.raises(AttributeError, match="no attribute 'nonexistent'"):
            _ = loaded.nonexistent

    def test_getattr_error_message_includes_model_type(self, sample_thread: Thread) -> None:
        loaded = LoadedConfig(sample_thread, "thread", "dim_customer")
        with pytest.raises(AttributeError, match="Thread"):
            _ = loaded.bogus

    def test_execution_plan_none_for_thread(self, sample_thread: Thread) -> None:
        loaded = LoadedConfig(sample_thread, "thread", "dim_customer")
        assert loaded.execution_plan is None

    def test_execution_plan_lazy(self, sample_weave: Weave) -> None:
        """Verify execution_plan is not built until accessed."""
        loaded = LoadedConfig(sample_weave, "weave", "dimensions")
        assert not loaded._plan_built  # noqa: SLF001
        # Access it — no threads provided so plan won't build
        _ = loaded.execution_plan
        assert loaded._plan_built  # noqa: SLF001
