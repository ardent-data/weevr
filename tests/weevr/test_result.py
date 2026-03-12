"""Unit tests for RunResult, LoadedConfig, and ExecutionMode."""

from __future__ import annotations

from typing import Any

import pytest

from weevr.engine.planner import ExecutionPlan
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

    def test_summary_execute_thread_with_error(self) -> None:
        from weevr.engine.result import ThreadResult

        detail = ThreadResult(
            status="failure",
            thread_name="dim_customer",
            rows_written=0,
            write_mode="",
            target_path="",
            error="Source path not found: /data/raw_customers",
        )
        result = RunResult(
            status="failure",
            mode=ExecutionMode.EXECUTE,
            config_type="thread",
            config_name="dim_customer",
            duration_ms=50,
            detail=detail,
        )
        s = result.summary()
        assert "Errors:" in s
        assert "[dim_customer] Source path not found: /data/raw_customers" in s

    def test_summary_execute_weave_with_thread_errors(self) -> None:
        from weevr.engine.result import ThreadResult, WeaveResult

        detail = WeaveResult(
            status="partial",
            weave_name="dims",
            thread_results=[
                ThreadResult(
                    status="success",
                    thread_name="dim_product",
                    rows_written=50,
                    write_mode="overwrite",
                    target_path="/data/dim_product",
                ),
                ThreadResult(
                    status="failure",
                    thread_name="dim_customer",
                    rows_written=0,
                    write_mode="",
                    target_path="",
                    error="Column 'email' not found",
                ),
            ],
            threads_skipped=[],
            duration_ms=200,
        )
        result = RunResult(
            status="partial",
            mode=ExecutionMode.EXECUTE,
            config_type="weave",
            config_name="dims",
            duration_ms=200,
            detail=detail,
        )
        s = result.summary()
        assert "Errors:" in s
        assert "[dim_customer] Column 'email' not found" in s
        assert "dim_product" not in s.split("Errors:")[1]

    def test_summary_execute_loom_with_thread_errors(self) -> None:
        from weevr.engine.result import LoomResult, ThreadResult, WeaveResult

        detail = LoomResult(
            status="partial",
            loom_name="nightly",
            weave_results=[
                WeaveResult(
                    status="success",
                    weave_name="dims",
                    thread_results=[
                        ThreadResult(
                            status="success",
                            thread_name="dim_product",
                            rows_written=100,
                            write_mode="overwrite",
                            target_path="/data/dim_product",
                        ),
                    ],
                    threads_skipped=[],
                    duration_ms=100,
                ),
                WeaveResult(
                    status="failure",
                    weave_name="facts",
                    thread_results=[
                        ThreadResult(
                            status="failure",
                            thread_name="fact_orders",
                            rows_written=0,
                            write_mode="",
                            target_path="",
                            error="Bad Request: file not found",
                        ),
                    ],
                    threads_skipped=["fact_revenue"],
                    duration_ms=50,
                ),
            ],
            duration_ms=150,
        )
        result = RunResult(
            status="partial",
            mode=ExecutionMode.EXECUTE,
            config_type="loom",
            config_name="nightly",
            duration_ms=150,
            detail=detail,
        )
        s = result.summary()
        assert "Errors:" in s
        assert "[fact_orders] Bad Request: file not found" in s

    def test_summary_execute_no_errors_when_all_succeed(self) -> None:
        from weevr.engine.result import ThreadResult, WeaveResult

        detail = WeaveResult(
            status="success",
            weave_name="dims",
            thread_results=[
                ThreadResult(
                    status="success",
                    thread_name="dim_product",
                    rows_written=50,
                    write_mode="overwrite",
                    target_path="/data/dim_product",
                ),
            ],
            threads_skipped=[],
            duration_ms=100,
        )
        result = RunResult(
            status="success",
            mode=ExecutionMode.EXECUTE,
            config_type="weave",
            config_name="dims",
            duration_ms=100,
            detail=detail,
        )
        s = result.summary()
        assert "Errors:" not in s


class TestSummaryPlanEnriched:
    """Tests for enriched plan summary with cache markers and footer counts."""

    def _make_plan_result(
        self,
        *,
        cache_targets: list[str] | None = None,
        lookup_schedule: dict[int, list[str]] | None = None,
    ) -> RunResult:
        plan = ExecutionPlan(
            weave_name="dimensions",
            threads=["a", "b", "c"],
            dependencies={"a": [], "b": ["a"], "c": ["b"]},
            dependents={"a": ["b"], "b": ["c"], "c": []},
            execution_order=[["a"], ["b"], ["c"]],
            cache_targets=cache_targets or [],
            inferred_dependencies={"a": [], "b": ["a"], "c": ["b"]},
            explicit_dependencies={"a": [], "b": [], "c": []},
            lookup_schedule=lookup_schedule,
        )
        return RunResult(
            status="success",
            mode=ExecutionMode.PLAN,
            config_type="weave",
            config_name="dimensions",
            execution_plan=[plan],
        )

    def test_summary_plan_cache_markers(self) -> None:
        result = self._make_plan_result(cache_targets=["a"])
        s = result.summary()
        assert "a*" in s
        assert "b*" not in s

    def test_summary_plan_no_cache(self) -> None:
        result = self._make_plan_result()
        s = result.summary()
        assert "*" not in s

    def test_summary_plan_footer_counts(self) -> None:
        result = self._make_plan_result(
            cache_targets=["a"],
            lookup_schedule={0: ["ext"]},
        )
        s = result.summary()
        assert "3 threads" in s
        assert "1 cached" in s
        assert "1 lookups" in s

    def test_summary_plan_footer_no_lookups(self) -> None:
        result = self._make_plan_result(cache_targets=["a"])
        s = result.summary()
        assert "3 threads" in s
        assert "1 cached" in s
        assert "lookups" not in s

    def test_summary_plan_footer_with_lookups(self) -> None:
        result = self._make_plan_result(
            lookup_schedule={0: ["ext1", "ext2"]},
        )
        s = result.summary()
        assert "2 lookups" in s


class TestExplain:
    """Tests for the explain() method on RunResult."""

    def _make_plan(
        self,
        *,
        threads: list[str] | None = None,
        dependencies: dict[str, list[str]] | None = None,
        execution_order: list[list[str]] | None = None,
        cache_targets: list[str] | None = None,
        lookup_schedule: dict[int, list[str]] | None = None,
        inferred_dependencies: dict[str, list[str]] | None = None,
        explicit_dependencies: dict[str, list[str]] | None = None,
        weave_name: str = "dimensions",
    ) -> ExecutionPlan:
        t = threads or ["a", "b", "c"]
        deps = dependencies or {"a": [], "b": ["a"], "c": ["b"]}
        dependents: dict[str, list[str]] = {n: [] for n in t}
        for name, upstream in deps.items():
            for u in upstream:
                if name not in dependents.get(u, []):
                    dependents.setdefault(u, []).append(name)
        return ExecutionPlan(
            weave_name=weave_name,
            threads=t,
            dependencies=deps,
            dependents=dependents,
            execution_order=execution_order or [["a"], ["b"], ["c"]],
            cache_targets=cache_targets or [],
            inferred_dependencies=inferred_dependencies or {n: deps.get(n, []) for n in t},
            explicit_dependencies=explicit_dependencies or {n: [] for n in t},
            lookup_schedule=lookup_schedule,
        )

    def _make_result(
        self,
        plans: list[ExecutionPlan],
        *,
        config_type: str = "weave",
        config_name: str = "dimensions",
    ) -> RunResult:
        return RunResult(
            status="success",
            mode=ExecutionMode.PLAN,
            config_type=config_type,
            config_name=config_name,
            execution_plan=plans,
        )

    def test_explain_non_plan_mode(self) -> None:
        result = RunResult(
            status="success",
            mode=ExecutionMode.EXECUTE,
            config_type="weave",
            config_name="test",
        )
        assert result.explain() == ""

    def test_explain_execution_order(self) -> None:
        plan = self._make_plan()
        result = self._make_result([plan])
        text = result.explain()
        assert "Execution order:" in text
        assert "1. [a]" in text
        assert "2. [b]" in text
        assert "3. [c]" in text

    def test_explain_dependencies_inferred(self) -> None:
        plan = self._make_plan()
        result = self._make_result([plan])
        text = result.explain()
        assert "b \u2190 a (inferred)" in text

    def test_explain_dependencies_explicit(self) -> None:
        plan = self._make_plan(
            inferred_dependencies={"a": [], "b": [], "c": []},
            explicit_dependencies={"a": [], "b": ["a"], "c": ["b"]},
        )
        result = self._make_result([plan])
        text = result.explain()
        assert "b \u2190 a (explicit)" in text

    def test_explain_dependencies_mixed(self) -> None:
        plan = self._make_plan(
            threads=["a", "b", "c"],
            dependencies={"a": [], "b": [], "c": ["a", "b"]},
            execution_order=[["a", "b"], ["c"]],
            inferred_dependencies={"a": [], "b": [], "c": ["a"]},
            explicit_dependencies={"a": [], "b": [], "c": ["b"]},
        )
        result = self._make_result([plan])
        text = result.explain()
        assert "a (inferred)" in text
        assert "b (explicit)" in text

    def test_explain_dependencies_none(self) -> None:
        plan = self._make_plan()
        result = self._make_result([plan])
        text = result.explain()
        assert "a  (none)" in text

    def test_explain_cache_targets(self) -> None:
        plan = self._make_plan(cache_targets=["a"])
        result = self._make_result([plan])
        text = result.explain()
        assert "Cache targets:" in text
        assert "a  1 consumer: b" in text

    def test_explain_cache_targets_omitted(self) -> None:
        plan = self._make_plan()
        result = self._make_result([plan])
        text = result.explain()
        assert "Cache targets:" not in text

    def test_explain_lookup_schedule(self) -> None:
        plan = self._make_plan(lookup_schedule={0: ["ext_ref"]})
        result = self._make_result([plan])
        text = result.explain()
        assert "Lookup schedule:" in text
        assert "before group 0: ext_ref" in text

    def test_explain_lookup_schedule_omitted(self) -> None:
        plan = self._make_plan()
        result = self._make_result([plan])
        text = result.explain()
        assert "Lookup schedule:" not in text

    def test_explain_thread_detail(self) -> None:
        plan = self._make_plan(threads=["a"], execution_order=[["a"]])

        class _FakeSource:
            type = "delta"
            alias = "raw/customers"
            path = None

        class _FakeTarget:
            alias = None
            path = "/data/curated/customers"

        class _FakeThread:
            sources = {"main": _FakeSource()}
            target = _FakeTarget()
            steps: list[Any] = []

        result = self._make_result([plan])
        result._resolved_threads = {"a": _FakeThread()}
        text = result.explain()
        assert "Thread detail:" in text
        assert "delta:raw/customers" in text
        assert "/data/curated/customers" in text
        assert "0 steps" in text

    def test_explain_thread_detail_with_joins(self) -> None:
        plan = self._make_plan(threads=["a"], execution_order=[["a"]])

        class _FakeJoinStep:
            join = True

        class _FakeOtherStep:
            rename = True

        class _FakeSource:
            type = "delta"
            alias = "raw/data"
            path = None

        class _FakeTarget:
            alias = "out/data"
            path = None

        class _FakeThread:
            sources = {"main": _FakeSource()}
            target = _FakeTarget()
            steps = [_FakeJoinStep(), _FakeOtherStep(), _FakeJoinStep()]

        result = self._make_result([plan])
        result._resolved_threads = {"a": _FakeThread()}
        text = result.explain()
        assert "3 steps" in text
        assert "2 joins" in text

    def test_explain_thread_detail_singular(self) -> None:
        """Verify singular 'step' and 'join' when count is 1."""
        plan = self._make_plan(threads=["a"], execution_order=[["a"]])

        class _FakeJoinStep:
            join = True

        class _FakeSource:
            type = "delta"
            alias = "raw/data"
            path = None

        class _FakeTarget:
            alias = "out/data"
            path = None

        class _FakeThread:
            sources = {"main": _FakeSource()}
            target = _FakeTarget()
            steps = [_FakeJoinStep()]

        result = self._make_result([plan])
        result._resolved_threads = {"a": _FakeThread()}
        text = result.explain()
        assert "1 step," in text
        assert "1 join" in text
        assert "1 steps" not in text
        assert "1 joins" not in text

    def test_explain_thread_detail_missing(self) -> None:
        plan = self._make_plan()
        result = self._make_result([plan])
        text = result.explain()
        assert "Thread detail:" not in text

    def test_explain_loom_header(self) -> None:
        plan = self._make_plan()
        result = self._make_result([plan], config_type="loom", config_name="nightly")
        text = result.explain()
        assert "Loom: nightly" in text
        assert "1 weaves" in text

    def test_explain_multi_weave(self) -> None:
        plan1 = self._make_plan(weave_name="dims")
        plan2 = self._make_plan(weave_name="facts")
        result = self._make_result([plan1, plan2], config_type="loom", config_name="nightly")
        text = result.explain()
        assert "Plan: dims" in text
        assert "Plan: facts" in text
        assert "2 weaves" in text


class TestReprHtml:
    """Tests for _repr_html_() on RunResult."""

    def test_repr_html_non_plan(self) -> None:
        result = RunResult(
            status="success",
            mode=ExecutionMode.EXECUTE,
            config_type="weave",
            config_name="test",
            duration_ms=1200,
        )
        html_out = result._repr_html_()
        assert html_out is not None
        assert "<pre" in html_out
        assert "success" in html_out

    def test_repr_html_plan_mode(self) -> None:
        plan = ExecutionPlan(
            weave_name="dims",
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            dependents={"a": ["b"], "b": []},
            execution_order=[["a"], ["b"]],
            cache_targets=[],
            inferred_dependencies={"a": [], "b": ["a"]},
            explicit_dependencies={"a": [], "b": []},
        )
        result = RunResult(
            status="success",
            mode=ExecutionMode.PLAN,
            config_type="weave",
            config_name="dims",
            execution_plan=[plan],
        )
        html_out = result._repr_html_()
        assert html_out is not None
        assert "<svg" in html_out
        assert "Plan Summary" in html_out

    def test_repr_html_plan_mode_summary_table(self) -> None:
        plan = ExecutionPlan(
            weave_name="dims",
            threads=["a"],
            dependencies={"a": []},
            dependents={"a": []},
            execution_order=[["a"]],
            cache_targets=[],
            inferred_dependencies={"a": []},
            explicit_dependencies={"a": []},
        )
        result = RunResult(
            status="success",
            mode=ExecutionMode.PLAN,
            config_type="weave",
            config_name="dims",
            execution_plan=[plan],
        )
        html_out = result._repr_html_()
        assert html_out is not None
        assert "PLANNED" not in html_out  # status is "success"
        assert "success" in html_out
        assert "1 threads" in html_out

    def test_repr_html_escapes_user_strings(self) -> None:
        plan = ExecutionPlan(
            weave_name="<script>alert(1)</script>",
            threads=["<b>bold</b>"],
            dependencies={"<b>bold</b>": []},
            dependents={"<b>bold</b>": []},
            execution_order=[["<b>bold</b>"]],
            cache_targets=[],
            inferred_dependencies={"<b>bold</b>": []},
            explicit_dependencies={"<b>bold</b>": []},
        )
        result = RunResult(
            status="success",
            mode=ExecutionMode.PLAN,
            config_type="weave",
            config_name="test",
            execution_plan=[plan],
        )
        html_out = result._repr_html_()
        assert html_out is not None
        assert "<script>" not in html_out
        assert "<b>bold</b>" not in html_out


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


class TestPlanDisplayIntegration:
    """Integration tests verifying the full plan mode display workflow."""

    def _build_plan_result(self) -> RunResult:
        plan = ExecutionPlan(
            weave_name="dims",
            threads=["raw", "staging", "curated"],
            dependencies={"raw": [], "staging": ["raw"], "curated": ["staging"]},
            dependents={"raw": ["staging"], "staging": ["curated"], "curated": []},
            execution_order=[["raw"], ["staging"], ["curated"]],
            cache_targets=["raw"],
            inferred_dependencies={
                "raw": [],
                "staging": ["raw"],
                "curated": ["staging"],
            },
            explicit_dependencies={"raw": [], "staging": [], "curated": []},
        )
        result = RunResult(
            status="success",
            mode=ExecutionMode.PLAN,
            config_type="weave",
            config_name="dims",
            execution_plan=[plan],
        )
        return result

    def test_plan_result_full_workflow(self) -> None:
        result = self._build_plan_result()
        # summary has cache markers and footer
        s = result.summary()
        assert "raw*" in s
        assert "3 threads" in s
        assert "1 cached" in s

        # explain has key sections
        e = result.explain()
        assert "Execution order:" in e
        assert "Dependencies:" in e
        assert "Cache targets:" in e
        assert "raw  1 consumer: staging" in e

        # _repr_html_ returns valid HTML with SVG
        h = result._repr_html_()
        assert h is not None
        assert "<svg" in h
        assert "Plan Summary" in h

    def test_plan_result_with_lookups(self) -> None:
        plan = ExecutionPlan(
            weave_name="facts",
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            dependents={"a": ["b"], "b": []},
            execution_order=[["a"], ["b"]],
            cache_targets=[],
            inferred_dependencies={"a": [], "b": ["a"]},
            explicit_dependencies={"a": [], "b": []},
            lookup_schedule={0: ["customer_dim"]},
        )
        result = RunResult(
            status="success",
            mode=ExecutionMode.PLAN,
            config_type="weave",
            config_name="facts",
            execution_plan=[plan],
        )
        e = result.explain()
        assert "Lookup schedule:" in e
        assert "customer_dim" in e

    def test_loom_plan_result(self) -> None:
        plan1 = ExecutionPlan(
            weave_name="dims",
            threads=["a"],
            dependencies={"a": []},
            dependents={"a": []},
            execution_order=[["a"]],
            cache_targets=[],
            inferred_dependencies={"a": []},
            explicit_dependencies={"a": []},
        )
        plan2 = ExecutionPlan(
            weave_name="facts",
            threads=["x"],
            dependencies={"x": []},
            dependents={"x": []},
            execution_order=[["x"]],
            cache_targets=[],
            inferred_dependencies={"x": []},
            explicit_dependencies={"x": []},
        )
        result = RunResult(
            status="success",
            mode=ExecutionMode.PLAN,
            config_type="loom",
            config_name="nightly",
            execution_plan=[plan1, plan2],
        )
        e = result.explain()
        assert "Loom: nightly" in e
        assert "2 weaves" in e
        assert "Plan: dims" in e
        assert "Plan: facts" in e

        h = result._repr_html_()
        assert h is not None
        assert h.count("<svg") == 2

    def test_plan_result_no_resolved_threads(self) -> None:
        result = self._build_plan_result()
        e = result.explain()
        assert "Thread detail:" not in e

    def test_dag_export_roundtrip(self, tmp_path: Any) -> None:
        plan = ExecutionPlan(
            weave_name="test",
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            dependents={"a": ["b"], "b": []},
            execution_order=[["a"], ["b"]],
            cache_targets=[],
            inferred_dependencies={"a": [], "b": ["a"]},
            explicit_dependencies={"a": [], "b": []},
        )
        diagram = plan.dag()
        out_path = str(tmp_path / "test.svg")
        diagram.save(out_path)

        import xml.etree.ElementTree as ET
        from pathlib import Path

        content = Path(out_path).read_text(encoding="utf-8")
        assert "<svg" in content
        ET.fromstring(content)
