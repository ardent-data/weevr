"""Unit tests for WeaveRunner, LoomRunner, and CacheManager logic.

All tests are fast (no Spark required). execute_thread is patched to return
controlled ThreadResult objects, allowing pure logic testing.
"""

from typing import Any
from unittest.mock import MagicMock, patch

from weevr.engine.cache_manager import CacheManager
from weevr.engine.planner import build_plan
from weevr.engine.result import ThreadResult, WeaveResult
from weevr.engine.runner import execute_loom, execute_weave
from weevr.errors.exceptions import ExecutionError
from weevr.model.loom import Loom
from weevr.model.thread import Thread
from weevr.model.weave import ConditionSpec, ThreadEntry, Weave

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MOCK_SPARK = MagicMock()


def _make_thread(
    name: str,
    target_alias: str | None = None,
    source_aliases: list[str] | None = None,
    on_failure: str | None = None,
    cache: bool | None = None,
) -> Thread:
    """Create a minimal Thread with configurable source/target and failure policy."""
    sources: dict[str, Any] = {}
    if source_aliases:
        for alias in source_aliases:
            sources[alias] = {"type": "delta", "alias": alias}
    else:
        sources["_src"] = {"type": "delta", "alias": "_src"}

    target: dict[str, Any] = {"alias": target_alias or f"test.{name}"}

    data: dict[str, Any] = {
        "name": name,
        "config_version": "1.0",
        "sources": sources,
        "target": target,
    }
    if on_failure is not None:
        data["failure"] = {"on_failure": on_failure}
    if cache is not None:
        data["cache"] = cache
    return Thread.model_validate(data)


def _make_result(name: str, status: str = "success") -> ThreadResult:
    return ThreadResult(
        status=status,  # type: ignore[arg-type]
        thread_name=name,
        rows_written=10,
        write_mode="overwrite",
        target_path=f"/data/{name}",
    )


def _entries(*names: str) -> list[ThreadEntry]:
    return [ThreadEntry(name=n) for n in names]


def _run_weave(threads: dict[str, Thread], thread_names: list[str] | None = None) -> WeaveResult:
    """Build a plan for the given threads and run execute_weave."""
    names = thread_names or list(threads.keys())
    plan = build_plan("test_weave", threads, _entries(*names))
    return execute_weave(_MOCK_SPARK, plan, threads)


# ---------------------------------------------------------------------------
# WeaveRunner tests — happy path
# ---------------------------------------------------------------------------


class TestWeaveRunnerHappyPath:
    @patch("weevr.engine.runner.execute_thread")
    def test_single_thread_succeeds(self, mock_exec):
        """Single thread weave with success → status='success'."""
        mock_exec.return_value = _make_result("A")
        threads = {"A": _make_thread("A")}
        result = _run_weave(threads)
        assert result.status == "success"
        assert len(result.thread_results) == 1
        assert result.threads_skipped == []

    @patch("weevr.engine.runner.execute_thread")
    def test_independent_threads_all_complete(self, mock_exec):
        """Independent threads all execute and all complete."""
        mock_exec.side_effect = lambda spark, thread, **kwargs: _make_result(thread.name)
        threads = {
            "A": _make_thread("A"),
            "B": _make_thread("B"),
            "C": _make_thread("C"),
        }
        result = _run_weave(threads)
        assert result.status == "success"
        assert len(result.thread_results) == 3
        assert result.threads_skipped == []

    @patch("weevr.engine.runner.execute_thread")
    def test_dependent_thread_executes_after_dependency(self, mock_exec):
        """B depends on A — both succeed, result contains both."""
        completed: list[str] = []

        def side_effect(spark, thread, **kwargs):
            completed.append(thread.name)
            return _make_result(thread.name)

        mock_exec.side_effect = side_effect
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", source_aliases=["out_a"]),
        }
        result = _run_weave(threads)
        assert result.status == "success"
        assert len(result.thread_results) == 2
        # A must complete before B (A is at level 0, B at level 1)
        assert completed.index("A") < completed.index("B")

    @patch("weevr.engine.runner.execute_thread")
    def test_weave_result_duration_ms_positive(self, mock_exec):
        """WeaveResult.duration_ms is a non-negative integer."""
        mock_exec.return_value = _make_result("A")
        threads = {"A": _make_thread("A")}
        result = _run_weave(threads)
        assert isinstance(result.duration_ms, int)
        assert result.duration_ms >= 0


# ---------------------------------------------------------------------------
# WeaveRunner tests — failure handling
# ---------------------------------------------------------------------------


class TestWeaveRunnerFailureHandling:
    @patch("weevr.engine.runner.execute_thread")
    def test_abort_weave_skips_remaining(self, mock_exec):
        """on_failure=abort_weave: A fails → B and C (both depend on A) skipped.

        A is alone at level 0. B and C are at level 1 (both depend on A),
        so they are still pending when abort_weave triggers.
        """

        def side_effect(spark, thread, **kwargs):
            if thread.name == "A":
                raise ExecutionError("A failed", thread_name="A")
            return _make_result(thread.name)

        mock_exec.side_effect = side_effect
        # B and C both depend on A — they're at level 1, guaranteed pending when A fails
        threads = {
            "A": _make_thread("A", target_alias="out_a", on_failure="abort_weave"),
            "B": _make_thread("B", source_aliases=["out_a"]),
            "C": _make_thread("C", source_aliases=["out_a"]),
        }
        result = _run_weave(threads)
        assert result.status == "failure"
        # B and C should be skipped (abort stops everything)
        assert set(result.threads_skipped) >= {"B", "C"}
        # Failed thread result carries the error message
        failed = [r for r in result.thread_results if r.status == "failure"]
        assert len(failed) == 1
        assert failed[0].thread_name == "A"
        assert failed[0].error is not None
        assert "A failed" in failed[0].error
        assert failed[0].error.startswith("ExecutionError:")

    @patch("weevr.engine.runner.execute_thread")
    def test_skip_downstream_skips_only_dependents(self, mock_exec):
        """on_failure=skip_downstream: A fails → B (depends on A) skipped, C continues."""

        def side_effect(spark, thread, **kwargs):
            if thread.name == "A":
                raise ExecutionError("A failed", thread_name="A")
            return _make_result(thread.name)

        mock_exec.side_effect = side_effect
        threads = {
            "A": _make_thread("A", target_alias="out_a", on_failure="skip_downstream"),
            "B": _make_thread("B", source_aliases=["out_a"]),
            "C": _make_thread("C"),
        }
        result = _run_weave(threads)
        assert result.status == "partial"
        assert "B" in result.threads_skipped
        assert "C" not in result.threads_skipped
        succeeded = [r.thread_name for r in result.thread_results if r.status == "success"]
        assert "C" in succeeded

    @patch("weevr.engine.runner.execute_thread")
    def test_continue_same_as_skip_downstream(self, mock_exec):
        """on_failure=continue: dependents skipped, independents continue."""

        def side_effect(spark, thread, **kwargs):
            if thread.name == "A":
                raise ExecutionError("A failed", thread_name="A")
            return _make_result(thread.name)

        mock_exec.side_effect = side_effect
        threads = {
            "A": _make_thread("A", target_alias="out_a", on_failure="continue"),
            "B": _make_thread("B", source_aliases=["out_a"]),
            "C": _make_thread("C"),
        }
        result = _run_weave(threads)
        assert result.status == "partial"
        assert "B" in result.threads_skipped
        succeeded = [r.thread_name for r in result.thread_results if r.status == "success"]
        assert "C" in succeeded

    @patch("weevr.engine.runner.execute_thread")
    def test_all_threads_succeed_status_success(self, mock_exec):
        """All threads succeed → status='success'."""
        mock_exec.side_effect = lambda spark, thread, **kwargs: _make_result(thread.name)
        threads = {"A": _make_thread("A"), "B": _make_thread("B")}
        result = _run_weave(threads)
        assert result.status == "success"

    @patch("weevr.engine.runner.execute_thread")
    def test_all_threads_fail_abort_weave_status_failure(self, mock_exec):
        """All threads fail → status='failure'."""

        def side_effect(spark, thread, **kwargs):
            raise ExecutionError(f"{thread.name} failed", thread_name=thread.name)

        mock_exec.side_effect = side_effect
        threads = {
            "A": _make_thread("A", on_failure="abort_weave"),
            "B": _make_thread("B"),
        }
        result = _run_weave(threads)
        assert result.status == "failure"

    @patch("weevr.engine.runner.execute_thread")
    def test_mix_success_and_failure_partial_status(self, mock_exec):
        """Some succeed, some fail → status='partial'."""

        def side_effect(spark, thread, **kwargs):
            if thread.name == "B":
                raise ExecutionError("B failed", thread_name="B")
            return _make_result(thread.name)

        mock_exec.side_effect = side_effect
        threads = {
            "A": _make_thread("A"),
            "B": _make_thread("B", on_failure="continue"),
        }
        result = _run_weave(threads)
        assert result.status == "partial"

    @patch("weevr.engine.runner.execute_thread")
    def test_threads_skipped_contains_correct_names(self, mock_exec):
        """threads_skipped accurately tracks which threads were skipped."""

        def side_effect(spark, thread, **kwargs):
            if thread.name == "A":
                raise ExecutionError("A failed", thread_name="A")
            return _make_result(thread.name)

        mock_exec.side_effect = side_effect
        threads = {
            "A": _make_thread("A", target_alias="out_a", on_failure="skip_downstream"),
            "B": _make_thread("B", source_aliases=["out_a"]),
        }
        result = _run_weave(threads)
        assert "B" in result.threads_skipped

    @patch("weevr.engine.runner.execute_thread")
    def test_default_on_failure_is_abort_weave(self, mock_exec):
        """Thread with no failure config defaults to abort_weave behaviour.

        A is alone at level 0; B and C are at level 1 (both depend on A).
        """

        def side_effect(spark, thread, **kwargs):
            if thread.name == "A":
                raise ExecutionError("A failed", thread_name="A")
            return _make_result(thread.name)

        mock_exec.side_effect = side_effect
        # A writes to out_a; B and C both read from it — they're at level 1
        threads = {
            "A": _make_thread("A", target_alias="out_a"),  # no on_failure set → abort_weave
            "B": _make_thread("B", source_aliases=["out_a"]),
            "C": _make_thread("C", source_aliases=["out_a"]),
        }
        result = _run_weave(threads)
        assert result.status == "failure"
        assert set(result.threads_skipped) >= {"B", "C"}


# ---------------------------------------------------------------------------
# LoomRunner tests
# ---------------------------------------------------------------------------


class TestLoomRunner:
    @patch("weevr.engine.runner.execute_thread")
    def test_two_weaves_execute_sequentially(self, mock_exec):
        """Loom with 2 weaves: both execute in order, LoomResult has 2 WeaveResults."""
        mock_exec.side_effect = lambda spark, thread, **kwargs: _make_result(thread.name)

        loom = Loom.model_validate(
            {"name": "nightly", "config_version": "1.0", "weaves": ["dims", "facts"]}
        )
        weaves = {
            "dims": Weave.model_validate(
                {"config_version": "1.0", "name": "dims", "threads": ["dim_a"]}
            ),
            "facts": Weave.model_validate(
                {"config_version": "1.0", "name": "facts", "threads": ["fact_b"]}
            ),
        }
        threads = {
            "dims": {"dim_a": _make_thread("dim_a")},
            "facts": {"fact_b": _make_thread("fact_b")},
        }
        result = execute_loom(_MOCK_SPARK, loom, weaves, threads)
        assert result.status == "success"
        assert result.loom_name == "nightly"
        assert len(result.weave_results) == 2
        assert result.weave_results[0].weave_name == "dims"
        assert result.weave_results[1].weave_name == "facts"

    @patch("weevr.engine.runner.execute_thread")
    def test_first_weave_fails_loom_stops(self, mock_exec):
        """First weave fails → loom stops; LoomResult.status='failure'."""

        def side_effect(spark, thread, **kwargs):
            if thread.name == "dim_a":
                raise ExecutionError("dim_a failed", thread_name="dim_a")
            return _make_result(thread.name)

        mock_exec.side_effect = side_effect

        loom = Loom.model_validate(
            {"name": "nightly", "config_version": "1.0", "weaves": ["dims", "facts"]}
        )
        weaves = {
            "dims": Weave.model_validate(
                {"config_version": "1.0", "name": "dims", "threads": ["dim_a"]}
            ),
            "facts": Weave.model_validate(
                {"config_version": "1.0", "name": "facts", "threads": ["fact_b"]}
            ),
        }
        threads = {
            "dims": {"dim_a": _make_thread("dim_a")},
            "facts": {"fact_b": _make_thread("fact_b")},
        }
        result = execute_loom(_MOCK_SPARK, loom, weaves, threads)
        assert result.status == "failure"
        assert len(result.weave_results) == 1  # second weave never ran
        assert result.weave_results[0].weave_name == "dims"

    @patch("weevr.engine.runner.execute_thread")
    def test_all_weaves_succeed_loom_success(self, mock_exec):
        """All weaves succeed → LoomResult.status='success'."""
        mock_exec.side_effect = lambda spark, thread, **kwargs: _make_result(thread.name)

        loom = Loom.model_validate({"name": "l", "config_version": "1.0", "weaves": ["w1", "w2"]})
        weaves = {
            "w1": Weave.model_validate({"config_version": "1.0", "name": "w1", "threads": ["t1"]}),
            "w2": Weave.model_validate({"config_version": "1.0", "name": "w2", "threads": ["t2"]}),
        }
        threads = {
            "w1": {"t1": _make_thread("t1")},
            "w2": {"t2": _make_thread("t2")},
        }
        result = execute_loom(_MOCK_SPARK, loom, weaves, threads)
        assert result.status == "success"

    @patch("weevr.engine.runner.execute_thread")
    def test_first_succeeds_second_fails_partial(self, mock_exec):
        """First weave succeeds, second fails → LoomResult.status='partial'."""

        def side_effect(spark, thread, **kwargs):
            if thread.name == "t2":
                raise ExecutionError("t2 failed", thread_name="t2")
            return _make_result(thread.name)

        mock_exec.side_effect = side_effect

        loom = Loom.model_validate({"name": "l", "config_version": "1.0", "weaves": ["w1", "w2"]})
        weaves = {
            "w1": Weave.model_validate({"config_version": "1.0", "name": "w1", "threads": ["t1"]}),
            "w2": Weave.model_validate({"config_version": "1.0", "name": "w2", "threads": ["t2"]}),
        }
        threads = {
            "w1": {"t1": _make_thread("t1")},
            "w2": {"t2": _make_thread("t2")},
        }
        result = execute_loom(_MOCK_SPARK, loom, weaves, threads)
        assert result.status == "partial"
        assert len(result.weave_results) == 2

    @patch("weevr.engine.runner.execute_thread")
    def test_single_partial_weave_loom_status_is_partial(self, mock_exec):
        """Loom with a single partial weave → LoomResult.status='partial', not 'failure'."""

        def side_effect(spark, thread, **kwargs):
            if thread.name == "t_fail":
                raise ExecutionError("t_fail failed", thread_name="t_fail")
            return _make_result(thread.name)

        mock_exec.side_effect = side_effect

        loom = Loom.model_validate({"name": "l", "config_version": "1.0", "weaves": ["w1"]})
        weaves = {
            "w1": Weave.model_validate(
                {"config_version": "1.0", "name": "w1", "threads": ["t_ok", "t_fail"]}
            ),
        }
        threads = {
            "w1": {
                "t_ok": _make_thread("t_ok"),
                "t_fail": _make_thread("t_fail", on_failure="continue"),
            },
        }
        result = execute_loom(_MOCK_SPARK, loom, weaves, threads)
        assert result.status == "partial"
        assert len(result.weave_results) == 1
        assert result.weave_results[0].status == "partial"

    @patch("weevr.engine.runner.execute_thread")
    def test_loom_result_duration_ms_positive(self, mock_exec):
        """LoomResult.duration_ms is a non-negative integer."""
        mock_exec.side_effect = lambda spark, thread, **kwargs: _make_result(thread.name)

        loom = Loom.model_validate({"name": "l", "config_version": "1.0", "weaves": ["w1"]})
        weaves = {
            "w1": Weave.model_validate({"config_version": "1.0", "name": "w1", "threads": ["t1"]})
        }
        threads = {"w1": {"t1": _make_thread("t1")}}
        result = execute_loom(_MOCK_SPARK, loom, weaves, threads)
        assert isinstance(result.duration_ms, int)
        assert result.duration_ms >= 0


# ---------------------------------------------------------------------------
# Telemetry tests — weave and loom
# ---------------------------------------------------------------------------


class TestWeaveTelemetry:
    @patch("weevr.engine.runner.execute_thread")
    def test_weave_telemetry_populated_with_collector(self, mock_exec):
        """WeaveResult includes telemetry when a collector is provided."""
        mock_exec.side_effect = lambda spark, thread, **kwargs: _make_result(thread.name)
        threads = {"A": _make_thread("A")}
        plan = build_plan("tel_weave", threads, _entries("A"))

        from weevr.telemetry.collector import SpanCollector
        from weevr.telemetry.span import generate_trace_id

        collector = SpanCollector(generate_trace_id())
        result = execute_weave(_MOCK_SPARK, plan, threads, collector=collector)

        assert result.status == "success"
        assert result.telemetry is not None
        assert result.telemetry.span is not None
        assert result.telemetry.span.name == "weave:tel_weave"

    @patch("weevr.engine.runner.execute_thread")
    def test_weave_telemetry_span_uses_qualified_key(self, mock_exec):
        """Weave span label uses weave_span_label when provided."""
        mock_exec.side_effect = lambda spark, thread, **kwargs: _make_result(thread.name)
        threads = {"A": _make_thread("A")}
        plan = build_plan("tel_weave", threads, _entries("A"))

        from weevr.telemetry.collector import SpanCollector
        from weevr.telemetry.span import generate_trace_id

        collector = SpanCollector(generate_trace_id())
        result = execute_weave(
            _MOCK_SPARK,
            plan,
            threads,
            collector=collector,
            weave_span_label="staging.weave",
        )

        assert result.status == "success"
        assert result.telemetry is not None
        assert result.telemetry.span is not None
        assert result.telemetry.span.name == "weave:staging.weave"

    @patch("weevr.engine.runner.execute_thread")
    def test_weave_telemetry_none_without_collector(self, mock_exec):
        """WeaveResult.telemetry is None when no collector is provided."""
        mock_exec.side_effect = lambda spark, thread, **kwargs: _make_result(thread.name)
        threads = {"A": _make_thread("A")}
        result = _run_weave(threads)

        assert result.status == "success"
        assert result.telemetry is None


class TestLoomTelemetry:
    @patch("weevr.engine.runner.execute_thread")
    def test_loom_telemetry_always_populated(self, mock_exec):
        """LoomResult always has telemetry (collector created internally)."""
        mock_exec.side_effect = lambda spark, thread, **kwargs: _make_result(thread.name)

        loom = Loom.model_validate({"name": "l", "config_version": "1.0", "weaves": ["w1"]})
        weaves = {
            "w1": Weave.model_validate({"config_version": "1.0", "name": "w1", "threads": ["t1"]})
        }
        threads = {"w1": {"t1": _make_thread("t1")}}
        result = execute_loom(_MOCK_SPARK, loom, weaves, threads)

        assert result.telemetry is not None
        assert result.telemetry.span is not None
        assert result.telemetry.span.name == "loom:l"

    @patch("weevr.engine.runner.execute_thread")
    def test_loom_telemetry_contains_weave_telemetry(self, mock_exec):
        """LoomTelemetry aggregates weave-level telemetry."""
        mock_exec.side_effect = lambda spark, thread, **kwargs: _make_result(thread.name)

        loom = Loom.model_validate({"name": "l", "config_version": "1.0", "weaves": ["w1", "w2"]})
        weaves = {
            "w1": Weave.model_validate({"config_version": "1.0", "name": "w1", "threads": ["t1"]}),
            "w2": Weave.model_validate({"config_version": "1.0", "name": "w2", "threads": ["t2"]}),
        }
        threads = {
            "w1": {"t1": _make_thread("t1")},
            "w2": {"t2": _make_thread("t2")},
        }
        result = execute_loom(_MOCK_SPARK, loom, weaves, threads)

        assert result.telemetry is not None
        assert "w1" in result.telemetry.weave_telemetry
        assert "w2" in result.telemetry.weave_telemetry


# ---------------------------------------------------------------------------
# CacheManager unit tests
# ---------------------------------------------------------------------------


class TestCacheManager:
    def test_cache_targets_tracked(self):
        """CacheManager tracks declared cache targets."""
        cm = CacheManager(cache_targets=["A"], dependents={"A": ["B", "C"]})
        assert cm.is_cache_target("A")
        assert not cm.is_cache_target("B")

    def test_cleanup_called_on_empty_cache(self):
        """cleanup() on empty cache does not raise."""
        cm = CacheManager(cache_targets=[], dependents={})
        cm.cleanup()  # must not raise

    def test_notify_complete_does_not_raise_without_cached_df(self):
        """notify_complete for a consumer of a non-cached thread does not raise."""
        cm = CacheManager(cache_targets=["A"], dependents={"A": ["B"]})
        # A was never persisted — notify_complete("B") should be a no-op
        cm.notify_complete("B")

    def test_persist_failure_does_not_raise(self):
        """persist() failure is silently logged, not raised."""
        mock_spark = MagicMock()
        mock_spark.read.format.return_value.load.side_effect = RuntimeError("disk full")
        cm = CacheManager(cache_targets=["A"], dependents={"A": ["B", "C"]})
        # Should not raise
        cm.persist("A", mock_spark, "/data/A")
        assert "A" not in cm._cached

    def test_cleanup_unpersists_remaining(self):
        """cleanup() unpersists any still-cached DataFrames."""
        mock_df = MagicMock()
        mock_spark = MagicMock()
        mock_spark.read.format.return_value.load.return_value = mock_df
        cm = CacheManager(cache_targets=["A"], dependents={"A": ["B", "C"]})
        cm.persist("A", mock_spark, "/data/A")
        assert "A" in cm._cached
        cm.cleanup()
        mock_df.unpersist.assert_called_once()
        assert "A" not in cm._cached


# ---------------------------------------------------------------------------
# Conditional Execution tests
# ---------------------------------------------------------------------------


class TestConditionalThreadExecution:
    """Test that threads with conditions are evaluated and skipped correctly."""

    @patch("weevr.engine.runner.execute_thread")
    def test_condition_true_executes(self, mock_exec):
        """Thread with condition=True executes normally."""
        mock_exec.return_value = _make_result("A")
        threads = {"A": _make_thread("A")}
        entries = [ThreadEntry(name="A", condition=ConditionSpec(when="true"))]
        plan = build_plan("test_weave", threads, entries)
        conditions = {"A": ConditionSpec(when="true")}
        result = execute_weave(_MOCK_SPARK, plan, threads, thread_conditions=conditions)
        assert result.status == "success"
        assert len(result.thread_results) == 1
        mock_exec.assert_called_once()

    @patch("weevr.engine.runner.execute_thread")
    def test_condition_false_skips(self, mock_exec):
        """Thread with condition=False is skipped without execution."""
        threads = {"A": _make_thread("A")}
        entries = [ThreadEntry(name="A")]
        plan = build_plan("test_weave", threads, entries)
        conditions = {"A": ConditionSpec(when="false")}
        result = execute_weave(_MOCK_SPARK, plan, threads, thread_conditions=conditions)
        assert result.status == "failure"  # all threads skipped → failure
        assert "A" in result.threads_skipped
        mock_exec.assert_not_called()
        # Check the skipped result
        skipped = [r for r in result.thread_results if r.status == "skipped"]
        assert len(skipped) == 1
        assert skipped[0].thread_name == "A"
        assert skipped[0].skip_reason == "false"

    @patch("weevr.engine.runner.execute_thread")
    def test_mixed_conditions(self, mock_exec):
        """Some threads have conditions, others don't."""
        mock_exec.return_value = _make_result("B")
        threads = {
            "A": _make_thread("A"),
            "B": _make_thread("B"),
        }
        entries = [ThreadEntry(name="A"), ThreadEntry(name="B")]
        plan = build_plan("test_weave", threads, entries)
        conditions = {"A": ConditionSpec(when="false")}  # B has no condition
        result = execute_weave(_MOCK_SPARK, plan, threads, thread_conditions=conditions)
        assert result.status == "partial"
        assert "A" in result.threads_skipped
        assert any(r.thread_name == "B" and r.status == "success" for r in result.thread_results)

    @patch("weevr.engine.runner.execute_thread")
    def test_param_condition(self, mock_exec):
        """Thread condition with param reference evaluates correctly."""
        mock_exec.return_value = _make_result("A")
        threads = {"A": _make_thread("A")}
        entries = [ThreadEntry(name="A")]
        plan = build_plan("test_weave", threads, entries)
        conditions = {"A": ConditionSpec(when="${param.env} == 'prod'")}
        result = execute_weave(
            _MOCK_SPARK,
            plan,
            threads,
            thread_conditions=conditions,
            params={"env": "prod"},
        )
        assert result.status == "success"
        mock_exec.assert_called_once()

    @patch("weevr.engine.runner.execute_thread")
    def test_param_condition_false(self, mock_exec):
        """Thread condition with param that doesn't match is skipped."""
        threads = {"A": _make_thread("A")}
        entries = [ThreadEntry(name="A")]
        plan = build_plan("test_weave", threads, entries)
        conditions = {"A": ConditionSpec(when="${param.env} == 'prod'")}
        result = execute_weave(
            _MOCK_SPARK,
            plan,
            threads,
            thread_conditions=conditions,
            params={"env": "dev"},
        )
        assert "A" in result.threads_skipped
        mock_exec.assert_not_called()


class TestConditionalWeaveExecution:
    """Test that weaves with conditions are evaluated in loom execution."""

    @patch("weevr.engine.runner.execute_weave")
    def test_weave_condition_false_skips(self, mock_weave_exec):
        """Weave with condition=False is skipped with a WeaveResult."""
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": [
                    {"name": "dims", "condition": {"when": "false"}},
                ],
            }
        )
        weaves = {
            "dims": Weave.model_validate({"config_version": "1.0", "threads": ["t1"]}),
        }
        thread_map = {"dims": {"t1": _make_thread("t1")}}
        result = execute_loom(_MOCK_SPARK, loom, weaves, thread_map)
        # Weave was skipped — result recorded with status="skipped"
        assert len(result.weave_results) == 1
        assert result.weave_results[0].status == "skipped"
        assert result.weave_results[0].weave_name == "dims"
        assert result.weave_results[0].skip_reason == "false"
        assert result.weave_results[0].thread_results == []
        assert result.weave_results[0].duration_ms == 0
        # Skipped weaves still count as success for loom
        assert result.status == "success"
        mock_weave_exec.assert_not_called()

    @patch("weevr.engine.runner.execute_weave")
    @patch("weevr.engine.runner.evaluate_condition")
    def test_weave_condition_true_executes(self, mock_eval, mock_weave_exec):
        """Weave with condition=True executes normally."""
        mock_eval.return_value = True
        mock_weave_exec.return_value = WeaveResult(
            status="success",
            weave_name="dims",
            thread_results=[],
            threads_skipped=[],
            duration_ms=100,
        )
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": [
                    {"name": "dims", "condition": {"when": "true"}},
                ],
            }
        )
        weaves = {
            "dims": Weave.model_validate({"config_version": "1.0", "threads": ["t1"]}),
        }
        thread_map = {"dims": {"t1": _make_thread("t1")}}
        result = execute_loom(_MOCK_SPARK, loom, weaves, thread_map)
        assert len(result.weave_results) == 1
        assert result.weave_results[0].status == "success"

    @patch("weevr.engine.runner.execute_weave")
    def test_weave_param_condition_skips(self, mock_weave_exec):
        """Weave with param condition that doesn't match is skipped."""
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": [
                    {"name": "dims", "condition": {"when": "${param.schedule} == 'daily'"}},
                ],
            }
        )
        weaves = {
            "dims": Weave.model_validate({"config_version": "1.0", "threads": ["t1"]}),
        }
        thread_map = {"dims": {"t1": _make_thread("t1")}}
        result = execute_loom(_MOCK_SPARK, loom, weaves, thread_map, params={"schedule": "monthly"})
        assert result.weave_results[0].status == "skipped"
        assert result.weave_results[0].skip_reason == "${param.schedule} == 'daily'"
        mock_weave_exec.assert_not_called()

    @patch("weevr.engine.runner.execute_weave")
    @patch("weevr.engine.runner.evaluate_condition")
    def test_weave_param_condition_executes(self, mock_eval, mock_weave_exec):
        """Weave with param condition that matches executes normally."""
        mock_eval.return_value = True
        mock_weave_exec.return_value = WeaveResult(
            status="success",
            weave_name="dims",
            thread_results=[],
            threads_skipped=[],
            duration_ms=100,
        )
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": [
                    {"name": "dims", "condition": {"when": "${param.schedule} == 'daily'"}},
                ],
            }
        )
        weaves = {
            "dims": Weave.model_validate({"config_version": "1.0", "threads": ["t1"]}),
        }
        thread_map = {"dims": {"t1": _make_thread("t1")}}
        result = execute_loom(_MOCK_SPARK, loom, weaves, thread_map, params={"schedule": "daily"})
        assert len(result.weave_results) == 1
        assert result.weave_results[0].status == "success"
        # Verify params were passed to evaluate_condition
        mock_eval.assert_called_once()
        call_kwargs = mock_eval.call_args
        assert call_kwargs.kwargs.get("params") == {"schedule": "daily"}

    @patch("weevr.engine.runner.execute_weave")
    def test_loom_params_passed_to_execute_weave(self, mock_weave_exec):
        """Loom params are forwarded to execute_weave for thread conditions."""
        mock_weave_exec.return_value = WeaveResult(
            status="success",
            weave_name="dims",
            thread_results=[],
            threads_skipped=[],
            duration_ms=100,
        )
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": ["dims"],
            }
        )
        weaves = {
            "dims": Weave.model_validate({"config_version": "1.0", "threads": ["t1"]}),
        }
        thread_map = {"dims": {"t1": _make_thread("t1")}}
        execute_loom(_MOCK_SPARK, loom, weaves, thread_map, params={"env": "prod"})
        # Verify params passed through to execute_weave
        call_kwargs = mock_weave_exec.call_args
        assert call_kwargs.kwargs.get("params") == {"env": "prod"}


# ---------------------------------------------------------------------------
# Hook and lookup integration tests
# ---------------------------------------------------------------------------


class TestWeaveHooksIntegration:
    """Test hook lifecycle in execute_weave."""

    @patch("weevr.engine.runner.execute_thread")
    def test_pre_steps_execute_before_threads(self, mock_exec):
        """Pre-steps run before threads."""
        mock_exec.return_value = _make_result("A")

        from weevr.model.hooks import LogMessageStep

        pre = [LogMessageStep(type="log_message", message="starting")]
        threads = {"A": _make_thread("A")}
        plan = build_plan("test_weave", threads, _entries("A"))

        with patch("weevr.engine.runner.run_hook_steps", return_value=[]) as mock_hooks:
            result = execute_weave(_MOCK_SPARK, plan, threads, pre_steps=pre)

        assert result.status == "success"
        mock_hooks.assert_called_once()
        call_args = mock_hooks.call_args
        assert call_args[0][2] == "pre"  # phase arg

    @patch("weevr.engine.runner.execute_thread")
    def test_post_steps_execute_after_threads(self, mock_exec):
        """Post-steps run after threads complete."""
        mock_exec.return_value = _make_result("A")

        from weevr.model.hooks import LogMessageStep

        post = [LogMessageStep(type="log_message", message="done")]
        threads = {"A": _make_thread("A")}
        plan = build_plan("test_weave", threads, _entries("A"))

        with patch("weevr.engine.runner.run_hook_steps", return_value=[]) as mock_hooks:
            execute_weave(_MOCK_SPARK, plan, threads, post_steps=post)

        mock_hooks.assert_called_once()
        call_args = mock_hooks.call_args
        assert call_args[0][2] == "post"

    @patch("weevr.engine.runner.execute_thread")
    def test_pre_abort_skips_threads(self, mock_exec):
        """HookError from pre-steps prevents thread execution."""
        from weevr.errors.exceptions import HookError
        from weevr.model.hooks import QualityGateStep

        pre = [
            QualityGateStep(
                type="quality_gate",
                check="table_exists",
                source="db.missing",
            )
        ]
        threads = {"A": _make_thread("A")}
        plan = build_plan("test_weave", threads, _entries("A"))

        import contextlib

        with (
            patch(
                "weevr.engine.runner.run_hook_steps",
                side_effect=HookError("gate failed"),
            ),
            contextlib.suppress(HookError),
        ):
            execute_weave(_MOCK_SPARK, plan, threads, pre_steps=pre)

        mock_exec.assert_not_called()

    @patch("weevr.engine.runner.execute_thread")
    def test_no_hooks_backward_compat(self, mock_exec):
        """execute_weave without hooks works as before."""
        mock_exec.return_value = _make_result("A")
        threads = {"A": _make_thread("A")}
        result = _run_weave(threads)
        assert result.status == "success"


class TestWeaveLookupIntegration:
    """Test lookup lifecycle in execute_weave."""

    @patch("weevr.engine.runner.execute_thread")
    @patch("weevr.engine.runner.materialize_lookups")
    def test_lookups_materialized(self, mock_mat, mock_exec):
        """Lookups are materialized before thread execution."""
        from weevr.engine.lookups import LookupResult
        from weevr.model.lookup import Lookup
        from weevr.model.source import Source

        mock_df = MagicMock()
        mock_mat.return_value = (
            {"ref": mock_df},
            [LookupResult(name="ref", materialized=True, row_count=10)],
        )
        mock_exec.return_value = _make_result("A")

        lookups = {
            "ref": Lookup(
                source=Source(type="delta", alias="db.ref"),
                materialize=True,
            )
        }
        threads = {"A": _make_thread("A")}
        plan = build_plan("test_weave", threads, _entries("A"))

        result = execute_weave(_MOCK_SPARK, plan, threads, lookups=lookups)

        assert result.status == "success"
        mock_mat.assert_called_once()

    @patch("weevr.engine.runner.execute_thread")
    @patch("weevr.engine.runner.cleanup_lookups")
    @patch("weevr.engine.runner.materialize_lookups")
    def test_lookups_cleaned_up(self, mock_mat, mock_cleanup, mock_exec):
        """Lookups are cleaned up after execution."""
        from weevr.engine.lookups import LookupResult
        from weevr.model.lookup import Lookup
        from weevr.model.source import Source

        mock_df = MagicMock()
        mock_mat.return_value = (
            {"ref": mock_df},
            [LookupResult(name="ref", materialized=True, row_count=5)],
        )
        mock_exec.return_value = _make_result("A")

        lookups = {
            "ref": Lookup(
                source=Source(type="delta", alias="db.ref"),
                materialize=True,
            )
        }
        threads = {"A": _make_thread("A")}
        plan = build_plan("test_weave", threads, _entries("A"))

        execute_weave(_MOCK_SPARK, plan, threads, lookups=lookups)

        mock_cleanup.assert_called_once_with({"ref": mock_df})


class TestDeferredLookupMaterialization:
    """Test schedule-aware lookup materialization in execute_weave."""

    @patch("weevr.engine.runner.execute_thread")
    @patch("weevr.engine.runner.materialize_lookups")
    def test_deferred_lookup_materialized_at_scheduled_group(self, mock_mat, mock_exec):
        """Internal lookup is materialized after its producer group, not upfront."""
        from weevr.engine.lookups import LookupResult
        from weevr.model.lookup import Lookup
        from weevr.model.source import Source

        # Track call ordering to verify materialization happens after producer
        call_order: list[str] = []

        mock_df = MagicMock()
        mock_mat.side_effect = lambda spark, lk_dict, **kwargs: (
            call_order.append(f"materialize:{','.join(lk_dict.keys())}"),
            (
                {n: mock_df for n in lk_dict},
                [LookupResult(name=n, materialized=True, row_count=10) for n in lk_dict],
            ),
        )[1]

        def exec_side_effect(spark, thread, **kwargs):
            call_order.append(f"execute:{thread.name}")
            return _make_result(thread.name)

        mock_exec.side_effect = exec_side_effect

        # A produces silver.dim_customer; lookup reads it; B consumes lookup
        lookups = {
            "cust": Lookup(
                source=Source(type="delta", alias="silver.dim_customer"),
                materialize=True,
            )
        }
        threads = {
            "A": _make_thread("A", target_alias="silver.dim_customer"),
            "B": Thread.model_validate(
                {
                    "name": "B",
                    "config_version": "1.0",
                    "sources": {"lk_cust": {"lookup": "cust"}},
                    "target": {"alias": "test"},
                }
            ),
        }
        plan = build_plan("test_weave", threads, _entries("A", "B"), lookups=lookups)
        assert plan.lookup_schedule is not None
        assert 1 in plan.lookup_schedule
        assert "cust" in plan.lookup_schedule[1]

        result = execute_weave(_MOCK_SPARK, plan, threads, lookups=lookups)
        assert result.status == "success"
        # materialize_lookups should be called once (deferred to after group 0)
        assert mock_mat.call_count == 1
        # The call should be for the "cust" lookup only
        call_lookups = mock_mat.call_args[0][1]
        assert "cust" in call_lookups
        # Producer must execute BEFORE the lookup is materialized
        assert call_order.index("execute:A") < call_order.index("materialize:cust")

    @patch("weevr.engine.runner.execute_thread")
    @patch("weevr.engine.runner.materialize_lookups")
    def test_no_schedule_falls_back_to_upfront(self, mock_mat, mock_exec):
        """Without lookup_schedule, all lookups are materialized upfront."""
        from weevr.engine.lookups import LookupResult
        from weevr.model.lookup import Lookup
        from weevr.model.source import Source

        mock_df = MagicMock()
        mock_mat.return_value = (
            {"ref": mock_df},
            [LookupResult(name="ref", materialized=True, row_count=5)],
        )
        mock_exec.return_value = _make_result("A")

        lookups = {
            "ref": Lookup(
                source=Source(type="delta", alias="ext.table"),
                materialize=True,
            )
        }
        threads = {"A": _make_thread("A")}
        # Build plan without lookups → no schedule
        plan = build_plan("test_weave", threads, _entries("A"))
        assert plan.lookup_schedule is None

        result = execute_weave(_MOCK_SPARK, plan, threads, lookups=lookups)
        assert result.status == "success"
        # Upfront materialization should be called once with all lookups
        mock_mat.assert_called_once()

    @patch("weevr.engine.runner.execute_thread")
    @patch("weevr.engine.runner.materialize_lookups")
    def test_external_lookup_materialized_at_group_0(self, mock_mat, mock_exec):
        """External lookup (no producer) is materialized before first group."""
        from weevr.engine.lookups import LookupResult
        from weevr.model.lookup import Lookup
        from weevr.model.source import Source

        mock_df = MagicMock()
        mock_mat.return_value = (
            {"ext": mock_df},
            [LookupResult(name="ext", materialized=True, row_count=3)],
        )
        mock_exec.return_value = _make_result("A")

        lookups = {
            "ext": Lookup(
                source=Source(type="delta", alias="external.codes"),
                materialize=True,
            )
        }
        threads = {
            "A": Thread.model_validate(
                {
                    "name": "A",
                    "config_version": "1.0",
                    "sources": {"lk_ext": {"lookup": "ext"}},
                    "target": {"alias": "test"},
                }
            )
        }
        plan = build_plan("test_weave", threads, _entries("A"), lookups=lookups)
        assert plan.lookup_schedule is not None
        assert 0 in plan.lookup_schedule

        result = execute_weave(_MOCK_SPARK, plan, threads, lookups=lookups)
        assert result.status == "success"
        mock_mat.assert_called_once()

    @patch("weevr.engine.runner.execute_thread")
    @patch("weevr.engine.runner.cleanup_lookups")
    @patch("weevr.engine.runner.materialize_lookups")
    def test_cleanup_handles_deferred_lookups(self, mock_mat, mock_cleanup, mock_exec):
        """Cleanup receives all cached lookups regardless of materialization timing."""
        from weevr.engine.lookups import LookupResult
        from weevr.model.lookup import Lookup
        from weevr.model.source import Source

        mock_df = MagicMock()
        mock_mat.return_value = (
            {"cust": mock_df},
            [LookupResult(name="cust", materialized=True, row_count=5)],
        )
        mock_exec.side_effect = lambda spark, thread, **kwargs: _make_result(thread.name)

        lookups = {
            "cust": Lookup(
                source=Source(type="delta", alias="silver.dim"),
                materialize=True,
            )
        }
        threads = {
            "A": _make_thread("A", target_alias="silver.dim"),
            "B": Thread.model_validate(
                {
                    "name": "B",
                    "config_version": "1.0",
                    "sources": {"lk_cust": {"lookup": "cust"}},
                    "target": {"alias": "test"},
                }
            ),
        }
        plan = build_plan("test_weave", threads, _entries("A", "B"), lookups=lookups)
        execute_weave(_MOCK_SPARK, plan, threads, lookups=lookups)

        mock_cleanup.assert_called_once()
        cleaned = mock_cleanup.call_args[0][0]
        assert "cust" in cleaned

    @patch("weevr.engine.runner.execute_thread")
    @patch("weevr.engine.runner.materialize_lookups")
    def test_mixed_lookups_materialized_at_different_groups(self, mock_mat, mock_exec):
        """External lookup at group 0, internal lookup deferred to after producer."""
        from weevr.engine.lookups import LookupResult
        from weevr.model.lookup import Lookup
        from weevr.model.source import Source

        call_log: list[dict] = []

        def mat_side_effect(spark, lk_dict, **kwargs):
            names = list(lk_dict.keys())
            call_log.append({"lookups": names})
            result_dfs = {n: MagicMock() for n in names}
            results = [LookupResult(name=n, materialized=True, row_count=1) for n in names]
            return result_dfs, results

        mock_mat.side_effect = mat_side_effect
        mock_exec.side_effect = lambda spark, thread, **kwargs: _make_result(thread.name)

        lookups = {
            "ext_codes": Lookup(
                source=Source(type="delta", alias="external.codes"),
                materialize=True,
            ),
            "cust": Lookup(
                source=Source(type="delta", alias="silver.dim"),
                materialize=True,
            ),
        }
        threads = {
            "A": _make_thread("A", target_alias="silver.dim"),
            "B": Thread.model_validate(
                {
                    "name": "B",
                    "config_version": "1.0",
                    "sources": {"lk_cust": {"lookup": "cust"}, "lk_ext": {"lookup": "ext_codes"}},
                    "target": {"alias": "test"},
                }
            ),
        }
        plan = build_plan("test_weave", threads, _entries("A", "B"), lookups=lookups)
        result = execute_weave(_MOCK_SPARK, plan, threads, lookups=lookups)

        assert result.status == "success"
        # Two separate calls: ext_codes at group 0, cust after group 0
        assert len(call_log) == 2
        assert "ext_codes" in call_log[0]["lookups"]
        assert "cust" in call_log[1]["lookups"]


class TestWeaveVariableIntegration:
    """Test variable context in execute_weave."""

    @patch("weevr.engine.runner.execute_thread")
    def test_variables_initialized(self, mock_exec):
        """VariableContext is initialized from variable specs."""
        mock_exec.return_value = _make_result("A")

        from weevr.model.variable import VariableSpec

        variables = {"batch": VariableSpec(type="string", default="B001")}
        threads = {"A": _make_thread("A")}
        plan = build_plan("test_weave", threads, _entries("A"))

        from weevr.telemetry.collector import SpanCollector
        from weevr.telemetry.span import generate_trace_id

        collector = SpanCollector(generate_trace_id())

        result = execute_weave(
            _MOCK_SPARK,
            plan,
            threads,
            collector=collector,
            variables=variables,
        )

        assert result.status == "success"
        assert result.telemetry is not None
        assert result.telemetry.variables == {"batch": "B001"}


# ---------------------------------------------------------------------------
# column_sets cascade tests (loom → weave inheritance)
# ---------------------------------------------------------------------------


class TestColumnSetsCascade:
    """Test that column_sets defined at loom level cascade to weaves."""

    @patch("weevr.engine.runner.execute_weave")
    def test_loom_column_sets_forwarded_when_weave_has_none(self, mock_exec):
        """Weave with no column_sets receives loom-level definitions."""
        from weevr.model.column_set import ColumnSet, ColumnSetSource

        mock_exec.return_value = WeaveResult(
            status="success",
            weave_name="w1",
            thread_results=[],
            threads_skipped=[],
            duration_ms=0,
        )

        loom_cs = ColumnSet(
            source=ColumnSetSource(type="delta", alias="gold.sap_map"),
        )
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": ["w1"],
                "column_sets": {"sap": loom_cs.model_dump()},
            }
        )
        weaves = {
            "w1": Weave.model_validate({"config_version": "1.0", "name": "w1", "threads": ["t1"]})
        }
        threads = {"w1": {"t1": _make_thread("t1")}}

        execute_loom(_MOCK_SPARK, loom, weaves, threads)

        call_kwargs = mock_exec.call_args.kwargs
        merged = call_kwargs.get("column_set_defs")
        assert merged is not None
        assert "sap" in merged
        assert merged["sap"] == loom_cs

    @patch("weevr.engine.runner.execute_weave")
    def test_weave_column_sets_override_loom(self, mock_exec):
        """Weave-level column_sets win over loom-level for the same key."""
        from weevr.model.column_set import ColumnSet, ColumnSetSource

        mock_exec.return_value = WeaveResult(
            status="success",
            weave_name="w1",
            thread_results=[],
            threads_skipped=[],
            duration_ms=0,
        )

        loom_cs = ColumnSet(
            source=ColumnSetSource(type="delta", alias="gold.loom_map"),
        )
        weave_cs = ColumnSet(
            source=ColumnSetSource(type="delta", alias="gold.weave_map"),
        )

        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": ["w1"],
                "column_sets": {"sap": loom_cs.model_dump()},
            }
        )
        weaves = {
            "w1": Weave.model_validate(
                {
                    "config_version": "1.0",
                    "name": "w1",
                    "threads": ["t1"],
                    "column_sets": {"sap": weave_cs.model_dump()},
                }
            )
        }
        threads = {"w1": {"t1": _make_thread("t1")}}

        execute_loom(_MOCK_SPARK, loom, weaves, threads)

        call_kwargs = mock_exec.call_args.kwargs
        merged = call_kwargs.get("column_set_defs")
        assert merged is not None
        assert "sap" in merged
        # Weave definition wins
        assert merged["sap"] == weave_cs

    @patch("weevr.engine.runner.execute_weave")
    def test_loom_and_weave_column_sets_merged(self, mock_exec):
        """Loom and weave column_sets with different keys are merged together."""
        from weevr.model.column_set import ColumnSet, ColumnSetSource

        mock_exec.return_value = WeaveResult(
            status="success",
            weave_name="w1",
            thread_results=[],
            threads_skipped=[],
            duration_ms=0,
        )

        loom_cs = ColumnSet(
            source=ColumnSetSource(type="delta", alias="gold.sap_map"),
        )
        weave_cs = ColumnSet(
            source=ColumnSetSource(type="delta", alias="gold.erp_map"),
        )

        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": ["w1"],
                "column_sets": {"sap": loom_cs.model_dump()},
            }
        )
        weaves = {
            "w1": Weave.model_validate(
                {
                    "config_version": "1.0",
                    "name": "w1",
                    "threads": ["t1"],
                    "column_sets": {"erp": weave_cs.model_dump()},
                }
            )
        }
        threads = {"w1": {"t1": _make_thread("t1")}}

        execute_loom(_MOCK_SPARK, loom, weaves, threads)

        call_kwargs = mock_exec.call_args.kwargs
        merged = call_kwargs.get("column_set_defs")
        assert merged is not None
        assert "sap" in merged
        assert "erp" in merged
        assert merged["sap"] == loom_cs
        assert merged["erp"] == weave_cs

    @patch("weevr.engine.runner.execute_weave")
    def test_no_column_sets_passes_none(self, mock_exec):
        """When neither loom nor weave define column_sets, None is forwarded."""

        mock_exec.return_value = WeaveResult(
            status="success",
            weave_name="w1",
            thread_results=[],
            threads_skipped=[],
            duration_ms=0,
        )

        loom = Loom.model_validate({"config_version": "1.0", "weaves": ["w1"]})
        weaves = {
            "w1": Weave.model_validate({"config_version": "1.0", "name": "w1", "threads": ["t1"]})
        }
        threads = {"w1": {"t1": _make_thread("t1")}}

        execute_loom(_MOCK_SPARK, loom, weaves, threads)

        call_kwargs = mock_exec.call_args.kwargs
        assert call_kwargs.get("column_set_defs") is None


# ---------------------------------------------------------------------------
# Lifecycle order tests
# ---------------------------------------------------------------------------


class TestWeaveLifecycleOrder:
    """Verify that execute_weave runs lifecycle phases in the correct order.

    Lifecycle phases in order: variables → pre_steps → lookups → column_sets →
    threads → post_steps → cleanup.
    """

    @patch("weevr.engine.runner.execute_thread")
    @patch("weevr.engine.runner.materialize_column_sets")
    @patch("weevr.engine.runner.materialize_lookups")
    @patch("weevr.engine.runner.run_hook_steps")
    def test_pre_steps_run_before_lookups_and_column_sets(
        self, mock_hooks, mock_mat_lookups, mock_mat_cs, mock_exec
    ):
        """Pre-steps must execute before lookups and column sets are materialized."""
        from weevr.engine.lookups import LookupResult
        from weevr.model.column_set import ColumnSet, ColumnSetSource
        from weevr.model.hooks import LogMessageStep
        from weevr.model.lookup import Lookup
        from weevr.model.source import Source

        call_order: list[str] = []

        mock_hooks.side_effect = lambda spark, steps, phase, *a, **kw: (
            call_order.append(f"hooks:{phase}"),
            [],
        )[1]

        mock_df = MagicMock()
        mock_mat_lookups.side_effect = lambda spark, lk_dict, **kw: (
            call_order.append("materialize_lookups"),
            (
                {n: mock_df for n in lk_dict},
                [LookupResult(name=n, materialized=True, row_count=1) for n in lk_dict],
            ),
        )[1]

        mock_mat_cs.side_effect = lambda spark, cs_defs, params, **kw: (
            call_order.append("materialize_column_sets"),
            ({}, []),
        )[1]

        mock_exec.side_effect = lambda spark, thread, **kw: (
            call_order.append(f"execute:{thread.name}"),
            _make_result(thread.name),
        )[1]

        pre = [LogMessageStep(type="log_message", message="pre")]
        post = [LogMessageStep(type="log_message", message="post")]
        lookups = {"ref": Lookup(source=Source(type="delta", alias="db.ref"), materialize=True)}
        col_set = ColumnSet(source=ColumnSetSource(type="delta", alias="gold.map"))
        column_set_defs = {"cs": col_set}

        threads = {"A": _make_thread("A")}
        plan = build_plan("test_weave", threads, _entries("A"))

        result = execute_weave(
            _MOCK_SPARK,
            plan,
            threads,
            pre_steps=pre,
            post_steps=post,
            lookups=lookups,
            column_set_defs=column_set_defs,
        )

        assert result.status == "success"

        # pre_steps must come before lookups and column_sets
        pre_idx = call_order.index("hooks:pre")
        lookups_idx = call_order.index("materialize_lookups")
        cs_idx = call_order.index("materialize_column_sets")
        exec_idx = call_order.index("execute:A")
        post_idx = call_order.index("hooks:post")

        assert pre_idx < lookups_idx, (
            f"pre_steps ({pre_idx}) must run before lookups ({lookups_idx})"
        )
        assert pre_idx < cs_idx, f"pre_steps ({pre_idx}) must run before column_sets ({cs_idx})"
        assert lookups_idx < exec_idx, (
            f"lookups ({lookups_idx}) must be materialized before thread execution ({exec_idx})"
        )
        assert cs_idx < exec_idx, (
            f"column_sets ({cs_idx}) must be materialized before thread execution ({exec_idx})"
        )
        assert exec_idx < post_idx, f"threads ({exec_idx}) must run before post_steps ({post_idx})"

    @patch("weevr.engine.runner.execute_thread")
    def test_params_forwarded_to_execute_thread(self, mock_exec):
        """Weave-level params are forwarded as resolved_params to execute_thread."""
        mock_exec.side_effect = lambda spark, thread, **kw: _make_result(thread.name)

        threads = {"A": _make_thread("A")}
        plan = build_plan("test_weave", threads, _entries("A"))
        runtime_params = {"env": "prod", "pk_columns": ["id", "ts"]}

        execute_weave(
            _MOCK_SPARK,
            plan,
            threads,
            params=runtime_params,
        )

        call_kwargs = mock_exec.call_args[1]
        assert call_kwargs["resolved_params"] == runtime_params


# ---------------------------------------------------------------------------
# Loom resource lifecycle tests
# ---------------------------------------------------------------------------


class TestLoomResourceLifecycle:
    """Test loom-level resource lifecycle: variables, pre/post_steps, lookups.

    These tests mock execute_weave (and supporting functions) at the runner
    module level, verifying that execute_loom orchestrates the lifecycle in
    the correct order and passes merged resources to each weave.
    """

    def _make_loom(self, **overrides):
        """Build a minimal Loom with optional resource overrides."""
        data = {"config_version": "1.0", "weaves": ["w1"], **overrides}
        return Loom.model_validate(data)

    def _make_weave(self, name="w1", **overrides):
        data = {"config_version": "1.0", "name": name, "threads": ["t1"], **overrides}
        return Weave.model_validate(data)

    def _weave_result(self, name="w1"):
        return WeaveResult(
            status="success",
            weave_name=name,
            thread_results=[],
            threads_skipped=[],
            duration_ms=0,
        )

    @patch("weevr.engine.runner.execute_weave")
    @patch("weevr.engine.runner.run_hook_steps", return_value=[])
    def test_loom_pre_steps_run_before_weaves(self, mock_hooks, mock_weave_exec):
        """Loom pre_steps execute before any weave runs."""
        from weevr.model.hooks import LogMessageStep

        mock_weave_exec.return_value = self._weave_result()

        call_order: list[str] = []
        mock_hooks.side_effect = lambda spark, steps, phase, *a, **kw: (
            call_order.append(f"hooks:{phase}"),
            [],
        )[1]
        mock_weave_exec.side_effect = lambda *a, **kw: (
            call_order.append("execute_weave"),
            self._weave_result(),
        )[1]

        pre = [LogMessageStep(type="log_message", message="loom start")]
        loom = self._make_loom(pre_steps=[s.model_dump() for s in pre])
        weaves = {"w1": self._make_weave()}
        threads = {"w1": {"t1": _make_thread("t1")}}

        result = execute_loom(_MOCK_SPARK, loom, weaves, threads)

        assert result.status == "success"
        assert "hooks:pre" in call_order
        assert "execute_weave" in call_order
        pre_idx = call_order.index("hooks:pre")
        weave_idx = call_order.index("execute_weave")
        assert pre_idx < weave_idx, "pre_steps must run before weave execution"

    @patch("weevr.engine.runner.execute_weave")
    @patch("weevr.engine.runner.run_hook_steps", return_value=[])
    def test_loom_post_steps_run_after_weaves(self, mock_hooks, mock_weave_exec):
        """Loom post_steps execute after all weaves complete."""
        from weevr.model.hooks import LogMessageStep

        call_order: list[str] = []
        mock_hooks.side_effect = lambda spark, steps, phase, *a, **kw: (
            call_order.append(f"hooks:{phase}"),
            [],
        )[1]
        mock_weave_exec.side_effect = lambda *a, **kw: (
            call_order.append("execute_weave"),
            self._weave_result(),
        )[1]

        post = [LogMessageStep(type="log_message", message="loom done")]
        loom = self._make_loom(post_steps=[s.model_dump() for s in post])
        weaves = {"w1": self._make_weave()}
        threads = {"w1": {"t1": _make_thread("t1")}}

        result = execute_loom(_MOCK_SPARK, loom, weaves, threads)

        assert result.status == "success"
        weave_idx = call_order.index("execute_weave")
        post_idx = call_order.index("hooks:post")
        assert weave_idx < post_idx, "post_steps must run after weave execution"

    @patch("weevr.engine.runner.execute_weave")
    @patch("weevr.engine.runner.cleanup_lookups")
    @patch("weevr.engine.runner.materialize_lookups")
    def test_loom_lookups_materialize_and_cleanup(self, mock_mat, mock_cleanup, mock_weave_exec):
        """Loom lookups are materialized before weaves and cleaned up after."""
        from weevr.engine.lookups import LookupResult
        from weevr.model.lookup import Lookup
        from weevr.model.source import Source

        mock_df = MagicMock()
        mock_mat.return_value = (
            {"ref": mock_df},
            [LookupResult(name="ref", materialized=True, row_count=10)],
        )
        mock_weave_exec.return_value = self._weave_result()

        loom_lookup = Lookup(
            source=Source(type="delta", alias="gold.ref"),
            materialize=True,
        )
        loom = self._make_loom(lookups={"ref": loom_lookup.model_dump()})
        weaves = {"w1": self._make_weave()}
        threads = {"w1": {"t1": _make_thread("t1")}}

        execute_loom(_MOCK_SPARK, loom, weaves, threads)

        # Loom lookups materialized
        mock_mat.assert_called_once()
        mat_lookups = mock_mat.call_args[0][1]
        assert "ref" in mat_lookups

        # Cleanup called with the cached DFs
        mock_cleanup.assert_called_once()
        cleaned = mock_cleanup.call_args[0][0]
        assert "ref" in cleaned

    @patch("weevr.engine.runner.execute_weave")
    @patch("weevr.engine.runner.materialize_lookups")
    def test_loom_lookups_passed_as_pre_cached(self, mock_mat, mock_weave_exec):
        """Loom cached lookup DFs are passed to execute_weave as pre_cached_lookups."""
        from weevr.engine.lookups import LookupResult
        from weevr.model.lookup import Lookup
        from weevr.model.source import Source

        mock_df = MagicMock()
        mock_mat.return_value = (
            {"ref": mock_df},
            [LookupResult(name="ref", materialized=True, row_count=5)],
        )
        mock_weave_exec.return_value = self._weave_result()

        loom_lookup = Lookup(
            source=Source(type="delta", alias="gold.ref"),
            materialize=True,
        )
        loom = self._make_loom(lookups={"ref": loom_lookup.model_dump()})
        weaves = {"w1": self._make_weave()}
        threads = {"w1": {"t1": _make_thread("t1")}}

        execute_loom(_MOCK_SPARK, loom, weaves, threads)

        call_kwargs = mock_weave_exec.call_args.kwargs
        pre_cached = call_kwargs.get("pre_cached_lookups")
        assert pre_cached is not None
        assert "ref" in pre_cached
        assert pre_cached["ref"] is mock_df

    @patch("weevr.engine.runner.execute_weave")
    def test_loom_variables_cascade_into_weave(self, mock_weave_exec):
        """Loom variables merge with weave variables; weave wins on conflict."""
        mock_weave_exec.return_value = self._weave_result()

        loom = self._make_loom(
            variables={
                "batch": {"type": "string", "default": "L001"},
                "env": {"type": "string", "default": "prod"},
            }
        )
        weaves = {
            "w1": self._make_weave(
                variables={
                    "batch": {"type": "string", "default": "W001"},
                }
            ),
        }
        threads = {"w1": {"t1": _make_thread("t1")}}

        execute_loom(_MOCK_SPARK, loom, weaves, threads)

        call_kwargs = mock_weave_exec.call_args.kwargs
        merged_vars = call_kwargs.get("variables")
        assert merged_vars is not None
        # Weave wins on "batch"
        assert merged_vars["batch"].default == "W001"
        # Loom "env" is inherited
        assert "env" in merged_vars
        assert merged_vars["env"].default == "prod"

    @patch("weevr.engine.runner.execute_weave")
    @patch("weevr.engine.runner.cleanup_lookups")
    @patch("weevr.engine.runner.materialize_lookups")
    def test_loom_cleanup_runs_on_failure(self, mock_mat, mock_cleanup, mock_weave_exec):
        """Loom lookup cleanup runs even when a weave fails."""
        from weevr.engine.lookups import LookupResult
        from weevr.model.lookup import Lookup
        from weevr.model.source import Source

        mock_df = MagicMock()
        mock_mat.return_value = (
            {"ref": mock_df},
            [LookupResult(name="ref", materialized=True, row_count=5)],
        )
        mock_weave_exec.return_value = WeaveResult(
            status="failure",
            weave_name="w1",
            thread_results=[],
            threads_skipped=[],
            duration_ms=0,
        )

        loom_lookup = Lookup(
            source=Source(type="delta", alias="gold.ref"),
            materialize=True,
        )
        loom = self._make_loom(lookups={"ref": loom_lookup.model_dump()})
        weaves = {"w1": self._make_weave()}
        threads = {"w1": {"t1": _make_thread("t1")}}

        result = execute_loom(_MOCK_SPARK, loom, weaves, threads)

        assert result.status == "failure"
        # Cleanup must still be called
        mock_cleanup.assert_called_once()
        cleaned = mock_cleanup.call_args[0][0]
        assert "ref" in cleaned

    @patch("weevr.engine.runner.execute_weave")
    def test_loom_column_sets_use_merge_helper(self, mock_weave_exec):
        """Column sets now use merge_resource_dicts helper for loom/weave merge."""
        from weevr.model.column_set import ColumnSet, ColumnSetSource

        mock_weave_exec.return_value = self._weave_result()

        loom_cs = ColumnSet(
            source=ColumnSetSource(type="delta", alias="gold.loom_map"),
        )
        weave_cs = ColumnSet(
            source=ColumnSetSource(type="delta", alias="gold.weave_map"),
        )

        loom = self._make_loom(column_sets={"shared": loom_cs.model_dump()})
        weaves = {
            "w1": self._make_weave(
                column_sets={"shared": weave_cs.model_dump(), "extra": weave_cs.model_dump()}
            ),
        }
        threads = {"w1": {"t1": _make_thread("t1")}}

        execute_loom(_MOCK_SPARK, loom, weaves, threads)

        call_kwargs = mock_weave_exec.call_args.kwargs
        merged = call_kwargs.get("column_set_defs")
        assert merged is not None
        # Weave wins on conflict
        assert merged["shared"] == weave_cs
        # Weave-only key present
        assert "extra" in merged

    @patch("weevr.engine.runner.execute_weave")
    @patch("weevr.engine.runner.run_hook_steps", return_value=[])
    def test_loom_post_steps_run_after_weave_failure(self, mock_hooks, mock_weave_exec):
        """Loom post_steps execute even when a weave fails."""
        from weevr.model.hooks import LogMessageStep

        call_order: list[str] = []
        mock_hooks.side_effect = lambda spark, steps, phase, *a, **kw: (
            call_order.append(f"hooks:{phase}"),
            [],
        )[1]
        mock_weave_exec.side_effect = lambda *a, **kw: (
            call_order.append("execute_weave"),
            WeaveResult(
                status="failure",
                weave_name="w1",
                thread_results=[],
                threads_skipped=[],
                duration_ms=0,
            ),
        )[1]

        post = [LogMessageStep(type="log_message", message="loom done")]
        loom = self._make_loom(post_steps=[s.model_dump() for s in post])
        weaves = {"w1": self._make_weave()}
        threads = {"w1": {"t1": _make_thread("t1")}}

        result = execute_loom(_MOCK_SPARK, loom, weaves, threads)

        assert result.status == "failure"
        assert "hooks:post" in call_order, "post_steps must run even after weave failure"
        weave_idx = call_order.index("execute_weave")
        post_idx = call_order.index("hooks:post")
        assert weave_idx < post_idx
