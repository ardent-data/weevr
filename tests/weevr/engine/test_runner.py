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

    target: dict[str, Any] = {}
    if target_alias:
        target["alias"] = target_alias

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
        mock_exec.side_effect = lambda spark, thread: _make_result(thread.name)
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

        def side_effect(spark, thread):
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

        def side_effect(spark, thread):
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

    @patch("weevr.engine.runner.execute_thread")
    def test_skip_downstream_skips_only_dependents(self, mock_exec):
        """on_failure=skip_downstream: A fails → B (depends on A) skipped, C continues."""

        def side_effect(spark, thread):
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
        """on_failure=continue: dependents skipped, independents continue (DEC-007)."""

        def side_effect(spark, thread):
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
        mock_exec.side_effect = lambda spark, thread: _make_result(thread.name)
        threads = {"A": _make_thread("A"), "B": _make_thread("B")}
        result = _run_weave(threads)
        assert result.status == "success"

    @patch("weevr.engine.runner.execute_thread")
    def test_all_threads_fail_abort_weave_status_failure(self, mock_exec):
        """All threads fail → status='failure'."""

        def side_effect(spark, thread):
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

        def side_effect(spark, thread):
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

        def side_effect(spark, thread):
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

        def side_effect(spark, thread):
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
        """Weave with condition=False is skipped entirely."""
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
        # Weave was skipped, so no weave results
        assert len(result.weave_results) == 0
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
