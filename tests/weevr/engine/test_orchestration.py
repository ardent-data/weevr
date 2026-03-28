"""Integration tests for DAG orchestration — weave and loom execution with real Spark/Delta.

Covers exit criteria for DAG orchestration design.
"""

import pytest
from pyspark.sql import SparkSession

from spark_helpers import create_delta_table
from weevr.engine import build_plan, execute_loom, execute_weave
from weevr.errors.exceptions import ConfigError
from weevr.model.loom import Loom
from weevr.model.thread import Thread
from weevr.model.weave import ThreadEntry, Weave

pytestmark = pytest.mark.spark


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _thread(
    name: str,
    source_path: str,
    target_path: str,
    *,
    on_failure: str | None = None,
    cache: bool | None = None,
) -> Thread:
    """Construct a minimal Thread for orchestration integration testing."""
    data: dict = {
        "name": name,
        "config_version": "1",
        "sources": {"main": {"type": "delta", "alias": source_path}},
        "target": {"path": target_path},
    }
    if on_failure is not None:
        data["failure"] = {"on_failure": on_failure}
    if cache is not None:
        data["cache"] = cache
    return Thread.model_validate(data)


def _thread_using_alias(name: str, source_alias: str, target_alias: str) -> Thread:
    """Construct a Thread that uses alias for both source and target (for DAG path matching)."""
    return Thread.model_validate(
        {
            "name": name,
            "config_version": "1",
            "sources": {"main": {"type": "delta", "alias": source_alias}},
            "target": {"alias": target_alias},
        }
    )


def _entries(*names: str) -> list[ThreadEntry]:
    return [ThreadEntry(name=n) for n in names]


def _run_weave_directly(
    spark: SparkSession,
    threads: dict[str, Thread],
    weave_name: str = "test_weave",
):
    """Build a plan and execute the weave."""
    plan = build_plan(weave_name, threads, _entries(*threads.keys()))
    return plan, execute_weave(spark, plan, threads)


# ---------------------------------------------------------------------------
# 3-thread weave with dependency chain and independent thread
# ---------------------------------------------------------------------------


class TestEC001DependencyChain:
    def test_three_thread_weave_dependency_chain_and_parallel(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """A writes table_x; B reads table_x (depends on A); C is independent.
        B must run after A; A and C can run in parallel.
        All three should succeed.
        """
        src_a = tmp_delta_path("ec001_src_a")
        src_c = tmp_delta_path("ec001_src_c")
        tgt_a = tmp_delta_path("ec001_tgt_a")
        tgt_b = tmp_delta_path("ec001_tgt_b")
        tgt_c = tmp_delta_path("ec001_tgt_c")

        create_delta_table(spark, src_a, [{"id": 1, "val": "from_a"}])
        create_delta_table(spark, src_c, [{"id": 3, "val": "from_c"}])

        # A writes to tgt_a; B reads from tgt_a; C is independent
        threads = {
            "A": _thread("A", src_a, tgt_a),
            "B": _thread("B", tgt_a, tgt_b),  # reads A's output → inferred dep on A
            "C": _thread("C", src_c, tgt_c),
        }
        plan, result = _run_weave_directly(spark, threads)

        # Verify dependency inference
        assert "A" in plan.dependencies["B"]
        assert plan.dependencies["C"] == []

        # Verify execution
        assert result.status == "success"
        assert result.threads_skipped == []
        assert len(result.thread_results) == 3
        assert all(r.status == "success" for r in result.thread_results)

        # Verify data written
        assert spark.read.format("delta").load(tgt_a).count() == 1
        assert spark.read.format("delta").load(tgt_b).count() == 1
        assert spark.read.format("delta").load(tgt_c).count() == 1


# ---------------------------------------------------------------------------
# Circular dependency detection
# ---------------------------------------------------------------------------


class TestEC002CircularDependency:
    def test_circular_dependency_raises_at_plan_time(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Circular dependency detected and reported at plan construction time."""
        tgt_a = tmp_delta_path("ec002_tgt_a")
        tgt_b = tmp_delta_path("ec002_tgt_b")

        # A's target alias = B's source alias and vice versa → cycle
        threads = {
            "A": _thread_using_alias("A", source_alias=tgt_b, target_alias=tgt_a),
            "B": _thread_using_alias("B", source_alias=tgt_a, target_alias=tgt_b),
        }
        with pytest.raises(ConfigError, match="Circular dependency"):
            build_plan("test_weave", threads, _entries("A", "B"))


# ---------------------------------------------------------------------------
# Loom with 2+ weaves executes sequentially
# ---------------------------------------------------------------------------


class TestEC003LoomSequential:
    def test_loom_two_weaves_run_sequentially(self, spark: SparkSession, tmp_delta_path) -> None:
        """Loom executes weave1 then weave2; LoomResult contains both WeaveResults."""
        src1 = tmp_delta_path("ec003_src1")
        tgt1 = tmp_delta_path("ec003_tgt1")
        src2 = tmp_delta_path("ec003_src2")
        tgt2 = tmp_delta_path("ec003_tgt2")

        create_delta_table(spark, src1, [{"id": 1}])
        create_delta_table(spark, src2, [{"id": 2}])

        loom = Loom.model_validate(
            {"name": "nightly", "config_version": "1", "weaves": ["weave1", "weave2"]}
        )
        weaves = {
            "weave1": Weave.model_validate(
                {"config_version": "1", "name": "weave1", "threads": ["t1"]}
            ),
            "weave2": Weave.model_validate(
                {"config_version": "1", "name": "weave2", "threads": ["t2"]}
            ),
        }
        threads = {
            "weave1": {"t1": _thread("t1", src1, tgt1)},
            "weave2": {"t2": _thread("t2", src2, tgt2)},
        }

        result = execute_loom(spark, loom, weaves, threads)

        assert result.status == "success"
        assert result.loom_name == "nightly"
        assert len(result.weave_results) == 2
        assert result.weave_results[0].weave_name == "weave1"
        assert result.weave_results[1].weave_name == "weave2"
        assert result.weave_results[0].status == "success"
        assert result.weave_results[1].status == "success"


# ---------------------------------------------------------------------------
# on_failure=abort_weave
# ---------------------------------------------------------------------------


class TestEC004AbortWeave:
    def test_abort_weave_stops_remaining_threads(self, spark: SparkSession, tmp_delta_path) -> None:
        """Thread A fails with abort_weave → B and C (both depend on A) are aborted."""
        tgt_a = tmp_delta_path("ec004_tgt_a")
        tgt_b = tmp_delta_path("ec004_tgt_b")
        tgt_c = tmp_delta_path("ec004_tgt_c")

        # A reads from a non-existent path → will fail
        threads = {
            "A": _thread("A", "/nonexistent/ec004", tgt_a, on_failure="abort_weave"),
            "B": _thread("B", tgt_a, tgt_b),  # depends on A
            "C": _thread("C", tgt_a, tgt_c),  # also depends on A (at level 1)
        }
        _, result = _run_weave_directly(spark, threads)

        assert result.status == "failure"
        assert set(result.threads_skipped) >= {"B", "C"}
        # A failed, B and C were skipped — no threads succeeded
        succeeded = [r.thread_name for r in result.thread_results if r.status == "success"]
        assert succeeded == []


# ---------------------------------------------------------------------------
# on_failure=skip_downstream
# ---------------------------------------------------------------------------


class TestEC005SkipDownstream:
    def test_skip_downstream_skips_only_dependents(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Thread A fails; B (depends on A) is skipped; C (independent) succeeds."""
        src_c = tmp_delta_path("ec005_src_c")
        tgt_a = tmp_delta_path("ec005_tgt_a")
        tgt_b = tmp_delta_path("ec005_tgt_b")
        tgt_c = tmp_delta_path("ec005_tgt_c")

        create_delta_table(spark, src_c, [{"id": 3}])

        threads = {
            "A": _thread("A", "/nonexistent/ec005", tgt_a, on_failure="skip_downstream"),
            "B": _thread("B", tgt_a, tgt_b),  # depends on A — at level 1
            "C": _thread("C", src_c, tgt_c),  # independent — at level 0 with A
        }
        _, result = _run_weave_directly(spark, threads)

        assert result.status == "partial"
        assert "B" in result.threads_skipped
        assert "C" not in result.threads_skipped
        succeeded = [r.thread_name for r in result.thread_results if r.status == "success"]
        assert "C" in succeeded


# ---------------------------------------------------------------------------
# on_failure=continue (same observable behaviour as skip_downstream)
# ---------------------------------------------------------------------------


class TestEC006Continue:
    def test_continue_skips_dependents_continues_independents(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """on_failure=continue — B (dependent) skipped, C (independent) continues."""
        src_c = tmp_delta_path("ec006_src_c")
        tgt_a = tmp_delta_path("ec006_tgt_a")
        tgt_b = tmp_delta_path("ec006_tgt_b")
        tgt_c = tmp_delta_path("ec006_tgt_c")

        create_delta_table(spark, src_c, [{"id": 3}])

        threads = {
            "A": _thread("A", "/nonexistent/ec006", tgt_a, on_failure="continue"),
            "B": _thread("B", tgt_a, tgt_b),  # depends on A
            "C": _thread("C", src_c, tgt_c),  # independent
        }
        _, result = _run_weave_directly(spark, threads)

        assert result.status == "partial"
        assert "B" in result.threads_skipped
        succeeded = [r.thread_name for r in result.thread_results if r.status == "success"]
        assert "C" in succeeded


# ---------------------------------------------------------------------------
# Auto-cache — DataFrame consumed by 2+ threads is cached
# ---------------------------------------------------------------------------


class TestEC007AutoCache:
    def test_multi_consumer_thread_marked_as_cache_target(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Thread A's output is read by both B and C → A is in cache_targets.
        Correctness is verified by successful row counts in B and C's targets.
        """
        src_a = tmp_delta_path("ec007_src_a")
        tgt_a = tmp_delta_path("ec007_tgt_a")
        tgt_b = tmp_delta_path("ec007_tgt_b")
        tgt_c = tmp_delta_path("ec007_tgt_c")

        create_delta_table(spark, src_a, [{"id": 1}, {"id": 2}])

        threads = {
            "A": _thread("A", src_a, tgt_a),
            "B": _thread("B", tgt_a, tgt_b),  # reads A's output
            "C": _thread("C", tgt_a, tgt_c),  # reads A's output
        }
        plan, result = _run_weave_directly(spark, threads)

        # A has 2 consumers — should be a cache target
        assert "A" in plan.cache_targets

        # All threads succeed and data is correct
        assert result.status == "success"
        assert spark.read.format("delta").load(tgt_b).count() == 2
        assert spark.read.format("delta").load(tgt_c).count() == 2


# ---------------------------------------------------------------------------
# Thread with cache=False suppresses auto-caching
# ---------------------------------------------------------------------------


class TestEC008CacheOverride:
    def test_cache_false_excludes_thread_from_cache_targets(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Thread A with cache=False is not in cache_targets even with 2 consumers."""
        src_a = tmp_delta_path("ec008_src_a")
        tgt_a = tmp_delta_path("ec008_tgt_a")
        tgt_b = tmp_delta_path("ec008_tgt_b")
        tgt_c = tmp_delta_path("ec008_tgt_c")

        create_delta_table(spark, src_a, [{"id": 1}])

        threads = {
            "A": _thread("A", src_a, tgt_a, cache=False),
            "B": _thread("B", tgt_a, tgt_b),
            "C": _thread("C", tgt_a, tgt_c),
        }
        plan, result = _run_weave_directly(spark, threads)

        # cache=False suppresses auto-caching even with 2 consumers
        assert "A" not in plan.cache_targets
        # Data correctness is unaffected — consumers read from Delta directly
        assert result.status == "success"


# ---------------------------------------------------------------------------
# ExecutionPlan is inspectable
# ---------------------------------------------------------------------------


class TestEC009PlanInspectability:
    def test_execution_plan_exposes_full_metadata(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """ExecutionPlan exposes dependency graph, parallel groups, and cache decisions."""
        src_a = tmp_delta_path("ec009_src_a")
        tgt_a = tmp_delta_path("ec009_tgt_a")
        tgt_b = tmp_delta_path("ec009_tgt_b")
        tgt_c = tmp_delta_path("ec009_tgt_c")

        create_delta_table(spark, src_a, [{"id": 1}])

        threads = {
            "A": _thread("A", src_a, tgt_a),
            "B": _thread("B", tgt_a, tgt_b),
            "C": _thread("C", tgt_a, tgt_c),
        }
        plan = build_plan("ec009", threads, _entries(*threads.keys()))

        # Dependency graph
        assert "A" in plan.dependencies["B"]
        assert "A" in plan.dependencies["C"]
        assert plan.dependencies["A"] == []

        # Dependents graph (reverse)
        assert "B" in plan.dependents["A"]
        assert "C" in plan.dependents["A"]

        # Parallel groups — A first, then B and C together
        assert plan.execution_order[0] == ["A"]
        assert sorted(plan.execution_order[1]) == ["B", "C"]

        # Cache targets — A has 2 consumers
        assert "A" in plan.cache_targets

        # Inferred vs explicit split
        assert "A" in plan.inferred_dependencies["B"]
        assert "A" in plan.inferred_dependencies["C"]


# ---------------------------------------------------------------------------
# WeaveResult.status="partial" on mixed success/failure
# ---------------------------------------------------------------------------


class TestEC010PartialStatus:
    def test_partial_status_when_some_succeed_some_fail(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """A fails (skip_downstream), B skipped, C succeeds → status='partial'."""
        src_c = tmp_delta_path("ec010_src_c")
        tgt_a = tmp_delta_path("ec010_tgt_a")
        tgt_b = tmp_delta_path("ec010_tgt_b")
        tgt_c = tmp_delta_path("ec010_tgt_c")

        create_delta_table(spark, src_c, [{"id": 99}])

        threads = {
            "A": _thread("A", "/nonexistent/ec010", tgt_a, on_failure="skip_downstream"),
            "B": _thread("B", tgt_a, tgt_b),  # depends on A
            "C": _thread("C", src_c, tgt_c),  # independent
        }
        _, result = _run_weave_directly(spark, threads)

        assert result.status == "partial"
        failed = [r for r in result.thread_results if r.status == "failure"]
        succeeded = [r for r in result.thread_results if r.status == "success"]
        assert any(r.thread_name == "A" for r in failed)
        assert any(r.thread_name == "C" for r in succeeded)
        assert "B" in result.threads_skipped
        assert result.duration_ms >= 0
