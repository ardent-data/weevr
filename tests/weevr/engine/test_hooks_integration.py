"""End-to-end integration tests for execution hooks lifecycle.

Tests the full hook → thread → hook lifecycle through execute_weave,
verifying that quality gates, variables, and lookups work together
with real Spark operations.
"""

from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession

from spark_helpers import create_delta_table
from weevr.engine.planner import build_plan
from weevr.engine.result import ThreadResult
from weevr.engine.runner import execute_weave
from weevr.errors.exceptions import HookError
from weevr.model.hooks import LogMessageStep, QualityGateStep, SqlStatementStep
from weevr.model.lookup import Lookup
from weevr.model.source import Source
from weevr.model.target import Target
from weevr.model.thread import Thread
from weevr.model.variable import VariableSpec
from weevr.model.weave import ThreadEntry
from weevr.telemetry.collector import SpanCollector
from weevr.telemetry.span import generate_trace_id

pytestmark = pytest.mark.spark


def _make_result(name: str) -> ThreadResult:
    return ThreadResult(
        status="success",
        thread_name=name,
        rows_written=10,
        write_mode="overwrite",
        target_path=f"/data/{name}",
    )


def _make_thread_model(name: str) -> Thread:
    return Thread.model_validate(
        {
            "name": name,
            "config_version": "1.0",
            "sources": {"_src": {"type": "delta", "alias": "_src"}},
            "target": {},
        }
    )


def _entries(*names: str) -> list[ThreadEntry]:
    return [ThreadEntry(name=n) for n in names]


# ---------------------------------------------------------------------------
# Test 1: Quality gate pre-step pass → threads run → post-step log
# ---------------------------------------------------------------------------


class TestHookLifecyclePassthrough:
    """Full lifecycle: passing pre-step → threads → post-step."""

    @patch("weevr.engine.runner.execute_thread")
    def test_passing_gate_allows_threads_and_post_steps(
        self, mock_exec, spark: SparkSession, tmp_delta_path
    ) -> None:
        mock_exec.return_value = _make_result("A")

        # Create a real table so table_exists check passes
        tbl_path = tmp_delta_path("gate_exists_src")
        create_delta_table(spark, tbl_path, [{"id": 1}])
        spark.read.format("delta").load(tbl_path).createOrReplaceTempView("gate_exists_src")

        pre = [
            QualityGateStep(
                type="quality_gate",
                check="table_exists",
                source="gate_exists_src",
                name="check_src",
            ),
        ]
        post = [
            LogMessageStep(type="log_message", message="weave complete", name="done_log"),
        ]

        threads = {"A": _make_thread_model("A")}
        plan = build_plan("lifecycle", threads, _entries("A"))
        collector = SpanCollector(generate_trace_id())

        result = execute_weave(
            spark, plan, threads, collector=collector, pre_steps=pre, post_steps=post
        )

        assert result.status == "success"
        mock_exec.assert_called_once()

        # Telemetry includes both hook results
        assert result.telemetry is not None
        assert len(result.telemetry.hook_results) == 2
        assert result.telemetry.hook_results[0].phase == "pre"
        assert result.telemetry.hook_results[0].status == "passed"
        assert result.telemetry.hook_results[1].phase == "post"


# ---------------------------------------------------------------------------
# Test 2: Quality gate pre-step fail (abort) → threads NOT run
# ---------------------------------------------------------------------------


class TestHookPreAbort:
    """Pre-step failure with on_failure=abort prevents thread execution."""

    @patch("weevr.engine.runner.execute_thread")
    def test_failing_pre_gate_aborts_weave(self, mock_exec, spark: SparkSession) -> None:
        # table_exists on a non-existent table will fail
        pre = [
            QualityGateStep(
                type="quality_gate",
                check="table_exists",
                source="nonexistent_table_xyz",
                on_failure="abort",
                name="must_exist",
            ),
        ]

        threads = {"A": _make_thread_model("A")}
        plan = build_plan("abort_weave", threads, _entries("A"))

        with pytest.raises(HookError, match="must_exist|table_exists"):
            execute_weave(spark, plan, threads, pre_steps=pre)

        # Thread never executed
        mock_exec.assert_not_called()


# ---------------------------------------------------------------------------
# Test 3: Quality gate post-step fail (warn) → threads succeed
# ---------------------------------------------------------------------------


class TestHookPostWarn:
    """Post-step failure with on_failure=warn does not fail the weave."""

    @patch("weevr.engine.runner.execute_thread")
    def test_post_gate_warn_does_not_fail_weave(self, mock_exec, spark: SparkSession) -> None:
        mock_exec.return_value = _make_result("A")

        # Post-step: row_count check with impossible thresholds → will fail
        # but on_failure=warn means we just log
        post = [
            QualityGateStep(
                type="quality_gate",
                check="expression",
                sql="SELECT 1 = 0",
                on_failure="warn",
                name="soft_check",
            ),
        ]

        threads = {"A": _make_thread_model("A")}
        plan = build_plan("warn_weave", threads, _entries("A"))
        collector = SpanCollector(generate_trace_id())

        result = execute_weave(spark, plan, threads, collector=collector, post_steps=post)

        # Weave succeeds despite post-step warning
        assert result.status == "success"
        mock_exec.assert_called_once()

        assert result.telemetry is not None
        assert len(result.telemetry.hook_results) == 1
        assert result.telemetry.hook_results[0].status == "warned"


# ---------------------------------------------------------------------------
# Test 4: Variable chaining — sql_statement sets var, next step reads it
# ---------------------------------------------------------------------------


class TestVariableChaining:
    """Variables set by sql_statement are available to subsequent steps."""

    @patch("weevr.engine.runner.execute_thread")
    def test_sql_step_sets_var_for_log_step(self, mock_exec, spark: SparkSession) -> None:
        mock_exec.return_value = _make_result("A")

        pre = [
            SqlStatementStep(
                type="sql_statement",
                sql="SELECT 42",
                set_var="answer",
                name="set_answer",
            ),
            LogMessageStep(
                type="log_message",
                message="The answer is ${var.answer}",
                name="log_answer",
            ),
        ]

        variables: dict[str, VariableSpec] = {
            "answer": VariableSpec(type="int"),
        }

        threads = {"A": _make_thread_model("A")}
        plan = build_plan("var_chain", threads, _entries("A"))
        collector = SpanCollector(generate_trace_id())

        result = execute_weave(
            spark, plan, threads, collector=collector, pre_steps=pre, variables=variables
        )

        assert result.status == "success"
        assert result.telemetry is not None
        # Variable was set by the sql step
        assert result.telemetry.variables["answer"] == 42


# ---------------------------------------------------------------------------
# Test 5: Lookup materialization — cached lookup used by thread
# ---------------------------------------------------------------------------


class TestLookupMaterialization:
    """Materialized lookup DataFrame is available to thread execution."""

    def test_broadcast_lookup_used_by_thread(self, spark: SparkSession, tmp_delta_path) -> None:
        lookup_src = tmp_delta_path("lkp_int_src")
        tgt = tmp_delta_path("lkp_int_tgt")
        create_delta_table(spark, lookup_src, [{"id": 1, "code": "A"}, {"id": 2, "code": "B"}])

        # Thread with a single lookup-based source
        thread = Thread(
            name="lkp_thread",
            config_version="1",
            sources={"ref": Source(lookup="codes")},
            steps=[],
            target=Target(path=tgt),
        )

        lookups: dict[str, Lookup] = {
            "codes": Lookup(
                source=Source(type="delta", alias=lookup_src),
                materialize=True,
                strategy="broadcast",
            ),
        }

        threads: dict[str, Thread] = {"lkp_thread": thread}
        plan = build_plan("lookup_weave", threads, _entries("lkp_thread"))
        collector = SpanCollector(generate_trace_id())

        result = execute_weave(spark, plan, threads, collector=collector, lookups=lookups)

        assert result.status == "success"
        assert len(result.thread_results) == 1
        assert result.thread_results[0].rows_written == 2

        # Verify lookup telemetry
        assert result.telemetry is not None
        assert len(result.telemetry.lookup_results) == 1
        assert result.telemetry.lookup_results[0].name == "codes"
        assert result.telemetry.lookup_results[0].materialized is True


# ---------------------------------------------------------------------------
# Test 6: Non-materialized lookup — thread reads on demand
# ---------------------------------------------------------------------------


class TestLookupOnDemand:
    """Non-materialized lookup is read on-demand by the thread."""

    def test_non_materialized_lookup_read_on_demand(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        lookup_src = tmp_delta_path("lkp_demand_int_src")
        tgt = tmp_delta_path("lkp_demand_int_tgt")
        create_delta_table(spark, lookup_src, [{"id": 10, "val": "x"}])

        thread = Thread(
            name="demand_thread",
            config_version="1",
            sources={"ref": Source(lookup="lazy_ref")},
            steps=[],
            target=Target(path=tgt),
        )

        lookups: dict[str, Lookup] = {
            "lazy_ref": Lookup(
                source=Source(type="delta", alias=lookup_src),
                materialize=False,
            ),
        }

        threads_map: dict[str, Thread] = {"demand_thread": thread}
        plan = build_plan("demand_weave", threads_map, _entries("demand_thread"))
        collector = SpanCollector(generate_trace_id())

        result = execute_weave(spark, plan, threads_map, collector=collector, lookups=lookups)

        assert result.status == "success"
        assert result.thread_results[0].rows_written == 1

        # Non-materialized lookup still appears in telemetry
        assert result.telemetry is not None
        assert len(result.telemetry.lookup_results) == 1
        assert result.telemetry.lookup_results[0].materialized is False
