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
from weevr.errors.exceptions import HookError, LookupResolutionError
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
            "target": {"alias": "test"},
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


# ---------------------------------------------------------------------------
# Test 7: Narrow lookup — projection reduces columns
# ---------------------------------------------------------------------------


class TestNarrowLookupMaterialization:
    """Narrow lookups apply filter, projection, and unique_key during materialization."""

    def test_narrow_broadcast_projection(self, spark: SparkSession, tmp_delta_path) -> None:
        """Lookup with key + values projects to only those columns."""
        lookup_src = tmp_delta_path("narrow_proj_src")
        tgt = tmp_delta_path("narrow_proj_tgt")
        create_delta_table(
            spark,
            lookup_src,
            [
                {"id": 1, "code": "A", "name": "Alpha", "region": "US", "flag": True},
                {"id": 2, "code": "B", "name": "Beta", "region": "EU", "flag": False},
            ],
        )

        thread = Thread(
            name="narrow_proj_thread",
            config_version="1",
            sources={"ref": Source(lookup="dim")},
            steps=[],
            target=Target(path=tgt),
        )

        lookups: dict[str, Lookup] = {
            "dim": Lookup(
                source=Source(type="delta", alias=lookup_src),
                materialize=True,
                strategy="broadcast",
                key=["id"],
                values=["code"],
            ),
        }

        threads: dict[str, Thread] = {"narrow_proj_thread": thread}
        plan = build_plan("narrow_proj", threads, _entries("narrow_proj_thread"))
        collector = SpanCollector(generate_trace_id())

        result = execute_weave(spark, plan, threads, collector=collector, lookups=lookups)

        assert result.status == "success"
        assert result.thread_results[0].rows_written == 2

        # Verify the target has only projected columns
        written = spark.read.format("delta").load(tgt)
        assert set(written.columns) == {"id", "code"}

        # Verify telemetry metadata
        assert result.telemetry is not None
        lkp_result = result.telemetry.lookup_results[0]
        assert lkp_result.key_columns == ["id"]
        assert lkp_result.value_columns == ["code"]

    def test_narrow_filter(self, spark: SparkSession, tmp_delta_path) -> None:
        """Lookup with filter retains only matching rows."""
        lookup_src = tmp_delta_path("narrow_filt_src")
        tgt = tmp_delta_path("narrow_filt_tgt")
        create_delta_table(
            spark,
            lookup_src,
            [
                {"id": 1, "is_active": True},
                {"id": 2, "is_active": False},
                {"id": 3, "is_active": True},
            ],
        )

        thread = Thread(
            name="narrow_filt_thread",
            config_version="1",
            sources={"ref": Source(lookup="active_dim")},
            steps=[],
            target=Target(path=tgt),
        )

        lookups: dict[str, Lookup] = {
            "active_dim": Lookup(
                source=Source(type="delta", alias=lookup_src),
                materialize=True,
                strategy="cache",
                filter="is_active = true",
            ),
        }

        threads: dict[str, Thread] = {"narrow_filt_thread": thread}
        plan = build_plan("narrow_filt", threads, _entries("narrow_filt_thread"))

        result = execute_weave(spark, plan, threads, lookups=lookups)

        assert result.status == "success"
        assert result.thread_results[0].rows_written == 2

    def test_narrow_filter_and_projection(self, spark: SparkSession, tmp_delta_path) -> None:
        """Filter + projection compose: filter first, then select columns."""
        lookup_src = tmp_delta_path("narrow_combo_src")
        tgt = tmp_delta_path("narrow_combo_tgt")
        create_delta_table(
            spark,
            lookup_src,
            [
                {"id": 1, "code": "A", "is_current": True},
                {"id": 2, "code": "B", "is_current": False},
                {"id": 3, "code": "C", "is_current": True},
            ],
        )

        thread = Thread(
            name="narrow_combo_thread",
            config_version="1",
            sources={"ref": Source(lookup="filtered_dim")},
            steps=[],
            target=Target(path=tgt),
        )

        lookups: dict[str, Lookup] = {
            "filtered_dim": Lookup(
                source=Source(type="delta", alias=lookup_src),
                materialize=True,
                strategy="broadcast",
                key=["id"],
                values=["code"],
                filter="is_current = true",
            ),
        }

        threads: dict[str, Thread] = {"narrow_combo_thread": thread}
        plan = build_plan("narrow_combo", threads, _entries("narrow_combo_thread"))

        result = execute_weave(spark, plan, threads, lookups=lookups)

        assert result.status == "success"
        # 2 rows pass filter, only id + code columns
        assert result.thread_results[0].rows_written == 2
        written = spark.read.format("delta").load(tgt)
        assert set(written.columns) == {"id", "code"}

    def test_unique_key_pass(self, spark: SparkSession, tmp_delta_path) -> None:
        """Unique key check passes when keys are unique."""
        lookup_src = tmp_delta_path("uk_pass_src")
        tgt = tmp_delta_path("uk_pass_tgt")
        create_delta_table(
            spark,
            lookup_src,
            [{"id": 1, "sk": 100}, {"id": 2, "sk": 200}],
        )

        thread = Thread(
            name="uk_pass_thread",
            config_version="1",
            sources={"ref": Source(lookup="uk_dim")},
            steps=[],
            target=Target(path=tgt),
        )

        lookups: dict[str, Lookup] = {
            "uk_dim": Lookup(
                source=Source(type="delta", alias=lookup_src),
                materialize=True,
                strategy="cache",
                key=["id"],
                values=["sk"],
                unique_key=True,
            ),
        }

        threads: dict[str, Thread] = {"uk_pass_thread": thread}
        plan = build_plan("uk_pass", threads, _entries("uk_pass_thread"))
        collector = SpanCollector(generate_trace_id())

        result = execute_weave(spark, plan, threads, collector=collector, lookups=lookups)

        assert result.status == "success"
        assert result.telemetry is not None
        lkp = result.telemetry.lookup_results[0]
        assert lkp.unique_key_checked is True
        assert lkp.unique_key_passed is True

    def test_unique_key_fail_abort(self, spark: SparkSession, tmp_delta_path) -> None:
        """Duplicate keys with on_failure=abort raises LookupResolutionError."""
        lookup_src = tmp_delta_path("uk_abort_src")
        create_delta_table(
            spark,
            lookup_src,
            [{"id": 1, "sk": 100}, {"id": 1, "sk": 101}, {"id": 2, "sk": 200}],
        )

        thread = Thread(
            name="uk_abort_thread",
            config_version="1",
            sources={"ref": Source(lookup="dup_dim")},
            steps=[],
            target=Target(path=tmp_delta_path("uk_abort_tgt")),
        )

        lookups: dict[str, Lookup] = {
            "dup_dim": Lookup(
                source=Source(type="delta", alias=lookup_src),
                materialize=True,
                strategy="cache",
                key=["id"],
                values=["sk"],
                unique_key=True,
                on_failure="abort",
            ),
        }

        threads: dict[str, Thread] = {"uk_abort_thread": thread}
        plan = build_plan("uk_abort", threads, _entries("uk_abort_thread"))

        with pytest.raises(LookupResolutionError, match="unique_key check failed"):
            execute_weave(spark, plan, threads, lookups=lookups)

    def test_unique_key_fail_warn(self, spark: SparkSession, tmp_delta_path) -> None:
        """Duplicate keys with on_failure=warn succeeds with warning."""
        lookup_src = tmp_delta_path("uk_warn_src")
        tgt = tmp_delta_path("uk_warn_tgt")
        create_delta_table(
            spark,
            lookup_src,
            [{"id": 1, "sk": 100}, {"id": 1, "sk": 101}, {"id": 2, "sk": 200}],
        )

        thread = Thread(
            name="uk_warn_thread",
            config_version="1",
            sources={"ref": Source(lookup="warn_dim")},
            steps=[],
            target=Target(path=tgt),
        )

        lookups: dict[str, Lookup] = {
            "warn_dim": Lookup(
                source=Source(type="delta", alias=lookup_src),
                materialize=True,
                strategy="cache",
                key=["id"],
                values=["sk"],
                unique_key=True,
                on_failure="warn",
            ),
        }

        threads: dict[str, Thread] = {"uk_warn_thread": thread}
        plan = build_plan("uk_warn", threads, _entries("uk_warn_thread"))
        collector = SpanCollector(generate_trace_id())

        result = execute_weave(spark, plan, threads, collector=collector, lookups=lookups)

        assert result.status == "success"
        assert result.telemetry is not None
        lkp = result.telemetry.lookup_results[0]
        assert lkp.unique_key_checked is True
        assert lkp.unique_key_passed is False


# ---------------------------------------------------------------------------
# Test 8: Narrow lookup — on-demand read with narrow pipeline
# ---------------------------------------------------------------------------


class TestNarrowLookupOnDemand:
    """Non-materialized narrow lookups apply the pipeline at read time."""

    def test_on_demand_narrow(self, spark: SparkSession, tmp_delta_path) -> None:
        """On-demand lookup with key + values + filter narrows the DataFrame."""
        lookup_src = tmp_delta_path("narrow_demand_src")
        tgt = tmp_delta_path("narrow_demand_tgt")
        create_delta_table(
            spark,
            lookup_src,
            [
                {"id": 1, "code": "A", "active": True},
                {"id": 2, "code": "B", "active": False},
                {"id": 3, "code": "C", "active": True},
            ],
        )

        thread = Thread(
            name="narrow_demand_thread",
            config_version="1",
            sources={"ref": Source(lookup="lazy_narrow")},
            steps=[],
            target=Target(path=tgt),
        )

        lookups: dict[str, Lookup] = {
            "lazy_narrow": Lookup(
                source=Source(type="delta", alias=lookup_src),
                materialize=False,
                key=["id"],
                values=["code"],
                filter="active = true",
            ),
        }

        threads: dict[str, Thread] = {"narrow_demand_thread": thread}
        plan = build_plan("narrow_demand", threads, _entries("narrow_demand_thread"))

        result = execute_weave(spark, plan, threads, lookups=lookups)

        assert result.status == "success"
        assert result.thread_results[0].rows_written == 2

        written = spark.read.format("delta").load(tgt)
        assert set(written.columns) == {"id", "code"}


# ---------------------------------------------------------------------------
# Test 9: Backward compatibility — lookup without narrow fields
# ---------------------------------------------------------------------------


class TestNarrowLookupBackwardCompat:
    """Lookups without narrow fields work identically to previous behavior."""

    def test_full_table_lookup_unchanged(self, spark: SparkSession, tmp_delta_path) -> None:
        """Lookup without narrow fields caches the full source table."""
        lookup_src = tmp_delta_path("compat_src")
        tgt = tmp_delta_path("compat_tgt")
        create_delta_table(
            spark,
            lookup_src,
            [
                {"id": 1, "code": "A", "name": "Alpha"},
                {"id": 2, "code": "B", "name": "Beta"},
                {"id": 3, "code": "C", "name": "Charlie"},
            ],
        )

        thread = Thread(
            name="compat_thread",
            config_version="1",
            sources={"ref": Source(lookup="full_ref")},
            steps=[],
            target=Target(path=tgt),
        )

        lookups: dict[str, Lookup] = {
            "full_ref": Lookup(
                source=Source(type="delta", alias=lookup_src),
                materialize=True,
                strategy="cache",
            ),
        }

        threads: dict[str, Thread] = {"compat_thread": thread}
        plan = build_plan("compat_weave", threads, _entries("compat_thread"))

        result = execute_weave(spark, plan, threads, lookups=lookups)

        assert result.status == "success"
        assert result.thread_results[0].rows_written == 3

        # All columns preserved — no narrow projection
        written = spark.read.format("delta").load(tgt)
        assert set(written.columns) == {"id", "code", "name"}
