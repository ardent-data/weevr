"""End-to-end integration tests for telemetry, validation, and assertions.

These tests use a real SparkSession to verify the full execution path from
loom → weave → thread with validation rules, assertions, quarantine writes,
and telemetry collection.
"""

import pytest
from pyspark.sql import SparkSession

from spark_helpers import create_delta_table
from weevr.engine.executor import execute_thread
from weevr.engine.planner import build_plan
from weevr.engine.runner import execute_loom, execute_weave
from weevr.errors.exceptions import DataValidationError
from weevr.model.loom import Loom
from weevr.model.source import Source
from weevr.model.target import Target
from weevr.model.thread import Thread
from weevr.model.types import SparkExpr
from weevr.model.validation import Assertion, ValidationRule
from weevr.model.weave import Weave
from weevr.telemetry.collector import SpanCollector
from weevr.telemetry.span import SpanStatus, generate_trace_id

pytestmark = pytest.mark.spark


def _make_thread(
    name: str,
    source_path: str,
    target_path: str,
    *,
    validations: list[ValidationRule] | None = None,
    assertions: list[Assertion] | None = None,
) -> Thread:
    return Thread(
        name=name,
        config_version="1",
        sources={"main": Source(type="delta", alias=source_path)},
        steps=[],
        target=Target(path=target_path),
        validations=validations,
        assertions=assertions,
    )


class TestThreadTelemetryIntegration:
    """Thread-level telemetry with collector."""

    def test_thread_span_created_with_collector(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        src = tmp_delta_path("tel_int_src")
        tgt = tmp_delta_path("tel_int_tgt")
        create_delta_table(spark, src, [{"id": 1}, {"id": 2}])

        collector = SpanCollector(generate_trace_id())
        thread = _make_thread("t1", src, tgt)
        result = execute_thread(spark, thread, collector=collector)

        assert result.telemetry is not None
        assert result.telemetry.span.name == "thread:t1"
        assert result.telemetry.span.status == SpanStatus.OK
        assert result.telemetry.rows_read == 2
        assert result.telemetry.rows_written == 2

        # Span should be recorded in the collector
        spans = collector.get_spans()
        thread_spans = [s for s in spans if s.name == "thread:t1"]
        assert len(thread_spans) == 1

    def test_thread_span_with_parent(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        src = tmp_delta_path("parent_src")
        tgt = tmp_delta_path("parent_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        collector = SpanCollector(generate_trace_id())
        parent_builder = collector.start_span("weave:test")
        parent_id = parent_builder.span_id

        thread = _make_thread("t1", src, tgt)
        result = execute_thread(
            spark, thread, collector=collector, parent_span_id=parent_id
        )

        assert result.telemetry is not None
        assert result.telemetry.span.parent_span_id == parent_id


class TestWeaveExecutionIntegration:
    """Weave-level telemetry through real Spark execution."""

    def test_weave_with_validation_and_assertions(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        src = tmp_delta_path("weave_val_src")
        tgt = tmp_delta_path("weave_val_tgt")
        create_delta_table(
            spark, src, [{"id": 1, "amount": 100}, {"id": 2, "amount": -5}]
        )

        rules = [
            ValidationRule(
                rule=SparkExpr("amount > 0"), severity="error", name="positive"
            )
        ]
        assertions = [Assertion(type="row_count", min=1)]
        thread = _make_thread(
            "val_thread", src, tgt, validations=rules, assertions=assertions
        )
        threads = {"val_thread": thread}

        plan = build_plan(
            "val_weave", threads, list(Weave.model_validate({
                "config_version": "1.0", "name": "val_weave", "threads": ["val_thread"]
            }).threads)
        )

        collector = SpanCollector(generate_trace_id())
        result = execute_weave(spark, plan, threads, collector=collector)

        assert result.status == "success"
        assert result.telemetry is not None
        assert result.telemetry.span.name == "weave:val_weave"

        # Thread telemetry should be in weave telemetry
        assert "val_thread" in result.telemetry.thread_telemetry
        t_tel = result.telemetry.thread_telemetry["val_thread"]
        assert t_tel.rows_read == 2
        assert t_tel.rows_written == 1  # 1 clean row
        assert t_tel.rows_quarantined == 1
        assert len(t_tel.validation_results) == 1
        assert len(t_tel.assertion_results) == 1

        # Quarantine table should exist
        q_df = spark.read.format("delta").load(f"{tgt}_quarantine")
        assert q_df.count() == 1

    def test_weave_row_count_reconciliation(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """rows_read == rows_written + rows_quarantined for error-severity rules."""
        src = tmp_delta_path("recon_src")
        tgt = tmp_delta_path("recon_tgt")
        create_delta_table(
            spark,
            src,
            [
                {"id": 1, "val": 10},
                {"id": 2, "val": -1},
                {"id": 3, "val": 20},
                {"id": 4, "val": -2},
            ],
        )

        rules = [
            ValidationRule(
                rule=SparkExpr("val > 0"), severity="error", name="positive_val"
            )
        ]
        thread = _make_thread("recon_thread", src, tgt, validations=rules)
        threads = {"recon_thread": thread}

        plan = build_plan(
            "recon_weave", threads, list(Weave.model_validate({
                "config_version": "1.0", "name": "recon_weave", "threads": ["recon_thread"]
            }).threads)
        )

        collector = SpanCollector(generate_trace_id())
        result = execute_weave(spark, plan, threads, collector=collector)

        assert result.telemetry is not None
        t_tel = result.telemetry.thread_telemetry["recon_thread"]
        assert t_tel.rows_read == 4
        assert t_tel.rows_written == 2
        assert t_tel.rows_quarantined == 2
        assert t_tel.rows_read == t_tel.rows_written + t_tel.rows_quarantined


class TestLoomExecutionIntegration:
    """Full loom → weave → thread execution with telemetry."""

    def test_loom_telemetry_hierarchy(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        src = tmp_delta_path("loom_src")
        tgt = tmp_delta_path("loom_tgt")
        create_delta_table(spark, src, [{"id": 1}, {"id": 2}, {"id": 3}])

        thread = _make_thread("loom_thread", src, tgt)
        loom = Loom.model_validate({
            "name": "test_loom", "config_version": "1.0", "weaves": ["w1"]
        })
        weaves = {
            "w1": Weave.model_validate({
                "config_version": "1.0", "name": "w1", "threads": ["loom_thread"]
            })
        }
        threads = {"w1": {"loom_thread": thread}}

        result = execute_loom(spark, loom, weaves, threads)

        assert result.status == "success"
        assert result.telemetry is not None
        assert result.telemetry.span.name == "loom:test_loom"

        # Weave telemetry nested
        assert "w1" in result.telemetry.weave_telemetry
        w_tel = result.telemetry.weave_telemetry["w1"]
        assert w_tel.span.name == "weave:w1"

        # Thread telemetry nested inside weave
        assert "loom_thread" in w_tel.thread_telemetry
        t_tel = w_tel.thread_telemetry["loom_thread"]
        assert t_tel.rows_read == 3
        assert t_tel.rows_written == 3

    def test_loom_with_validation_quarantine_assertions(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        src = tmp_delta_path("full_src")
        tgt = tmp_delta_path("full_tgt")
        create_delta_table(
            spark,
            src,
            [{"id": 1, "score": 90}, {"id": 2, "score": -10}, {"id": 3, "score": 75}],
        )

        rules = [
            ValidationRule(
                rule=SparkExpr("score >= 0"), severity="error", name="non_negative"
            )
        ]
        assertions = [Assertion(type="row_count", min=1, max=100)]

        thread = _make_thread(
            "scored_thread", src, tgt, validations=rules, assertions=assertions
        )
        loom = Loom.model_validate({
            "name": "scoring_loom", "config_version": "1.0", "weaves": ["scoring"]
        })
        weaves = {
            "scoring": Weave.model_validate({
                "config_version": "1.0", "name": "scoring", "threads": ["scored_thread"]
            })
        }
        threads = {"scoring": {"scored_thread": thread}}

        result = execute_loom(spark, loom, weaves, threads)

        assert result.status == "success"
        assert result.telemetry is not None

        # Navigate telemetry hierarchy
        t_tel = result.telemetry.weave_telemetry["scoring"].thread_telemetry["scored_thread"]
        assert t_tel.rows_read == 3
        assert t_tel.rows_written == 2
        assert t_tel.rows_quarantined == 1
        assert len(t_tel.validation_results) == 1
        assert t_tel.validation_results[0].rows_failed == 1
        assert len(t_tel.assertion_results) == 1
        assert t_tel.assertion_results[0].passed is True

        # Quarantine table written
        q_df = spark.read.format("delta").load(f"{tgt}_quarantine")
        assert q_df.count() == 1

        # Target table written with clean rows
        tgt_df = spark.read.format("delta").load(tgt)
        assert tgt_df.count() == 2


class TestFatalValidationIntegration:
    """Fatal validation aborts thread, span has ERROR status."""

    def test_fatal_validation_error_span_status(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        src = tmp_delta_path("fatal_src")
        tgt = tmp_delta_path("fatal_tgt")
        create_delta_table(spark, src, [{"id": 1, "val": -5}])

        rules = [
            ValidationRule(
                rule=SparkExpr("val > 0"), severity="fatal", name="must_positive"
            )
        ]

        collector = SpanCollector(generate_trace_id())
        thread = _make_thread("fatal_thread", src, tgt, validations=rules)

        with pytest.raises(DataValidationError):
            execute_thread(spark, thread, collector=collector)

        # Span should have ERROR status
        spans = collector.get_spans()
        thread_spans = [s for s in spans if s.name == "thread:fatal_thread"]
        assert len(thread_spans) == 1
        assert thread_spans[0].status == SpanStatus.ERROR
