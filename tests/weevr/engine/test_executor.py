"""Tests for execute_thread() — thread executor orchestration."""

import pytest
from pyspark.sql import SparkSession

from spark_helpers import create_delta_table
from weevr.engine.executor import execute_thread
from weevr.engine.result import ThreadResult
from weevr.errors.exceptions import (
    DataValidationError,
    ExecutionError,
    ExportError,
    LookupResolutionError,
)
from weevr.model.connection import OneLakeConnection
from weevr.model.export import Export
from weevr.model.keys import KeyConfig, SurrogateKeyConfig
from weevr.model.lookup import Lookup
from weevr.model.pipeline import FilterParams, FilterStep, Step
from weevr.model.source import Source
from weevr.model.target import Target
from weevr.model.thread import Thread
from weevr.model.types import SparkExpr
from weevr.model.validation import Assertion, ValidationRule
from weevr.model.write import WriteConfig

pytestmark = pytest.mark.spark


def _make_thread(
    name: str,
    source_path: str,
    target_path: str,
    *,
    steps: list[Step] | None = None,
    write: WriteConfig | None = None,
    keys: KeyConfig | None = None,
    validations: list[ValidationRule] | None = None,
    assertions: list[Assertion] | None = None,
    exports: list[Export] | None = None,
) -> Thread:
    """Helper to construct a minimal Thread for testing."""
    return Thread(
        name=name,
        config_version="1",
        sources={"main": Source(type="delta", alias=source_path)},
        steps=steps or [],
        target=Target(path=target_path),
        write=write,
        keys=keys,
        validations=validations,
        assertions=assertions,
        exports=exports,
    )


class TestExecuteThreadResult:
    """ThreadResult fields populated correctly by execute_thread."""

    def test_returns_thread_result_on_success(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("exec_src")
        tgt = tmp_delta_path("exec_tgt")
        create_delta_table(spark, src, [{"id": 1}, {"id": 2}])

        thread = _make_thread("my_thread", src, tgt)
        result = execute_thread(spark, thread)

        assert isinstance(result, ThreadResult)
        assert result.status == "success"

    def test_result_thread_name_matches(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("name_src")
        tgt = tmp_delta_path("name_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        thread = _make_thread("named_thread", src, tgt)
        result = execute_thread(spark, thread)

        assert result.thread_name == "named_thread"

    def test_result_rows_written_matches_source(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("rows_src")
        tgt = tmp_delta_path("rows_tgt")
        create_delta_table(spark, src, [{"id": i} for i in range(5)])

        thread = _make_thread("rows_thread", src, tgt)
        result = execute_thread(spark, thread)

        assert result.rows_written == 5

    def test_result_write_mode_overwrite(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("mode_src")
        tgt = tmp_delta_path("mode_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        thread = _make_thread("mode_thread", src, tgt, write=WriteConfig(mode="overwrite"))
        result = execute_thread(spark, thread)

        assert result.write_mode == "overwrite"

    def test_result_write_mode_defaults_to_overwrite(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        src = tmp_delta_path("default_src")
        tgt = tmp_delta_path("default_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        thread = _make_thread("default_mode", src, tgt)
        result = execute_thread(spark, thread)

        assert result.write_mode == "overwrite"

    def test_result_target_path_populated(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("path_src")
        tgt = tmp_delta_path("path_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        thread = _make_thread("path_thread", src, tgt)
        result = execute_thread(spark, thread)

        assert result.target_path == tgt


class TestExecuteThreadWrites:
    """execute_thread actually writes data to the target."""

    def test_data_written_to_target_delta_table(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("write_src")
        tgt = tmp_delta_path("write_tgt")
        create_delta_table(spark, src, [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])

        thread = _make_thread("write_thread", src, tgt)
        execute_thread(spark, thread)

        written = spark.read.format("delta").load(tgt)
        assert written.count() == 2

    def test_pipeline_steps_applied_before_write(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("steps_src")
        tgt = tmp_delta_path("steps_tgt")
        create_delta_table(
            spark,
            src,
            [
                {"id": 1, "amount": 100},
                {"id": 2, "amount": 30},
            ],
        )

        steps: list[Step] = [FilterStep(filter=FilterParams(expr=SparkExpr("amount > 50")))]
        thread = _make_thread("steps_thread", src, tgt, steps=steps)
        result = execute_thread(spark, thread)

        assert result.rows_written == 1
        written = spark.read.format("delta").load(tgt)
        assert written.count() == 1

    def test_keys_computed_before_write(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("keys_src")
        tgt = tmp_delta_path("keys_tgt")
        create_delta_table(spark, src, [{"id": 1, "name": "alice"}])

        keys = KeyConfig(
            business_key=["id"],
            surrogate_key=SurrogateKeyConfig(name="sk"),
        )
        thread = _make_thread("keys_thread", src, tgt, keys=keys)
        execute_thread(spark, thread)

        written = spark.read.format("delta").load(tgt)
        assert "sk" in written.columns

    def test_append_mode_accumulates_rows(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("append_src")
        tgt = tmp_delta_path("append_tgt")
        create_delta_table(spark, src, [{"id": 1}])
        create_delta_table(spark, tgt, [{"id": 0}])

        thread = _make_thread("append_thread", src, tgt, write=WriteConfig(mode="append"))
        execute_thread(spark, thread)

        written = spark.read.format("delta").load(tgt)
        assert written.count() == 2


class TestExecuteThreadErrors:
    """Error handling in execute_thread."""

    def test_missing_source_raises_execution_error(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        tgt = tmp_delta_path("err_tgt")
        thread = _make_thread("err_thread", "/nonexistent/path", tgt)

        with pytest.raises(ExecutionError) as exc_info:
            execute_thread(spark, thread)

        assert exc_info.value.thread_name == "err_thread"

    def test_missing_target_path_raises_validation_error(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        from pydantic import ValidationError

        # Target with neither alias nor path is rejected at model construction
        with pytest.raises(ValidationError, match="alias.*path"):
            Target()  # no alias, no path

    def test_execution_error_carries_thread_name(self, spark: SparkSession, tmp_delta_path) -> None:
        tgt = tmp_delta_path("carry_tgt")
        thread = _make_thread("carry_thread", "/nonexistent", tgt)

        with pytest.raises(ExecutionError) as exc_info:
            execute_thread(spark, thread)

        assert exc_info.value.thread_name == "carry_thread"


class TestExecuteThreadTelemetry:
    """Telemetry populated on ThreadResult."""

    def test_telemetry_populated_without_rules(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("tel_src")
        tgt = tmp_delta_path("tel_tgt")
        create_delta_table(spark, src, [{"id": 1}, {"id": 2}])

        thread = _make_thread("tel_thread", src, tgt)
        result = execute_thread(spark, thread)

        assert result.telemetry is not None
        assert result.telemetry.validation_results == []
        assert result.telemetry.assertion_results == []
        assert result.telemetry.rows_read == 2
        assert result.telemetry.rows_written == 2
        assert result.telemetry.rows_quarantined == 0


class TestExecuteThreadValidation:
    """Validation integration in execute_thread."""

    def test_error_severity_quarantines_rows(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("val_src")
        tgt = tmp_delta_path("val_tgt")
        create_delta_table(spark, src, [{"id": 1, "amount": 100}, {"id": 2, "amount": -5}])

        rules = [ValidationRule(rule=SparkExpr("amount > 0"), severity="error", name="positive")]
        thread = _make_thread("val_thread", src, tgt, validations=rules)
        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.rows_written == 1  # Only clean row written
        assert result.telemetry is not None
        assert result.telemetry.rows_quarantined > 0
        assert len(result.telemetry.validation_results) == 1
        assert result.telemetry.validation_results[0].rows_failed == 1

        # Verify quarantine table exists
        q_df = spark.read.format("delta").load(f"{tgt}_quarantine")
        assert q_df.count() >= 1

    def test_fatal_validation_raises(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("fatal_src")
        tgt = tmp_delta_path("fatal_tgt")
        create_delta_table(spark, src, [{"id": 1, "amount": -5}])

        rules = [
            ValidationRule(rule=SparkExpr("amount > 0"), severity="fatal", name="must_positive")
        ]
        thread = _make_thread("fatal_thread", src, tgt, validations=rules)

        with pytest.raises(DataValidationError):
            execute_thread(spark, thread)


class TestExecuteThreadAssertions:
    """Assertion integration in execute_thread."""

    def test_passing_assertions_recorded(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("assert_src")
        tgt = tmp_delta_path("assert_tgt")
        create_delta_table(spark, src, [{"id": 1}, {"id": 2}])

        assertions = [Assertion(type="row_count", min=1, max=10)]
        thread = _make_thread("assert_thread", src, tgt, assertions=assertions)
        result = execute_thread(spark, thread)

        assert result.telemetry is not None
        assert len(result.telemetry.assertion_results) == 1
        assert result.telemetry.assertion_results[0].passed is True


class TestExecuteThreadLookups:
    """Lookup source resolution in execute_thread."""

    def test_lookup_source_resolved_from_cached(self, spark: SparkSession, tmp_delta_path) -> None:
        """A lookup source is resolved from a pre-cached DataFrame."""
        tgt = tmp_delta_path("lkp_cached_tgt")
        lookup_src = tmp_delta_path("lkp_data")
        create_delta_table(spark, lookup_src, [{"id": 1, "val": "x"}, {"id": 2, "val": "y"}])

        # Thread with a single lookup-based source
        thread = Thread(
            name="lookup_thread",
            config_version="1",
            sources={"ref": Source(lookup="my_lookup")},
            steps=[],
            target=Target(path=tgt),
        )

        # Pre-cached DataFrame
        cached_df = spark.read.format("delta").load(lookup_src)
        cached_lookups = {"my_lookup": cached_df}

        # Lookup definition (not used since it's cached)
        weave_lookups = {
            "my_lookup": Lookup(source=Source(type="delta", alias=lookup_src)),
        }

        result = execute_thread(
            spark, thread, cached_lookups=cached_lookups, weave_lookups=weave_lookups
        )

        assert result.status == "success"
        assert result.rows_written == 2

    def test_lookup_source_read_on_demand(self, spark: SparkSession, tmp_delta_path) -> None:
        """A non-materialized lookup source is read on-demand from the definition."""
        tgt = tmp_delta_path("lkp_demand_tgt")
        lookup_src = tmp_delta_path("lkp_demand_data")
        create_delta_table(spark, lookup_src, [{"id": 10}])

        thread = Thread(
            name="demand_thread",
            config_version="1",
            sources={"ref": Source(lookup="demand_lookup")},
            steps=[],
            target=Target(path=tgt),
        )

        # No cached DataFrame — only the definition
        weave_lookups = {
            "demand_lookup": Lookup(source=Source(type="delta", alias=lookup_src)),
        }

        result = execute_thread(spark, thread, weave_lookups=weave_lookups)

        assert result.status == "success"
        assert result.rows_written == 1

    def test_mixed_lookup_and_normal_sources(self, spark: SparkSession, tmp_delta_path) -> None:
        """Thread with both a normal source and a lookup source works correctly."""
        normal_src = tmp_delta_path("lkp_mix_normal")
        lookup_src = tmp_delta_path("lkp_mix_lookup")
        tgt = tmp_delta_path("lkp_mix_tgt")
        create_delta_table(spark, normal_src, [{"id": 1}, {"id": 2}])
        create_delta_table(spark, lookup_src, [{"id": 100}])

        thread = Thread(
            name="mixed_thread",
            config_version="1",
            sources={
                "main": Source(type="delta", alias=normal_src),
                "ref": Source(lookup="ref_lookup"),
            },
            steps=[],
            target=Target(path=tgt),
        )

        cached_df = spark.read.format("delta").load(lookup_src)
        cached_lookups = {"ref_lookup": cached_df}
        weave_lookups = {
            "ref_lookup": Lookup(source=Source(type="delta", alias=lookup_src)),
        }

        result = execute_thread(
            spark, thread, cached_lookups=cached_lookups, weave_lookups=weave_lookups
        )

        # Primary source (main) drives the write — 2 rows
        assert result.status == "success"
        assert result.rows_written == 2

    def test_undefined_lookup_raises_error(self, spark: SparkSession, tmp_delta_path) -> None:
        """Referencing an undefined lookup raises ExecutionError wrapping the resolution error."""
        tgt = tmp_delta_path("lkp_undef_tgt")

        thread = Thread(
            name="undef_thread",
            config_version="1",
            sources={"ref": Source(lookup="nonexistent")},
            steps=[],
            target=Target(path=tgt),
        )

        with pytest.raises(ExecutionError) as exc_info:
            execute_thread(spark, thread, weave_lookups={})

        assert isinstance(exc_info.value.__cause__, LookupResolutionError)
        assert "nonexistent" in str(exc_info.value.__cause__)

    def test_no_lookups_backward_compatible(self, spark: SparkSession, tmp_delta_path) -> None:
        """Thread without lookups works as before (no cached_lookups / weave_lookups)."""
        src = tmp_delta_path("lkp_compat_src")
        tgt = tmp_delta_path("lkp_compat_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        thread = _make_thread("compat_thread", src, tgt)
        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.rows_written == 1


class TestExecuteThreadExports:
    """Test export execution in execute_thread."""

    def test_exports_write_data(self, spark: SparkSession, tmp_delta_path) -> None:
        """Thread with exports writes to secondary destinations."""
        src = tmp_delta_path("exp_src")
        tgt = tmp_delta_path("exp_tgt")
        exp_path = tmp_delta_path("exp_parquet")
        create_delta_table(spark, src, [{"id": 1}, {"id": 2}])

        exports = [Export(name="archive", type="parquet", path=exp_path)]
        thread = _make_thread("export_thread", src, tgt, exports=exports)
        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.rows_written == 2
        assert result.telemetry is not None
        assert len(result.telemetry.export_results) == 1
        assert result.telemetry.export_results[0].name == "archive"
        assert result.telemetry.export_results[0].status == "success"
        assert result.telemetry.export_results[0].rows_written == 2
        # Verify export data was written
        exported = spark.read.parquet(exp_path)
        assert exported.count() == 2

    def test_export_warn_continues(self, spark: SparkSession, tmp_delta_path) -> None:
        """Export with on_failure=warn logs error but thread succeeds."""
        src = tmp_delta_path("warn_src")
        tgt = tmp_delta_path("warn_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        exports = [
            Export(
                name="bad_export",
                type="parquet",
                path="/nonexistent/\x00invalid",
                on_failure="warn",
            )
        ]
        thread = _make_thread("warn_thread", src, tgt, exports=exports)
        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.telemetry is not None
        assert len(result.telemetry.export_results) == 1
        assert result.telemetry.export_results[0].status == "warned"
        assert result.telemetry.export_results[0].error is not None

    def test_export_abort_raises(self, spark: SparkSession, tmp_delta_path) -> None:
        """Export with on_failure=abort raises ExportError."""
        src = tmp_delta_path("abort_src")
        tgt = tmp_delta_path("abort_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        exports = [
            Export(
                name="critical",
                type="parquet",
                path="/nonexistent/\x00invalid",
                on_failure="abort",
            )
        ]
        thread = _make_thread("abort_thread", src, tgt, exports=exports)
        with pytest.raises(ExportError, match="critical"):
            execute_thread(spark, thread)

    def test_no_exports_backward_compatible(self, spark: SparkSession, tmp_delta_path) -> None:
        """Thread without exports works as before."""
        src = tmp_delta_path("noexp_src")
        tgt = tmp_delta_path("noexp_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        thread = _make_thread("no_export_thread", src, tgt)
        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.telemetry is not None
        assert result.telemetry.export_results == []

    def test_multiple_exports(self, spark: SparkSession, tmp_delta_path) -> None:
        """Thread with multiple exports writes all of them."""
        src = tmp_delta_path("multi_src")
        tgt = tmp_delta_path("multi_tgt")
        exp1 = tmp_delta_path("multi_exp1")
        exp2 = tmp_delta_path("multi_exp2")
        create_delta_table(spark, src, [{"id": 1}, {"id": 2}, {"id": 3}])

        exports = [
            Export(name="pq_archive", type="parquet", path=exp1),
            Export(name="json_feed", type="json", path=exp2),
        ]
        thread = _make_thread("multi_export_thread", src, tgt, exports=exports)
        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.telemetry is not None
        assert len(result.telemetry.export_results) == 2
        assert result.telemetry.export_results[0].name == "pq_archive"
        assert result.telemetry.export_results[1].name == "json_feed"
        assert spark.read.parquet(exp1).count() == 3
        assert spark.read.json(exp2).count() == 3


class TestThreadResourceLifecycle:
    """Thread-level resource lifecycle in execute_thread."""

    def test_thread_pre_steps_run_before_execution(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Thread pre_steps execute before the core read/transform/write pipeline."""
        src = tmp_delta_path("pre_src")
        tgt = tmp_delta_path("pre_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        # Use a log_message pre_step; verify via mock logging
        from unittest.mock import patch

        from weevr.model.hooks import HookStep, LogMessageStep

        pre_steps: list[HookStep] = [
            LogMessageStep(type="log_message", message="thread-pre-fired", level="info"),
        ]
        thread = Thread(
            name="pre_thread",
            config_version="1",
            sources={"main": Source(type="delta", alias=src)},
            steps=[],
            target=Target(path=tgt),
            pre_steps=pre_steps,
        )

        with patch("weevr.engine.hooks.logger") as mock_logger:
            result = execute_thread(spark, thread)

        assert result.status == "success"
        # Verify the log message was emitted by the pre_step
        log_messages = [str(call) for call in mock_logger.log.call_args_list]
        assert any("thread-pre-fired" in msg for msg in log_messages)

    def test_thread_post_steps_run_after_execution(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Thread post_steps execute after the core pipeline completes."""
        src = tmp_delta_path("post_src")
        tgt = tmp_delta_path("post_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        from unittest.mock import patch

        from weevr.model.hooks import HookStep, LogMessageStep

        post_steps: list[HookStep] = [
            LogMessageStep(type="log_message", message="thread-post-fired", level="info"),
        ]
        thread = Thread(
            name="post_thread",
            config_version="1",
            sources={"main": Source(type="delta", alias=src)},
            steps=[],
            target=Target(path=tgt),
            post_steps=post_steps,
        )

        with patch("weevr.engine.hooks.logger") as mock_logger:
            result = execute_thread(spark, thread)

        assert result.status == "success"
        log_messages = [str(call) for call in mock_logger.log.call_args_list]
        assert any("thread-post-fired" in msg for msg in log_messages)

    def test_thread_lookups_materialize_and_merge(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Thread-level lookups are materialized and merged with weave-level cached lookups."""
        src = tmp_delta_path("tlkp_src")
        tgt = tmp_delta_path("tlkp_tgt")
        thread_lookup_src = tmp_delta_path("tlkp_ref")
        create_delta_table(spark, src, [{"id": 1}])
        create_delta_table(spark, thread_lookup_src, [{"ref_id": 10, "val": "x"}])

        # Thread with a source that references a thread-level lookup
        thread = Thread(
            name="tlkp_thread",
            config_version="1",
            sources={
                "main": Source(type="delta", alias=src),
                "ref": Source(lookup="thread_ref"),
            },
            steps=[],
            target=Target(path=tgt),
            lookups={
                "thread_ref": Lookup(
                    source=Source(type="delta", alias=thread_lookup_src),
                    materialize=True,
                ),
            },
        )

        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.rows_written == 1

    def test_thread_lookups_cleanup_on_failure(self, spark: SparkSession, tmp_delta_path) -> None:
        """Thread-level lookup cleanup runs even when execution fails."""
        from unittest.mock import patch

        thread_lookup_src = tmp_delta_path("tlkp_fail_ref")
        create_delta_table(spark, thread_lookup_src, [{"ref_id": 1}])

        # Thread with invalid source path but valid thread-level lookup
        thread = Thread(
            name="tlkp_fail_thread",
            config_version="1",
            sources={"main": Source(type="delta", alias="/nonexistent/path")},
            steps=[],
            target=Target(path="/tmp/fail_tgt"),
            lookups={
                "fail_ref": Lookup(
                    source=Source(type="delta", alias=thread_lookup_src),
                    materialize=True,
                ),
            },
        )

        with patch("weevr.engine.executor.cleanup_lookups") as mock_cleanup:
            with pytest.raises(ExecutionError):
                execute_thread(spark, thread)
            # cleanup_lookups should have been called with the thread-level lookups
            mock_cleanup.assert_called_once()
            cleanup_arg = mock_cleanup.call_args[0][0]
            assert "fail_ref" in cleanup_arg

    def test_thread_column_sets_merged_with_weave(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Thread-level column_sets merge with weave-level, thread wins on conflict."""
        from weevr.model.column_set import ColumnSet
        from weevr.model.pipeline import RenameParams, RenameStep

        src = tmp_delta_path("tcs_src")
        tgt = tmp_delta_path("tcs_tgt")
        create_delta_table(spark, src, [{"old_name": 1, "other": 2}])

        # Thread-level column_set overrides weave-level
        thread = Thread(
            name="tcs_thread",
            config_version="1",
            sources={"main": Source(type="delta", alias=src)},
            steps=[
                RenameStep(rename=RenameParams(column_set="my_cs")),
            ],
            target=Target(path=tgt),
            column_sets={
                "my_cs": ColumnSet(
                    param="cs_param",
                ),
            },
        )

        # Weave-level column_sets passed to execute_thread (different mapping)
        weave_column_sets = {"old_name": "weave_renamed"}
        weave_column_set_defs = {
            "my_cs": ColumnSet(param="cs_param"),
        }

        # Thread-level should win — pass thread_cs via the thread model
        # The resolved_params provide the mapping for the param-based column set
        result = execute_thread(
            spark,
            thread,
            resolved_params={"cs_param": {"old_name": "new_name"}},
            column_sets={"my_cs": weave_column_sets},
            column_set_defs=weave_column_set_defs,
        )

        assert result.status == "success"
        written = spark.read.format("delta").load(tgt)
        # Thread-level column_set should be used (new_name, not weave_renamed)
        assert "new_name" in written.columns

    def test_thread_variables_merged_with_weave(self, spark: SparkSession, tmp_delta_path) -> None:
        """Thread-level variables create a VariableContext for hook step resolution."""
        src = tmp_delta_path("tvar_src")
        tgt = tmp_delta_path("tvar_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        from unittest.mock import patch

        from weevr.model.hooks import LogMessageStep
        from weevr.model.variable import VariableSpec

        # Thread has a variable with a default, used in a pre_step log message
        thread = Thread(
            name="tvar_thread",
            config_version="1",
            sources={"main": Source(type="delta", alias=src)},
            steps=[],
            target=Target(path=tgt),
            variables={
                "my_var": VariableSpec(type="string", default="hello-from-thread"),
            },
            pre_steps=[
                LogMessageStep(
                    type="log_message",
                    message="var=${var.my_var}",
                    level="info",
                ),
            ],
        )

        with patch("weevr.engine.hooks.logger") as mock_logger:
            result = execute_thread(spark, thread)

        assert result.status == "success"
        # Check that the variable was resolved in the log message
        log_messages = [str(call) for call in mock_logger.log.call_args_list]
        assert any("var=hello-from-thread" in msg for msg in log_messages)

    def test_thread_without_resources_backward_compatible(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Threads without any resource fields still work as before."""
        src = tmp_delta_path("norc_src")
        tgt = tmp_delta_path("norc_tgt")
        create_delta_table(spark, src, [{"id": 1}, {"id": 2}])

        thread = _make_thread("norc_thread", src, tgt)
        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.rows_written == 2


class TestExecuteThreadConnections:
    """Connection resolution integration in execute_thread."""

    def test_thread_without_connections_backward_compatible(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Thread with path-based source and target works when no connections passed."""
        src = tmp_delta_path("conn_compat_src")
        tgt = tmp_delta_path("conn_compat_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        thread = _make_thread("compat_conn_thread", src, tgt)
        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.rows_written == 1

    def test_target_connection_undefined_raises_execution_error(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Target referencing an undefined connection raises ExecutionError."""
        src = tmp_delta_path("conn_undef_src")
        create_delta_table(spark, src, [{"id": 1}])

        thread = Thread(
            name="undef_conn_thread",
            config_version="1",
            sources={"main": Source(type="delta", alias=src)},
            steps=[],
            target=Target(connection="missing_conn", table="my_table"),
        )

        with pytest.raises(ExecutionError, match="missing_conn"):
            execute_thread(spark, thread)

    def test_target_connection_resolved_to_abfss_path(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Target with a connection resolves to an abfss:// path and writes there."""
        from unittest.mock import patch

        src = tmp_delta_path("conn_tgt_src")
        create_delta_table(spark, src, [{"id": 1}, {"id": 2}])

        conn = OneLakeConnection(
            type="onelake",
            workspace="ws-guid-1234",
            lakehouse="lh-guid-5678",
        )
        connections = {"my_conn": conn}

        thread = Thread(
            name="conn_tgt_thread",
            config_version="1",
            sources={"main": Source(type="delta", alias=src)},
            steps=[],
            target=Target(connection="my_conn", table="sales"),
        )

        expected_path = (
            "abfss://ws-guid-1234@onelake.dfs.fabric.microsoft.com/lh-guid-5678/Tables/sales"
        )

        captured_paths: list[str] = []

        def fake_write_target(spark_arg, df_arg, target_arg, write_arg, path_arg):
            captured_paths.append(path_arg)
            return df_arg.count()

        with patch("weevr.engine.executor.write_target", side_effect=fake_write_target):
            execute_thread(spark, thread, connections=connections)

        assert len(captured_paths) == 1
        assert captured_paths[0] == expected_path

    def test_target_connection_with_schema_override(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Target with connection and schema override includes schema in resolved path."""
        from unittest.mock import patch

        src = tmp_delta_path("conn_schema_src")
        create_delta_table(spark, src, [{"id": 1}])

        conn = OneLakeConnection(
            type="onelake",
            workspace="ws-abc",
            lakehouse="lh-def",
        )
        connections = {"silver_conn": conn}

        thread = Thread(
            name="schema_conn_thread",
            config_version="1",
            sources={"main": Source(type="delta", alias=src)},
            steps=[],
            target=Target(connection="silver_conn", schema_override="silver", table="customers"),  # type: ignore[call-arg]
        )

        expected_path = (
            "abfss://ws-abc@onelake.dfs.fabric.microsoft.com/lh-def/Tables/silver/customers"
        )

        captured_paths: list[str] = []

        def fake_write_target(spark_arg, df_arg, target_arg, write_arg, path_arg):
            captured_paths.append(path_arg)
            return df_arg.count()

        with patch("weevr.engine.executor.write_target", side_effect=fake_write_target):
            execute_thread(spark, thread, connections=connections)

        assert len(captured_paths) == 1
        assert captured_paths[0] == expected_path

    def test_export_connection_undefined_raises_export_error(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Export referencing an undefined connection raises ExportError."""
        src = tmp_delta_path("exp_conn_undef_src")
        tgt = tmp_delta_path("exp_conn_undef_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        exports = [
            Export(
                name="bad_export",
                type="delta",
                connection="nonexistent_conn",
                table="output",
                on_failure="abort",
            )
        ]
        thread = _make_thread("exp_conn_undef_thread", src, tgt, exports=exports)

        with pytest.raises(ExportError, match="nonexistent_conn"):
            execute_thread(spark, thread, connections={})

    def test_export_connection_resolved_to_abfss_path(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Export with connection resolves to abfss:// path when writing."""
        from unittest.mock import patch

        src = tmp_delta_path("exp_conn_src")
        tgt = tmp_delta_path("exp_conn_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        conn = OneLakeConnection(
            type="onelake",
            workspace="ws-exp",
            lakehouse="lh-exp",
        )
        connections = {"export_conn": conn}

        exports = [
            Export(
                name="conn_export",
                type="delta",
                connection="export_conn",
                table="archive",
                on_failure="abort",
            )
        ]
        thread = _make_thread("exp_conn_thread", src, tgt, exports=exports)

        expected_path = "abfss://ws-exp@onelake.dfs.fabric.microsoft.com/lh-exp/Tables/archive"

        captured_export_paths: list[str] = []

        def fake_write_export(spark_arg, df_arg, export_arg, row_count=None):
            from weevr.telemetry.results import ExportResult

            captured_export_paths.append(export_arg.path or "")
            return ExportResult(
                name=export_arg.name,
                type=export_arg.type,
                target=export_arg.path or "",
                status="success",
                rows_written=df_arg.count(),
                duration_ms=0.0,
            )

        with patch("weevr.engine.executor.write_export", side_effect=fake_write_export):
            execute_thread(spark, thread, connections=connections)

        assert len(captured_export_paths) == 1
        assert captured_export_paths[0] == expected_path


class TestResolveWithBlock:
    """Unit tests for _resolve_with_block — CTE resolution phase."""

    def test_single_cte_filter_registered_in_sources(self, spark: SparkSession) -> None:
        """A CTE with a filter step produces a filtered DataFrame in the enriched registry."""
        from weevr.engine.executor import _resolve_with_block

        orders = spark.createDataFrame(
            [{"id": 1, "amount": 200}, {"id": 2, "amount": 30}, {"id": 3, "amount": 150}]
        )
        sources_map = {"orders": orders}

        thread = Thread.model_validate(
            {
                "name": "t",
                "config_version": "1",
                "sources": {"orders": {"type": "delta", "alias": "/fake/path"}},
                "steps": [],
                "target": {"path": "/fake/target"},
                "with": {
                    "big_orders": {
                        "from": "orders",
                        "steps": [{"filter": {"expr": "amount > 100"}}],
                    }
                },
            }
        )

        result = _resolve_with_block(thread, sources_map)

        assert "big_orders" in result
        assert result["big_orders"].count() == 2

    def test_original_sources_preserved(self, spark: SparkSession) -> None:
        """_resolve_with_block does not mutate the original sources_map."""
        from weevr.engine.executor import _resolve_with_block

        orders = spark.createDataFrame([{"id": 1, "amount": 50}])
        sources_map = {"orders": orders}

        thread = Thread.model_validate(
            {
                "name": "t",
                "config_version": "1",
                "sources": {"orders": {"type": "delta", "alias": "/fake/path"}},
                "steps": [],
                "target": {"path": "/fake/target"},
                "with": {
                    "filtered": {"from": "orders", "steps": [{"filter": {"expr": "amount > 10"}}]}
                },
            }
        )

        _resolve_with_block(thread, sources_map)

        assert set(sources_map.keys()) == {"orders"}

    def test_cte_result_joinable_in_main_pipeline(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """CTE registered by _resolve_with_block can be joined in the main pipeline."""
        src = tmp_delta_path("cte_join_src")
        tgt = tmp_delta_path("cte_join_tgt")

        create_delta_table(
            spark,
            src,
            [
                {"id": 1, "amount": 200},
                {"id": 2, "amount": 30},
                {"id": 3, "amount": 150},
            ],
        )

        # CTE: filter to high-value rows, then keep only `id` to avoid join column ambiguity
        thread = Thread.model_validate(
            {
                "name": "cte_join",
                "config_version": "1",
                "sources": {"orders": {"type": "delta", "alias": src}},
                "steps": [{"join": {"source": "big_orders", "on": ["id"], "type": "inner"}}],
                "target": {"path": tgt},
                "with": {
                    "big_orders": {
                        "from": "orders",
                        "steps": [
                            {"filter": {"expr": "amount > 100"}},
                            {"select": {"columns": ["id"]}},
                        ],
                    }
                },
            }
        )

        result = execute_thread(spark, thread)

        assert result.rows_written == 2

    def test_cte_chaining_b_from_a(self, spark: SparkSession) -> None:
        """CTE B declared after CTE A can reference CTE A as its source."""
        from weevr.engine.executor import _resolve_with_block

        data = spark.createDataFrame(
            [{"id": 1, "val": 10}, {"id": 2, "val": 20}, {"id": 3, "val": 30}]
        )
        sources_map = {"raw": data}

        thread = Thread.model_validate(
            {
                "name": "chain",
                "config_version": "1",
                "sources": {"raw": {"type": "delta", "alias": "/fake/path"}},
                "steps": [],
                "target": {"path": "/fake/target"},
                "with": {
                    "cte_a": {"from": "raw", "steps": [{"filter": {"expr": "val >= 20"}}]},
                    "cte_b": {"from": "cte_a", "steps": [{"filter": {"expr": "val >= 30"}}]},
                },
            }
        )

        result = _resolve_with_block(thread, sources_map)

        assert result["cte_a"].count() == 2
        assert result["cte_b"].count() == 1

    def test_thread_without_with_block_unchanged(self, spark: SparkSession, tmp_delta_path) -> None:
        """Thread with no with: block executes without change — backward compatibility."""
        src = tmp_delta_path("no_cte_src")
        tgt = tmp_delta_path("no_cte_tgt")
        create_delta_table(spark, src, [{"id": 1}, {"id": 2}, {"id": 3}])

        thread = Thread(
            name="no_cte",
            config_version="1",
            sources={"main": Source(type="delta", alias=src)},
            steps=[],
            target=Target(path=tgt),
        )

        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.rows_written == 3


class TestResolveWithBlockTelemetry:
    """CTE telemetry spans emitted by _resolve_with_block."""

    def test_two_ctes_produce_two_child_spans(self, spark: SparkSession) -> None:
        """A thread with 2 CTEs produces 2 cte:* child spans in the collector."""
        from weevr.engine.executor import _resolve_with_block
        from weevr.telemetry.collector import SpanCollector
        from weevr.telemetry.span import generate_trace_id

        data = spark.createDataFrame(
            [{"id": 1, "val": 10}, {"id": 2, "val": 20}, {"id": 3, "val": 30}]
        )
        sources_map = {"raw": data}

        thread = Thread.model_validate(
            {
                "name": "multi_cte",
                "config_version": "1",
                "sources": {"raw": {"type": "delta", "alias": "/fake/path"}},
                "steps": [],
                "target": {"path": "/fake/target"},
                "with": {
                    "cte_low": {"from": "raw", "steps": [{"filter": {"expr": "val <= 20"}}]},
                    "cte_high": {"from": "raw", "steps": [{"filter": {"expr": "val >= 20"}}]},
                },
            }
        )

        collector = SpanCollector(generate_trace_id())
        _resolve_with_block(thread, sources_map, collector=collector)

        spans = collector.get_spans()
        cte_spans = [s for s in spans if s.name.startswith("cte:")]
        assert len(cte_spans) == 2
        cte_names = {s.name for s in cte_spans}
        assert cte_names == {"cte:cte_low", "cte:cte_high"}

    def test_cte_span_has_duration(self, spark: SparkSession) -> None:
        """Each CTE span records a non-None end_time so duration can be derived."""
        from weevr.engine.executor import _resolve_with_block
        from weevr.telemetry.collector import SpanCollector
        from weevr.telemetry.span import generate_trace_id

        data = spark.createDataFrame([{"id": 1, "val": 5}, {"id": 2, "val": 15}])
        sources_map = {"raw": data}

        thread = Thread.model_validate(
            {
                "name": "dur_cte",
                "config_version": "1",
                "sources": {"raw": {"type": "delta", "alias": "/fake/path"}},
                "steps": [],
                "target": {"path": "/fake/target"},
                "with": {"filtered": {"from": "raw", "steps": [{"filter": {"expr": "val > 10"}}]}},
            }
        )

        collector = SpanCollector(generate_trace_id())
        _resolve_with_block(thread, sources_map, collector=collector)

        spans = collector.get_spans()
        cte_spans = [s for s in spans if s.name.startswith("cte:")]
        assert len(cte_spans) == 1
        assert cte_spans[0].end_time is not None
        assert cte_spans[0].end_time >= cte_spans[0].start_time

    def test_cte_span_has_row_count_attribute(self, spark: SparkSession) -> None:
        """Each CTE span includes a row_count attribute matching the output row count."""
        from weevr.engine.executor import _resolve_with_block
        from weevr.telemetry.collector import SpanCollector
        from weevr.telemetry.span import generate_trace_id

        data = spark.createDataFrame(
            [{"id": 1, "val": 10}, {"id": 2, "val": 20}, {"id": 3, "val": 30}]
        )
        sources_map = {"raw": data}

        thread = Thread.model_validate(
            {
                "name": "rowcount_cte",
                "config_version": "1",
                "sources": {"raw": {"type": "delta", "alias": "/fake/path"}},
                "steps": [],
                "target": {"path": "/fake/target"},
                "with": {"big": {"from": "raw", "steps": [{"filter": {"expr": "val >= 20"}}]}},
            }
        )

        collector = SpanCollector(generate_trace_id())
        _resolve_with_block(thread, sources_map, collector=collector)

        spans = collector.get_spans()
        cte_spans = [s for s in spans if s.name == "cte:big"]
        assert len(cte_spans) == 1
        assert cte_spans[0].attributes["row_count"] == 2

    def test_cte_span_parented_to_thread_span(self, spark: SparkSession) -> None:
        """CTE spans use the supplied parent_span_id so they appear under the thread span."""
        from weevr.engine.executor import _resolve_with_block
        from weevr.telemetry.collector import SpanCollector
        from weevr.telemetry.span import generate_span_id, generate_trace_id

        data = spark.createDataFrame([{"id": 1}, {"id": 2}])
        sources_map = {"raw": data}

        thread = Thread.model_validate(
            {
                "name": "parented_cte",
                "config_version": "1",
                "sources": {"raw": {"type": "delta", "alias": "/fake/path"}},
                "steps": [],
                "target": {"path": "/fake/target"},
                "with": {"subset": {"from": "raw", "steps": [{"filter": {"expr": "id > 0"}}]}},
            }
        )

        collector = SpanCollector(generate_trace_id())
        thread_span_id = generate_span_id()
        _resolve_with_block(thread, sources_map, collector=collector, parent_span_id=thread_span_id)

        spans = collector.get_spans()
        cte_spans = [s for s in spans if s.name.startswith("cte:")]
        assert len(cte_spans) == 1
        assert cte_spans[0].parent_span_id == thread_span_id

    def test_no_collector_no_spans_no_error(self, spark: SparkSession) -> None:
        """_resolve_with_block works correctly when no collector is provided."""
        from weevr.engine.executor import _resolve_with_block

        data = spark.createDataFrame([{"id": 1}, {"id": 2}])
        sources_map = {"raw": data}

        thread = Thread.model_validate(
            {
                "name": "no_collector",
                "config_version": "1",
                "sources": {"raw": {"type": "delta", "alias": "/fake/path"}},
                "steps": [],
                "target": {"path": "/fake/target"},
                "with": {"subset": {"from": "raw", "steps": [{"filter": {"expr": "id > 0"}}]}},
            }
        )

        result = _resolve_with_block(thread, sources_map)
        assert "subset" in result

    def test_cte_spans_visible_in_execute_thread_collector(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """CTE spans appear in the collector when execute_thread is called with collector."""
        from weevr.telemetry.collector import SpanCollector
        from weevr.telemetry.span import generate_trace_id

        src = tmp_delta_path("cte_tel_src")
        tgt = tmp_delta_path("cte_tel_tgt")

        create_delta_table(
            spark,
            src,
            [{"id": 1, "val": 10}, {"id": 2, "val": 20}, {"id": 3, "val": 30}],
        )

        thread = Thread.model_validate(
            {
                "name": "cte_tel_thread",
                "config_version": "1",
                "sources": {"orders": {"type": "delta", "alias": src}},
                "steps": [],
                "target": {"path": tgt},
                "with": {
                    "cte_a": {"from": "orders", "steps": [{"filter": {"expr": "val >= 20"}}]},
                    "cte_b": {"from": "cte_a", "steps": [{"filter": {"expr": "val >= 30"}}]},
                },
            }
        )

        collector = SpanCollector(generate_trace_id())
        execute_thread(spark, thread, collector=collector)

        spans = collector.get_spans()
        cte_spans = [s for s in spans if s.name.startswith("cte:")]
        assert len(cte_spans) == 2
        cte_names = {s.name for s in cte_spans}
        assert cte_names == {"cte:cte_a", "cte:cte_b"}

        # Each CTE span should have a row_count attribute
        for span in cte_spans:
            assert "row_count" in span.attributes


class TestWithBlockIntegration:
    """End-to-end tests for the with: block — CTE + alias + filter patterns."""

    def test_sap_invoices_pattern(self, spark: SparkSession, tmp_delta_path) -> None:
        """Multi-CTE pipeline: aggregate CTEs and windowed CTE joined three times with alias+filter.

        Mirrors the SAP customer invoices pattern: high-value aggregation, category count,
        and ranked invoices per customer joined for top-3 positions.
        """
        customers_path = tmp_delta_path("sap_customers")
        invoices_path = tmp_delta_path("sap_invoices")
        products_path = tmp_delta_path("sap_products")
        regions_path = tmp_delta_path("sap_regions")
        tgt = tmp_delta_path("sap_tgt")

        # 3 customers
        create_delta_table(
            spark,
            customers_path,
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Carol"},
            ],
        )

        # 9 invoices total across 3 customers — mix of categories and amounts.
        # Customer 1: 3 premium invoices → ranked 1st=300, 2nd=200, 3rd=100
        # Customer 2: 2 premium invoices → ranked 1st=500, 2nd=150; 3rd rank absent
        # Customer 3: 1 premium invoice → ranked 1st=400; 2nd/3rd ranks absent
        #             2 category=A invoices for the product_count CTE
        create_delta_table(
            spark,
            invoices_path,
            [
                # customer 1 — three premium invoices
                {"id": 1, "customer_id": 1, "amount": 300, "category": "premium"},
                {"id": 2, "customer_id": 1, "amount": 200, "category": "premium"},
                {"id": 3, "customer_id": 1, "amount": 100, "category": "premium"},
                # customer 2 — two premium, one category=B (not picked up by any CTE filter)
                {"id": 4, "customer_id": 2, "amount": 500, "category": "premium"},
                {"id": 5, "customer_id": 2, "amount": 150, "category": "premium"},
                {"id": 6, "customer_id": 2, "amount": 50, "category": "B"},
                # customer 3 — one premium, two category=A
                {"id": 7, "customer_id": 3, "amount": 400, "category": "premium"},
                {"id": 8, "customer_id": 3, "amount": 250, "category": "A"},
                {"id": 9, "customer_id": 3, "amount": 120, "category": "A"},
            ],
        )

        # Products and regions are declared sources — used to demonstrate 4+ source support
        create_delta_table(
            spark,
            products_path,
            [{"id": 1, "name": "Widget"}, {"id": 2, "name": "Gadget"}],
        )
        create_delta_table(
            spark,
            regions_path,
            [{"id": 1, "name": "North"}, {"id": 2, "name": "South"}],
        )

        thread = Thread.model_validate(
            {
                "name": "sap_invoices",
                "config_version": "1",
                "sources": {
                    "customers": {"type": "delta", "alias": customers_path},
                    "invoices": {"type": "delta", "alias": invoices_path},
                    "products": {"type": "delta", "alias": products_path},
                    "regions": {"type": "delta", "alias": regions_path},
                },
                "target": {"path": tgt},
                "with": {
                    # CTE 1: filter amount > 100, aggregate total by customer_id
                    "high_value": {
                        "from": "invoices",
                        "steps": [
                            {"filter": {"expr": "amount > 100"}},
                            {
                                "aggregate": {
                                    "group_by": ["customer_id"],
                                    "measures": {"total_amount": "sum(amount)"},
                                }
                            },
                        ],
                    },
                    # CTE 2: filter category=A, count invoices per customer_id
                    "product_count": {
                        "from": "invoices",
                        "steps": [
                            {"filter": {"expr": "category = 'A'"}},
                            {
                                "aggregate": {
                                    "group_by": ["customer_id"],
                                    "measures": {"cat_a_count": "count(*)"},
                                }
                            },
                        ],
                    },
                    # CTE 3: filter premium, derive tier, window rank by amount desc per customer
                    "ranked": {
                        "from": "invoices",
                        "steps": [
                            {"filter": {"expr": "category = 'premium'"}},
                            {
                                "derive": {
                                    "columns": {
                                        "tier": (
                                            "CASE WHEN amount >= 300"
                                            " THEN 'high' ELSE 'standard' END"
                                        )
                                    }
                                }
                            },
                            {
                                "window": {
                                    "functions": {"inv_rank": "rank()"},
                                    "partition_by": ["customer_id"],
                                    "order_by": ["amount DESC"],
                                }
                            },
                            # Drop invoice id to avoid ambiguity with customers.id in joins
                            {"select": {"columns": ["customer_id", "amount", "tier", "inv_rank"]}},
                        ],
                    },
                },
                "steps": [
                    # Start from customers, join high_value (only bring total_amount)
                    {
                        "join": {
                            "source": "high_value",
                            "on": [{"left": "id", "right": "customer_id"}],
                            "type": "left",
                            "include": ["total_amount"],
                        }
                    },
                    # Join product_count (only bring cat_a_count)
                    {
                        "join": {
                            "source": "product_count",
                            "on": [{"left": "id", "right": "customer_id"}],
                            "type": "left",
                            "include": ["cat_a_count"],
                        }
                    },
                    # Join ranked three times: rank 1, rank 2, rank 3 — each with a unique alias
                    {
                        "join": {
                            "source": "ranked",
                            "on": [{"left": "id", "right": "customer_id"}],
                            "type": "left",
                            "filter": "inv_rank = 1",
                            "alias": "r1",
                            "include": ["amount"],
                            "prefix": "top1_",
                        }
                    },
                    {
                        "join": {
                            "source": "ranked",
                            "on": [{"left": "id", "right": "customer_id"}],
                            "type": "left",
                            "filter": "inv_rank = 2",
                            "alias": "r2",
                            "include": ["amount"],
                            "prefix": "top2_",
                        }
                    },
                    {
                        "join": {
                            "source": "ranked",
                            "on": [{"left": "id", "right": "customer_id"}],
                            "type": "left",
                            "filter": "inv_rank = 3",
                            "alias": "r3",
                            "include": ["amount"],
                            "prefix": "top3_",
                        }
                    },
                ],
            }
        )

        result = execute_thread(spark, thread)

        assert result.status == "success"
        # All 3 customers survive the left joins
        assert result.rows_written == 3

        written = spark.read.format("delta").load(tgt)

        # Schema: id, name, total_amount, cat_a_count, top1_amount, top2_amount, top3_amount
        col_names = set(written.columns)
        assert "id" in col_names
        assert "name" in col_names
        assert "total_amount" in col_names
        assert "cat_a_count" in col_names
        assert "top1_amount" in col_names
        assert "top2_amount" in col_names
        assert "top3_amount" in col_names

        rows = {r["id"]: r for r in written.collect()}

        # Customer 1: premium invoices — 300 and 200 are > 100 (100 is excluded)
        # Ranked (premium only): rank 1=300, rank 2=200, rank 3=100
        assert rows[1]["total_amount"] == 500
        assert rows[1]["cat_a_count"] is None  # no category=A invoices
        assert rows[1]["top1_amount"] == 300
        assert rows[1]["top2_amount"] == 200
        assert rows[1]["top3_amount"] == 100

        # Customer 2: 500 and 150 are > 100; category=B invoice (50) is excluded
        # Ranked (premium only): rank 1=500, rank 2=150; no rank 3
        assert rows[2]["total_amount"] == 650
        assert rows[2]["cat_a_count"] is None
        assert rows[2]["top1_amount"] == 500
        assert rows[2]["top2_amount"] == 150
        assert rows[2]["top3_amount"] is None  # only 2 premium invoices

        # Customer 3: 400 (premium), 250 and 120 (category=A) all > 100 → total 770
        # Ranked (premium only): rank 1=400; no rank 2 or 3
        assert rows[3]["total_amount"] == 770
        assert rows[3]["cat_a_count"] == 2
        assert rows[3]["top1_amount"] == 400
        assert rows[3]["top2_amount"] is None  # only 1 premium invoice
        assert rows[3]["top3_amount"] is None


class TestWithBlockColumnControlIntegration:
    """Integration tests for CTE + join column control (include/exclude/rename/prefix).

    Mirrors the SAP dimension notebook pattern where a self-join is used three
    times against a hierarchy table to pull in labelled description columns.
    """

    def test_sap_dimension_pattern_end_to_end(self, spark: SparkSession, tmp_delta_path) -> None:
        """Full SAP dimension pattern: hierarchy CTE + triple self-join with column control.

        Verifies:
        - derive step in a CTE produces a new column
        - include restricts which source columns come through each join
        - rename relabels the included column to its semantic name
        - prefix + rename works together (prefix applied first, rename refs post-prefix name)
        - excluded columns are absent from the result
        - the same source joined multiple times under different aliases works correctly
        """
        from weevr.engine.executor import _resolve_with_block

        materials = spark.createDataFrame(
            [
                {"mat_id": 1, "mat_name": "Widget A", "group_code": "G1", "subgroup_code": "S1"},
                {"mat_id": 2, "mat_name": "Widget B", "group_code": "G2", "subgroup_code": "S2"},
                {"mat_id": 3, "mat_name": "Gadget X", "group_code": "G1", "subgroup_code": "S3"},
            ]
        )
        hierarchy = spark.createDataFrame(
            [
                {"code": "G1", "description": "Group One", "level": 1},
                {"code": "G2", "description": "Group Two", "level": 1},
                {"code": "S1", "description": "Subgroup One", "level": 2},
                {"code": "S2", "description": "Subgroup Two", "level": 2},
                {"code": "S3", "description": "Subgroup Three", "level": 2},
            ]
        )
        sources_map = {"materials": materials, "hierarchy": hierarchy}

        thread = Thread.model_validate(
            {
                "name": "sap_dim",
                "config_version": "1",
                "sources": {
                    "materials": {"type": "delta", "alias": "/fake/materials"},
                    "hierarchy": {"type": "delta", "alias": "/fake/hierarchy"},
                },
                "steps": [],
                "target": {"path": "/fake/target"},
                "with": {
                    # CTE 1: enrich hierarchy with a concatenated label column
                    "hierarchy_enriched": {
                        "from": "hierarchy",
                        "steps": [
                            {
                                "derive": {
                                    "columns": {
                                        "enriched_label": "concat(code, ' - ', description)"
                                    }
                                }
                            }
                        ],
                    },
                    # CTE 2: build the product dimension via triple self-join
                    "product_dim": {
                        "from": "materials",
                        "steps": [
                            # Join 1: pull group description, rename to group_desc
                            {
                                "join": {
                                    "source": "hierarchy_enriched",
                                    "on": [{"left": "group_code", "right": "code"}],
                                    "type": "left",
                                    "alias": "grp",
                                    "include": ["description"],
                                    "rename": {"description": "group_desc"},
                                }
                            },
                            # Join 2: pull subgroup description, rename to subgroup_desc
                            {
                                "join": {
                                    "source": "hierarchy_enriched",
                                    "on": [{"left": "subgroup_code", "right": "code"}],
                                    "type": "left",
                                    "alias": "sub",
                                    "include": ["description"],
                                    "rename": {"description": "subgroup_desc"},
                                }
                            },
                            # Join 3: pull enriched label with prefix + rename
                            {
                                "join": {
                                    "source": "hierarchy_enriched",
                                    "on": [{"left": "group_code", "right": "code"}],
                                    "type": "left",
                                    "alias": "lbl",
                                    "include": ["enriched_label"],
                                    "prefix": "h_",
                                    "rename": {"h_enriched_label": "group_label"},
                                }
                            },
                        ],
                    },
                },
            }
        )

        enriched = _resolve_with_block(thread, sources_map)

        # hierarchy_enriched CTE should have the derived column
        assert "enriched_label" in enriched["hierarchy_enriched"].columns
        sample = enriched["hierarchy_enriched"].filter("code = 'G1'").collect()
        assert sample[0]["enriched_label"] == "G1 - Group One"

        # product_dim CTE should have all three renamed columns
        product_dim_df = enriched["product_dim"]
        cols = set(product_dim_df.columns)

        assert "group_desc" in cols, "group_desc missing — join 1 rename failed"
        assert "subgroup_desc" in cols, "subgroup_desc missing — join 2 rename failed"
        assert "group_label" in cols, "group_label missing — join 3 prefix+rename failed"

        # raw hierarchy columns must not bleed through
        assert "description" not in cols, "description should have been excluded via include"
        assert "level" not in cols, "level was never included, must be absent"
        assert "h_enriched_label" not in cols, "intermediate prefix name must be renamed away"
        assert "enriched_label" not in cols, "enriched_label must not appear (prefix + rename)"

        # verify row count and spot-check values for a known material
        assert product_dim_df.count() == 3
        row = product_dim_df.filter("mat_id = 1").collect()[0]
        assert row["group_desc"] == "Group One"
        assert row["subgroup_desc"] == "Subgroup One"
        assert row["group_label"] == "G1 - Group One"

    def test_sap_dimension_full_pipeline_with_two_ctes(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Full end-to-end: 2 CTEs + main pipeline via execute_thread.

        CTE 1 (hierarchy_enriched): enriches hierarchy with a derived label.
        CTE 2 (product_dim): triple self-join with include+rename+prefix.
        Main pipeline: joins product_dim CTE into materials.

        Verifies column control flows through to the written Delta output
        and that excluded/raw columns are absent.
        """
        src_materials = tmp_delta_path("sap_mat_src")
        src_hierarchy = tmp_delta_path("sap_hier_src")
        tgt = tmp_delta_path("sap_dim_tgt")

        create_delta_table(
            spark,
            src_materials,
            [
                {
                    "mat_id": 10,
                    "mat_name": "Part A",
                    "group_code": "GX",
                    "subgroup_code": "SX",
                },
            ],
        )
        create_delta_table(
            spark,
            src_hierarchy,
            [
                {"code": "GX", "description": "Group X", "level": 5},
                {"code": "SX", "description": "Subgroup X", "level": 6},
            ],
        )

        thread = Thread.model_validate(
            {
                "name": "sap_dim_full",
                "config_version": "1",
                "sources": {
                    "materials": {"type": "delta", "alias": src_materials},
                    "hierarchy": {"type": "delta", "alias": src_hierarchy},
                },
                "steps": [
                    {
                        "join": {
                            "source": "product_dim",
                            "on": [{"left": "mat_id", "right": "mat_id"}],
                            "type": "inner",
                            "include": [
                                "group_desc",
                                "subgroup_desc",
                                "group_label",
                            ],
                        }
                    },
                ],
                "target": {"path": tgt},
                "with": {
                    # CTE 1: enrich hierarchy with derived label
                    "hierarchy_enriched": {
                        "from": "hierarchy",
                        "steps": [
                            {
                                "derive": {
                                    "columns": {
                                        "enriched_label": ("concat(code, ' - ', description)")
                                    }
                                }
                            }
                        ],
                    },
                    # CTE 2: triple self-join with column control
                    "product_dim": {
                        "from": "materials",
                        "steps": [
                            {
                                "join": {
                                    "source": "hierarchy_enriched",
                                    "on": [
                                        {
                                            "left": "group_code",
                                            "right": "code",
                                        }
                                    ],
                                    "type": "left",
                                    "alias": "grp",
                                    "include": ["description"],
                                    "rename": {"description": "group_desc"},
                                }
                            },
                            {
                                "join": {
                                    "source": "hierarchy_enriched",
                                    "on": [
                                        {
                                            "left": "subgroup_code",
                                            "right": "code",
                                        }
                                    ],
                                    "type": "left",
                                    "alias": "sub",
                                    "include": ["description"],
                                    "rename": {"description": "subgroup_desc"},
                                }
                            },
                            {
                                "join": {
                                    "source": "hierarchy_enriched",
                                    "on": [
                                        {
                                            "left": "group_code",
                                            "right": "code",
                                        }
                                    ],
                                    "type": "left",
                                    "alias": "lbl",
                                    "include": ["enriched_label"],
                                    "prefix": "h_",
                                    "rename": {"h_enriched_label": "group_label"},
                                }
                            },
                            {
                                "select": {
                                    "columns": [
                                        "mat_id",
                                        "group_desc",
                                        "subgroup_desc",
                                        "group_label",
                                    ]
                                }
                            },
                        ],
                    },
                },
            }
        )

        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.rows_written == 1

        written = spark.read.format("delta").load(tgt)
        cols = set(written.columns)
        assert "group_desc" in cols
        assert "subgroup_desc" in cols
        assert "group_label" in cols
        assert "description" not in cols
        assert "level" not in cols
        assert "h_enriched_label" not in cols

        row = written.collect()[0]
        assert row["group_desc"] == "Group X"
        assert row["subgroup_desc"] == "Subgroup X"
        assert row["group_label"] == "GX - Group X"

        written = spark.read.format("delta").load(tgt)
        cols = set(written.columns)
        assert "group_desc" in cols
        assert "subgroup_desc" in cols
        assert "description" not in cols
        assert "level" not in cols
