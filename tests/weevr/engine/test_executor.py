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

    def test_missing_target_path_raises_execution_error(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        src = tmp_delta_path("no_tgt_src")
        create_delta_table(spark, src, [{"id": 1}])

        # Thread with target that has neither alias nor path
        thread = Thread(
            name="no_path_thread",
            config_version="1",
            sources={"main": Source(type="delta", alias=src)},
            steps=[],
            target=Target(),  # no alias, no path
        )
        with pytest.raises(ExecutionError) as exc_info:
            execute_thread(spark, thread)

        assert "no_path_thread" in str(exc_info.value)

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
