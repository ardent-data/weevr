"""Tests for audit column resolution, injection, and context variable handling."""

import json

import pytest
from pyspark.sql import SparkSession

from weevr.errors.exceptions import ConfigError, ExecutionError
from weevr.model.source import Source
from weevr.operations.audit import (
    BUILTIN_AUDIT_PRESETS,
    AuditContext,
    apply_audit_exclusions,
    build_sources_json,
    inject_audit_columns,
    resolve_audit_columns,
    resolve_template_columns,
)


def _ctx(**overrides: str) -> AuditContext:
    """Build an AuditContext with sensible defaults, overriding any field."""
    defaults = {
        "thread_name": "stg_orders",
        "thread_qualified_key": "staging.stg_orders",
        "thread_source": "bronze.orders",
        "thread_sources_json": "[]",
        "weave_name": "staging",
        "loom_name": "my-project",
    }
    defaults.update(overrides)
    return AuditContext(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# resolve_audit_columns
# ---------------------------------------------------------------------------


class TestResolveAuditColumns:
    """Test additive merge across loom → weave → thread."""

    def test_loom_only(self):
        """Loom columns returned when weave and thread are None."""
        result = resolve_audit_columns({"_a": "1", "_b": "2"}, None, None)
        assert result == {"_a": "1", "_b": "2"}

    def test_weave_only(self):
        """Weave columns returned when loom and thread are None."""
        result = resolve_audit_columns(None, {"_w": "w_expr"}, None)
        assert result == {"_w": "w_expr"}

    def test_thread_only(self):
        """Thread columns returned when loom and weave are None."""
        result = resolve_audit_columns(None, None, {"_t": "t_expr"})
        assert result == {"_t": "t_expr"}

    def test_all_three_additive(self):
        """All three levels merge additively."""
        result = resolve_audit_columns(
            {"_a": "loom"},
            {"_b": "weave"},
            {"_c": "thread"},
        )
        assert result == {"_a": "loom", "_b": "weave", "_c": "thread"}

    def test_weave_overrides_loom(self):
        """Weave value overrides loom for the same key."""
        result = resolve_audit_columns(
            {"_x": "from_loom"},
            {"_x": "from_weave"},
            None,
        )
        assert result["_x"] == "from_weave"

    def test_thread_overrides_weave(self):
        """Thread value overrides weave for the same key."""
        result = resolve_audit_columns(
            None,
            {"_x": "from_weave"},
            {"_x": "from_thread"},
        )
        assert result["_x"] == "from_thread"

    def test_all_none_returns_empty(self):
        """All None returns empty dict."""
        assert resolve_audit_columns(None, None, None) == {}

    def test_multiple_columns_per_level(self):
        """Multiple columns at each level merge correctly (EC-002)."""
        result = resolve_audit_columns(
            {"_l1": "a", "_l2": "b"},
            {"_w1": "c", "_w2": "d"},
            {"_t1": "e"},
        )
        assert len(result) == 5


# ---------------------------------------------------------------------------
# apply_audit_exclusions
# ---------------------------------------------------------------------------


class TestApplyAuditExclusions:
    """Test glob-pattern exclusion of inherited audit columns."""

    def test_exclude_exact_match(self):
        """Exact column name is removed from the result."""
        columns = {"_loaded_at": "current_timestamp()", "_run_id": "uuid()"}
        result = apply_audit_exclusions(columns, ["_loaded_at"])
        assert "_loaded_at" not in result
        assert "_run_id" in result

    def test_exclude_glob_pattern(self):
        """Glob pattern removes all matching columns."""
        columns = {
            "_batch_id": "uuid()",
            "_batch_ts": "current_timestamp()",
            "_run_id": "uuid()",
        }
        result = apply_audit_exclusions(columns, ["_batch_*"])
        assert "_batch_id" not in result
        assert "_batch_ts" not in result
        assert "_run_id" in result

    def test_exclude_no_match_is_noop(self):
        """Pattern that matches nothing returns all columns unchanged."""
        columns = {"_loaded_at": "current_timestamp()", "_run_id": "uuid()"}
        result = apply_audit_exclusions(columns, ["_nonexistent_*"])
        assert result == columns

    def test_exclude_multiple_patterns(self):
        """Multiple patterns are each applied; union of matches is removed."""
        columns = {
            "_batch_id": "uuid()",
            "_loaded_at": "current_timestamp()",
            "_run_id": "uuid()",
            "_source": "'bronze'",
        }
        result = apply_audit_exclusions(columns, ["_batch_*", "_loaded_at"])
        assert "_batch_id" not in result
        assert "_loaded_at" not in result
        assert "_run_id" in result
        assert "_source" in result

    def test_exclude_empty_list(self):
        """Empty exclude list returns all columns unchanged."""
        columns = {"_loaded_at": "current_timestamp()", "_run_id": "uuid()"}
        result = apply_audit_exclusions(columns, [])
        assert result == columns

    def test_exclude_none(self):
        """None exclude returns all columns unchanged."""
        columns = {"_loaded_at": "current_timestamp()", "_run_id": "uuid()"}
        result = apply_audit_exclusions(columns, None)
        assert result == columns


# ---------------------------------------------------------------------------
# Context variable resolution (EC-004) — tested through inject_audit_columns
# ---------------------------------------------------------------------------


@pytest.mark.spark
class TestContextVariableResolution:
    """Test ${namespace.property} substitution via the public inject interface."""

    @pytest.fixture()
    def simple_df(self, spark: SparkSession):
        """Simple DataFrame for context variable tests."""
        return spark.createDataFrame([{"id": 1}])

    def test_thread_name(self, simple_df):
        """${thread.name} resolves to thread name."""
        ctx = _ctx(thread_name="orders")
        result = inject_audit_columns(simple_df, {"_v": "'${thread.name}'"}, ctx)
        assert result.collect()[0]["_v"] == "orders"

    def test_thread_qualified_key(self, simple_df):
        """${thread.qualified_key} resolves to qualified key."""
        ctx = _ctx(thread_qualified_key="staging.orders")
        result = inject_audit_columns(simple_df, {"_v": "'${thread.qualified_key}'"}, ctx)
        assert result.collect()[0]["_v"] == "staging.orders"

    def test_thread_source(self, simple_df):
        """${thread.source} resolves to primary source alias."""
        ctx = _ctx(thread_source="bronze.raw_orders")
        result = inject_audit_columns(simple_df, {"_v": "'${thread.source}'"}, ctx)
        assert result.collect()[0]["_v"] == "bronze.raw_orders"

    def test_thread_source_none_resolves_empty(self, simple_df):
        """${thread.source} resolves to empty string when source is None."""
        ctx = _ctx()
        # Override thread_source to None via dataclass replacement
        ctx_none = AuditContext(
            thread_name=ctx.thread_name,
            thread_qualified_key=ctx.thread_qualified_key,
            thread_source=None,
            thread_sources_json=ctx.thread_sources_json,
            weave_name=ctx.weave_name,
            loom_name=ctx.loom_name,
        )
        result = inject_audit_columns(simple_df, {"_v": "'${thread.source}'"}, ctx_none)
        assert result.collect()[0]["_v"] == ""

    def test_thread_sources(self, simple_df):
        """${thread.sources} resolves to JSON array."""
        sources_json = '[{"name": "raw", "alias": "bronze.raw", "type": "primary"}]'
        ctx = _ctx(thread_sources_json=sources_json)
        result = inject_audit_columns(simple_df, {"_v": "'${thread.sources}'"}, ctx)
        assert result.collect()[0]["_v"] == sources_json

    def test_weave_name(self, simple_df):
        """${weave.name} resolves to weave name."""
        ctx = _ctx(weave_name="staging")
        result = inject_audit_columns(simple_df, {"_v": "'${weave.name}'"}, ctx)
        assert result.collect()[0]["_v"] == "staging"

    def test_loom_name(self, simple_df):
        """${loom.name} resolves to loom name."""
        ctx = _ctx(loom_name="my-project")
        result = inject_audit_columns(simple_df, {"_v": "'${loom.name}'"}, ctx)
        assert result.collect()[0]["_v"] == "my-project"

    def test_multiple_vars_in_one_expression(self, simple_df):
        """Multiple variables in a single expression all resolve."""
        ctx = _ctx(thread_name="orders", weave_name="staging")
        result = inject_audit_columns(
            simple_df,
            {"_v": "concat('${thread.name}', '-', '${weave.name}')"},
            ctx,
        )
        assert result.collect()[0]["_v"] == "orders-staging"

    def test_unknown_var_left_unresolved(self, simple_df):
        """Unknown context variable passes through without error."""
        ctx = _ctx()
        # Unknown properties are left in the expression string; Spark may
        # interpret ${} in SQL string literals as empty via variable substitution.
        result = inject_audit_columns(simple_df, {"_v": "'${thread.unknown_prop}'"}, ctx)
        # Should not raise — the expression is valid SQL regardless of resolution
        assert "_v" in result.columns

    def test_no_vars_passthrough(self, simple_df):
        """Expression without variables evaluates directly."""
        ctx = _ctx()
        result = inject_audit_columns(simple_df, {"_v": "current_timestamp()"}, ctx)
        assert result.collect()[0]["_v"] is not None


# ---------------------------------------------------------------------------
# inject_audit_columns (requires Spark)
# ---------------------------------------------------------------------------


@pytest.mark.spark
class TestInjectAuditColumns:
    """Test audit column injection into DataFrames."""

    @pytest.fixture()
    def simple_df(self, spark: SparkSession):
        """Simple DataFrame with two columns."""
        return spark.createDataFrame([{"id": 1, "value": "a"}, {"id": 2, "value": "b"}])

    def test_single_column_literal(self, simple_df):
        """Inject a single audit column with a literal expression."""
        result = inject_audit_columns(simple_df, {"_loaded": "'yes'"}, _ctx())
        assert "_loaded" in result.columns
        rows = result.select("_loaded").distinct().collect()
        assert rows[0]["_loaded"] == "yes"

    def test_multiple_columns(self, simple_df):
        """Inject multiple audit columns."""
        result = inject_audit_columns(
            simple_df,
            {"_a": "1", "_b": "2"},
            _ctx(),
        )
        assert "_a" in result.columns
        assert "_b" in result.columns

    def test_current_timestamp_expression(self, simple_df):
        """current_timestamp() expression evaluates without error."""
        result = inject_audit_columns(simple_df, {"_ts": "current_timestamp()"}, _ctx())
        assert "_ts" in result.columns
        rows = result.select("_ts").collect()
        assert rows[0]["_ts"] is not None

    def test_context_var_in_expression(self, simple_df):
        """Context variables resolve within injected expressions."""
        ctx = _ctx(thread_name="orders")
        result = inject_audit_columns(simple_df, {"_thread": "'${thread.name}'"}, ctx)
        rows = result.select("_thread").distinct().collect()
        assert rows[0]["_thread"] == "orders"

    def test_empty_dict_noop(self, simple_df):
        """Empty audit_columns dict returns original DataFrame."""
        result = inject_audit_columns(simple_df, {}, _ctx())
        assert result.columns == simple_df.columns

    def test_conflict_raises_error(self, simple_df):
        """Column name conflict raises ExecutionError (EC-006)."""
        with pytest.raises(ExecutionError, match="Audit column name conflict"):
            inject_audit_columns(simple_df, {"id": "0"}, _ctx())

    def test_conflict_reports_column_names(self, simple_df):
        """Error message includes the conflicting column names."""
        with pytest.raises(ExecutionError, match="id"):
            inject_audit_columns(simple_df, {"id": "0"}, _ctx())

    def test_preserves_original_data(self, simple_df):
        """Audit columns do not alter existing data."""
        result = inject_audit_columns(simple_df, {"_audit": "'x'"}, _ctx())
        original_ids = sorted(r["id"] for r in result.select("id").collect())
        assert original_ids == [1, 2]


# ---------------------------------------------------------------------------
# build_sources_json
# ---------------------------------------------------------------------------


class TestBuildSourcesJson:
    """Test ${thread.sources} JSON construction."""

    def test_single_primary_source(self):
        """Single source is classified as primary."""
        sources = {"raw": Source(type="delta", alias="bronze.raw")}
        result = json.loads(build_sources_json(sources))
        assert len(result) == 1
        assert result[0]["type"] == "primary"
        assert result[0]["name"] == "raw"
        assert result[0]["alias"] == "bronze.raw"

    def test_lookup_source_classified(self):
        """Lookup source gets type=lookup."""
        sources = {
            "main": Source(type="delta", alias="bronze.main"),
            "ref": Source(lookup="currency_lookup"),
        }
        result = json.loads(build_sources_json(sources))
        assert result[0]["type"] == "primary"
        assert result[1]["type"] == "lookup"

    def test_secondary_sources(self):
        """Non-primary, non-lookup sources are secondary."""
        sources = {
            "main": Source(type="delta", alias="bronze.main"),
            "dim": Source(type="delta", alias="silver.dim"),
        }
        result = json.loads(build_sources_json(sources))
        assert result[0]["type"] == "primary"
        assert result[1]["type"] == "secondary"

    def test_empty_sources(self):
        """Empty sources produces empty JSON array."""
        result = json.loads(build_sources_json({}))
        assert result == []


# ---------------------------------------------------------------------------
# Mapping mode bypass (DEC-007) — audit columns survive target mapping
# ---------------------------------------------------------------------------


@pytest.mark.spark
class TestMappingModeBypass:
    """Verify audit columns are present after injection regardless of mapping mode."""

    @pytest.fixture()
    def mapped_df(self, spark: SparkSession):
        """DataFrame that has been through target mapping."""
        return spark.createDataFrame([{"id": 1, "name": "alice"}])

    def test_audit_columns_after_auto_mode(self, mapped_df):
        """Audit columns present alongside auto-mapped columns."""
        # Simulate post-mapping injection (auto mode already ran)
        result = inject_audit_columns(mapped_df, {"_audit": "'auto'"}, _ctx())
        assert "id" in result.columns
        assert "name" in result.columns
        assert "_audit" in result.columns

    def test_audit_columns_after_explicit_mode(self, mapped_df):
        """Audit columns present even when explicit mapping narrowed columns."""
        # After explicit mode, only declared columns remain.
        # Audit injection happens after mapping, so they bypass the filter.
        narrowed = mapped_df.select("id")
        result = inject_audit_columns(narrowed, {"_audit": "'explicit'"}, _ctx())
        assert "id" in result.columns
        assert "_audit" in result.columns
        # 'name' was dropped by explicit mapping — audit col still added
        assert "name" not in result.columns


class TestRunContextVariables:
    """Test ${run.timestamp} and ${run.id} context variable resolution."""

    def test_run_timestamp_resolves(self, spark: SparkSession) -> None:
        """${run.timestamp} resolves to the context value."""
        ctx = _ctx(run_timestamp="2026-03-15T10:30:00+00:00")
        df = spark.createDataFrame([(1,)], ["id"])
        result = inject_audit_columns(df, {"_loaded_at": "'${run.timestamp}'"}, ctx)
        rows = result.collect()
        assert rows[0]["_loaded_at"] == "2026-03-15T10:30:00+00:00"

    def test_run_id_resolves(self, spark: SparkSession) -> None:
        """${run.id} resolves to the context value."""
        ctx = _ctx(run_id="abc-123-def")
        df = spark.createDataFrame([(1,)], ["id"])
        result = inject_audit_columns(df, {"_run_id": "'${run.id}'"}, ctx)
        rows = result.collect()
        assert rows[0]["_run_id"] == "abc-123-def"

    def test_run_vars_mixed_with_thread(self, spark: SparkSession) -> None:
        """${run.*} and ${thread.*} resolve together."""
        ctx = _ctx(run_timestamp="2026-03-15T00:00:00+00:00")
        df = spark.createDataFrame([(1,)], ["id"])
        result = inject_audit_columns(
            df,
            {"_meta": "concat('${thread.name}', '_', '${run.timestamp}')"},
            ctx,
        )
        rows = result.collect()
        assert rows[0]["_meta"] == "stg_orders_2026-03-15T00:00:00+00:00"


# ---------------------------------------------------------------------------
# BUILTIN_AUDIT_PRESETS
# ---------------------------------------------------------------------------


class TestBuiltinAuditPresets:
    """Test the built-in preset constants."""

    def test_fabric_preset_has_9_columns(self):
        """fabric preset contains exactly the 9 specified columns with correct expressions."""
        preset = BUILTIN_AUDIT_PRESETS["fabric"]
        assert len(preset) == 9
        assert preset["_batch_id"] == "${param.batch_id}"
        assert preset["_batch_version"] == "${param.batch_version}"
        assert preset["_batch_source"] == "${param.batch_source}"
        assert preset["_batch_process_ts"] == "current_timestamp()"
        assert preset["_pipeline_id"] == "${param.pipeline_id}"
        assert preset["_pipeline_name"] == "${param.pipeline_name}"
        assert preset["_workspace_id"] == "${param.workspace_id}"
        assert preset["_spark_app_id"] == "spark_context().applicationId"
        assert preset["_task_ts"] == "current_timestamp()"

    def test_minimal_preset_has_3_columns(self):
        """minimal preset contains exactly the 3 specified columns with correct expressions."""
        preset = BUILTIN_AUDIT_PRESETS["minimal"]
        assert len(preset) == 3
        assert preset["_weevr_loaded_at"] == "current_timestamp()"
        assert preset["_weevr_run_id"] == "${param.run_id}"
        assert preset["_weevr_thread"] == "${thread.name}"


# ---------------------------------------------------------------------------
# resolve_template_columns
# ---------------------------------------------------------------------------


class TestResolveTemplateColumns:
    """Test template name resolution to merged column dicts."""

    def test_resolve_user_defined_template(self):
        """User-defined template is found by name."""
        user_templates = {"my_audit": {"_col": "expr()"}}
        result = resolve_template_columns(["my_audit"], user_templates)
        assert result == {"_col": "expr()"}

    def test_resolve_builtin_fallback(self):
        """Name not in user-defined is found in built-in presets."""
        result = resolve_template_columns(["minimal"], user_templates=None)
        assert "_weevr_loaded_at" in result
        assert "_weevr_run_id" in result
        assert "_weevr_thread" in result

    def test_resolve_unknown_raises_config_error(self):
        """Unknown template name raises ConfigError listing available templates."""
        with pytest.raises(ConfigError, match="unknown_tpl"):
            resolve_template_columns(["unknown_tpl"], user_templates=None)

    def test_resolve_unknown_error_lists_available(self):
        """ConfigError message includes available template names."""
        user_templates = {"my_tpl": {"_col": "1"}}
        with pytest.raises(ConfigError) as exc_info:
            resolve_template_columns(["unknown_tpl"], user_templates=user_templates)
        msg = str(exc_info.value)
        # Should mention built-in names and user-defined names
        assert "fabric" in msg or "minimal" in msg or "my_tpl" in msg

    def test_resolve_shadow_warning(self, caplog):
        """User-defined template with a built-in name logs a WARNING."""
        import logging

        user_templates = {"fabric": {"_col": "override()"}}
        with caplog.at_level(logging.WARNING, logger="weevr.operations.audit"):
            resolve_template_columns(["fabric"], user_templates)
        assert any("fabric" in record.message for record in caplog.records)
        assert any(record.levelno == logging.WARNING for record in caplog.records)

    def test_resolve_template_list_merges_in_order(self):
        """List of names merges columns; later names override earlier on collision."""
        user_templates = {
            "base": {"_a": "base_a", "_b": "base_b"},
            "override": {"_b": "override_b", "_c": "override_c"},
        }
        result = resolve_template_columns(["base", "override"], user_templates)
        assert result["_a"] == "base_a"
        assert result["_b"] == "override_b"
        assert result["_c"] == "override_c"

    def test_resolve_template_list_builtin_then_user(self):
        """Built-in followed by user-defined: user overrides built-in on collision."""
        user_templates = {"my_ext": {"_weevr_loaded_at": "custom_ts()", "_extra": "1"}}
        result = resolve_template_columns(["minimal", "my_ext"], user_templates)
        assert result["_weevr_loaded_at"] == "custom_ts()"
        assert result["_extra"] == "1"
        assert result["_weevr_run_id"] == "${param.run_id}"
