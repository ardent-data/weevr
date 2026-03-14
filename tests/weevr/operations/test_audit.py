"""Tests for audit column resolution, injection, and context variable handling."""

import json

import pytest
from pyspark.sql import SparkSession

from weevr.errors.exceptions import ExecutionError
from weevr.model.source import Source
from weevr.operations.audit import (
    AuditContext,
    _resolve_context_variables,
    build_sources_json,
    inject_audit_columns,
    resolve_audit_columns,
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
# _resolve_context_variables (internal, but critical for EC-004)
# ---------------------------------------------------------------------------


class TestContextVariableResolution:
    """Test ${namespace.property} substitution in expressions."""

    def test_thread_name(self):
        """${thread.name} resolves to thread name."""
        ctx = _ctx(thread_name="orders")
        assert _resolve_context_variables("'${thread.name}'", ctx) == "'orders'"

    def test_thread_qualified_key(self):
        """${thread.qualified_key} resolves to qualified key."""
        ctx = _ctx(thread_qualified_key="staging.orders")
        result = _resolve_context_variables("'${thread.qualified_key}'", ctx)
        assert result == "'staging.orders'"

    def test_thread_source(self):
        """${thread.source} resolves to primary source alias."""
        ctx = _ctx(thread_source="bronze.raw_orders")
        result = _resolve_context_variables("'${thread.source}'", ctx)
        assert result == "'bronze.raw_orders'"

    def test_thread_sources(self):
        """${thread.sources} resolves to JSON array."""
        sources_json = '[{"name": "raw", "alias": "bronze.raw", "type": "primary"}]'
        ctx = _ctx(thread_sources_json=sources_json)
        result = _resolve_context_variables("'${thread.sources}'", ctx)
        assert result == f"'{sources_json}'"

    def test_weave_name(self):
        """${weave.name} resolves to weave name."""
        ctx = _ctx(weave_name="staging")
        assert _resolve_context_variables("'${weave.name}'", ctx) == "'staging'"

    def test_loom_name(self):
        """${loom.name} resolves to loom name."""
        ctx = _ctx(loom_name="my-project")
        assert _resolve_context_variables("'${loom.name}'", ctx) == "'my-project'"

    def test_multiple_vars_in_one_expression(self):
        """Multiple variables in a single expression all resolve."""
        ctx = _ctx(thread_name="orders", weave_name="staging")
        result = _resolve_context_variables("concat('${thread.name}', '-', '${weave.name}')", ctx)
        assert result == "concat('orders', '-', 'staging')"

    def test_unknown_var_left_unresolved(self):
        """Unknown context variable is left as-is."""
        ctx = _ctx()
        result = _resolve_context_variables("'${thread.unknown_prop}'", ctx)
        assert result == "'${thread.unknown_prop}'"

    def test_no_vars_passthrough(self):
        """Expression without variables passes through unchanged."""
        ctx = _ctx()
        assert _resolve_context_variables("current_timestamp()", ctx) == "current_timestamp()"


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
