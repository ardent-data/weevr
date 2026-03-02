"""Tests for lookup materializer."""

from unittest.mock import MagicMock, patch

import pytest

from weevr.engine.lookups import (
    LookupResult,
    _apply_narrow_pipeline,
    _check_unique_key,
    _validate_columns,
    cleanup_lookups,
    materialize_lookups,
    resolve_thread_lookups,
)
from weevr.errors.exceptions import LookupResolutionError
from weevr.model.lookup import Lookup
from weevr.model.source import Source
from weevr.telemetry.collector import SpanCollector
from weevr.telemetry.span import generate_trace_id


def _make_lookup(
    *,
    materialize: bool = True,
    strategy: str = "cache",
    key: list[str] | None = None,
    values: list[str] | None = None,
    filter: str | None = None,
    unique_key: bool = False,
    on_failure: str = "abort",
) -> Lookup:
    """Create a Lookup with a delta source for testing."""
    return Lookup(
        source=Source(type="delta", alias="db.ref_table"),
        materialize=materialize,
        strategy=strategy,  # type: ignore[arg-type]
        key=key,
        values=values,
        filter=filter,
        unique_key=unique_key,
        on_failure=on_failure,  # type: ignore[arg-type]
    )


class TestLookupResult:
    """Test LookupResult model."""

    def test_defaults(self):
        """LookupResult has sensible defaults."""
        r = LookupResult(name="test")
        assert r.name == "test"
        assert r.materialized is False
        assert r.strategy == "cache"
        assert r.row_count == 0

    def test_full_construction(self):
        """LookupResult with all fields."""
        r = LookupResult(
            name="ref",
            materialized=True,
            strategy="broadcast",
            row_count=500,
            duration_ms=123.4,
        )
        assert r.materialized is True
        assert r.strategy == "broadcast"

    def test_narrow_fields_defaults(self):
        """Narrow metadata fields default correctly."""
        r = LookupResult(name="test")
        assert r.key_columns is None
        assert r.value_columns is None
        assert r.filter_applied is False
        assert r.unique_key_checked is False
        assert r.unique_key_passed is None

    def test_narrow_fields_populated(self):
        """Narrow metadata fields accept values."""
        r = LookupResult(
            name="dim",
            materialized=True,
            key_columns=["id"],
            value_columns=["sk"],
            filter_applied=True,
            unique_key_checked=True,
            unique_key_passed=True,
        )
        assert r.key_columns == ["id"]
        assert r.value_columns == ["sk"]
        assert r.filter_applied is True
        assert r.unique_key_passed is True


class TestMaterializeLookups:
    """Test materialize_lookups."""

    @patch("weevr.engine.lookups.read_source")
    def test_materialize_cache(self, mock_read):
        """Cache strategy persists and counts the DataFrame."""
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_read.return_value = mock_df

        spark = MagicMock()
        lookups = {"ref": _make_lookup(materialize=True, strategy="cache")}

        cached, results = materialize_lookups(spark, lookups)

        assert "ref" in cached
        assert len(results) == 1
        assert results[0].materialized is True
        assert results[0].strategy == "cache"
        assert results[0].row_count == 100
        mock_df.persist.assert_called_once()

    @patch("weevr.engine.lookups.read_source")
    def test_materialize_broadcast(self, mock_read):
        """Broadcast strategy applies F.broadcast hint."""
        mock_df = MagicMock()
        mock_df.count.return_value = 50
        mock_read.return_value = mock_df

        spark = MagicMock()
        lookups = {"codes": _make_lookup(materialize=True, strategy="broadcast")}

        with patch("weevr.engine.lookups.F.broadcast", return_value=mock_df) as mock_bc:
            cached, results = materialize_lookups(spark, lookups)

        mock_bc.assert_called_once_with(mock_df)
        assert results[0].strategy == "broadcast"
        assert results[0].row_count == 50

    @patch("weevr.engine.lookups.read_source")
    def test_skip_non_materialized(self, mock_read):
        """Non-materialized lookups are skipped."""
        spark = MagicMock()
        lookups = {"lazy": _make_lookup(materialize=False)}

        cached, results = materialize_lookups(spark, lookups)

        assert "lazy" not in cached
        assert len(results) == 1
        assert results[0].materialized is False
        mock_read.assert_not_called()

    @patch("weevr.engine.lookups.read_source")
    def test_mixed_lookups(self, mock_read):
        """Mix of materialized and non-materialized lookups."""
        mock_df = MagicMock()
        mock_df.count.return_value = 10
        mock_read.return_value = mock_df

        spark = MagicMock()
        lookups = {
            "cached_ref": _make_lookup(materialize=True, strategy="cache"),
            "lazy_ref": _make_lookup(materialize=False),
        }

        cached, results = materialize_lookups(spark, lookups)

        assert "cached_ref" in cached
        assert "lazy_ref" not in cached
        assert len(results) == 2

    @patch("weevr.engine.lookups.read_source")
    def test_read_failure_raises(self, mock_read):
        """Read failure raises LookupResolutionError."""
        mock_read.side_effect = RuntimeError("read failed")
        spark = MagicMock()
        lookups = {"bad": _make_lookup(materialize=True)}

        with pytest.raises(LookupResolutionError, match="Failed to materialize"):
            materialize_lookups(spark, lookups)

    @patch("weevr.engine.lookups.read_source")
    def test_telemetry_spans(self, mock_read):
        """Materialization creates telemetry spans."""
        mock_df = MagicMock()
        mock_df.count.return_value = 25
        mock_read.return_value = mock_df

        spark = MagicMock()
        collector = SpanCollector(generate_trace_id())
        lookups = {"ref": _make_lookup(materialize=True, strategy="cache")}

        materialize_lookups(spark, lookups, collector=collector, parent_span_id="p1")

        spans = collector.get_spans()
        assert len(spans) == 1
        assert spans[0].name == "lookup:materialize:ref"
        assert spans[0].parent_span_id == "p1"

    @patch("weevr.engine.lookups.F.expr")
    @patch("weevr.engine.lookups.read_source")
    def test_materialize_with_narrow_fields(self, mock_read, mock_expr):
        """Narrow fields (key, values, filter) are applied during materialization."""
        raw_df = MagicMock()
        raw_df.columns = ["cust_id", "cust_sk", "name", "is_current"]
        filtered_df = MagicMock()
        filtered_df.columns = ["cust_id", "cust_sk", "name", "is_current"]
        projected_df = MagicMock()
        projected_df.count.return_value = 42
        raw_df.filter.return_value = filtered_df
        filtered_df.select.return_value = projected_df
        mock_read.return_value = raw_df
        mock_expr.return_value = "mock_col"

        spark = MagicMock()
        lookups = {
            "dim_cust": _make_lookup(
                materialize=True,
                strategy="cache",
                key=["cust_id"],
                values=["cust_sk"],
                filter="is_current = true",
            ),
        }

        cached, results = materialize_lookups(spark, lookups)

        assert "dim_cust" in cached
        raw_df.filter.assert_called_once()
        filtered_df.select.assert_called_once_with("cust_id", "cust_sk")
        assert results[0].key_columns == ["cust_id"]
        assert results[0].value_columns == ["cust_sk"]
        assert results[0].filter_applied is True

    @patch("weevr.engine.lookups.read_source")
    def test_materialize_unique_key_abort(self, mock_read):
        """Materialization aborts on unique_key failure."""
        df = MagicMock()
        df.columns = ["id", "sk"]
        df.select.return_value = df
        df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 3
        mock_read.return_value = df

        spark = MagicMock()
        lookups = {
            "dim": _make_lookup(
                materialize=True,
                key=["id"],
                values=["sk"],
                unique_key=True,
                on_failure="abort",
            ),
        }

        with pytest.raises(LookupResolutionError, match="unique_key check failed"):
            materialize_lookups(spark, lookups)

    @patch("weevr.engine.lookups.read_source")
    def test_narrow_telemetry_attributes(self, mock_read):
        """Narrow metadata is recorded in telemetry span attributes."""
        df = MagicMock()
        df.columns = ["id", "sk"]
        df.select.return_value = df
        df.count.return_value = 10
        df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 0
        mock_read.return_value = df

        spark = MagicMock()
        collector = SpanCollector(generate_trace_id())
        lookups = {
            "ref": _make_lookup(
                materialize=True,
                key=["id"],
                values=["sk"],
                unique_key=True,
            ),
        }

        materialize_lookups(spark, lookups, collector=collector, parent_span_id="p1")

        spans = collector.get_spans()
        assert len(spans) == 1
        attrs = spans[0].attributes
        assert attrs["lookup.key_columns"] == "id"
        assert attrs["lookup.value_columns"] == "sk"
        assert attrs["lookup.filter_applied"] is False
        assert attrs["lookup.unique_key_checked"] is True


class TestValidateColumns:
    """Test _validate_columns helper."""

    def test_all_columns_present(self):
        """No error when all columns exist."""
        df = MagicMock()
        df.columns = ["id", "name", "sk"]
        _validate_columns(df, ["id", "sk"], "dim")  # should not raise

    def test_missing_column_raises(self):
        """Missing column raises LookupResolutionError."""
        df = MagicMock()
        df.columns = ["id", "name"]
        with pytest.raises(LookupResolutionError, match="column 'sk' not found"):
            _validate_columns(df, ["id", "sk"], "dim")


class TestCheckUniqueKey:
    """Test _check_unique_key helper."""

    def test_unique_keys_pass(self):
        """Returns True when keys are unique."""
        df = MagicMock()
        # groupBy().count().filter().count() returns 0 (no duplicates)
        df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 0
        assert _check_unique_key(df, ["id"], "dim", "abort") is True

    def test_duplicate_keys_abort(self):
        """Raises LookupResolutionError when on_failure is abort."""
        df = MagicMock()
        df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 3
        with pytest.raises(LookupResolutionError, match="unique_key check failed"):
            _check_unique_key(df, ["id"], "dim", "abort")

    def test_duplicate_keys_warn(self):
        """Returns False when on_failure is warn."""
        df = MagicMock()
        df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 2
        assert _check_unique_key(df, ["id"], "dim", "warn") is False


class TestApplyNarrowPipeline:
    """Test _apply_narrow_pipeline."""

    @patch("weevr.engine.lookups.F.expr")
    def test_filter_applied(self, mock_expr):
        """Filter expression is applied to the DataFrame."""
        df = MagicMock()
        filtered_df = MagicMock()
        df.filter.return_value = filtered_df
        mock_expr.return_value = "mock_col"
        lookup = _make_lookup(filter="is_current = true")

        result_df, filter_applied, uk_checked, uk_passed = _apply_narrow_pipeline(df, lookup, "dim")

        mock_expr.assert_called_once_with("is_current = true")
        df.filter.assert_called_once_with("mock_col")
        assert filter_applied is True
        assert uk_checked is False
        assert uk_passed is None

    def test_projection_applied(self):
        """Key + values triggers column validation and select."""
        df = MagicMock()
        df.columns = ["cust_id", "cust_sk", "name"]
        projected_df = MagicMock()
        df.select.return_value = projected_df
        lookup = _make_lookup(key=["cust_id"], values=["cust_sk"])

        result_df, _, _, _ = _apply_narrow_pipeline(df, lookup, "dim")

        df.select.assert_called_once_with("cust_id", "cust_sk")
        assert result_df is projected_df

    def test_key_only_no_projection(self):
        """Key without values validates columns but does not project."""
        df = MagicMock()
        df.columns = ["cust_id", "name"]
        lookup = _make_lookup(key=["cust_id"])

        result_df, _, _, _ = _apply_narrow_pipeline(df, lookup, "dim")

        df.select.assert_not_called()
        assert result_df is df

    def test_column_validation_missing_key(self):
        """Missing key column raises LookupResolutionError."""
        df = MagicMock()
        df.columns = ["name", "sk"]
        lookup = _make_lookup(key=["cust_id"], values=["sk"])

        with pytest.raises(LookupResolutionError, match="column 'cust_id' not found"):
            _apply_narrow_pipeline(df, lookup, "dim")

    def test_column_validation_missing_value(self):
        """Missing value column raises LookupResolutionError."""
        df = MagicMock()
        df.columns = ["cust_id", "name"]
        lookup = _make_lookup(key=["cust_id"], values=["sk"])

        with pytest.raises(LookupResolutionError, match="column 'sk' not found"):
            _apply_narrow_pipeline(df, lookup, "dim")

    def test_unique_key_pass(self):
        """Unique key check passes when no duplicates."""
        df = MagicMock()
        df.columns = ["id", "sk"]
        df.select.return_value = df
        df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 0
        lookup = _make_lookup(key=["id"], values=["sk"], unique_key=True)

        _, _, uk_checked, uk_passed = _apply_narrow_pipeline(df, lookup, "dim")

        assert uk_checked is True
        assert uk_passed is True

    def test_unique_key_fail_abort(self):
        """Unique key abort raises LookupResolutionError."""
        df = MagicMock()
        df.columns = ["id", "sk"]
        df.select.return_value = df
        df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 5
        lookup = _make_lookup(key=["id"], values=["sk"], unique_key=True, on_failure="abort")

        with pytest.raises(LookupResolutionError, match="unique_key check failed"):
            _apply_narrow_pipeline(df, lookup, "dim")

    def test_unique_key_fail_warn(self):
        """Unique key warn returns False without raising."""
        df = MagicMock()
        df.columns = ["id", "sk"]
        df.select.return_value = df
        df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 2
        lookup = _make_lookup(key=["id"], values=["sk"], unique_key=True, on_failure="warn")

        _, _, uk_checked, uk_passed = _apply_narrow_pipeline(df, lookup, "dim")

        assert uk_checked is True
        assert uk_passed is False

    def test_no_narrow_fields_passthrough(self):
        """No narrow fields passes DataFrame through unchanged."""
        df = MagicMock()
        lookup = _make_lookup()

        result_df, filter_applied, uk_checked, uk_passed = _apply_narrow_pipeline(df, lookup, "ref")

        assert result_df is df
        assert filter_applied is False
        assert uk_checked is False
        assert uk_passed is None
        df.filter.assert_not_called()
        df.select.assert_not_called()

    @patch("weevr.engine.lookups.F.expr")
    def test_filter_and_project_composed(self, mock_expr):
        """Filter then projection applied in correct order."""
        df = MagicMock()
        filtered_df = MagicMock()
        filtered_df.columns = ["id", "sk", "extra"]
        projected_df = MagicMock()
        df.filter.return_value = filtered_df
        filtered_df.select.return_value = projected_df
        mock_expr.return_value = "mock_col"
        lookup = _make_lookup(key=["id"], values=["sk"], filter="active = true")

        result_df, filter_applied, _, _ = _apply_narrow_pipeline(df, lookup, "dim")

        assert filter_applied is True
        mock_expr.assert_called_once_with("active = true")
        df.filter.assert_called_once_with("mock_col")
        filtered_df.select.assert_called_once_with("id", "sk")
        assert result_df is projected_df


class TestCleanupLookups:
    """Test cleanup_lookups."""

    def test_unpersist_called(self):
        """Unpersist is called on each cached DataFrame."""
        df1 = MagicMock()
        df2 = MagicMock()
        cleanup_lookups({"a": df1, "b": df2})
        df1.unpersist.assert_called_once()
        df2.unpersist.assert_called_once()

    def test_swallows_errors(self):
        """Exceptions during unpersist are swallowed."""
        df = MagicMock()
        df.unpersist.side_effect = RuntimeError("already gone")
        # Should not raise
        cleanup_lookups({"x": df})

    def test_empty_dict(self):
        """Empty dict is a no-op."""
        cleanup_lookups({})


class TestResolveThreadLookups:
    """Test resolve_thread_lookups."""

    def test_resolve_cached_lookup(self):
        """Cached lookup resolves to pre-materialized DataFrame."""
        spark = MagicMock()
        cached_df = MagicMock()

        source = MagicMock()
        source.lookup = "ref"

        weave_lookups = {"ref": _make_lookup(materialize=True)}

        resolved = resolve_thread_lookups({"src": source}, weave_lookups, {"ref": cached_df}, spark)

        assert resolved["src"] is cached_df

    @patch("weevr.engine.lookups.read_source")
    def test_resolve_on_demand(self, mock_read):
        """Non-materialized lookup reads on-demand."""
        spark = MagicMock()
        on_demand_df = MagicMock()
        mock_read.return_value = on_demand_df

        source = MagicMock()
        source.lookup = "lazy"

        weave_lookups = {"lazy": _make_lookup(materialize=False)}

        resolved = resolve_thread_lookups({"src": source}, weave_lookups, {}, spark)

        assert resolved["src"] is on_demand_df
        mock_read.assert_called_once()

    def test_undefined_lookup_raises(self):
        """Referencing an undefined lookup raises LookupResolutionError."""
        spark = MagicMock()
        source = MagicMock()
        source.lookup = "unknown"

        with pytest.raises(LookupResolutionError, match="undefined lookup"):
            resolve_thread_lookups({"src": source}, {}, {}, spark)

    @patch("weevr.engine.lookups.F.expr")
    @patch("weevr.engine.lookups.read_source")
    def test_on_demand_narrow_pipeline(self, mock_read, mock_expr):
        """Non-materialized lookup applies narrow pipeline at read time."""
        spark = MagicMock()
        raw_df = MagicMock()
        raw_df.columns = ["id", "sk", "extra"]
        filtered_df = MagicMock()
        filtered_df.columns = ["id", "sk", "extra"]
        projected_df = MagicMock()
        raw_df.filter.return_value = filtered_df
        filtered_df.select.return_value = projected_df
        mock_read.return_value = raw_df
        mock_expr.return_value = "mock_col"

        source = MagicMock()
        source.lookup = "dim"

        weave_lookups = {
            "dim": _make_lookup(
                materialize=False,
                key=["id"],
                values=["sk"],
                filter="active = true",
            ),
        }

        resolved = resolve_thread_lookups({"src": source}, weave_lookups, {}, spark)

        assert resolved["src"] is projected_df
        raw_df.filter.assert_called_once()
        filtered_df.select.assert_called_once_with("id", "sk")

    def test_skip_sources_without_lookup(self):
        """Sources without lookup field are skipped."""
        spark = MagicMock()
        source = MagicMock()
        source.lookup = None

        resolved = resolve_thread_lookups({"src": source}, {}, {}, spark)

        assert len(resolved) == 0
