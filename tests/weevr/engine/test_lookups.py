"""Tests for lookup materializer."""

from unittest.mock import MagicMock, patch

import pytest

from weevr.engine.lookups import (
    LookupResult,
    cleanup_lookups,
    materialize_lookups,
    resolve_thread_lookups,
)
from weevr.errors.exceptions import LookupResolutionError
from weevr.model.lookup import Lookup
from weevr.model.source import Source
from weevr.telemetry.collector import SpanCollector
from weevr.telemetry.span import generate_trace_id


def _make_lookup(*, materialize: bool = True, strategy: str = "cache") -> Lookup:
    """Create a Lookup with a delta source for testing."""
    return Lookup(
        source=Source(type="delta", alias="db.ref_table"),
        materialize=materialize,
        strategy=strategy,  # type: ignore[arg-type]
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

    def test_skip_sources_without_lookup(self):
        """Sources without lookup field are skipped."""
        spark = MagicMock()
        source = MagicMock()
        source.lookup = None

        resolved = resolve_thread_lookups({"src": source}, {}, {}, spark)

        assert len(resolved) == 0
