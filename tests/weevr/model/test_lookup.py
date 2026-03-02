"""Tests for Lookup model."""

import pytest
from pydantic import ValidationError

from weevr.model.lookup import Lookup
from weevr.model.source import Source


class TestLookup:
    """Test Lookup construction and validation."""

    def test_non_materialized(self):
        """Non-materialized lookup with defaults."""
        source = Source(type="delta", alias="ref_categories")
        lookup = Lookup(source=source)
        assert lookup.materialize is False
        assert lookup.strategy == "cache"
        assert lookup.source.alias == "ref_categories"

    def test_materialized_broadcast(self):
        """Materialized lookup with broadcast strategy."""
        source = Source(type="delta", alias="dim_region")
        lookup = Lookup(source=source, materialize=True, strategy="broadcast")
        assert lookup.materialize is True
        assert lookup.strategy == "broadcast"

    def test_materialized_cache_default(self):
        """Materialized lookup defaults to cache strategy."""
        source = Source(type="delta", alias="dim_status")
        lookup = Lookup(source=source, materialize=True)
        assert lookup.strategy == "cache"

    def test_from_dict(self):
        """Construct from nested dict (YAML-like)."""
        data = {
            "source": {"type": "delta", "alias": "ref_codes"},
            "materialize": True,
            "strategy": "broadcast",
        }
        lookup = Lookup(**data)
        assert lookup.source.type == "delta"
        assert lookup.materialize is True

    def test_invalid_source_rejected(self):
        """Invalid source (delta without alias) raises ValidationError."""
        with pytest.raises(ValidationError, match="alias"):
            Lookup(source=Source(type="delta"))

    def test_invalid_strategy_rejected(self):
        """Invalid strategy raises ValidationError."""
        with pytest.raises(ValidationError):
            Lookup(
                source=Source(type="delta", alias="t"),
                strategy="replicate",  # type: ignore[arg-type]
            )

    def test_frozen(self):
        """Lookup is immutable."""
        source = Source(type="delta", alias="t")
        lookup = Lookup(source=source)
        with pytest.raises(ValidationError):
            lookup.materialize = True  # type: ignore[misc]
