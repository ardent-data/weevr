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

    # -- Narrow lookup field tests (M101a) --

    def test_narrow_defaults(self):
        """New narrow fields default to None/False without affecting existing configs."""
        source = Source(type="delta", alias="ref_t")
        lookup = Lookup(source=source)
        assert lookup.key is None
        assert lookup.values is None
        assert lookup.filter is None
        assert lookup.unique_key is False
        assert lookup.on_failure == "abort"

    def test_key_and_values(self):
        """Key + values accepted and stored correctly."""
        source = Source(type="delta", alias="dim_t")
        lookup = Lookup(source=source, key=["cust_id"], values=["cust_sk"])
        assert lookup.key == ["cust_id"]
        assert lookup.values == ["cust_sk"]

    def test_key_only(self):
        """Key without values is valid (column-existence assertion only)."""
        source = Source(type="delta", alias="dim_t")
        lookup = Lookup(source=source, key=["cust_id"])
        assert lookup.key == ["cust_id"]
        assert lookup.values is None

    def test_values_without_key_rejected(self):
        """Values without key raises ValidationError."""
        with pytest.raises(ValidationError, match="requires 'key'"):
            Lookup(
                source=Source(type="delta", alias="t"),
                values=["cust_sk"],
            )

    def test_key_values_overlap_rejected(self):
        """Overlapping key and values raises ValidationError."""
        with pytest.raises(ValidationError, match="must not overlap"):
            Lookup(
                source=Source(type="delta", alias="t"),
                key=["id", "code"],
                values=["code", "name"],
            )

    def test_on_failure_without_unique_key_rejected(self):
        """Setting on_failure to warn without unique_key raises ValidationError."""
        with pytest.raises(ValidationError, match="unique_key"):
            Lookup(
                source=Source(type="delta", alias="t"),
                on_failure="warn",
            )

    def test_unique_key_with_on_failure(self):
        """unique_key: true with on_failure: warn is accepted."""
        source = Source(type="delta", alias="dim_t")
        lookup = Lookup(source=source, unique_key=True, on_failure="warn")
        assert lookup.unique_key is True
        assert lookup.on_failure == "warn"

    def test_unique_key_default_on_failure(self):
        """unique_key: true defaults on_failure to abort."""
        source = Source(type="delta", alias="dim_t")
        lookup = Lookup(source=source, unique_key=True)
        assert lookup.on_failure == "abort"

    def test_filter_standalone(self):
        """Filter without key/values is valid."""
        source = Source(type="delta", alias="dim_t")
        lookup = Lookup(source=source, filter="is_current = true")
        assert lookup.filter == "is_current = true"
        assert lookup.key is None

    def test_narrow_from_dict(self):
        """Full narrow config constructed from YAML-like dict."""
        data = {
            "source": {"type": "delta", "alias": "silver.dim_customer"},
            "key": ["customer_id"],
            "values": ["customer_sk"],
            "filter": "is_current = true",
            "unique_key": True,
            "on_failure": "abort",
            "materialize": True,
            "strategy": "broadcast",
        }
        lookup = Lookup(**data)
        assert lookup.key == ["customer_id"]
        assert lookup.values == ["customer_sk"]
        assert lookup.filter == "is_current = true"
        assert lookup.unique_key is True
        assert lookup.on_failure == "abort"
        assert lookup.materialize is True
        assert lookup.strategy == "broadcast"

    def test_composite_key(self):
        """Composite key (multiple columns) is accepted."""
        source = Source(type="delta", alias="dim_t")
        lookup = Lookup(source=source, key=["region", "product_id"], values=["sk"])
        assert lookup.key == ["region", "product_id"]
