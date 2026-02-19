"""Tests for Source and DedupConfig models."""

import pytest
from pydantic import ValidationError

from weevr.model.source import DedupConfig, Source


class TestDedupConfig:
    """Test DedupConfig model."""

    def test_minimal_dedup(self):
        """DedupConfig with only required keys field."""
        d = DedupConfig(keys=["customer_id"])
        assert d.keys == ["customer_id"]
        assert d.order_by is None

    def test_full_dedup(self):
        """DedupConfig with all fields."""
        d = DedupConfig(keys=["id", "date"], order_by="modified_date")
        assert d.order_by == "modified_date"

    def test_frozen(self):
        """DedupConfig is immutable."""
        d = DedupConfig(keys=["id"])
        with pytest.raises(ValidationError):
            d.keys = ["other"]  # type: ignore[misc]

    def test_round_trip(self):
        """DedupConfig round-trips through model_dump/model_validate."""
        d = DedupConfig(keys=["id"], order_by="ts")
        assert DedupConfig.model_validate(d.model_dump()) == d


class TestSource:
    """Test Source model."""

    def test_valid_delta_source(self):
        """Delta source requires alias."""
        s = Source(type="delta", alias="raw.customers")
        assert s.type == "delta"
        assert s.alias == "raw.customers"

    def test_valid_csv_source(self):
        """CSV source requires path."""
        s = Source(type="csv", path="/data/files/orders.csv")
        assert s.path == "/data/files/orders.csv"

    def test_valid_json_source(self):
        """JSON source requires path."""
        s = Source(type="json", path="/data/raw/events/")
        assert s.type == "json"

    def test_valid_parquet_source(self):
        """Parquet source requires path."""
        s = Source(type="parquet", path="/mnt/data/")
        assert s.type == "parquet"

    def test_delta_missing_alias_raises(self):
        """Delta source without alias raises ValidationError."""
        with pytest.raises(ValidationError, match="alias"):
            Source(type="delta")

    def test_csv_missing_path_raises(self):
        """CSV source without path raises ValidationError."""
        with pytest.raises(ValidationError, match="path"):
            Source(type="csv")

    def test_json_missing_path_raises(self):
        """JSON source without path raises ValidationError."""
        with pytest.raises(ValidationError, match="path"):
            Source(type="json")

    def test_parquet_missing_path_raises(self):
        """Parquet source without path raises ValidationError."""
        with pytest.raises(ValidationError, match="path"):
            Source(type="parquet")

    def test_custom_type_no_alias_or_path(self):
        """Custom source type passes without alias or path requirement."""
        s = Source(type="api")
        assert s.type == "api"
        assert s.alias is None
        assert s.path is None

    def test_source_with_options(self):
        """Source accepts arbitrary options dict."""
        s = Source(type="csv", path="/data/", options={"delimiter": "|", "header": True})
        assert s.options["delimiter"] == "|"

    def test_source_with_dedup(self):
        """Source accepts nested DedupConfig."""
        s = Source(
            type="delta",
            alias="raw.customers",
            dedup={"keys": ["id"], "order_by": "modified_at"},
        )
        assert isinstance(s.dedup, DedupConfig)
        assert s.dedup.keys == ["id"]

    def test_frozen(self):
        """Source is immutable."""
        s = Source(type="delta", alias="raw.orders")
        with pytest.raises(ValidationError):
            s.type = "csv"  # type: ignore[misc]

    def test_round_trip(self):
        """Source round-trips through model_dump/model_validate."""
        s = Source(
            type="delta",
            alias="raw.customers",
            options={"timeout": 30},
            dedup={"keys": ["id"]},
        )
        assert Source.model_validate(s.model_dump()) == s

    def test_default_options_is_empty_dict(self):
        """options defaults to empty dict, not None."""
        s = Source(type="api")
        assert s.options == {}
