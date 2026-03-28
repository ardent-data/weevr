"""Tests for FactConfig model."""

import pytest
from pydantic import ValidationError

from weevr.model.fact import FactConfig, SentinelValueConfig


class TestSentinelValueConfig:
    """Test SentinelValueConfig model."""

    def test_valid_construction_with_defaults(self):
        """SentinelValueConfig with default sentinel values is valid."""
        svc = SentinelValueConfig()
        assert svc.invalid == -4
        assert svc.missing == -1

    def test_valid_construction_with_custom_values(self):
        """SentinelValueConfig with custom sentinel values is valid."""
        svc = SentinelValueConfig(invalid=-10, missing=-5)
        assert svc.invalid == -10
        assert svc.missing == -5

    def test_sentinel_values_must_be_integers(self):
        """SentinelValueConfig rejects non-integer sentinel values."""
        with pytest.raises(ValidationError):
            SentinelValueConfig(invalid="not an int")  # type: ignore[arg-type]

    def test_frozen(self):
        """SentinelValueConfig is immutable."""
        svc = SentinelValueConfig()
        with pytest.raises(ValidationError):
            svc.invalid = -5  # type: ignore[misc]

    def test_round_trip(self):
        """SentinelValueConfig round-trips via model_dump and model_validate."""
        svc = SentinelValueConfig(invalid=-8, missing=-2)
        assert SentinelValueConfig.model_validate(svc.model_dump()) == svc


class TestFactConfig:
    """Test FactConfig model."""

    def test_valid_construction_with_foreign_keys_and_defaults(self):
        """FactConfig with foreign_keys and default sentinel values is valid."""
        fc = FactConfig(foreign_keys=["fact_id", "dimension_id"])
        assert fc.foreign_keys == ["fact_id", "dimension_id"]
        assert fc.sentinel_values.invalid == -4
        assert fc.sentinel_values.missing == -1

    def test_valid_construction_with_custom_sentinel_values(self):
        """FactConfig with custom sentinel_values is valid."""
        fc = FactConfig(
            foreign_keys=["fact_id"],
            sentinel_values=SentinelValueConfig(invalid=-10, missing=-5),
        )
        assert fc.foreign_keys == ["fact_id"]
        assert fc.sentinel_values.invalid == -10
        assert fc.sentinel_values.missing == -5

    def test_foreign_keys_required(self):
        """FactConfig requires foreign_keys field."""
        with pytest.raises(ValidationError):
            FactConfig()  # type: ignore[call-arg]

    def test_foreign_keys_must_be_non_empty(self):
        """FactConfig rejects empty foreign_keys list."""
        with pytest.raises(ValidationError):
            FactConfig(foreign_keys=[])

    def test_foreign_keys_must_be_list(self):
        """FactConfig requires foreign_keys to be a list."""
        with pytest.raises(ValidationError):
            FactConfig(foreign_keys="fact_id")  # type: ignore[arg-type]

    def test_frozen(self):
        """FactConfig is immutable."""
        fc = FactConfig(foreign_keys=["fact_id"])
        with pytest.raises(ValidationError):
            fc.foreign_keys = ["other_id"]  # type: ignore[misc]

    def test_round_trip(self):
        """FactConfig round-trips via model_dump and model_validate."""
        fc = FactConfig(
            foreign_keys=["fact_id", "dim_id"],
            sentinel_values=SentinelValueConfig(invalid=-7, missing=-3),
        )
        assert FactConfig.model_validate(fc.model_dump()) == fc
