"""Tests for KeyConfig, SurrogateKeyConfig, and ChangeDetectionConfig models."""

import pytest
from pydantic import ValidationError

from weevr.model.keys import ChangeDetectionConfig, KeyConfig, SurrogateKeyConfig


class TestSurrogateKeyConfig:
    """Test SurrogateKeyConfig model."""

    def test_minimal(self):
        """SurrogateKeyConfig with just a name."""
        s = SurrogateKeyConfig(name="sk_id")
        assert s.name == "sk_id"
        assert s.algorithm == "sha256"

    def test_md5_algorithm(self):
        """SurrogateKeyConfig accepts md5 algorithm."""
        s = SurrogateKeyConfig(name="sk_id", algorithm="md5")
        assert s.algorithm == "md5"

    def test_invalid_algorithm_raises(self):
        """Unknown algorithm raises ValidationError."""
        with pytest.raises(ValidationError):
            SurrogateKeyConfig(name="sk", algorithm="sha512")  # type: ignore[arg-type]

    def test_frozen(self):
        """SurrogateKeyConfig is immutable."""
        s = SurrogateKeyConfig(name="sk_id")
        with pytest.raises(ValidationError):
            s.name = "other"  # type: ignore[misc]

    def test_round_trip(self):
        """SurrogateKeyConfig round-trips."""
        s = SurrogateKeyConfig(name="sk_id", algorithm="md5")
        assert SurrogateKeyConfig.model_validate(s.model_dump()) == s


class TestChangeDetectionConfig:
    """Test ChangeDetectionConfig model."""

    def test_minimal(self):
        """ChangeDetectionConfig with name and columns."""
        c = ChangeDetectionConfig(name="hash_row", columns=["col_a", "col_b"])
        assert c.name == "hash_row"
        assert c.columns == ["col_a", "col_b"]
        assert c.algorithm == "md5"

    def test_sha256_algorithm(self):
        """ChangeDetectionConfig accepts sha256."""
        c = ChangeDetectionConfig(name="h", columns=["x"], algorithm="sha256")
        assert c.algorithm == "sha256"

    def test_frozen(self):
        """ChangeDetectionConfig is immutable."""
        c = ChangeDetectionConfig(name="h", columns=["x"])
        with pytest.raises(ValidationError):
            c.columns = ["y"]  # type: ignore[misc]

    def test_round_trip(self):
        """ChangeDetectionConfig round-trips."""
        c = ChangeDetectionConfig(name="h", columns=["a", "b"], algorithm="sha256")
        assert ChangeDetectionConfig.model_validate(c.model_dump()) == c


class TestKeyConfig:
    """Test KeyConfig model."""

    def test_all_none_defaults(self):
        """KeyConfig with no fields is valid."""
        k = KeyConfig()
        assert k.business_key is None
        assert k.surrogate_key is None
        assert k.change_detection is None

    def test_with_business_key(self):
        """KeyConfig with business_key list."""
        k = KeyConfig(business_key=["customer_id", "order_date"])
        assert k.business_key == ["customer_id", "order_date"]

    def test_with_surrogate_key(self):
        """KeyConfig with nested SurrogateKeyConfig."""
        k = KeyConfig(surrogate_key={"name": "sk_id", "algorithm": "sha256"})
        assert isinstance(k.surrogate_key, SurrogateKeyConfig)
        assert k.surrogate_key.name == "sk_id"

    def test_with_change_detection(self):
        """KeyConfig with nested ChangeDetectionConfig."""
        k = KeyConfig(change_detection={"name": "row_hash", "columns": ["a", "b"]})
        assert isinstance(k.change_detection, ChangeDetectionConfig)

    def test_all_sub_configs(self):
        """KeyConfig with all sub-configs populated."""
        k = KeyConfig(
            business_key=["id"],
            surrogate_key={"name": "sk", "algorithm": "md5"},
            change_detection={"name": "ch", "columns": ["col1"]},
        )
        assert k.business_key == ["id"]
        assert isinstance(k.surrogate_key, SurrogateKeyConfig)
        assert isinstance(k.change_detection, ChangeDetectionConfig)

    def test_frozen(self):
        """KeyConfig is immutable."""
        k = KeyConfig(business_key=["id"])
        with pytest.raises(ValidationError):
            k.business_key = ["other"]  # type: ignore[misc]

    def test_round_trip(self):
        """KeyConfig round-trips."""
        k = KeyConfig(
            business_key=["id"],
            surrogate_key={"name": "sk", "algorithm": "sha256"},
            change_detection={"name": "ch", "columns": ["a"]},
        )
        assert KeyConfig.model_validate(k.model_dump()) == k
