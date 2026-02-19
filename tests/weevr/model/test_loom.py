"""Tests for Loom composite model."""

import pytest
from pydantic import ValidationError

from weevr.model.loom import Loom


class TestLoom:
    """Test Loom model."""

    def test_minimal_loom(self):
        """Loom from minimal dict."""
        loom = Loom.model_validate({"config_version": "1.0", "weaves": ["dimensions"]})
        assert loom.config_version == "1.0"
        assert loom.weaves == ["dimensions"]

    def test_missing_weaves_raises(self):
        """Loom without weaves raises ValidationError."""
        with pytest.raises(ValidationError):
            Loom.model_validate({"config_version": "1.0"})

    def test_weaves_not_list_raises(self):
        """weaves field must be a list."""
        with pytest.raises(ValidationError):
            Loom.model_validate({"config_version": "1.0", "weaves": "not_a_list"})

    def test_with_defaults(self):
        """Loom accepts optional defaults dict."""
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": ["w1"],
                "defaults": {"tags": ["nightly"]},
            }
        )
        assert loom.defaults["tags"] == ["nightly"]

    def test_with_params(self):
        """Loom accepts optional params dict."""
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": ["w1"],
                "params": {"env": {"name": "env", "type": "string"}},
            }
        )
        assert "env" in loom.params

    def test_frozen(self):
        """Loom is immutable."""
        loom = Loom.model_validate({"config_version": "1.0", "weaves": ["w1"]})
        with pytest.raises(ValidationError):
            loom.config_version = "2.0"  # type: ignore[misc]

    def test_round_trip(self):
        """Loom round-trips."""
        loom = Loom.model_validate(
            {"config_version": "1.0", "weaves": ["a", "b"], "defaults": {"y": 2}}
        )
        assert Loom.model_validate(loom.model_dump()) == loom
