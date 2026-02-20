"""Tests for Weave composite model."""

import pytest
from pydantic import ValidationError

from weevr.model.weave import Weave


class TestWeave:
    """Test Weave model."""

    def test_minimal_weave(self):
        """Weave from minimal dict."""
        w = Weave.model_validate({"config_version": "1.0", "threads": ["dim_customer"]})
        assert w.config_version == "1.0"
        assert w.threads == ["dim_customer"]

    def test_missing_threads_raises(self):
        """Weave without threads raises ValidationError."""
        with pytest.raises(ValidationError):
            Weave.model_validate({"config_version": "1.0"})

    def test_threads_not_list_raises(self):
        """threads field must be a list."""
        with pytest.raises(ValidationError):
            Weave.model_validate({"config_version": "1.0", "threads": "not_a_list"})

    def test_with_defaults(self):
        """Weave accepts optional defaults dict."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": ["t1"],
                "defaults": {"write": {"mode": "merge"}},
            }
        )
        assert w.defaults is not None
        assert w.defaults["write"]["mode"] == "merge"

    def test_with_params(self):
        """Weave accepts optional params dict."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": ["t1"],
                "params": {"env": {"name": "env", "type": "string"}},
            }
        )
        assert w.params is not None
        assert "env" in w.params

    def test_frozen(self):
        """Weave is immutable."""
        w = Weave.model_validate({"config_version": "1.0", "threads": ["t1"]})
        with pytest.raises(ValidationError):
            w.config_version = "2.0"  # type: ignore[misc]

    def test_round_trip(self):
        """Weave round-trips."""
        w = Weave.model_validate(
            {"config_version": "1.0", "threads": ["a", "b"], "defaults": {"x": 1}}
        )
        assert Weave.model_validate(w.model_dump()) == w

    def test_name_defaults_to_empty_string(self):
        """name defaults to empty string when not provided."""
        w = Weave.model_validate({"config_version": "1.0", "threads": ["t1"]})
        assert w.name == ""

    def test_name_can_be_set(self):
        """name is stored when explicitly provided."""
        w = Weave.model_validate({"config_version": "1.0", "threads": ["t1"], "name": "dimensions"})
        assert w.name == "dimensions"
