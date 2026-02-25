"""Tests for Loom composite model."""

import pytest
from pydantic import ValidationError

from weevr.model.loom import Loom, WeaveEntry
from weevr.model.weave import ConditionSpec


class TestLoom:
    """Test Loom model."""

    def test_minimal_loom(self):
        """Loom from minimal dict."""
        loom = Loom.model_validate({"config_version": "1.0", "weaves": ["dimensions"]})
        assert loom.config_version == "1.0"
        assert loom.weaves == [WeaveEntry(name="dimensions")]

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
        assert loom.defaults is not None
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
        assert loom.params is not None
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

    def test_name_defaults_to_empty_string(self):
        """name defaults to empty string when not provided."""
        loom = Loom.model_validate({"config_version": "1.0", "weaves": ["w1"]})
        assert loom.name == ""

    def test_name_can_be_set(self):
        """name is stored when explicitly provided."""
        loom = Loom.model_validate({"config_version": "1.0", "weaves": ["w1"], "name": "nightly"})
        assert loom.name == "nightly"


class TestWeaveEntry:
    """Tests for WeaveEntry model and Loom normalization."""

    def test_string_weave_normalizes_to_weave_entry(self):
        """Loom with string-only weave list normalizes to list[WeaveEntry]."""
        loom = Loom.model_validate({"config_version": "1.0", "weaves": ["a", "b"]})
        assert all(isinstance(e, WeaveEntry) for e in loom.weaves)
        assert [e.name for e in loom.weaves] == ["a", "b"]

    def test_string_weave_entry_has_no_condition(self):
        """String-normalized WeaveEntry has condition=None."""
        loom = Loom.model_validate({"config_version": "1.0", "weaves": ["dim"]})
        assert loom.weaves[0].condition is None

    def test_dict_weave_entry_with_condition(self):
        """Dict weave entry with condition parsed correctly."""
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": [
                    {"name": "dims", "condition": {"when": "table_exists('raw.data')"}},
                ],
            }
        )
        assert loom.weaves[0].name == "dims"
        assert loom.weaves[0].condition is not None
        assert loom.weaves[0].condition.when == "table_exists('raw.data')"

    def test_mixed_string_and_dict_entries(self):
        """Loom with mixed string/dict weave entries normalizes correctly."""
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": [
                    "always",
                    {"name": "conditional", "condition": {"when": "${env} == 'prod'"}},
                    "cleanup",
                ],
            }
        )
        assert len(loom.weaves) == 3
        assert loom.weaves[0].name == "always"
        assert loom.weaves[0].condition is None
        assert loom.weaves[1].name == "conditional"
        assert loom.weaves[1].condition is not None
        assert loom.weaves[2].name == "cleanup"
        assert loom.weaves[2].condition is None

    def test_weave_entry_frozen(self):
        """WeaveEntry is immutable."""
        entry = WeaveEntry(name="w1")
        with pytest.raises((ValidationError, TypeError)):
            entry.name = "other"  # type: ignore[misc]

    def test_round_trip_with_conditions(self):
        """Loom with conditional WeaveEntry round-trips correctly."""
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": [
                    "simple",
                    {"name": "cond", "condition": {"when": "true"}},
                ],
            }
        )
        restored = Loom.model_validate(loom.model_dump())
        assert restored == loom

    def test_condition_spec_direct(self):
        """WeaveEntry accepts ConditionSpec directly."""
        entry = WeaveEntry(
            name="dims",
            condition=ConditionSpec(when="row_count('staging') > 0"),
        )
        assert entry.condition is not None
        assert entry.condition.when == "row_count('staging') > 0"
