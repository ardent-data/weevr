"""Tests for Loom composite model."""

import pytest
from pydantic import ValidationError

from weevr.model.connection import OneLakeConnection
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

    def test_shared_resource_fields_default_to_none(self):
        """lookups, variables, pre_steps, post_steps all default to None."""
        loom = Loom.model_validate({"config_version": "1.0", "weaves": ["w1"]})
        assert loom.lookups is None
        assert loom.variables is None
        assert loom.pre_steps is None
        assert loom.post_steps is None

    def test_with_lookups(self):
        """Loom accepts optional lookups dict."""
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": ["w1"],
                "lookups": {
                    "customer_lookup": {
                        "source": {"type": "delta", "alias": "ref.customers"},
                        "key": ["customer_id"],
                    }
                },
            }
        )
        assert loom.lookups is not None
        assert "customer_lookup" in loom.lookups

    def test_with_variables(self):
        """Loom accepts optional variables dict."""
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": ["w1"],
                "variables": {"record_count": {"type": "int"}},
            }
        )
        assert loom.variables is not None
        assert "record_count" in loom.variables

    def test_with_pre_steps(self):
        """Loom accepts optional pre_steps list."""
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": ["w1"],
                "pre_steps": [
                    {"type": "quality_gate", "check": "table_exists", "source": "raw.customers"}
                ],
            }
        )
        assert loom.pre_steps is not None
        assert len(loom.pre_steps) == 1

    def test_with_post_steps(self):
        """Loom accepts optional post_steps list."""
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": ["w1"],
                "post_steps": [
                    {
                        "type": "quality_gate",
                        "check": "row_count",
                        "target": "dim.customers",
                        "min_count": 1,
                    }
                ],
            }
        )
        assert loom.post_steps is not None
        assert len(loom.post_steps) == 1


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


class TestLoomConnectionsField:
    """Tests for the Loom.connections field."""

    def test_connections_defaults_to_none(self):
        """Loom without connections has connections=None (backward compatible)."""
        loom = Loom.model_validate({"config_version": "1.0", "weaves": ["w1"]})
        assert loom.connections is None

    def test_connections_with_valid_onelake_connection(self):
        """Loom with connections dict containing a valid OneLakeConnection parses."""
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": ["w1"],
                "connections": {
                    "main": {
                        "type": "onelake",
                        "workspace": "ws-guid-1234",
                        "lakehouse": "lh-guid-5678",
                    }
                },
            }
        )
        assert loom.connections is not None
        assert "main" in loom.connections
        assert isinstance(loom.connections["main"], OneLakeConnection)
        assert loom.connections["main"].workspace == "ws-guid-1234"
        assert loom.connections["main"].lakehouse == "lh-guid-5678"

    def test_connections_with_default_schema(self):
        """Loom connections entry accepts optional default_schema."""
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": ["w1"],
                "connections": {
                    "silver": {
                        "type": "onelake",
                        "workspace": "${param.workspace}",
                        "lakehouse": "${param.lakehouse}",
                        "default_schema": "silver",
                    }
                },
            }
        )
        assert loom.connections is not None
        assert loom.connections["silver"].default_schema == "silver"

    def test_connections_with_invalid_type_raises(self):
        """Loom rejects connections with invalid connection type."""
        with pytest.raises(ValidationError):
            Loom.model_validate(
                {
                    "config_version": "1.0",
                    "weaves": ["w1"],
                    "connections": {
                        "bad": {
                            "type": "gcs",
                            "workspace": "ws",
                            "lakehouse": "lh",
                        }
                    },
                }
            )

    def test_connections_missing_required_fields_raises(self):
        """Loom rejects connections entry missing required workspace/lakehouse."""
        with pytest.raises(ValidationError):
            Loom.model_validate(
                {
                    "config_version": "1.0",
                    "weaves": ["w1"],
                    "connections": {
                        "bad": {"type": "onelake"},
                    },
                }
            )

    def test_connections_round_trip(self):
        """Loom with connections round-trips through model_dump/model_validate."""
        loom = Loom.model_validate(
            {
                "config_version": "1.0",
                "weaves": ["w1"],
                "connections": {
                    "primary": {
                        "type": "onelake",
                        "workspace": "ws-1",
                        "lakehouse": "lh-1",
                    }
                },
            }
        )
        restored = Loom.model_validate(loom.model_dump())
        assert restored.connections == loom.connections
