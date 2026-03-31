"""Tests for Weave composite model."""

import pytest
from pydantic import ValidationError

from weevr.model.connection import OneLakeConnection
from weevr.model.hooks import LogMessageStep, QualityGateStep
from weevr.model.lookup import Lookup
from weevr.model.variable import VariableSpec
from weevr.model.weave import ConditionSpec, ThreadEntry, Weave


class TestWeave:
    """Test Weave model."""

    def test_minimal_weave(self):
        """Weave from minimal dict normalizes string threads to ThreadEntry objects."""
        w = Weave.model_validate({"config_version": "1.0", "threads": ["dim_customer"]})
        assert w.config_version == "1.0"
        assert len(w.threads) == 1
        assert isinstance(w.threads[0], ThreadEntry)
        assert w.threads[0].name == "dim_customer"

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


class TestThreadEntry:
    """Tests for ThreadEntry model and Weave normalization."""

    def test_string_thread_list_normalizes_to_thread_entry(self):
        """Weave with string-only thread list normalizes to list[ThreadEntry]."""
        w = Weave.model_validate({"config_version": "1.0", "threads": ["a", "b", "c"]})
        assert all(isinstance(e, ThreadEntry) for e in w.threads)
        assert [e.name for e in w.threads] == ["a", "b", "c"]

    def test_string_thread_entry_has_no_dependencies(self):
        """String-normalized ThreadEntry has dependencies=None."""
        w = Weave.model_validate({"config_version": "1.0", "threads": ["a"]})
        assert w.threads[0].dependencies is None

    def test_map_thread_entry_with_dependencies(self):
        """Dict thread entry with dependencies parsed correctly."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": [
                    "a",
                    {"name": "b", "dependencies": ["a"]},
                ],
            }
        )
        assert w.threads[0].name == "a"
        assert w.threads[0].dependencies is None
        assert w.threads[1].name == "b"
        assert w.threads[1].dependencies == ["a"]

    def test_map_thread_entry_no_dependencies(self):
        """Dict thread entry without dependencies has dependencies=None."""
        w = Weave.model_validate({"config_version": "1.0", "threads": [{"name": "x"}]})
        assert w.threads[0].name == "x"
        assert w.threads[0].dependencies is None

    def test_mixed_string_and_map_entries(self):
        """Weave with mixed string/map entries normalizes correctly."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": [
                    "load_raw",
                    {"name": "clean", "dependencies": ["load_raw"]},
                    "summarize",
                ],
            }
        )
        assert len(w.threads) == 3
        assert w.threads[0].name == "load_raw"
        assert w.threads[1].name == "clean"
        assert w.threads[1].dependencies == ["load_raw"]
        assert w.threads[2].name == "summarize"

    def test_thread_entry_frozen(self):
        """ThreadEntry is immutable."""
        e = ThreadEntry(name="t")
        with pytest.raises((ValidationError, TypeError)):
            e.name = "other"  # type: ignore[misc]

    def test_round_trip_with_thread_entries(self):
        """Weave with ThreadEntry objects round-trips correctly."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": [
                    "a",
                    {"name": "b", "dependencies": ["a"]},
                ],
            }
        )
        restored = Weave.model_validate(w.model_dump())
        assert restored == w


class TestConditionSpec:
    """Tests for ConditionSpec model."""

    def test_basic_condition(self):
        """ConditionSpec stores a when expression."""
        cond = ConditionSpec(when="table_exists('raw.customers')")
        assert cond.when == "table_exists('raw.customers')"

    def test_frozen(self):
        """ConditionSpec is immutable."""
        cond = ConditionSpec(when="true")
        with pytest.raises((ValidationError, TypeError)):
            cond.when = "false"  # type: ignore[misc]

    def test_missing_when_raises(self):
        """ConditionSpec without when raises ValidationError."""
        with pytest.raises(ValidationError):
            ConditionSpec.model_validate({})


class TestThreadEntryCondition:
    """Tests for ThreadEntry with condition field."""

    def test_thread_entry_without_condition(self):
        """ThreadEntry defaults condition to None."""
        entry = ThreadEntry(name="t1")
        assert entry.condition is None

    def test_thread_entry_with_condition(self):
        """ThreadEntry accepts a ConditionSpec."""
        entry = ThreadEntry(
            name="t1",
            condition=ConditionSpec(when="table_exists('raw.data')"),
        )
        assert entry.condition is not None
        assert entry.condition.when == "table_exists('raw.data')"

    def test_thread_entry_condition_from_dict(self):
        """ThreadEntry condition hydrates from dict."""
        entry = ThreadEntry.model_validate(
            {"name": "t1", "condition": {"when": "${param.enabled} == true"}}
        )
        assert entry.condition is not None
        assert entry.condition.when == "${param.enabled} == true"

    def test_weave_with_conditional_thread(self):
        """Weave accepts thread entries with conditions."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": [
                    "always_run",
                    {
                        "name": "conditional",
                        "condition": {"when": "table_exists('staging.data')"},
                    },
                ],
            }
        )
        assert w.threads[0].condition is None
        assert w.threads[1].condition is not None
        assert w.threads[1].condition.when == "table_exists('staging.data')"

    def test_round_trip_with_condition(self):
        """Weave with conditional threads round-trips correctly."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": [
                    {"name": "a", "condition": {"when": "true"}},
                    {"name": "b", "dependencies": ["a"]},
                ],
            }
        )
        restored = Weave.model_validate(w.model_dump())
        assert restored == w


class TestWeaveHookFields:
    """Test Weave with lookups, variables, pre_steps, post_steps."""

    def test_backward_compatible(self):
        """Existing weaves without hook fields still work."""
        w = Weave.model_validate({"config_version": "1.0", "threads": ["t1"]})
        assert w.lookups is None
        assert w.variables is None
        assert w.pre_steps is None
        assert w.post_steps is None

    def test_with_lookups(self):
        """Weave with lookups dict."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": ["t1"],
                "lookups": {
                    "categories": {
                        "source": {"type": "delta", "alias": "ref.categories"},
                        "materialize": True,
                        "strategy": "broadcast",
                    }
                },
            }
        )
        assert w.lookups is not None
        assert "categories" in w.lookups
        assert isinstance(w.lookups["categories"], Lookup)
        assert w.lookups["categories"].materialize is True

    def test_with_variables(self):
        """Weave with variables dict."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": ["t1"],
                "variables": {
                    "batch_id": {"type": "string"},
                    "row_count": {"type": "int", "default": 0},
                },
            }
        )
        assert w.variables is not None
        assert "batch_id" in w.variables
        assert isinstance(w.variables["batch_id"], VariableSpec)
        assert w.variables["row_count"].default == 0

    def test_with_pre_steps(self):
        """Weave with pre_steps list."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": ["t1"],
                "pre_steps": [
                    {
                        "type": "quality_gate",
                        "check": "table_exists",
                        "source": "raw_data",
                    },
                    {
                        "type": "log_message",
                        "message": "Starting weave",
                    },
                ],
            }
        )
        assert w.pre_steps is not None
        assert len(w.pre_steps) == 2
        assert isinstance(w.pre_steps[0], QualityGateStep)
        assert isinstance(w.pre_steps[1], LogMessageStep)

    def test_with_post_steps(self):
        """Weave with post_steps list."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": ["t1"],
                "post_steps": [
                    {
                        "type": "log_message",
                        "message": "Weave complete",
                        "level": "info",
                    }
                ],
            }
        )
        assert w.post_steps is not None
        assert len(w.post_steps) == 1

    def test_with_all_hook_fields(self):
        """Weave with all hook-related fields set."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": ["t1"],
                "lookups": {
                    "regions": {
                        "source": {"type": "delta", "alias": "ref.regions"},
                    }
                },
                "variables": {"status": {"type": "string", "default": "pending"}},
                "pre_steps": [{"type": "log_message", "message": "starting"}],
                "post_steps": [{"type": "log_message", "message": "done"}],
            }
        )
        assert w.lookups is not None
        assert w.variables is not None
        assert w.pre_steps is not None
        assert w.post_steps is not None

    def test_round_trip_with_hooks(self):
        """Weave with hook fields round-trips."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": ["t1"],
                "lookups": {"cats": {"source": {"type": "delta", "alias": "ref.cats"}}},
                "variables": {"x": {"type": "int"}},
                "pre_steps": [{"type": "log_message", "message": "hi"}],
            }
        )
        restored = Weave.model_validate(w.model_dump())
        assert restored == w


class TestWeaveConnectionsField:
    """Tests for the Weave.connections field."""

    def test_connections_defaults_to_none(self):
        """Weave without connections has connections=None (backward compatible)."""
        w = Weave.model_validate({"config_version": "1.0", "threads": ["t1"]})
        assert w.connections is None

    def test_connections_with_valid_onelake_connection(self):
        """Weave with connections dict containing a valid OneLakeConnection parses."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": ["t1"],
                "connections": {
                    "main": {
                        "type": "onelake",
                        "workspace": "ws-guid-1234",
                        "lakehouse": "lh-guid-5678",
                    }
                },
            }
        )
        assert w.connections is not None
        assert "main" in w.connections
        assert isinstance(w.connections["main"], OneLakeConnection)
        assert w.connections["main"].workspace == "ws-guid-1234"

    def test_connections_with_invalid_type_raises(self):
        """Weave rejects connections with invalid connection type."""
        with pytest.raises(ValidationError):
            Weave.model_validate(
                {
                    "config_version": "1.0",
                    "threads": ["t1"],
                    "connections": {
                        "bad": {
                            "type": "azure_blob",
                            "workspace": "ws",
                            "lakehouse": "lh",
                        }
                    },
                }
            )

    def test_connections_missing_required_fields_raises(self):
        """Weave rejects connections entry missing required workspace/lakehouse."""
        with pytest.raises(ValidationError):
            Weave.model_validate(
                {
                    "config_version": "1.0",
                    "threads": ["t1"],
                    "connections": {
                        "bad": {"type": "onelake"},
                    },
                }
            )

    def test_connections_round_trip(self):
        """Weave with connections round-trips through model_dump/model_validate."""
        w = Weave.model_validate(
            {
                "config_version": "1.0",
                "threads": ["t1"],
                "connections": {
                    "primary": {
                        "type": "onelake",
                        "workspace": "ws-1",
                        "lakehouse": "lh-1",
                    }
                },
            }
        )
        restored = Weave.model_validate(w.model_dump())
        assert restored.connections == w.connections
