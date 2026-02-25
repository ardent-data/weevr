"""Tests for Weave composite model."""

import pytest
from pydantic import ValidationError

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
