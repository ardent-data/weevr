"""Tests for Thread composite model."""

import pytest
from pydantic import ValidationError

from weevr.model.failure import FailureConfig
from weevr.model.keys import KeyConfig
from weevr.model.load import LoadConfig
from weevr.model.pipeline import DeriveStep, FilterStep
from weevr.model.source import Source
from weevr.model.target import Target
from weevr.model.thread import Thread
from weevr.model.validation import Assertion, ValidationRule
from weevr.model.write import WriteConfig

_MINIMAL = {
    "config_version": "1.0",
    "sources": {"customers": {"type": "delta", "alias": "raw.customers"}},
    "target": {},
}


class TestThreadConstruction:
    """Test Thread model construction."""

    def test_minimal_thread(self):
        """Thread from minimal dict constructs correctly."""
        t = Thread.model_validate(_MINIMAL)
        assert t.config_version == "1.0"
        assert isinstance(t.sources["customers"], Source)
        assert isinstance(t.target, Target)

    def test_missing_sources_raises(self):
        """Thread without sources raises ValidationError."""
        with pytest.raises(ValidationError):
            Thread.model_validate({"config_version": "1.0", "target": {}})

    def test_missing_target_raises(self):
        """Thread without target raises ValidationError."""
        with pytest.raises(ValidationError):
            Thread.model_validate(
                {
                    "config_version": "1.0",
                    "sources": {"s": {"type": "delta", "alias": "a"}},
                }
            )

    def test_sources_auto_hydrate(self):
        """sources dict values are hydrated into Source models."""
        t = Thread.model_validate(_MINIMAL)
        assert isinstance(t.sources["customers"], Source)
        assert t.sources["customers"].alias == "raw.customers"

    def test_steps_default_empty(self):
        """Thread steps default to an empty list."""
        t = Thread.model_validate(_MINIMAL)
        assert t.steps == []

    def test_steps_hydrated_via_union(self):
        """Step dicts in steps list are hydrated via Step discriminated union."""
        data = {
            **_MINIMAL,
            "steps": [
                {"filter": {"expr": "amount > 0"}},
                {"derive": {"columns": {"amount_usd": "amount * rate"}}},
            ],
        }
        t = Thread.model_validate(data)
        assert len(t.steps) == 2
        assert isinstance(t.steps[0], FilterStep)
        assert isinstance(t.steps[1], DeriveStep)

    def test_write_config_hydrated(self):
        """write dict is hydrated into WriteConfig model."""
        data = {**_MINIMAL, "write": {"mode": "merge", "match_keys": ["id"]}}
        t = Thread.model_validate(data)
        assert isinstance(t.write, WriteConfig)
        assert t.write.mode == "merge"

    def test_keys_hydrated(self):
        """keys dict is hydrated into KeyConfig model."""
        data = {**_MINIMAL, "keys": {"business_key": ["id"]}}
        t = Thread.model_validate(data)
        assert isinstance(t.keys, KeyConfig)

    def test_load_hydrated(self):
        """load dict is hydrated into LoadConfig model."""
        data = {
            **_MINIMAL,
            "load": {"mode": "incremental_watermark", "watermark_column": "modified_at"},
        }
        t = Thread.model_validate(data)
        assert isinstance(t.load, LoadConfig)

    def test_validations_hydrated(self):
        """validations list items are hydrated into ValidationRule models."""
        data = {
            **_MINIMAL,
            "validations": [{"rule": "id IS NOT NULL", "name": "id_not_null"}],
        }
        t = Thread.model_validate(data)
        assert t.validations is not None
        assert isinstance(t.validations[0], ValidationRule)

    def test_assertions_hydrated(self):
        """assertions list items are hydrated into Assertion models."""
        data = {**_MINIMAL, "assertions": [{"type": "row_count", "min": 1}]}
        t = Thread.model_validate(data)
        assert t.assertions is not None
        assert isinstance(t.assertions[0], Assertion)

    def test_tags_accepted(self):
        """tags list is stored as-is."""
        data = {**_MINIMAL, "tags": ["daily", "critical"]}
        t = Thread.model_validate(data)
        assert t.tags == ["daily", "critical"]

    def test_full_thread(self):
        """Thread from a fully populated dict constructs correctly."""
        data = {
            "config_version": "1.0",
            "sources": {
                "customers": {"type": "delta", "alias": "raw.customers"},
                "orders": {"type": "csv", "path": "/data/orders.csv"},
            },
            "steps": [
                {"filter": {"expr": "active = true"}},
                {"select": {"columns": ["id", "name", "amount"]}},
            ],
            "target": {"mapping_mode": "explicit", "partition_by": ["year"]},
            "write": {"mode": "merge", "match_keys": ["id"]},
            "keys": {"business_key": ["id"]},
            "load": {"mode": "incremental_watermark", "watermark_column": "ts"},
            "validations": [{"rule": "id IS NOT NULL", "name": "r1"}],
            "assertions": [{"type": "unique", "columns": ["id"]}],
            "tags": ["nightly"],
            "defaults": {"write": {"mode": "append"}},
        }
        t = Thread.model_validate(data)
        assert len(t.sources) == 2
        assert len(t.steps) == 2
        assert t.write is not None
        assert t.write.mode == "merge"
        assert t.keys is not None
        assert t.keys.business_key == ["id"]


class TestThreadFreezeAndRoundTrip:
    """Test Thread immutability and round-trip."""

    def test_frozen(self):
        """Thread is immutable."""
        t = Thread.model_validate(_MINIMAL)
        with pytest.raises(ValidationError):
            t.config_version = "2.0"  # type: ignore[misc]

    def test_round_trip(self):
        """Thread round-trips through model_dump/model_validate."""
        data = {
            **_MINIMAL,
            "steps": [{"filter": {"expr": "x > 0"}}],
            "write": {"mode": "append"},
            "tags": ["test"],
        }
        t = Thread.model_validate(data)
        restored = Thread.model_validate(t.model_dump())
        assert restored.config_version == t.config_version
        assert len(restored.steps) == 1


class TestThreadNameField:
    """Test Thread.name field behaviour."""

    def test_name_defaults_to_empty_string(self):
        """name defaults to empty string when not provided."""
        t = Thread.model_validate(_MINIMAL)
        assert t.name == ""

    def test_name_can_be_set(self):
        """name is stored when explicitly provided."""
        t = Thread.model_validate({**_MINIMAL, "name": "dimensions.dim_customer"})
        assert t.name == "dimensions.dim_customer"

    def test_name_preserved_in_round_trip(self):
        """name survives a model_dump/model_validate round-trip."""
        t = Thread.model_validate({**_MINIMAL, "name": "facts.fact_order"})
        restored = Thread.model_validate(t.model_dump())
        assert restored.name == "facts.fact_order"


class TestThreadFailureConfig:
    """Tests for the Thread.failure field and FailureConfig model."""

    def test_failure_defaults_to_none(self):
        """Thread without failure block has failure=None (backward compatible)."""
        t = Thread.model_validate(_MINIMAL)
        assert t.failure is None

    def test_failure_config_hydrated(self):
        """Thread with failure block is hydrated into FailureConfig."""
        data = {**_MINIMAL, "failure": {"on_failure": "skip_downstream"}}
        t = Thread.model_validate(data)
        assert isinstance(t.failure, FailureConfig)
        assert t.failure.on_failure == "skip_downstream"

    def test_failure_config_default_on_failure(self):
        """FailureConfig defaults on_failure to 'abort_weave'."""
        cfg = FailureConfig()
        assert cfg.on_failure == "abort_weave"

    def test_failure_config_abort_weave(self):
        """FailureConfig accepts on_failure='abort_weave'."""
        cfg = FailureConfig(on_failure="abort_weave")
        assert cfg.on_failure == "abort_weave"

    def test_failure_config_skip_downstream(self):
        """FailureConfig accepts on_failure='skip_downstream'."""
        cfg = FailureConfig(on_failure="skip_downstream")
        assert cfg.on_failure == "skip_downstream"

    def test_failure_config_continue(self):
        """FailureConfig accepts on_failure='continue'."""
        cfg = FailureConfig(on_failure="continue")
        assert cfg.on_failure == "continue"

    def test_failure_config_invalid_raises(self):
        """FailureConfig rejects unknown on_failure values."""
        with pytest.raises(ValidationError):
            FailureConfig(on_failure="retry")  # type: ignore[arg-type]

    def test_thread_with_abort_weave_failure(self):
        """Thread parses failure block with abort_weave mode."""
        data = {**_MINIMAL, "failure": {"on_failure": "abort_weave"}}
        t = Thread.model_validate(data)
        assert t.failure is not None
        assert t.failure.on_failure == "abort_weave"

    def test_failure_config_frozen(self):
        """FailureConfig is immutable."""
        cfg = FailureConfig()
        with pytest.raises((ValidationError, TypeError)):
            cfg.on_failure = "continue"  # type: ignore[misc]


class TestThreadCacheField:
    """Tests for the Thread.cache field."""

    def test_cache_defaults_to_none(self):
        """Thread without cache field has cache=None (auto-cache by planner)."""
        t = Thread.model_validate(_MINIMAL)
        assert t.cache is None

    def test_cache_false_suppresses_auto_cache(self):
        """Thread with cache=False stores the value."""
        t = Thread.model_validate({**_MINIMAL, "cache": False})
        assert t.cache is False

    def test_cache_true_forces_caching(self):
        """Thread with cache=True stores the value."""
        t = Thread.model_validate({**_MINIMAL, "cache": True})
        assert t.cache is True
