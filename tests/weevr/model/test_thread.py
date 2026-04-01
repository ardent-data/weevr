"""Tests for Thread composite model."""

import logging

import pytest
from pydantic import ValidationError

from weevr.model.connection import OneLakeConnection
from weevr.model.failure import FailureConfig
from weevr.model.keys import KeyConfig
from weevr.model.load import LoadConfig
from weevr.model.pipeline import DeriveStep, FilterStep
from weevr.model.source import Source
from weevr.model.sub_pipeline import SubPipeline
from weevr.model.target import Target
from weevr.model.thread import Thread
from weevr.model.validation import Assertion, ValidationRule
from weevr.model.write import WriteConfig

_MINIMAL = {
    "config_version": "1.0",
    "sources": {"customers": {"type": "delta", "alias": "raw.customers"}},
    "target": {"alias": "test"},
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
            Thread.model_validate({"config_version": "1.0", "target": {"alias": "test"}})

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
            "target": {
                "alias": "curated.customers",
                "mapping_mode": "explicit",
                "partition_by": ["year"],
            },
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


class TestThreadSharedResourceFields:
    """Tests for Thread shared resource fields."""

    def test_all_shared_resource_fields_default_to_none(self):
        """All five shared resource fields default to None when omitted."""
        t = Thread.model_validate(_MINIMAL)
        assert t.lookups is None
        assert t.column_sets is None
        assert t.variables is None
        assert t.pre_steps is None
        assert t.post_steps is None

    def test_lookups_accepted(self):
        """Thread accepts lookups as a dict of Lookup models."""
        data = {
            **_MINIMAL,
            "lookups": {
                "customer_lookup": {
                    "source": {"type": "delta", "alias": "ref.customers"},
                    "key": ["customer_id"],
                }
            },
        }
        t = Thread.model_validate(data)
        assert t.lookups is not None
        assert "customer_lookup" in t.lookups

    def test_column_sets_accepted(self):
        """Thread accepts column_sets as a dict of ColumnSet models."""
        data = {
            **_MINIMAL,
            "column_sets": {
                "sap_rename": {
                    "source": {"type": "delta", "alias": "ref.mappings"},
                    "key_column": "source_name",
                    "value_column": "target_name",
                }
            },
        }
        t = Thread.model_validate(data)
        assert t.column_sets is not None
        assert "sap_rename" in t.column_sets

    def test_variables_accepted(self):
        """Thread accepts variables as a dict of VariableSpec models."""
        data = {
            **_MINIMAL,
            "variables": {"record_count": {"type": "int"}},
        }
        t = Thread.model_validate(data)
        assert t.variables is not None
        assert "record_count" in t.variables

    def test_pre_steps_accepted(self):
        """Thread accepts pre_steps as a list of HookStep models."""
        data = {
            **_MINIMAL,
            "pre_steps": [
                {"type": "quality_gate", "check": "table_exists", "source": "raw.customers"}
            ],
        }
        t = Thread.model_validate(data)
        assert t.pre_steps is not None
        assert len(t.pre_steps) == 1

    def test_post_steps_accepted(self):
        """Thread accepts post_steps as a list of HookStep models."""
        data = {
            **_MINIMAL,
            "post_steps": [
                {
                    "type": "quality_gate",
                    "check": "row_count",
                    "target": "dim.customers",
                    "min_count": 1,
                }
            ],
        }
        t = Thread.model_validate(data)
        assert t.post_steps is not None
        assert len(t.post_steps) == 1

    def test_lookups_must_be_dict(self):
        """Thread rejects lookups that are not a dict."""
        with pytest.raises(ValidationError):
            Thread.model_validate({**_MINIMAL, "lookups": ["not", "a", "dict"]})

    def test_column_sets_must_be_dict(self):
        """Thread rejects column_sets that are not a dict."""
        with pytest.raises(ValidationError):
            Thread.model_validate({**_MINIMAL, "column_sets": "not_a_dict"})

    def test_variables_must_be_dict(self):
        """Thread rejects variables that are not a dict."""
        with pytest.raises(ValidationError):
            Thread.model_validate({**_MINIMAL, "variables": [{"type": "int"}]})

    def test_pre_steps_must_be_list(self):
        """Thread rejects pre_steps that are not a list."""
        with pytest.raises(ValidationError):
            Thread.model_validate({**_MINIMAL, "pre_steps": {"type": "quality_gate"}})

    def test_post_steps_must_be_list(self):
        """Thread rejects post_steps that are not a list."""
        with pytest.raises(ValidationError):
            Thread.model_validate({**_MINIMAL, "post_steps": {"type": "quality_gate"}})

    def test_empty_sources_raises(self):
        """Thread with empty sources dict raises ValidationError."""
        with pytest.raises(ValidationError, match="at least one entry"):
            Thread.model_validate(
                {
                    "config_version": "1.0",
                    "sources": {},
                    "target": {"alias": "test"},
                }
            )


class TestThreadConnectionsField:
    """Tests for the Thread.connections field."""

    def test_connections_defaults_to_none(self):
        """Thread without connections has connections=None (backward compatible)."""
        t = Thread.model_validate(_MINIMAL)
        assert t.connections is None

    def test_connections_with_valid_onelake_connection(self):
        """Thread with connections dict containing a valid OneLakeConnection parses."""
        data = {
            **_MINIMAL,
            "connections": {
                "main": {
                    "type": "onelake",
                    "workspace": "ws-guid-1234",
                    "lakehouse": "lh-guid-5678",
                }
            },
        }
        t = Thread.model_validate(data)
        assert t.connections is not None
        assert "main" in t.connections
        assert isinstance(t.connections["main"], OneLakeConnection)
        assert t.connections["main"].workspace == "ws-guid-1234"
        assert t.connections["main"].lakehouse == "lh-guid-5678"

    def test_connections_with_default_schema(self):
        """Thread connections entry accepts optional default_schema."""
        data = {
            **_MINIMAL,
            "connections": {
                "bronze": {
                    "type": "onelake",
                    "workspace": "${param.workspace}",
                    "lakehouse": "${param.lakehouse}",
                    "default_schema": "dbo",
                }
            },
        }
        t = Thread.model_validate(data)
        assert t.connections is not None
        assert t.connections["bronze"].default_schema == "dbo"

    def test_connections_with_invalid_type_raises(self):
        """Thread rejects connections with invalid connection type."""
        with pytest.raises(ValidationError):
            Thread.model_validate(
                {
                    **_MINIMAL,
                    "connections": {
                        "bad": {
                            "type": "s3",
                            "workspace": "ws",
                            "lakehouse": "lh",
                        }
                    },
                }
            )

    def test_connections_missing_required_fields_raises(self):
        """Thread rejects connections entry missing required workspace/lakehouse."""
        with pytest.raises(ValidationError):
            Thread.model_validate(
                {
                    **_MINIMAL,
                    "connections": {
                        "bad": {"type": "onelake"},
                    },
                }
            )

    def test_connections_round_trip(self):
        """Thread with connections round-trips through model_dump/model_validate."""
        data = {
            **_MINIMAL,
            "connections": {
                "primary": {
                    "type": "onelake",
                    "workspace": "ws-1",
                    "lakehouse": "lh-1",
                }
            },
        }
        t = Thread.model_validate(data)
        restored = Thread.model_validate(t.model_dump())
        assert restored.connections == t.connections


_MINIMAL_WITH_SOURCES = {
    "config_version": "1.0",
    "sources": {
        "customers": {"type": "delta", "alias": "raw.customers"},
        "orders": {"type": "delta", "alias": "raw.orders"},
    },
    "target": {"alias": "test"},
}


class TestThreadWithBlock:
    """Tests for the Thread with: block (named sub-pipelines / CTEs)."""

    def test_with_block_absent_defaults_to_none(self):
        """Thread without with: block has with_=None (backward compatible)."""
        t = Thread.model_validate(_MINIMAL)
        assert t.with_ is None

    def test_with_block_parses_correctly(self):
        """Thread with a valid with: block parses sub-pipelines into SubPipeline models."""
        data = {
            **_MINIMAL_WITH_SOURCES,
            "with": {
                "active_customers": {
                    "from": "customers",
                    "steps": [{"filter": {"expr": "active = true"}}],
                },
            },
        }
        t = Thread.model_validate(data)
        assert t.with_ is not None
        assert "active_customers" in t.with_
        assert isinstance(t.with_["active_customers"], SubPipeline)
        assert t.with_["active_customers"].from_ == "customers"

    def test_with_block_multiple_ctes(self):
        """Thread with multiple CTEs in order parses all entries."""
        data = {
            **_MINIMAL_WITH_SOURCES,
            "with": {
                "active_customers": {
                    "from": "customers",
                    "steps": [{"filter": {"expr": "active = true"}}],
                },
                "recent_orders": {
                    "from": "orders",
                    "steps": [{"filter": {"expr": "year = 2024"}}],
                },
            },
        }
        t = Thread.model_validate(data)
        assert t.with_ is not None
        assert len(t.with_) == 2
        assert "active_customers" in t.with_
        assert "recent_orders" in t.with_

    def test_cte_referencing_prior_cte(self):
        """CTE may reference a CTE declared above it (forward ref allowed upward)."""
        data = {
            **_MINIMAL_WITH_SOURCES,
            "with": {
                "active_customers": {
                    "from": "customers",
                    "steps": [{"filter": {"expr": "active = true"}}],
                },
                "enriched": {
                    "from": "active_customers",
                    "steps": [{"filter": {"expr": "score > 50"}}],
                },
            },
        }
        t = Thread.model_validate(data)
        assert t.with_ is not None
        assert t.with_["enriched"].from_ == "active_customers"

    def test_cte_name_collides_with_source_raises(self):
        """CTE name that matches a source name raises ValidationError."""
        data = {
            **_MINIMAL_WITH_SOURCES,
            "with": {
                "customers": {  # same as a source name
                    "from": "orders",
                    "steps": [{"filter": {"expr": "x > 0"}}],
                },
            },
        }
        with pytest.raises(ValidationError, match="customers"):
            Thread.model_validate(data)

    def test_cte_from_referencing_nonexistent_source_raises(self):
        """CTE from: referencing a name not in sources or prior CTEs raises ValidationError."""
        data = {
            **_MINIMAL_WITH_SOURCES,
            "with": {
                "derived": {
                    "from": "nonexistent_table",
                    "steps": [{"filter": {"expr": "x > 0"}}],
                },
            },
        }
        with pytest.raises(ValidationError, match="nonexistent_table"):
            Thread.model_validate(data)

    def test_cte_forward_reference_raises(self):
        """CTE referencing a CTE declared below it (forward reference) raises ValidationError."""
        data = {
            **_MINIMAL_WITH_SOURCES,
            "with": {
                "derived": {
                    "from": "active_customers",  # declared after
                    "steps": [{"filter": {"expr": "x > 0"}}],
                },
                "active_customers": {
                    "from": "customers",
                    "steps": [{"filter": {"expr": "active = true"}}],
                },
            },
        }
        with pytest.raises(ValidationError, match="active_customers"):
            Thread.model_validate(data)

    def test_unused_cte_produces_warning(self, caplog):
        """CTE that is never referenced by any join/union step produces a warning."""
        data = {
            **_MINIMAL_WITH_SOURCES,
            "with": {
                "active_customers": {
                    "from": "customers",
                    "steps": [{"filter": {"expr": "active = true"}}],
                },
            },
            # No join/union steps referencing active_customers
        }
        with caplog.at_level(logging.WARNING):
            Thread.model_validate(data)
        assert "unused" in caplog.text.lower() or "active_customers" in caplog.text

    def test_used_cte_no_warning(self, caplog):
        """CTE referenced in a join step does not produce an unused warning."""
        data = {
            **_MINIMAL_WITH_SOURCES,
            "with": {
                "active_customers": {
                    "from": "customers",
                    "steps": [{"filter": {"expr": "active = true"}}],
                },
            },
            "steps": [
                {
                    "join": {
                        "source": "active_customers",
                        "type": "inner",
                        "on": ["id"],
                    }
                }
            ],
        }
        with caplog.at_level(logging.WARNING):
            Thread.model_validate(data)
        assert "active_customers" not in caplog.text or "unused" not in caplog.text.lower()

    def test_join_alias_collides_with_source_raises(self):
        """Join step alias matching a source name raises ValidationError."""
        data = {
            **_MINIMAL_WITH_SOURCES,
            "steps": [
                {
                    "join": {
                        "source": "orders",
                        "type": "left",
                        "on": ["id"],
                        "alias": "customers",  # collides with source name
                    }
                }
            ],
        }
        with pytest.raises(ValidationError, match="customers"):
            Thread.model_validate(data)

    def test_join_alias_collides_with_cte_name_raises(self):
        """Join step alias matching a CTE name raises ValidationError."""
        data = {
            **_MINIMAL_WITH_SOURCES,
            "with": {
                "active_customers": {
                    "from": "customers",
                    "steps": [{"filter": {"expr": "active = true"}}],
                },
            },
            "steps": [
                {
                    "join": {
                        "source": "active_customers",
                        "type": "inner",
                        "on": ["id"],
                        "alias": "active_customers",  # collides with CTE name
                    }
                }
            ],
        }
        with pytest.raises(ValidationError, match="active_customers"):
            Thread.model_validate(data)

    def test_join_alias_duplicate_across_thread_raises(self):
        """Two join steps with the same alias raises ValidationError."""
        data = {
            **_MINIMAL_WITH_SOURCES,
            "steps": [
                {
                    "join": {
                        "source": "customers",
                        "type": "left",
                        "on": ["id"],
                        "alias": "cust",
                    }
                },
                {
                    "join": {
                        "source": "orders",
                        "type": "left",
                        "on": ["id"],
                        "alias": "cust",  # duplicate alias
                    }
                },
            ],
        }
        with pytest.raises(ValidationError, match="cust"):
            Thread.model_validate(data)

    def test_join_alias_duplicate_across_cte_and_main_raises(self):
        """Join alias duplicate spanning CTE steps and main steps raises ValidationError."""
        data = {
            **_MINIMAL_WITH_SOURCES,
            "with": {
                "active_customers": {
                    "from": "customers",
                    "steps": [
                        {
                            "join": {
                                "source": "orders",
                                "type": "left",
                                "on": ["id"],
                                "alias": "shared_alias",
                            }
                        }
                    ],
                },
            },
            "steps": [
                {
                    "join": {
                        "source": "active_customers",
                        "type": "inner",
                        "on": ["id"],
                        "alias": "shared_alias",  # duplicate of CTE join alias
                    }
                }
            ],
        }
        with pytest.raises(ValidationError, match="shared_alias"):
            Thread.model_validate(data)

    def test_join_alias_none_no_collision(self):
        """Join steps without aliases are not subject to alias collision checks."""
        data = {
            **_MINIMAL_WITH_SOURCES,
            "steps": [
                {
                    "join": {
                        "source": "orders",
                        "type": "left",
                        "on": ["id"],
                        # no alias
                    }
                }
            ],
        }
        t = Thread.model_validate(data)
        assert t.steps is not None
        assert len(t.steps) == 1

    def test_with_block_round_trip(self):
        """Thread with with: block round-trips through model_dump/model_validate."""
        data = {
            **_MINIMAL_WITH_SOURCES,
            "with": {
                "active_customers": {
                    "from": "customers",
                    "steps": [{"filter": {"expr": "active = true"}}],
                },
            },
            "steps": [
                {
                    "join": {
                        "source": "active_customers",
                        "type": "inner",
                        "on": ["id"],
                    }
                }
            ],
        }
        t = Thread.model_validate(data)
        restored = Thread.model_validate(t.model_dump())
        assert restored.with_ is not None
        assert "active_customers" in restored.with_
        assert isinstance(restored.with_["active_customers"], SubPipeline)
