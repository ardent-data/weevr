"""Integration tests: YAML config → load_config() / model_validate() → frozen model."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from weevr.config import load_config
from weevr.errors import ModelValidationError
from weevr.model import (
    Loom,
    Thread,
    Weave,
    WeaveEntry,
    WriteConfig,
)
from weevr.model.keys import KeyConfig
from weevr.model.load import LoadConfig
from weevr.model.pipeline import (
    CastStep,
    DedupStep,
    DeriveStep,
    DropStep,
    FilterStep,
    JoinStep,
    RenameStep,
    SelectStep,
    SortStep,
    UnionStep,
)
from weevr.model.source import DedupConfig, Source
from weevr.model.target import ColumnMapping, Target
from weevr.model.validation import Assertion, ValidationRule

FIXTURES = Path(__file__).parent / "fixtures"


class TestThreadHydrationViaPipeline:
    """End-to-end: YAML file → load_config() → frozen Thread."""

    def test_minimal_thread_hydrates(self):
        """Minimal thread YAML produces a valid frozen Thread."""
        result = load_config(FIXTURES / "thread_minimal.thread")

        assert isinstance(result, Thread)
        assert result.config_version == "1.0"
        assert isinstance(result.sources["customers"], Source)
        assert result.sources["customers"].type == "delta"
        assert result.sources["customers"].alias == "raw.customers"
        assert isinstance(result.target, Target)
        assert result.steps == []
        assert isinstance(result.name, str)  # name field exists; value set by loader (Task 3)

    def test_full_thread_all_nested_models(self):
        """Full thread YAML hydrates all nested model types correctly."""
        result = load_config(FIXTURES / "thread_full.thread")

        assert isinstance(result, Thread)

        # Sources
        assert len(result.sources) == 2
        assert isinstance(result.sources["customers"], Source)
        assert isinstance(result.sources["customers"].dedup, DedupConfig)
        assert isinstance(result.sources["orders"], Source)
        assert result.sources["orders"].type == "csv"

        # All 10 step types present and correctly typed
        assert len(result.steps) == 10
        assert isinstance(result.steps[0], FilterStep)
        assert isinstance(result.steps[1], DeriveStep)
        assert isinstance(result.steps[2], JoinStep)
        assert isinstance(result.steps[3], SelectStep)
        assert isinstance(result.steps[4], DropStep)
        assert isinstance(result.steps[5], RenameStep)
        assert isinstance(result.steps[6], CastStep)
        assert isinstance(result.steps[7], DedupStep)
        assert isinstance(result.steps[8], SortStep)
        assert isinstance(result.steps[9], UnionStep)

        # Step content preserved
        assert result.steps[0].filter.expr == "active = true"
        assert "full_name" in result.steps[1].derive.columns
        assert result.steps[2].join.source == "orders"
        assert result.steps[2].join.type == "left"

        # Target
        assert isinstance(result.target, Target)
        assert result.target.mapping_mode == "explicit"
        assert result.target.columns is not None
        assert isinstance(result.target.columns["id"], ColumnMapping)
        assert result.target.partition_by == ["year", "month"]
        assert result.target.audit_columns == {
            "_weevr_loaded_at": "current_timestamp()",
            "_weevr_run_id": "'standard_v1'",
        }

        # Write
        assert isinstance(result.write, WriteConfig)
        assert result.write.mode == "merge"
        assert result.write.match_keys == ["id"]

        # Keys
        assert isinstance(result.keys, KeyConfig)
        assert result.keys.business_key == ["id"]
        assert result.keys.surrogate_key is not None
        assert result.keys.surrogate_key.algorithm == "sha256"
        assert result.keys.change_detection is not None

        # Load
        assert isinstance(result.load, LoadConfig)
        assert result.load.mode == "incremental_watermark"
        assert result.load.watermark_column == "modified_at"
        assert result.load.watermark_type == "timestamp"

        # Validations and assertions
        assert result.validations is not None
        assert len(result.validations) == 2
        assert isinstance(result.validations[0], ValidationRule)
        assert result.validations[0].name == "id_not_null"
        assert result.validations[1].severity == "warn"

        assert result.assertions is not None
        assert len(result.assertions) == 2
        assert isinstance(result.assertions[0], Assertion)
        assert result.assertions[0].type == "row_count"

        # Tags
        assert result.tags == ["nightly", "critical"]

    def test_invalid_step_raises_model_validation_error(self):
        """Unknown step type raises ModelValidationError."""
        with pytest.raises(ModelValidationError) as exc_info:
            load_config(FIXTURES / "thread_invalid_step.thread")
        assert "thread" in str(exc_info.value).lower()

    def test_merge_write_without_keys_raises_model_validation_error(self):
        """Merge mode without match_keys raises ModelValidationError."""
        with pytest.raises(ModelValidationError):
            load_config(FIXTURES / "thread_merge_no_keys.thread")

    def test_join_explicit_key_pairs(self):
        """Thread with explicit join key pairs hydrates correctly."""
        result = load_config(FIXTURES / "thread_join_explicit_keys.thread")

        assert isinstance(result, Thread)
        join_step = result.steps[0]
        assert isinstance(join_step, JoinStep)
        assert join_step.join.on[0].left == "customer_id"
        assert join_step.join.on[0].right == "cust_id"

    def test_watermark_load_config(self):
        """Thread with incremental_watermark load config hydrates correctly."""
        result = load_config(FIXTURES / "thread_watermark.thread")

        assert isinstance(result, Thread)
        assert isinstance(result.load, LoadConfig)
        assert result.load.mode == "incremental_watermark"
        assert result.load.watermark_column == "event_ts"


class TestThreadFrozenAfterHydration:
    """Verify all hydrated models are frozen."""

    def test_thread_is_frozen(self):
        """Thread model is immutable after hydration."""
        result = load_config(FIXTURES / "thread_minimal.thread")
        assert isinstance(result, Thread)
        with pytest.raises(ValidationError):
            result.config_version = "2.0"  # type: ignore[misc]

    def test_nested_source_is_frozen(self):
        """Nested Source model is immutable after hydration."""
        result = load_config(FIXTURES / "thread_minimal.thread")
        assert isinstance(result, Thread)
        with pytest.raises(ValidationError):
            result.sources["customers"].type = "csv"  # type: ignore[misc]

    def test_full_thread_nested_models_frozen(self):
        """All nested models in a full thread are frozen."""
        result = load_config(FIXTURES / "thread_full.thread")
        assert isinstance(result, Thread)

        with pytest.raises(ValidationError):
            result.write.mode = "append"  # type: ignore[misc]
        with pytest.raises(ValidationError):
            result.load.watermark_column = "other"  # type: ignore[misc]
        with pytest.raises(ValidationError):
            result.keys.business_key = ["other"]  # type: ignore[misc]


class TestWeaveAndLoomModelHydration:
    """Weave and Loom hydration via model_validate() (pipeline coverage in test_load_config)."""

    def test_weave_hydrates_from_dict(self):
        """Weave model_validate() from a dict produces a frozen Weave."""
        data = {
            "config_version": "1.0",
            "threads": ["dimensions.dim_customer", "dimensions.dim_product"],
            "defaults": {"tags": ["nightly"]},
        }
        w = Weave.model_validate(data)
        assert isinstance(w, Weave)
        assert [e.name for e in w.threads] == ["dimensions.dim_customer", "dimensions.dim_product"]
        assert w.defaults is not None
        assert w.defaults["tags"] == ["nightly"]
        with pytest.raises(ValidationError):
            w.config_version = "2.0"  # type: ignore[misc]

    def test_loom_hydrates_from_dict(self):
        """Loom model_validate() from a dict produces a frozen Loom."""
        data = {
            "config_version": "1.0",
            "weaves": ["dimensions", "facts"],
            "defaults": {"tags": ["production"]},
        }
        loom = Loom.model_validate(data)
        assert isinstance(loom, Loom)
        assert loom.weaves == [WeaveEntry(name="dimensions"), WeaveEntry(name="facts")]
        with pytest.raises(ValidationError):
            loom.weaves = []  # type: ignore[misc]

    def test_extra_keys_silently_ignored(self):
        """Extra keys (_resolved_weaves etc.) are silently dropped by Pydantic."""
        data = {
            "config_version": "1.0",
            "weaves": ["dimensions"],
            "_resolved_weaves": [{"config_version": "1.0", "threads": ["t1"]}],
        }
        loom = Loom.model_validate(data)
        assert not hasattr(loom, "_resolved_weaves")
        assert loom.weaves == [WeaveEntry(name="dimensions")]
