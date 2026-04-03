"""Tests for WriteConfig model."""

import pytest
from pydantic import ValidationError

from weevr.model.write import WriteConfig


class TestWriteConfig:
    """Test WriteConfig model."""

    def test_default_mode_is_overwrite(self):
        """Default write mode is overwrite."""
        w = WriteConfig()
        assert w.mode == "overwrite"

    def test_append_mode(self):
        """Append mode is valid."""
        w = WriteConfig(mode="append")
        assert w.mode == "append"

    def test_insert_only_mode_invalid(self):
        """insert_only is no longer a valid write mode."""
        with pytest.raises(ValidationError):
            WriteConfig(mode="insert_only")  # type: ignore[arg-type]

    def test_merge_mode_with_keys(self):
        """Merge mode is valid when match_keys is supplied."""
        w = WriteConfig(mode="merge", match_keys=["id"])
        assert w.mode == "merge"
        assert w.match_keys == ["id"]

    def test_merge_without_keys_raises(self):
        """Merge mode without match_keys raises ValidationError."""
        with pytest.raises(ValidationError, match="match_keys"):
            WriteConfig(mode="merge")

    def test_default_on_match(self):
        """Default on_match is 'update'."""
        w = WriteConfig()
        assert w.on_match == "update"

    def test_default_on_no_match_target(self):
        """Default on_no_match_target is 'insert'."""
        w = WriteConfig()
        assert w.on_no_match_target == "insert"

    def test_default_on_no_match_source(self):
        """Default on_no_match_source is 'ignore'."""
        w = WriteConfig()
        assert w.on_no_match_source == "ignore"

    def test_invalid_mode_raises(self):
        """Unknown write mode raises ValidationError."""
        with pytest.raises(ValidationError):
            WriteConfig(mode="upsert")  # type: ignore[arg-type]

    def test_frozen(self):
        """WriteConfig is immutable."""
        w = WriteConfig(mode="append")
        with pytest.raises(ValidationError):
            w.mode = "overwrite"  # type: ignore[misc]

    def test_round_trip(self):
        """WriteConfig round-trips through model_dump/model_validate."""
        w = WriteConfig(
            mode="merge",
            match_keys=["id", "date"],
            on_match="ignore",
            on_no_match_source="delete",
        )
        assert WriteConfig.model_validate(w.model_dump()) == w

    def test_soft_delete_retain_value_defaults_to_none(self):
        """soft_delete_retain_value is None by default."""
        w = WriteConfig()
        assert w.soft_delete_retain_value is None

    def test_soft_delete_retain_value_false_round_trip(self):
        """soft_delete_retain_value=False round-trips correctly."""
        w = WriteConfig(
            mode="merge",
            match_keys=["id"],
            on_no_match_source="soft_delete",
            soft_delete_column="is_deleted",
            soft_delete_retain_value=False,
        )
        restored = WriteConfig.model_validate(w.model_dump())
        assert restored == w
        assert restored.soft_delete_retain_value is False

    def test_soft_delete_retain_value_true_round_trip(self):
        """soft_delete_retain_value=True round-trips correctly."""
        w = WriteConfig(
            mode="merge",
            match_keys=["id"],
            on_no_match_source="soft_delete",
            soft_delete_column="is_deleted",
            soft_delete_retain_value=True,
        )
        restored = WriteConfig.model_validate(w.model_dump())
        assert restored == w
        assert restored.soft_delete_retain_value is True
