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

    def test_insert_only_mode(self):
        """Insert-only mode is valid."""
        w = WriteConfig(mode="insert_only")
        assert w.mode == "insert_only"

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
