"""Tests for LoadConfig model."""

import pytest
from pydantic import ValidationError

from weevr.model.load import LoadConfig


class TestLoadConfig:
    """Test LoadConfig model."""

    def test_default_mode_is_full(self):
        """Default mode is 'full'."""
        lc = LoadConfig()
        assert lc.mode == "full"

    def test_full_mode_no_watermark(self):
        """Full mode without watermark fields is valid."""
        lc = LoadConfig(mode="full")
        assert lc.watermark_column is None
        assert lc.watermark_type is None

    def test_incremental_parameter_mode(self):
        """incremental_parameter mode is valid without watermark fields."""
        lc = LoadConfig(mode="incremental_parameter")
        assert lc.mode == "incremental_parameter"

    def test_cdc_mode(self):
        """cdc mode is valid."""
        lc = LoadConfig(mode="cdc")
        assert lc.mode == "cdc"

    def test_watermark_mode_with_column(self):
        """incremental_watermark with watermark_column is valid."""
        lc = LoadConfig(mode="incremental_watermark", watermark_column="modified_at")
        assert lc.watermark_column == "modified_at"

    def test_watermark_mode_with_type(self):
        """incremental_watermark can include watermark_type."""
        lc = LoadConfig(
            mode="incremental_watermark",
            watermark_column="event_date",
            watermark_type="date",
        )
        assert lc.watermark_type == "date"

    def test_watermark_mode_without_column_raises(self):
        """incremental_watermark without watermark_column raises ValidationError."""
        with pytest.raises(ValidationError, match="watermark_column"):
            LoadConfig(mode="incremental_watermark")

    def test_invalid_mode_raises(self):
        """Unknown mode raises ValidationError."""
        with pytest.raises(ValidationError):
            LoadConfig(mode="streaming")  # type: ignore[arg-type]

    def test_invalid_watermark_type_raises(self):
        """Unknown watermark type raises ValidationError."""
        with pytest.raises(ValidationError):
            LoadConfig(
                mode="incremental_watermark",
                watermark_column="ts",
                watermark_type="datetime",  # type: ignore[arg-type]
            )

    def test_frozen(self):
        """LoadConfig is immutable."""
        lc = LoadConfig(mode="full")
        with pytest.raises(ValidationError):
            lc.mode = "cdc"  # type: ignore[misc]

    def test_round_trip(self):
        """LoadConfig round-trips."""
        lc = LoadConfig(
            mode="incremental_watermark",
            watermark_column="modified_at",
            watermark_type="timestamp",
        )
        assert LoadConfig.model_validate(lc.model_dump()) == lc
