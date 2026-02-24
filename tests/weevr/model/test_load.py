"""Tests for LoadConfig model."""

import pytest
from pydantic import ValidationError

from weevr.model.load import CdcConfig, LoadConfig, WatermarkStoreConfig


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
        """cdc mode with cdc config is valid."""
        lc = LoadConfig(
            mode="cdc",
            cdc={"preset": "delta_cdf"},  # type: ignore[arg-type]
        )
        assert lc.mode == "cdc"
        assert lc.cdc is not None

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

    def test_cdc_mode_requires_cdc_config(self):
        """cdc mode without cdc config raises ValidationError."""
        with pytest.raises(ValidationError, match="cdc mode requires 'cdc' config"):
            LoadConfig(mode="cdc")

    def test_cdc_mode_must_not_have_watermark_column(self):
        """cdc mode with watermark_column raises ValidationError."""
        with pytest.raises(ValidationError, match="must not set 'watermark_column'"):
            LoadConfig(
                mode="cdc",
                cdc={"preset": "delta_cdf"},  # type: ignore[arg-type]
                watermark_column="ts",
            )

    def test_cdc_config_not_valid_on_non_cdc_mode(self):
        """cdc config on non-cdc mode raises ValidationError."""
        with pytest.raises(ValidationError, match="only valid when mode is 'cdc'"):
            LoadConfig(mode="full", cdc={"preset": "delta_cdf"})  # type: ignore[arg-type]

    def test_watermark_inclusive_defaults_false(self):
        """watermark_inclusive defaults to False."""
        lc = LoadConfig()
        assert lc.watermark_inclusive is False

    def test_watermark_inclusive_true(self):
        """watermark_inclusive=True is accepted."""
        lc = LoadConfig(
            mode="incremental_watermark",
            watermark_column="ts",
            watermark_type="timestamp",
            watermark_inclusive=True,
        )
        assert lc.watermark_inclusive is True

    def test_watermark_store_accepts_dict(self):
        """watermark_store can be set from a dict (inheritance)."""
        lc = LoadConfig(
            mode="incremental_watermark",
            watermark_column="ts",
            watermark_type="timestamp",
            watermark_store={"type": "metadata_table", "table_path": "/mnt/watermarks"},  # type: ignore[arg-type]
        )
        assert lc.watermark_store is not None
        assert lc.watermark_store.type == "metadata_table"
        assert lc.watermark_store.table_path == "/mnt/watermarks"

    def test_watermark_store_default_none(self):
        """watermark_store defaults to None (resolved at runtime to table_properties)."""
        lc = LoadConfig()
        assert lc.watermark_store is None

    def test_cdc_round_trip(self):
        """LoadConfig with cdc config round-trips."""
        lc = LoadConfig(
            mode="cdc",
            cdc=CdcConfig(
                operation_column="op",
                insert_value="I",
                update_value="U",
                delete_value="D",
            ),
        )
        assert LoadConfig.model_validate(lc.model_dump()) == lc


class TestCdcConfig:
    """Test CdcConfig model."""

    def test_preset_only_valid(self):
        """Preset-only CDC config is valid."""
        cfg = CdcConfig(preset="delta_cdf")
        assert cfg.preset == "delta_cdf"
        assert cfg.operation_column is None

    def test_explicit_only_valid(self):
        """Explicit column mapping is valid."""
        cfg = CdcConfig(
            operation_column="change_type",
            insert_value="I",
            update_value="U",
            delete_value="D",
        )
        assert cfg.operation_column == "change_type"
        assert cfg.preset is None

    def test_preset_and_explicit_raises(self):
        """Preset and explicit together raises ValidationError."""
        with pytest.raises(ValidationError, match="mutually exclusive"):
            CdcConfig(
                preset="delta_cdf",
                operation_column="op",
                insert_value="I",
            )

    def test_neither_preset_nor_explicit_raises(self):
        """Neither preset nor explicit raises ValidationError."""
        with pytest.raises(ValidationError, match="requires either"):
            CdcConfig()

    def test_preset_delta_cdf_valid(self):
        """delta_cdf preset is accepted."""
        cfg = CdcConfig(preset="delta_cdf")
        assert cfg.preset == "delta_cdf"

    def test_on_delete_defaults_hard(self):
        """on_delete defaults to hard_delete."""
        cfg = CdcConfig(preset="delta_cdf")
        assert cfg.on_delete == "hard_delete"

    def test_on_delete_soft(self):
        """on_delete=soft_delete is accepted."""
        cfg = CdcConfig(preset="delta_cdf", on_delete="soft_delete")
        assert cfg.on_delete == "soft_delete"

    def test_explicit_missing_operation_column_raises(self):
        """Explicit mapping without operation_column raises."""
        with pytest.raises(ValidationError, match="requires either"):
            CdcConfig(insert_value="I", update_value="U")

    def test_explicit_needs_insert_or_update(self):
        """Explicit mapping needs at least insert_value or update_value."""
        with pytest.raises(ValidationError, match="insert_value.*update_value"):
            CdcConfig(operation_column="op")

    def test_explicit_insert_only_valid(self):
        """Explicit mapping with only insert_value is valid."""
        cfg = CdcConfig(operation_column="op", insert_value="I")
        assert cfg.insert_value == "I"
        assert cfg.update_value is None

    def test_frozen(self):
        """CdcConfig is immutable."""
        cfg = CdcConfig(preset="delta_cdf")
        with pytest.raises(ValidationError):
            cfg.preset = None  # type: ignore[misc]


class TestWatermarkStoreConfig:
    """Test WatermarkStoreConfig model."""

    def test_default_type_is_table_properties(self):
        """Default store type is table_properties."""
        cfg = WatermarkStoreConfig()
        assert cfg.type == "table_properties"

    def test_metadata_table_requires_table_path(self):
        """metadata_table type requires table_path."""
        with pytest.raises(ValidationError, match="table_path"):
            WatermarkStoreConfig(type="metadata_table")

    def test_metadata_table_with_path(self):
        """metadata_table with table_path is valid."""
        cfg = WatermarkStoreConfig(type="metadata_table", table_path="/mnt/watermarks")
        assert cfg.table_path == "/mnt/watermarks"

    def test_table_properties_does_not_require_path(self):
        """table_properties does not require table_path."""
        cfg = WatermarkStoreConfig(type="table_properties")
        assert cfg.table_path is None

    def test_table_properties_ignores_path(self):
        """table_properties with table_path is still valid (ignored at runtime)."""
        cfg = WatermarkStoreConfig(type="table_properties", table_path="/ignored")
        assert cfg.table_path == "/ignored"

    def test_frozen(self):
        """WatermarkStoreConfig is immutable."""
        cfg = WatermarkStoreConfig()
        with pytest.raises(ValidationError):
            cfg.type = "metadata_table"  # type: ignore[misc]
