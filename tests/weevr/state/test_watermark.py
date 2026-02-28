"""Tests for WatermarkState model, WatermarkStore ABC, and resolve_store."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from weevr.model.load import LoadConfig, WatermarkStoreConfig
from weevr.state import WatermarkState, WatermarkStore, resolve_store
from weevr.state.metadata_table import MetadataTableStore
from weevr.state.table_properties import TablePropertiesStore


class TestWatermarkState:
    """Test WatermarkState model."""

    def test_construction(self):
        """WatermarkState can be constructed with valid fields."""
        state = WatermarkState(
            thread_name="orders_daily",
            watermark_column="modified_at",
            watermark_type="timestamp",
            last_value="2024-01-15T10:30:00",
            last_updated=datetime(2024, 1, 16, 8, 0, 0, tzinfo=UTC),
        )
        assert state.thread_name == "orders_daily"
        assert state.watermark_column == "modified_at"
        assert state.watermark_type == "timestamp"
        assert state.last_value == "2024-01-15T10:30:00"
        assert state.run_id is None

    def test_immutable(self):
        """WatermarkState is frozen."""
        state = WatermarkState(
            thread_name="t1",
            watermark_column="ts",
            watermark_type="timestamp",
            last_value="v1",
            last_updated=datetime(2024, 1, 1, tzinfo=UTC),
        )
        with pytest.raises(ValidationError):
            state.last_value = "v2"  # type: ignore[misc]

    def test_last_value_is_string(self):
        """last_value is always a string, even for numeric types."""
        state = WatermarkState(
            thread_name="t1",
            watermark_column="row_id",
            watermark_type="int",
            last_value="42",
            last_updated=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert isinstance(state.last_value, str)
        assert state.last_value == "42"

    def test_run_id_optional(self):
        """run_id is optional and defaults to None."""
        state = WatermarkState(
            thread_name="t1",
            watermark_column="ts",
            watermark_type="timestamp",
            last_value="v1",
            last_updated=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert state.run_id is None

    def test_run_id_set(self):
        """run_id can be set."""
        state = WatermarkState(
            thread_name="t1",
            watermark_column="ts",
            watermark_type="timestamp",
            last_value="v1",
            last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            run_id="run_123",
        )
        assert state.run_id == "run_123"

    def test_all_watermark_types(self):
        """All watermark types are accepted."""
        for wm_type in ("timestamp", "date", "int", "long"):
            state = WatermarkState(
                thread_name="t1",
                watermark_column="col",
                watermark_type=wm_type,  # type: ignore[arg-type]
                last_value="v",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
            assert state.watermark_type == wm_type

    def test_invalid_watermark_type_raises(self):
        """Invalid watermark type raises ValidationError."""
        with pytest.raises(ValidationError):
            WatermarkState(
                thread_name="t1",
                watermark_column="col",
                watermark_type="float",  # type: ignore[arg-type]
                last_value="v",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )

    def test_round_trip(self):
        """WatermarkState round-trips via model_dump/model_validate."""
        state = WatermarkState(
            thread_name="t1",
            watermark_column="ts",
            watermark_type="date",
            last_value="2024-01-15",
            last_updated=datetime(2024, 1, 16, tzinfo=UTC),
            run_id="run_abc",
        )
        assert WatermarkState.model_validate(state.model_dump()) == state


class TestResolveStore:
    """Test resolve_store factory function."""

    def test_default_returns_table_properties(self):
        """No watermark_store config returns TablePropertiesStore."""
        lc = LoadConfig(
            mode="incremental_watermark",
            watermark_column="ts",
            watermark_type="timestamp",
        )
        store = resolve_store(lc, "/mnt/target/table")
        assert isinstance(store, TablePropertiesStore)
        assert store.target_path == "/mnt/target/table"

    def test_explicit_table_properties(self):
        """Explicit table_properties returns TablePropertiesStore."""
        lc = LoadConfig(
            mode="incremental_watermark",
            watermark_column="ts",
            watermark_type="timestamp",
            watermark_store=WatermarkStoreConfig(type="table_properties"),
        )
        store = resolve_store(lc, "/mnt/target/table")
        assert isinstance(store, TablePropertiesStore)

    def test_metadata_table_returns_metadata_table_store(self):
        """metadata_table type returns MetadataTableStore."""
        lc = LoadConfig(
            mode="incremental_watermark",
            watermark_column="ts",
            watermark_type="timestamp",
            watermark_store=WatermarkStoreConfig(
                type="metadata_table",
                table_path="/mnt/watermarks",
            ),
        )
        store = resolve_store(lc, "/mnt/target/table")
        assert isinstance(store, MetadataTableStore)
        assert store.table_path == "/mnt/watermarks"


class TestWatermarkStoreABC:
    """Test WatermarkStore ABC compliance."""

    def test_metadata_table_is_watermark_store(self):
        """MetadataTableStore implements WatermarkStore."""
        store = MetadataTableStore("/mnt/watermarks")
        assert isinstance(store, WatermarkStore)

    def test_table_properties_is_watermark_store(self):
        """TablePropertiesStore implements WatermarkStore."""
        store = TablePropertiesStore("/mnt/target")
        assert isinstance(store, WatermarkStore)

    def test_abc_has_read_write(self):
        """WatermarkStore ABC declares read and write methods."""
        assert hasattr(WatermarkStore, "read")
        assert hasattr(WatermarkStore, "write")
