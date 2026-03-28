"""Tests for SeedConfig model."""

import pytest
from pydantic import ValidationError

from weevr.model.seed import SeedConfig


class TestSeedConfig:
    """Test SeedConfig model."""

    def test_valid_config_with_on_first_write(self):
        """SeedConfig with on=first_write and rows is valid."""
        rows = [{"id": 1, "name": "seed_row"}]
        config = SeedConfig(on="first_write", rows=rows)
        assert config.on == "first_write"
        assert config.rows == rows

    def test_default_on_first_write(self):
        """SeedConfig defaults to on=first_write when omitted."""
        rows = [{"id": 1, "name": "test"}]
        config = SeedConfig(rows=rows)
        assert config.on == "first_write"

    def test_valid_config_with_on_empty(self):
        """SeedConfig with on=empty and rows is valid."""
        rows = [{"id": 1}, {"id": 2}]
        config = SeedConfig(on="empty", rows=rows)
        assert config.on == "empty"
        assert config.rows == rows

    def test_invalid_on_value(self):
        """SeedConfig raises error for invalid on value."""
        rows = [{"id": 1}]
        with pytest.raises(ValidationError):
            SeedConfig(on="invalid_trigger", rows=rows)  # type: ignore[arg-type]

    def test_rows_must_be_non_empty(self):
        """SeedConfig raises error when rows is empty list."""
        with pytest.raises(ValidationError):
            SeedConfig(rows=[])

    def test_rows_with_multiple_dicts(self):
        """SeedConfig accepts multiple row dicts."""
        rows = [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
            {"id": 3, "name": "charlie"},
        ]
        config = SeedConfig(rows=rows)
        assert len(config.rows) == 3
        assert config.rows == rows

    def test_frozen(self):
        """SeedConfig is immutable."""
        config = SeedConfig(rows=[{"id": 1}])
        with pytest.raises(ValidationError):
            config.on = "empty"  # type: ignore[misc]

    def test_round_trip(self):
        """SeedConfig round-trips via model_dump and model_validate."""
        rows = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
        config = SeedConfig(on="empty", rows=rows)
        assert SeedConfig.model_validate(config.model_dump()) == config
