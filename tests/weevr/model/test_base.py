"""Tests for FrozenBase model mixin."""

import pytest
from pydantic import ValidationError

from weevr.model.base import FrozenBase


class _SampleModel(FrozenBase):
    name: str
    value: int = 0


class TestFrozenBase:
    """Test FrozenBase immutability and Pydantic compatibility."""

    def test_construction(self):
        """FrozenBase subclass constructs from keyword arguments."""
        m = _SampleModel(name="test", value=42)
        assert m.name == "test"
        assert m.value == 42

    def test_default_field(self):
        """Fields with defaults do not need to be supplied."""
        m = _SampleModel(name="x")
        assert m.value == 0

    def test_mutation_raises(self):
        """Assigning to a field after construction raises ValidationError."""
        m = _SampleModel(name="immutable")
        with pytest.raises(ValidationError):
            m.name = "changed"  # type: ignore[misc]

    def test_model_dump_round_trip(self):
        """model_dump() output can be fed back into model_validate()."""
        original = _SampleModel(name="round_trip", value=7)
        dumped = original.model_dump()
        restored = _SampleModel.model_validate(dumped)
        assert restored == original

    def test_model_dump_keys(self):
        """model_dump() returns expected keys."""
        m = _SampleModel(name="keys", value=3)
        d = m.model_dump()
        assert set(d.keys()) == {"name", "value"}
