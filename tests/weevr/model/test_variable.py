"""Tests for VariableSpec model."""

import pytest
from pydantic import ValidationError

from weevr.model.variable import VariableSpec


class TestVariableSpec:
    """Test VariableSpec construction and validation."""

    @pytest.mark.parametrize(
        "var_type",
        ["string", "int", "long", "float", "double", "boolean", "timestamp", "date"],
    )
    def test_all_types_accepted(self, var_type: str):
        """All declared scalar types are accepted."""
        spec = VariableSpec(type=var_type)
        assert spec.type == var_type
        assert spec.default is None

    def test_string_default(self):
        """String default value."""
        spec = VariableSpec(type="string", default="hello")
        assert spec.default == "hello"

    def test_int_default(self):
        """Integer default value."""
        spec = VariableSpec(type="int", default=42)
        assert spec.default == 42

    def test_float_default(self):
        """Float default value."""
        spec = VariableSpec(type="double", default=3.14)
        assert spec.default == 3.14

    def test_bool_default(self):
        """Boolean default value."""
        spec = VariableSpec(type="boolean", default=True)
        assert spec.default is True

    def test_no_default(self):
        """Default is None when omitted."""
        spec = VariableSpec(type="int")
        assert spec.default is None

    def test_unknown_type_rejected(self):
        """Unknown type raises ValidationError."""
        with pytest.raises(ValidationError):
            VariableSpec(type="binary")

    def test_frozen(self):
        """VariableSpec is immutable."""
        spec = VariableSpec(type="string", default="x")
        with pytest.raises(ValidationError):
            spec.type = "int"  # type: ignore[misc]
