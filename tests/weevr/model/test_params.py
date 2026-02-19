"""Tests for ParamSpec and ParamsConfig models."""

import pytest
from pydantic import ValidationError

from weevr.model.params import ParamsConfig, ParamSpec


class TestParamSpec:
    """Test ParamSpec model."""

    def test_all_seven_types(self):
        """ParamSpec accepts all 7 supported types."""
        types = ["string", "int", "float", "bool", "date", "timestamp", "list[string]"]
        for t in types:
            ps = ParamSpec(name="p", type=t)  # type: ignore[arg-type]
            assert ps.type == t

    def test_invalid_type_raises(self):
        """Unknown type raises ValidationError."""
        with pytest.raises(ValidationError):
            ParamSpec(name="p", type="bytes")  # type: ignore[arg-type]

    def test_required_defaults_true(self):
        """required defaults to True."""
        ps = ParamSpec(name="p", type="string")
        assert ps.required is True

    def test_optional_param(self):
        """ParamSpec with required=False."""
        ps = ParamSpec(name="p", type="string", required=False)
        assert ps.required is False

    def test_default_value(self):
        """ParamSpec with a default value."""
        ps = ParamSpec(name="env", type="string", default="dev")
        assert ps.default == "dev"

    def test_description(self):
        """ParamSpec with a description."""
        ps = ParamSpec(name="env", type="string", description="Deployment environment")
        assert ps.description == "Deployment environment"

    def test_frozen(self):
        """ParamSpec is immutable."""
        ps = ParamSpec(name="p", type="string")
        with pytest.raises(ValidationError):
            ps.name = "q"  # type: ignore[misc]

    def test_round_trip(self):
        """ParamSpec round-trips."""
        ps = ParamSpec(
            name="lakehouse",
            type="string",
            required=True,
            default="bronze",
            description="Target lakehouse",
        )
        assert ParamSpec.model_validate(ps.model_dump()) == ps


class TestParamsConfig:
    """Test ParamsConfig model."""

    def test_minimal(self):
        """ParamsConfig with just config_version."""
        pc = ParamsConfig(config_version="1.0")
        assert pc.config_version == "1.0"

    def test_extra_fields_allowed(self):
        """ParamsConfig accepts arbitrary extra fields."""
        pc = ParamsConfig(config_version="1.0", env="dev", lakehouse="bronze")  # type: ignore[call-arg]
        assert pc.config_version == "1.0"

    def test_nested_extra_fields(self):
        """ParamsConfig accepts nested extra fields."""
        pc = ParamsConfig(config_version="1.0", db={"host": "localhost", "port": 5432})  # type: ignore[call-arg]
        assert pc.config_version == "1.0"

    def test_mutable(self):
        """ParamsConfig is mutable (not FrozenBase)."""
        pc = ParamsConfig(config_version="1.0")
        pc.config_version = "2.0"
        assert pc.config_version == "2.0"

    def test_missing_config_version_raises(self):
        """config_version is required."""
        with pytest.raises(ValidationError):
            ParamsConfig()  # type: ignore[call-arg]
