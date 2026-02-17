"""Tests for Pydantic config schemas."""

import pytest

from weevr.config.schema import (
    LoomConfig,
    ParamSpec,
    ParamsConfig,
    ThreadConfig,
    WeaveConfig,
    validate_schema,
)
from weevr.errors import ConfigSchemaError


class TestThreadConfig:
    """Test ThreadConfig schema."""

    def test_minimal_valid_thread(self):
        """Valid minimal thread config."""
        data = {
            "config_version": "1.0",
            "sources": {"customers": "table://dim_customer"},
            "target": {"table": "fact_orders"},
        }
        config = ThreadConfig.model_validate(data)
        assert config.config_version == "1.0"
        assert "customers" in config.sources
        assert config.target["table"] == "fact_orders"

    def test_full_thread_config(self):
        """Valid thread config with all optional fields."""
        data = {
            "config_version": "1.0",
            "sources": {"customers": "table://dim_customer"},
            "steps": [{"operation": "select", "columns": ["id", "name"]}],
            "target": {"table": "fact_orders"},
            "write": {"mode": "merge"},
            "keys": {"primary": ["id"]},
            "validations": [{"type": "not_null", "column": "id"}],
            "assertions": [{"type": "row_count", "min": 1}],
            "load": {"mode": "incremental"},
            "tags": ["critical", "daily"],
        }
        config = ThreadConfig.model_validate(data)
        assert len(config.steps) == 1
        assert config.write["mode"] == "merge"
        assert "critical" in config.tags

    def test_missing_required_sources(self):
        """Missing required 'sources' field."""
        data = {
            "config_version": "1.0",
            "target": {"table": "fact_orders"},
        }
        with pytest.raises(Exception):  # Pydantic ValidationError
            ThreadConfig.model_validate(data)

    def test_missing_required_target(self):
        """Missing required 'target' field."""
        data = {
            "config_version": "1.0",
            "sources": {"customers": "table://dim_customer"},
        }
        with pytest.raises(Exception):  # Pydantic ValidationError
            ThreadConfig.model_validate(data)

    def test_wrong_field_type(self):
        """Wrong field type (sources should be dict)."""
        data = {
            "config_version": "1.0",
            "sources": "invalid_string",
            "target": {"table": "fact_orders"},
        }
        with pytest.raises(Exception):  # Pydantic ValidationError
            ThreadConfig.model_validate(data)


class TestWeaveConfig:
    """Test WeaveConfig schema."""

    def test_valid_weave(self):
        """Valid weave config with threads list."""
        data = {
            "config_version": "1.0",
            "threads": ["dimensions.dim_customer", "dimensions.dim_product"],
        }
        config = WeaveConfig.model_validate(data)
        assert len(config.threads) == 2
        assert "dimensions.dim_customer" in config.threads

    def test_weave_with_defaults(self):
        """Weave config with defaults."""
        data = {
            "config_version": "1.0",
            "threads": ["thread1"],
            "defaults": {"write": {"mode": "merge"}},
        }
        config = WeaveConfig.model_validate(data)
        assert config.defaults["write"]["mode"] == "merge"

    def test_missing_threads(self):
        """Missing required 'threads' field."""
        data = {"config_version": "1.0"}
        with pytest.raises(Exception):  # Pydantic ValidationError
            WeaveConfig.model_validate(data)

    def test_threads_not_list(self):
        """Threads field must be a list."""
        data = {
            "config_version": "1.0",
            "threads": "not_a_list",
        }
        with pytest.raises(Exception):  # Pydantic ValidationError
            WeaveConfig.model_validate(data)


class TestLoomConfig:
    """Test LoomConfig schema."""

    def test_valid_loom(self):
        """Valid loom config with weaves list."""
        data = {
            "config_version": "1.0",
            "weaves": ["dimensions", "facts"],
        }
        config = LoomConfig.model_validate(data)
        assert len(config.weaves) == 2
        assert "dimensions" in config.weaves

    def test_loom_with_defaults(self):
        """Loom config with defaults."""
        data = {
            "config_version": "1.0",
            "weaves": ["weave1"],
            "defaults": {"tags": ["nightly"]},
        }
        config = LoomConfig.model_validate(data)
        assert "nightly" in config.defaults["tags"]

    def test_missing_weaves(self):
        """Missing required 'weaves' field."""
        data = {"config_version": "1.0"}
        with pytest.raises(Exception):  # Pydantic ValidationError
            LoomConfig.model_validate(data)

    def test_weaves_not_list(self):
        """Weaves field must be a list."""
        data = {
            "config_version": "1.0",
            "weaves": {"invalid": "dict"},
        }
        with pytest.raises(Exception):  # Pydantic ValidationError
            LoomConfig.model_validate(data)


class TestParamsConfig:
    """Test ParamsConfig schema."""

    def test_valid_params(self):
        """Valid params file with arbitrary fields."""
        data = {
            "config_version": "1.0",
            "env": "dev",
            "lakehouse": "bronze",
            "source_path": "/data/raw",
        }
        config = ParamsConfig.model_validate(data)
        assert config.config_version == "1.0"
        # Extra fields are allowed in ParamsConfig

    def test_nested_params(self):
        """Params file with nested structure."""
        data = {
            "config_version": "1.0",
            "database": {
                "host": "localhost",
                "port": 5432,
            },
        }
        config = ParamsConfig.model_validate(data)
        assert config.config_version == "1.0"


class TestParamSpec:
    """Test ParamSpec model."""

    def test_all_supported_types(self):
        """ParamSpec accepts all 7 supported types."""
        types = ["string", "int", "float", "bool", "date", "timestamp", "list[string]"]
        for param_type in types:
            spec = ParamSpec(name="test_param", type=param_type)
            assert spec.type == param_type

    def test_invalid_type(self):
        """ParamSpec rejects invalid types."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            ParamSpec(name="test_param", type="invalid_type")

    def test_required_and_default(self):
        """ParamSpec with required and default fields."""
        spec = ParamSpec(
            name="env",
            type="string",
            required=True,
            default="dev",
            description="Environment name",
        )
        assert spec.required is True
        assert spec.default == "dev"
        assert spec.description == "Environment name"

    def test_optional_param(self):
        """ParamSpec with required=False."""
        spec = ParamSpec(name="optional_param", type="string", required=False)
        assert spec.required is False


class TestValidateSchema:
    """Test validate_schema dispatcher function."""

    def test_validate_thread(self):
        """Validate thread config."""
        data = {
            "config_version": "1.0",
            "sources": {"src": "table://data"},
            "target": {"table": "output"},
        }
        result = validate_schema(data, "thread")
        assert isinstance(result, ThreadConfig)

    def test_validate_weave(self):
        """Validate weave config."""
        data = {
            "config_version": "1.0",
            "threads": ["thread1"],
        }
        result = validate_schema(data, "weave")
        assert isinstance(result, WeaveConfig)

    def test_validate_loom(self):
        """Validate loom config."""
        data = {
            "config_version": "1.0",
            "weaves": ["weave1"],
        }
        result = validate_schema(data, "loom")
        assert isinstance(result, LoomConfig)

    def test_validate_params(self):
        """Validate params config."""
        data = {
            "config_version": "1.0",
            "env": "dev",
        }
        result = validate_schema(data, "params")
        assert isinstance(result, ParamsConfig)

    def test_unknown_config_type(self):
        """Raise ConfigSchemaError for unknown type."""
        data = {"config_version": "1.0"}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_schema(data, "unknown_type")
        assert "Unknown config type" in str(exc_info.value)

    def test_validation_failure_wrapped(self):
        """Pydantic ValidationError wrapped in ConfigSchemaError."""
        data = {
            "config_version": "1.0",
            # Missing required 'threads' field
        }
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_schema(data, "weave")
        assert "Schema validation failed" in str(exc_info.value)

    def test_type_mismatch_wrapped(self):
        """Type mismatch wrapped in ConfigSchemaError."""
        data = {
            "config_version": "1.0",
            "threads": "not_a_list",  # Should be list
        }
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_schema(data, "weave")
        assert "validation failed" in str(exc_info.value).lower()
