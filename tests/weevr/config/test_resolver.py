"""Tests for variable resolver."""

from pathlib import Path

import pytest

from weevr.config.resolver import build_param_context, resolve_variables, validate_params
from weevr.errors import ConfigSchemaError, VariableResolutionError

FIXTURES = Path(__file__).parent / "fixtures"


class TestBuildParamContext:
    """Test build_param_context function."""

    def test_runtime_params_only(self):
        """Build context with only runtime params."""
        runtime = {"env": "prod", "lakehouse": "gold"}
        context = build_param_context(runtime_params=runtime)
        assert context["env"] == "prod"
        assert context["lakehouse"] == "gold"

    def test_param_file_only(self):
        """Build context with only param file data."""
        param_file = {"env": "dev", "lakehouse": "bronze"}
        context = build_param_context(param_file_data=param_file)
        assert context["env"] == "dev"
        assert context["lakehouse"] == "bronze"

    def test_config_defaults_only(self):
        """Build context with only config defaults."""
        defaults = {"env": "dev", "mode": "test"}
        context = build_param_context(config_defaults=defaults)
        assert context["env"] == "dev"
        assert context["mode"] == "test"

    def test_runtime_overrides_param_file(self):
        """Runtime params should override param file."""
        runtime = {"env": "prod"}
        param_file = {"env": "dev", "lakehouse": "bronze"}
        context = build_param_context(runtime, param_file)
        assert context["env"] == "prod"  # Runtime wins
        assert context["lakehouse"] == "bronze"  # From param file

    def test_param_file_overrides_defaults(self):
        """Param file should override config defaults."""
        param_file = {"env": "staging"}
        defaults = {"env": "dev", "mode": "test"}
        context = build_param_context(None, param_file, defaults)
        assert context["env"] == "staging"  # Param file wins
        assert context["mode"] == "test"  # From defaults

    def test_full_priority_chain(self):
        """Test full priority: runtime > param_file > defaults."""
        runtime = {"env": "prod"}
        param_file = {"env": "staging", "lakehouse": "silver"}
        defaults = {"env": "dev", "lakehouse": "bronze", "mode": "test"}
        context = build_param_context(runtime, param_file, defaults)
        assert context["env"] == "prod"  # Runtime wins
        assert context["lakehouse"] == "silver"  # Param file wins
        assert context["mode"] == "test"  # Defaults (no override)


class TestResolveVariables:
    """Test resolve_variables function."""

    def test_simple_variable(self):
        """Resolve simple variable reference."""
        config = {"path": "${base_path}/data"}
        context = {"base_path": "/mnt/storage"}
        result = resolve_variables(config, context)
        assert result["path"] == "/mnt/storage/data"

    def test_variable_with_default_not_used(self):
        """Variable with default should use context value when present."""
        config = {"env": "${environment:-dev}"}
        context = {"environment": "prod"}
        result = resolve_variables(config, context)
        assert result["env"] == "prod"  # Context value used

    def test_variable_with_default_used(self):
        """Variable with default should use default when not in context."""
        config = {"env": "${missing_var:-dev}"}
        context = {}
        result = resolve_variables(config, context)
        assert result["env"] == "dev"  # Default used

    def test_dotted_key_access(self):
        """Resolve variable with dotted key access."""
        config = {"host": "${database.host}"}
        context = {"database": {"host": "localhost", "port": 5432}}
        result = resolve_variables(config, context)
        assert result["host"] == "localhost"

    def test_nested_dict_resolution(self):
        """Recursively resolve variables in nested dict."""
        config = {
            "source": {"path": "${base_path}/input"},
            "target": {"path": "${base_path}/output"},
        }
        context = {"base_path": "/data"}
        result = resolve_variables(config, context)
        assert result["source"]["path"] == "/data/input"
        assert result["target"]["path"] == "/data/output"

    def test_list_resolution(self):
        """Recursively resolve variables in list."""
        config = {"paths": ["${base}/input", "${base}/output"]}
        context = {"base": "/data"}
        result = resolve_variables(config, context)
        assert result["paths"][0] == "/data/input"
        assert result["paths"][1] == "/data/output"

    def test_multiple_variables_in_string(self):
        """Resolve multiple variables in one string."""
        config = {"url": "${protocol}://${host}:${port}"}
        context = {"protocol": "https", "host": "api.example.com", "port": "443"}
        result = resolve_variables(config, context)
        assert result["url"] == "https://api.example.com:443"

    def test_unresolved_variable_no_default(self):
        """Raise VariableResolutionError for missing variable without default."""
        config = {"path": "${missing_var}"}
        context = {}
        with pytest.raises(VariableResolutionError) as exc_info:
            resolve_variables(config, context)
        assert "Unresolved variable" in str(exc_info.value)
        assert "missing_var" in str(exc_info.value)

    def test_non_string_values_passthrough(self):
        """Non-string values should pass through unchanged."""
        config = {
            "count": 42,
            "enabled": True,
            "ratio": 3.14,
            "nullable": None,
        }
        context = {}
        result = resolve_variables(config, context)
        assert result["count"] == 42
        assert result["enabled"] is True
        assert result["ratio"] == 3.14
        assert result["nullable"] is None

    def test_empty_config(self):
        """Empty config should return empty."""
        config = {}
        context = {"var": "value"}
        result = resolve_variables(config, context)
        assert result == {}

    def test_empty_context(self):
        """Variables with defaults should work with empty context."""
        config = {"env": "${missing:-dev}"}
        context = {}
        result = resolve_variables(config, context)
        assert result["env"] == "dev"


class TestValidateParams:
    """Test validate_params function."""

    def test_string_type_valid(self):
        """Validate string parameter."""
        specs = {"env": {"type": "string", "required": True}}
        context = {"env": "dev"}
        validate_params(specs, context)  # Should not raise

    def test_string_type_invalid(self):
        """Reject non-string for string type."""
        specs = {"env": {"type": "string", "required": True}}
        context = {"env": 123}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "expected string" in str(exc_info.value).lower()

    def test_int_type_valid(self):
        """Validate int parameter."""
        specs = {"port": {"type": "int", "required": True}}
        context = {"port": 5432}
        validate_params(specs, context)  # Should not raise

    def test_int_type_invalid(self):
        """Reject non-int for int type."""
        specs = {"port": {"type": "int", "required": True}}
        context = {"port": "5432"}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "expected int" in str(exc_info.value).lower()

    def test_float_type_valid(self):
        """Validate float parameter."""
        specs = {"ratio": {"type": "float", "required": True}}
        context = {"ratio": 3.14}
        validate_params(specs, context)  # Should not raise

    def test_float_accepts_int(self):
        """Float type should accept int values."""
        specs = {"ratio": {"type": "float", "required": True}}
        context = {"ratio": 3}
        validate_params(specs, context)  # Should not raise

    def test_bool_type_valid(self):
        """Validate bool parameter."""
        specs = {"enabled": {"type": "bool", "required": True}}
        context = {"enabled": True}
        validate_params(specs, context)  # Should not raise

    def test_bool_type_invalid(self):
        """Reject non-bool for bool type."""
        specs = {"enabled": {"type": "bool", "required": True}}
        context = {"enabled": "true"}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "expected bool" in str(exc_info.value).lower()

    def test_date_type_valid_string(self):
        """Validate date parameter as ISO string."""
        specs = {"start_date": {"type": "date", "required": True}}
        context = {"start_date": "2024-01-15"}
        validate_params(specs, context)  # Should not raise

    def test_date_type_invalid_format(self):
        """Reject invalid date format."""
        specs = {"start_date": {"type": "date", "required": True}}
        context = {"start_date": "01/15/2024"}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "date format" in str(exc_info.value).lower()

    def test_timestamp_type_valid_string(self):
        """Validate timestamp parameter as ISO string."""
        specs = {"created_at": {"type": "timestamp", "required": True}}
        context = {"created_at": "2024-01-15T10:30:00"}
        validate_params(specs, context)  # Should not raise

    def test_timestamp_type_invalid_format(self):
        """Reject invalid timestamp format."""
        specs = {"created_at": {"type": "timestamp", "required": True}}
        context = {"created_at": "invalid-timestamp"}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "timestamp format" in str(exc_info.value).lower()

    def test_list_string_type_valid(self):
        """Validate list[string] parameter."""
        specs = {"tags": {"type": "list[string]", "required": True}}
        context = {"tags": ["dev", "critical"]}
        validate_params(specs, context)  # Should not raise

    def test_list_string_type_not_list(self):
        """Reject non-list for list[string] type."""
        specs = {"tags": {"type": "list[string]", "required": True}}
        context = {"tags": "dev"}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "expected list" in str(exc_info.value).lower()

    def test_list_string_type_non_string_items(self):
        """Reject list with non-string items."""
        specs = {"tags": {"type": "list[string]", "required": True}}
        context = {"tags": ["dev", 123]}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "list of strings" in str(exc_info.value).lower()

    def test_required_param_missing(self):
        """Raise error for missing required parameter."""
        specs = {"env": {"type": "string", "required": True}}
        context = {}
        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_params(specs, context)
        assert "Required parameter" in str(exc_info.value)
        assert "env" in str(exc_info.value)

    def test_required_param_with_default(self):
        """Use default for required param when missing."""
        specs = {"env": {"type": "string", "required": True, "default": "dev"}}
        context = {}
        validate_params(specs, context)
        assert context["env"] == "dev"  # Default applied

    def test_optional_param_missing(self):
        """Optional param missing should not raise."""
        specs = {"optional": {"type": "string", "required": False}}
        context = {}
        validate_params(specs, context)  # Should not raise

    def test_none_param_specs(self):
        """None param specs should not raise."""
        validate_params(None, {"env": "dev"})  # Should not raise

    def test_empty_param_specs(self):
        """Empty param specs should not raise."""
        validate_params({}, {"env": "dev"})  # Should not raise


class TestReferenceResolution:
    """Test reference resolution functions."""

    def test_resolve_logical_name_thread(self):
        """Resolve thread logical name to path."""
        from weevr.config.resolver import resolve_logical_name

        base = Path("/project")
        result = resolve_logical_name("dimensions.dim_customer", "thread", base)
        # Check path components (resolve() makes it absolute)
        assert "threads" in result.parts
        assert "dimensions" in result.parts
        assert result.name == "dim_customer.yaml"

    def test_resolve_logical_name_weave(self):
        """Resolve weave logical name to path."""
        from weevr.config.resolver import resolve_logical_name

        base = Path("/project")
        result = resolve_logical_name("dimensions", "weave", base)
        # Check path components
        assert "weaves" in result.parts
        assert result.name == "dimensions.yaml"

    def test_resolve_logical_name_loom(self):
        """Resolve loom logical name to path."""
        from weevr.config.resolver import resolve_logical_name

        base = Path("/project")
        result = resolve_logical_name("nightly", "loom", base)
        # Check path components
        assert "looms" in result.parts
        assert result.name == "nightly.yaml"

    def test_resolve_references_loom_to_weave_to_thread(self):
        """Resolve full hierarchy: loom -> weave -> thread."""
        from weevr.config.resolver import resolve_references

        project_root = FIXTURES / "project"
        loom_config = {
            "config_version": "1.0",
            "weaves": ["dimensions"],
        }

        result = resolve_references(
            loom_config,
            "loom",
            project_root,
        )

        assert "_resolved_weaves" in result
        assert len(result["_resolved_weaves"]) == 1
        weave = result["_resolved_weaves"][0]
        assert "_resolved_threads" in weave
        assert len(weave["_resolved_threads"]) == 1
        thread = weave["_resolved_threads"][0]
        assert "customers" in thread["sources"]

    def test_resolve_references_missing_file(self):
        """Raise ReferenceResolutionError for missing referenced file."""
        from weevr.config.resolver import resolve_references
        from weevr.errors import ReferenceResolutionError

        project_root = FIXTURES / "project"
        config = {
            "config_version": "1.0",
            "weaves": ["nonexistent"],
        }

        with pytest.raises(ReferenceResolutionError) as exc_info:
            resolve_references(config, "loom", project_root)
        assert "not found" in str(exc_info.value)
