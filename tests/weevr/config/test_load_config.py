"""Integration tests for full config loading pipeline."""

from pathlib import Path

import pytest

from weevr.config import load_config
from weevr.errors import (
    ConfigParseError,
    ConfigSchemaError,
    ConfigVersionError,
    ModelValidationError,
    VariableResolutionError,
)
from weevr.model import Loom, Thread, Weave

FIXTURES = Path(__file__).parent / "fixtures"


class TestLoadConfigHappyPath:
    """Test successful config loading scenarios."""

    def test_load_simple_thread(self):
        """load_config returns a Thread model for a thread config."""
        result = load_config(FIXTURES / "valid_thread.yaml")
        assert isinstance(result, Thread)
        assert result.config_version == "1.0"
        assert "customers" in result.sources
        assert result.target is not None

    def test_load_thread_from_project(self):
        """Load a thread from project fixtures."""
        thread_path = FIXTURES / "project" / "threads" / "dimensions" / "dim_customer.yaml"
        result = load_config(thread_path)
        assert isinstance(result, Thread)
        assert result.config_version == "1.0"
        assert "customers" in result.sources

    def test_load_thread_with_variable_resolution(self, tmp_path):
        """load_config resolves variables and returns a typed Thread."""
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data:
    type: delta
    alias: ${lakehouse}.customers
target:
  audit_template: ${env}_template
"""
        )

        result = load_config(
            thread_file,
            runtime_params={"lakehouse": "bronze", "env": "dev"},
        )

        assert isinstance(result, Thread)
        assert result.sources["data"].alias == "bronze.customers"
        assert result.target.audit_template == "dev_template"

    def test_load_thread_with_param_file(self, tmp_path):
        """load_config resolves variables from a param file."""
        param_file = tmp_path / "params.yaml"
        param_file.write_text(
            """
config_version: "1.0"
lakehouse: bronze
env: dev
"""
        )

        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data:
    type: delta
    alias: ${lakehouse}.customers
target:
  audit_template: ${env}_template
"""
        )

        result = load_config(thread_file, param_file=param_file)

        assert isinstance(result, Thread)
        assert result.sources["data"].alias == "bronze.customers"
        assert result.target.audit_template == "dev_template"

    def test_runtime_params_override_param_file(self, tmp_path):
        """Runtime params override param file values."""
        param_file = tmp_path / "params.yaml"
        param_file.write_text(
            """
config_version: "1.0"
env: dev
"""
        )

        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data:
    type: delta
    alias: lakehouse
target:
  audit_template: output_${env}
"""
        )

        result = load_config(
            thread_file,
            runtime_params={"env": "prod"},
            param_file=param_file,
        )

        assert isinstance(result, Thread)
        assert result.target.audit_template == "output_prod"

    def test_variable_with_fallback_default(self, tmp_path):
        """Variables with fallback defaults use the default when unset."""
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data:
    type: delta
    alias: lakehouse
target:
  audit_template: ${output_table:-default_output}
"""
        )

        result = load_config(thread_file)

        assert isinstance(result, Thread)
        assert result.target.audit_template == "default_output"

    def test_load_weave_returns_weave_model(self):
        """load_config returns a Weave model for a weave config."""
        result = load_config(FIXTURES / "project" / "weaves" / "dimensions.yaml")
        assert isinstance(result, Weave)
        assert result.config_version == "1.0"
        assert "dimensions.dim_customer" in result.threads

    def test_load_loom_returns_loom_model(self):
        """load_config returns a Loom model for a loom config."""
        result = load_config(FIXTURES / "project" / "looms" / "nightly.yaml")
        assert isinstance(result, Loom)
        assert result.config_version == "1.0"
        assert "dimensions" in result.weaves


class TestLoadConfigErrorHandling:
    """Test error handling in config loading."""

    def test_missing_file(self):
        """Raise ConfigParseError for missing file."""
        with pytest.raises(ConfigParseError) as exc_info:
            load_config(FIXTURES / "nonexistent.yaml")
        assert "not found" in str(exc_info.value)

    def test_invalid_yaml_syntax(self):
        """Raise ConfigParseError for invalid YAML."""
        with pytest.raises(ConfigParseError) as exc_info:
            load_config(FIXTURES / "invalid_yaml.yaml")
        assert "Invalid YAML syntax" in str(exc_info.value)

    def test_missing_config_version(self):
        """Raise ConfigParseError for missing config_version."""
        with pytest.raises(ConfigParseError) as exc_info:
            load_config(FIXTURES / "missing_version.yaml")
        assert "Missing required field 'config_version'" in str(exc_info.value)

    def test_unsupported_version(self):
        """Raise ConfigVersionError for unsupported version."""
        with pytest.raises(ConfigVersionError) as exc_info:
            load_config(FIXTURES / "bad_version.yaml")
        assert "Unsupported" in str(exc_info.value)
        assert "99.0" in str(exc_info.value)

    def test_schema_validation_failure(self, tmp_path):
        """Raise ConfigSchemaError for structurally invalid schema."""
        bad_file = tmp_path / "bad_schema.yaml"
        bad_file.write_text(
            """
config_version: "1.0"
threads: not_a_list
"""
        )

        with pytest.raises(ConfigSchemaError) as exc_info:
            load_config(bad_file)
        assert "Schema validation failed" in str(exc_info.value)

    def test_unresolved_variable(self, tmp_path):
        """Raise VariableResolutionError for missing variable."""
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data:
    type: delta
    alias: ${missing_var}.customers
target: {}
"""
        )

        with pytest.raises(VariableResolutionError) as exc_info:
            load_config(thread_file)
        assert "Unresolved variable" in str(exc_info.value)
        assert "missing_var" in str(exc_info.value)

    def test_model_validation_error_on_bad_thread(self, tmp_path):
        """Raise ModelValidationError when hydration fails (e.g., invalid step type)."""
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data:
    type: delta
    alias: lakehouse
target: {}
steps:
  - pivot:
      columns: [x]
"""
        )

        with pytest.raises(ModelValidationError):
            load_config(thread_file)
