"""Integration tests for full config loading pipeline."""

from pathlib import Path

import pytest

from weevr.config import load_config
from weevr.errors import (
    ConfigParseError,
    ConfigSchemaError,
    ConfigVersionError,
    ReferenceResolutionError,
    VariableResolutionError,
)

FIXTURES = Path(__file__).parent / "fixtures"


class TestLoadConfigHappyPath:
    """Test successful config loading scenarios."""

    def test_load_simple_thread(self):
        """Load a simple thread config."""
        result = load_config(FIXTURES / "valid_thread.yaml")
        assert result["config_version"] == "1.0"
        assert "sources" in result
        assert "target" in result

    def test_load_thread_from_project(self):
        """Load a thread from project fixtures."""
        thread_path = FIXTURES / "project" / "threads" / "dimensions" / "dim_customer.yaml"
        result = load_config(thread_path)
        assert result["config_version"] == "1.0"
        assert "sources" in result
        assert "target" in result

    def test_load_thread_with_variable_resolution(self, tmp_path):
        """Load thread with variable interpolation."""
        # Create a thread with variables
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data: table://${lakehouse}.customers
target:
  table: ${output_table}
"""
        )

        # Load with runtime params
        result = load_config(
            thread_file,
            runtime_params={"lakehouse": "bronze", "output_table": "dim_customer"},
        )

        assert result["sources"]["data"] == "table://bronze.customers"
        assert result["target"]["table"] == "dim_customer"

    def test_load_thread_with_param_file(self, tmp_path):
        """Load thread using param file."""
        # Create param file
        param_file = tmp_path / "params.yaml"
        param_file.write_text(
            """
config_version: "1.0"
lakehouse: bronze
output_table: dim_customer
"""
        )

        # Create thread with variables
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data: table://${lakehouse}.customers
target:
  table: ${output_table}
"""
        )

        # Load with param file
        result = load_config(thread_file, param_file=param_file)

        assert result["sources"]["data"] == "table://bronze.customers"
        assert result["target"]["table"] == "dim_customer"

    def test_runtime_params_override_param_file(self, tmp_path):
        """Runtime params should override param file values."""
        # Create param file
        param_file = tmp_path / "params.yaml"
        param_file.write_text(
            """
config_version: "1.0"
env: dev
"""
        )

        # Create thread with variable
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data: table://lakehouse
target:
  table: output_${env}
"""
        )

        # Load with runtime override
        result = load_config(
            thread_file,
            runtime_params={"env": "prod"},
            param_file=param_file,
        )

        # Runtime value should win
        assert result["target"]["table"] == "output_prod"

    def test_variable_with_fallback_default(self, tmp_path):
        """Variables with fallback defaults should work."""
        thread_file = tmp_path / "thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data: table://lakehouse
target:
  table: ${output_table:-default_output}
"""
        )

        # Load without providing output_table
        result = load_config(thread_file)

        # Should use default
        assert result["target"]["table"] == "default_output"


class TestLoadConfigWithReferences:
    """Test config loading with file references."""

    def test_load_loom_with_weave_and_thread_references(self):
        """Load loom that references weaves and threads."""
        loom_path = FIXTURES / "project" / "looms" / "nightly.yaml"
        result = load_config(loom_path)

        # Verify loom loaded
        assert result["config_version"] == "1.0"
        assert "weaves" in result

        # Verify weave was resolved
        assert "_resolved_weaves" in result
        assert len(result["_resolved_weaves"]) == 1

        weave = result["_resolved_weaves"][0]
        assert "threads" in weave

        # Verify thread was resolved
        assert "_resolved_threads" in weave
        assert len(weave["_resolved_threads"]) == 1

        thread = weave["_resolved_threads"][0]
        assert "sources" in thread
        assert "target" in thread

    def test_load_weave_with_thread_references(self):
        """Load weave that references threads."""
        weave_path = FIXTURES / "project" / "weaves" / "dimensions.yaml"
        result = load_config(weave_path)

        # Verify weave loaded
        assert result["config_version"] == "1.0"
        assert "threads" in result

        # Verify threads were resolved
        assert "_resolved_threads" in result
        assert len(result["_resolved_threads"]) == 1

        thread = result["_resolved_threads"][0]
        assert "dim_customer" in thread["target"]["table"]


class TestLoadConfigWithInheritance:
    """Test config loading with inheritance cascade."""

    def test_inheritance_applied_to_threads(self, tmp_path):
        """Thread should inherit from weave and loom defaults."""
        # Create loom with defaults
        loom_dir = tmp_path / "looms"
        loom_dir.mkdir()
        loom_file = loom_dir / "test.yaml"
        loom_file.write_text(
            """
config_version: "1.0"
weaves:
  - test_weave
defaults:
  tags:
    - loom_tag
  audit: enabled
"""
        )

        # Create weave with defaults
        weave_dir = tmp_path / "weaves"
        weave_dir.mkdir()
        weave_file = weave_dir / "test_weave.yaml"
        weave_file.write_text(
            """
config_version: "1.0"
threads:
  - test_thread
defaults:
  tags:
    - weave_tag
  mode: merge
"""
        )

        # Create thread
        thread_dir = tmp_path / "threads"
        thread_dir.mkdir()
        thread_file = thread_dir / "test_thread.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  data: table://customers
target:
  table: output
mode: append
"""
        )

        # Load loom
        result = load_config(loom_file)

        # Get the resolved thread
        weave = result["_resolved_weaves"][0]
        thread = weave["_resolved_threads"][0]

        # Verify thread has inherited defaults
        # (Note: inheritance cascade happens at load time)
        assert "mode" in thread
        assert "tags" in thread or "audit" in thread


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
        """Raise ConfigSchemaError for invalid schema."""
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
  data: table://${missing_var}
target:
  table: output
"""
        )

        with pytest.raises(VariableResolutionError) as exc_info:
            load_config(thread_file)
        assert "Unresolved variable" in str(exc_info.value)
        assert "missing_var" in str(exc_info.value)

    def test_missing_referenced_file(self, tmp_path):
        """Raise ReferenceResolutionError for missing referenced file."""
        weave_dir = tmp_path / "weaves"
        weave_dir.mkdir()
        weave_file = weave_dir / "test.yaml"
        weave_file.write_text(
            """
config_version: "1.0"
threads:
  - nonexistent_thread
"""
        )

        with pytest.raises(ReferenceResolutionError) as exc_info:
            load_config(weave_file)
        assert "not found" in str(exc_info.value)


class TestLoadConfigIntegration:
    """Full integration tests exercising complete pipeline."""

    def test_full_pipeline_loom_to_thread(self, tmp_path):
        """Test complete pipeline: loom -> weave -> thread with all features."""
        # Create param file
        param_file = tmp_path / "params.yaml"
        param_file.write_text(
            """
config_version: "1.0"
lakehouse: bronze
env: dev
"""
        )

        # Create thread with variables
        thread_dir = tmp_path / "threads" / "dimensions"
        thread_dir.mkdir(parents=True)
        thread_file = thread_dir / "dim_customer.yaml"
        thread_file.write_text(
            """
config_version: "1.0"
sources:
  customers: table://${lakehouse}.raw_customers
target:
  table: dim_customer_${env}
mode: append
"""
        )

        # Create weave referencing thread
        weave_dir = tmp_path / "weaves"
        weave_dir.mkdir()
        weave_file = weave_dir / "dimensions.yaml"
        weave_file.write_text(
            """
config_version: "1.0"
threads:
  - dimensions.dim_customer
defaults:
  tags:
    - dimensions
  mode: merge
"""
        )

        # Create loom referencing weave
        loom_dir = tmp_path / "looms"
        loom_dir.mkdir()
        loom_file = loom_dir / "nightly.yaml"
        loom_file.write_text(
            """
config_version: "1.0"
weaves:
  - dimensions
defaults:
  audit: enabled
"""
        )

        # Load with runtime override
        result = load_config(
            loom_file,
            runtime_params={"env": "prod"},
            param_file=param_file,
        )

        # Verify structure
        assert result["config_version"] == "1.0"
        assert "_resolved_weaves" in result

        weave = result["_resolved_weaves"][0]
        assert "_resolved_threads" in weave

        thread = weave["_resolved_threads"][0]

        # Verify variable resolution (runtime overrides param file)
        assert thread["sources"]["customers"] == "table://bronze.raw_customers"
        assert thread["target"]["table"] == "dim_customer_prod"  # Runtime env=prod

        # Verify config structure is complete
        assert "mode" in thread
        assert "sources" in thread
        assert "target" in thread
