"""Tests for exception hierarchy."""

import pytest

from weevr.errors import (
    ConfigError,
    ConfigParseError,
    ConfigSchemaError,
    ConfigVersionError,
    DataValidationError,
    ExecutionError,
    InheritanceError,
    ReferenceResolutionError,
    SparkError,
    VariableResolutionError,
    WeevError,
)


class TestExceptionHierarchy:
    """Test exception inheritance structure."""

    def test_weev_error_is_base(self):
        """WeevError should be the base of all weevr exceptions."""
        assert issubclass(ConfigError, WeevError)
        assert issubclass(ExecutionError, WeevError)
        assert issubclass(DataValidationError, WeevError)

    def test_config_error_hierarchy(self):
        """All config errors should inherit from ConfigError."""
        assert issubclass(ConfigParseError, ConfigError)
        assert issubclass(ConfigSchemaError, ConfigError)
        assert issubclass(ConfigVersionError, ConfigError)
        assert issubclass(VariableResolutionError, ConfigError)
        assert issubclass(ReferenceResolutionError, ConfigError)
        assert issubclass(InheritanceError, ConfigError)

    def test_execution_error_hierarchy(self):
        """SparkError should inherit from ExecutionError."""
        assert issubclass(SparkError, ExecutionError)

    def test_catchable_via_base(self):
        """All errors should be catchable via WeevError."""
        exceptions = [
            ConfigParseError("test"),
            ConfigSchemaError("test"),
            ExecutionError("test"),
            DataValidationError("test"),
        ]
        for exc in exceptions:
            assert isinstance(exc, WeevError)


class TestWeevError:
    """Test base WeevError functionality."""

    def test_message_accessible(self):
        """Error message should be accessible."""
        error = WeevError("test message")
        assert error.message == "test message"

    def test_cause_chaining(self):
        """Cause should chain correctly."""
        original = ValueError("original error")
        error = WeevError("wrapped error", cause=original)
        assert error.cause is original

    def test_str_without_cause(self):
        """String representation without cause."""
        error = WeevError("test message")
        assert str(error) == "test message"

    def test_str_with_cause(self):
        """String representation with cause."""
        original = ValueError("original")
        error = WeevError("wrapped", cause=original)
        assert "wrapped" in str(error)
        assert "original" in str(error)
        assert "caused by" in str(error)


class TestConfigError:
    """Test ConfigError context fields."""

    def test_file_path_context(self):
        """File path should be accessible."""
        error = ConfigParseError("test", file_path="/path/to/config.yaml")
        assert error.file_path == "/path/to/config.yaml"

    def test_config_key_context(self):
        """Config key should be accessible."""
        error = ConfigSchemaError("test", config_key="sources")
        assert error.config_key == "sources"

    def test_str_with_file_path(self):
        """String representation should include file path."""
        error = ConfigParseError("invalid YAML", file_path="/path/to/config.yaml")
        result = str(error)
        assert "invalid YAML" in result
        assert "/path/to/config.yaml" in result

    def test_str_with_config_key(self):
        """String representation should include config key."""
        error = ConfigSchemaError("missing field", config_key="sources")
        result = str(error)
        assert "missing field" in result
        assert "sources" in result

    def test_str_with_all_context(self):
        """String representation with all context fields."""
        original = ValueError("original")
        error = ConfigParseError(
            "parse error",
            cause=original,
            file_path="/path/config.yaml",
            config_key="threads",
        )
        result = str(error)
        assert "parse error" in result
        assert "/path/config.yaml" in result
        assert "threads" in result
        assert "caused by" in result
        assert "original" in result


class TestSpecificExceptions:
    """Test specific exception types can be raised and caught."""

    def test_config_parse_error(self):
        """ConfigParseError can be raised and caught."""
        with pytest.raises(ConfigParseError) as exc_info:
            raise ConfigParseError("YAML syntax error")
        assert "YAML syntax error" in str(exc_info.value)

    def test_config_schema_error(self):
        """ConfigSchemaError can be raised and caught."""
        with pytest.raises(ConfigSchemaError) as exc_info:
            raise ConfigSchemaError("Schema validation failed")
        assert "Schema validation failed" in str(exc_info.value)

    def test_config_version_error(self):
        """ConfigVersionError can be raised and caught."""
        with pytest.raises(ConfigVersionError) as exc_info:
            raise ConfigVersionError("Unsupported version")
        assert "Unsupported version" in str(exc_info.value)

    def test_variable_resolution_error(self):
        """VariableResolutionError can be raised and caught."""
        with pytest.raises(VariableResolutionError) as exc_info:
            raise VariableResolutionError("Unresolved variable")
        assert "Unresolved variable" in str(exc_info.value)

    def test_reference_resolution_error(self):
        """ReferenceResolutionError can be raised and caught."""
        with pytest.raises(ReferenceResolutionError) as exc_info:
            raise ReferenceResolutionError("Circular reference")
        assert "Circular reference" in str(exc_info.value)

    def test_inheritance_error(self):
        """InheritanceError can be raised and caught."""
        with pytest.raises(InheritanceError) as exc_info:
            raise InheritanceError("Cascade failed")
        assert "Cascade failed" in str(exc_info.value)

    def test_catch_via_base_type(self):
        """Specific errors catchable via ConfigError."""
        with pytest.raises(ConfigError):
            raise ConfigParseError("test")

        with pytest.raises(WeevError):
            raise ConfigSchemaError("test")
