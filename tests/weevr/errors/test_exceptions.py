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
    ModelValidationError,
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
        assert issubclass(ModelValidationError, ConfigError)

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

    def test_model_validation_error(self):
        """ModelValidationError can be raised and caught."""
        with pytest.raises(ModelValidationError) as exc_info:
            raise ModelValidationError("Hydration failed for thread")
        assert "Hydration failed for thread" in str(exc_info.value)

    def test_catch_via_base_type(self):
        """Specific errors catchable via ConfigError."""
        with pytest.raises(ConfigError):
            raise ConfigParseError("test")

        with pytest.raises(WeevError):
            raise ConfigSchemaError("test")


class TestExecutionError:
    """Test ExecutionError context fields."""

    def test_minimal_construction(self):
        """ExecutionError constructs with message only."""
        err = ExecutionError("execution failed")
        assert str(err) == "execution failed"
        assert err.thread_name is None
        assert err.step_index is None
        assert err.step_type is None
        assert err.source_name is None

    def test_with_thread_name(self):
        """thread_name appears in string representation."""
        err = ExecutionError("failed", thread_name="dimensions.dim_customer")
        assert "dimensions.dim_customer" in str(err)
        assert err.thread_name == "dimensions.dim_customer"

    def test_with_step_context(self):
        """step_index and step_type appear in string representation."""
        err = ExecutionError("bad step", step_index=2, step_type="filter")
        result = str(err)
        assert "step 2" in result
        assert "filter" in result
        assert err.step_index == 2
        assert err.step_type == "filter"

    def test_with_step_index_only(self):
        """step_index without step_type still appears."""
        err = ExecutionError("bad step", step_index=0)
        assert "step 0" in str(err)

    def test_with_source_name(self):
        """source_name appears in string representation."""
        err = ExecutionError("read failed", source_name="customers")
        assert "customers" in str(err)
        assert err.source_name == "customers"

    def test_with_all_context(self):
        """All context fields appear in string representation."""
        cause = ValueError("underlying error")
        err = ExecutionError(
            "pipeline failed",
            cause=cause,
            thread_name="facts.fact_order",
            step_index=3,
            step_type="join",
            source_name="orders",
        )
        result = str(err)
        assert "pipeline failed" in result
        assert "facts.fact_order" in result
        assert "step 3" in result
        assert "join" in result
        assert "orders" in result
        assert "caused by" in result
        assert "underlying error" in result

    def test_spark_error_inherits_context(self):
        """SparkError inherits ExecutionError context fields."""
        err = SparkError("spark failed", thread_name="t1", step_index=1)
        assert isinstance(err, ExecutionError)
        assert err.thread_name == "t1"
        assert "t1" in str(err)

    def test_catchable_as_execution_error(self):
        """SparkError is catchable as ExecutionError and WeevError."""
        with pytest.raises(ExecutionError):
            raise SparkError("spark failure")

        with pytest.raises(WeevError):
            raise SparkError("spark failure")


class TestModelValidationError:
    """Test ModelValidationError specific behaviour."""

    def test_is_config_error(self):
        """ModelValidationError is a ConfigError and WeevError."""
        err = ModelValidationError("semantic failure")
        assert isinstance(err, ConfigError)
        assert isinstance(err, WeevError)

    def test_file_path_context(self):
        """ModelValidationError accepts file_path context."""
        err = ModelValidationError("bad model", file_path="/configs/thread.yaml")
        assert err.file_path == "/configs/thread.yaml"
        assert "/configs/thread.yaml" in str(err)

    def test_cause_chaining(self):
        """ModelValidationError wraps a cause exception."""
        original = ValueError("pydantic detail")
        err = ModelValidationError("hydration failed", cause=original)
        assert err.cause is original
        assert "caused by" in str(err)
        assert "pydantic detail" in str(err)

    def test_str_with_all_context(self):
        """String representation includes message, file path, and cause."""
        original = ValueError("nested detail")
        err = ModelValidationError(
            "model hydration failed",
            cause=original,
            file_path="/path/to/thread.yaml",
        )
        result = str(err)
        assert "model hydration failed" in result
        assert "/path/to/thread.yaml" in result
        assert "nested detail" in result
