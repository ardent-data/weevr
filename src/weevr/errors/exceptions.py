"""Exception hierarchy for weevr."""

from typing import Any


class WeevError(Exception):
    """Base exception for all weevr errors."""

    def __init__(self, message: str, cause: Exception | None = None) -> None:
        """Initialize WeevError.

        Args:
            message: Human-readable error message
            cause: Optional underlying exception that triggered this error
        """
        super().__init__(message)
        self.message = message
        self.cause = cause

    def __str__(self) -> str:
        """Return string representation of the error."""
        if self.cause:
            return f"{self.message} (caused by: {self.cause})"
        return self.message


class ConfigError(WeevError):
    """Base exception for configuration-related errors."""

    def __init__(
        self,
        message: str,
        cause: Exception | None = None,
        file_path: str | None = None,
        config_key: str | None = None,
    ) -> None:
        """Initialize ConfigError.

        Args:
            message: Human-readable error message
            cause: Optional underlying exception
            file_path: Path to the config file where the error occurred
            config_key: Specific config key that caused the error
        """
        super().__init__(message, cause)
        self.file_path = file_path
        self.config_key = config_key

    def __str__(self) -> str:
        """Return string representation with context."""
        parts = [self.message]
        if self.file_path:
            parts.append(f"in {self.file_path}")
        if self.config_key:
            parts.append(f"at key '{self.config_key}'")
        base_msg = " ".join(parts)
        if self.cause:
            return f"{base_msg} (caused by: {self.cause})"
        return base_msg


class ConfigParseError(ConfigError):
    """YAML parsing errors, file not found, or invalid syntax."""

    pass


class ConfigSchemaError(ConfigError):
    """Schema validation errors from Pydantic."""

    pass


class ConfigVersionError(ConfigError):
    """Unsupported or invalid config_version."""

    pass


class VariableResolutionError(ConfigError):
    """Unresolved variable references in config."""

    pass


class ReferenceResolutionError(ConfigError):
    """Missing referenced files or circular dependencies."""

    pass


class InheritanceError(ConfigError):
    """Config inheritance cascade failures."""

    pass


class ExecutionError(WeevError):
    """Base exception for execution-time errors (M03+)."""

    pass


class SparkError(ExecutionError):
    """Spark-specific execution errors (M03+)."""

    pass


class DataValidationError(WeevError):
    """Data validation errors (M05+)."""

    pass
