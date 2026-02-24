"""Exception hierarchy for weevr."""


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


class ModelValidationError(ConfigError):
    """Semantic validation failures during model hydration."""

    pass


class ExecutionError(WeevError):
    """Base exception for execution-time errors.

    Carries optional execution context to pinpoint where a failure occurred
    within a thread pipeline.
    """

    def __init__(
        self,
        message: str,
        cause: Exception | None = None,
        thread_name: str | None = None,
        step_index: int | None = None,
        step_type: str | None = None,
        source_name: str | None = None,
    ) -> None:
        """Initialize ExecutionError.

        Args:
            message: Human-readable error message.
            cause: Optional underlying exception.
            thread_name: Name of the thread where the error occurred.
            step_index: Zero-based index of the pipeline step that failed.
            step_type: Step type key (e.g. "filter", "join") that failed.
            source_name: Source alias that caused the error.
        """
        super().__init__(message, cause)
        self.thread_name = thread_name
        self.step_index = step_index
        self.step_type = step_type
        self.source_name = source_name

    def __str__(self) -> str:
        """Return string representation with execution context."""
        parts = [self.message]
        if self.thread_name:
            parts.append(f"in thread '{self.thread_name}'")
        if self.step_index is not None:
            step_label = f"step {self.step_index}"
            if self.step_type:
                step_label += f" ({self.step_type})"
            parts.append(f"at {step_label}")
        if self.source_name:
            parts.append(f"reading source '{self.source_name}'")
        base_msg = " ".join(parts)
        if self.cause:
            return f"{base_msg} (caused by: {self.cause})"
        return base_msg


class SparkError(ExecutionError):
    """Spark-specific execution errors."""

    pass


class StateError(ExecutionError):
    """Watermark state persistence errors.

    Raised when watermark state cannot be read from or written to the
    configured store. Carries ``store_type`` context in addition to
    the standard execution context fields.
    """

    def __init__(
        self,
        message: str,
        cause: Exception | None = None,
        thread_name: str | None = None,
        store_type: str | None = None,
    ) -> None:
        """Initialize StateError.

        Args:
            message: Human-readable error message.
            cause: Optional underlying exception.
            thread_name: Name of the thread where the error occurred.
            store_type: Watermark store type (e.g. "table_properties", "metadata_table").
        """
        super().__init__(message, cause=cause, thread_name=thread_name)
        self.store_type = store_type

    def __str__(self) -> str:
        """Return string representation with store context."""
        parts = [self.message]
        if self.thread_name:
            parts.append(f"in thread '{self.thread_name}'")
        if self.store_type:
            parts.append(f"using store '{self.store_type}'")
        base_msg = " ".join(parts)
        if self.cause:
            return f"{base_msg} (caused by: {self.cause})"
        return base_msg


class DataValidationError(WeevError):
    """Data validation errors (M05+)."""

    pass
