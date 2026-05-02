"""Exception hierarchy for weevr."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from weevr.model.warp import DriftReport


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
    """Raised when a YAML config file cannot be read or parsed.

    Common triggers include missing files, empty content, invalid YAML syntax,
    non-dictionary top-level structures, and malformed ``config_version`` fields.
    """

    pass


class ConfigSchemaError(ConfigError):
    """Raised when config structure does not match the expected Pydantic schema.

    This covers missing required fields, unknown config types, parameter type
    mismatches, and invalid format for typed parameters (dates, timestamps).
    """

    pass


class ConfigVersionError(ConfigError):
    """Raised when ``config_version`` specifies an unsupported major version.

    weevr validates that the declared version is compatible with the current
    parser. Only major version ``1.x`` is supported.
    """

    pass


class VariableResolutionError(ConfigError):
    """Raised when a ``${variable}`` reference cannot be resolved.

    This occurs when a variable placeholder has no matching entry in the
    parameter context and no default value (``${var:-default}``) is provided.
    """

    pass


class ReferenceResolutionError(ConfigError):
    """Raised when a ``ref:`` entry cannot be loaded or creates a circular chain.

    Common causes include missing referenced files, incorrect relative paths,
    and circular reference chains between weaves or threads.
    """

    pass


class InheritanceError(ConfigError):
    """Raised when the configuration inheritance cascade fails.

    Reserved for failures during the loom → weave → thread default merging
    process. Not currently raised in v1.0.
    """

    pass


class ModelValidationError(ConfigError):
    """Raised when a fully resolved config fails to hydrate into a typed model.

    This occurs after variable resolution and inheritance, when the concrete
    values are validated through the Pydantic domain model (Thread, Weave, or
    Loom). Semantic constraints that span multiple fields are checked here.
    """

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
    """Raised for Spark API-level failures during thread execution.

    Intended for errors originating from PySpark operations (reader failures,
    write exceptions, catalyst errors). Reserved for future use — v1.0 uses
    ``ExecutionError`` for all runtime failures.
    """

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


class HookError(ExecutionError):
    """Raised when a hook step fails with on_failure: abort.

    Carries hook-specific context to identify which step failed and
    in which phase of the weave lifecycle.
    """

    def __init__(
        self,
        message: str,
        cause: Exception | None = None,
        hook_name: str | None = None,
        hook_type: str | None = None,
        phase: str | None = None,
    ) -> None:
        """Initialize HookError.

        Args:
            message: Human-readable error message.
            cause: Optional underlying exception.
            hook_name: Name of the hook step that failed.
            hook_type: Step type key (e.g. "quality_gate", "sql_statement").
            phase: Execution phase ("pre" or "post").
        """
        super().__init__(message, cause=cause)
        self.hook_name = hook_name
        self.hook_type = hook_type
        self.phase = phase

    def __str__(self) -> str:
        """Return string representation with hook context."""
        parts = [self.message]
        if self.phase:
            parts.append(f"in {self.phase} phase")
        if self.hook_type:
            label = self.hook_type
            if self.hook_name:
                label += f" '{self.hook_name}'"
            parts.append(f"at {label}")
        base_msg = " ".join(parts)
        if self.cause:
            return f"{base_msg} (caused by: {self.cause})"
        return base_msg


class ExportError(ExecutionError):
    """Raised when an export write fails with on_failure: abort.

    Carries export-specific context to identify which export failed
    and what format was being written.
    """

    def __init__(
        self,
        message: str,
        cause: Exception | None = None,
        thread_name: str | None = None,
        export_name: str | None = None,
        export_type: str | None = None,
    ) -> None:
        """Initialize ExportError.

        Args:
            message: Human-readable error message.
            cause: Optional underlying exception.
            thread_name: Name of the thread where the error occurred.
            export_name: Name of the export that failed.
            export_type: Format type of the export (e.g. "parquet", "csv").
        """
        super().__init__(message, cause=cause, thread_name=thread_name)
        self.export_name = export_name
        self.export_type = export_type

    def __str__(self) -> str:
        """Return string representation with export context."""
        parts = [self.message]
        if self.thread_name:
            parts.append(f"in thread '{self.thread_name}'")
        if self.export_name:
            label = self.export_name
            if self.export_type:
                label += f" ({self.export_type})"
            parts.append(f"at export '{label}'")
        base_msg = " ".join(parts)
        if self.cause:
            return f"{base_msg} (caused by: {self.cause})"
        return base_msg


class LookupResolutionError(ConfigError):
    """Raised when a thread references a lookup not defined in the weave.

    This is a fail-fast validation error caught during config resolution,
    before any data is read or threads are executed.
    """

    pass


class WarpEnforcementError(ExecutionError):
    """Raised when warp contract enforcement finds violations.

    Carries the list of enforcement findings (missing columns, type
    mismatches, nullable violations) that caused the failure.
    """

    def __init__(
        self,
        message: str,
        cause: Exception | None = None,
        thread_name: str | None = None,
        findings: list[dict[str, str]] | None = None,
    ) -> None:
        """Initialize WarpEnforcementError.

        Args:
            message: Human-readable error message.
            cause: Optional underlying exception.
            thread_name: Name of the thread where the error occurred.
            findings: List of enforcement findings (dicts with type, column, etc.).
        """
        super().__init__(message, cause=cause, thread_name=thread_name)
        self.findings = findings or []

    def __str__(self) -> str:
        """Return string representation with findings count."""
        parts = [self.message]
        if self.thread_name:
            parts.append(f"in thread '{self.thread_name}'")
        parts.append(f"({len(self.findings)} finding(s))")
        base_msg = " ".join(parts)
        if self.cause:
            return f"{base_msg} (caused by: {self.cause})"
        return base_msg


class SchemaDriftError(ExecutionError):
    """Raised when schema drift is detected in strict mode with on_drift: error.

    Carries the drift report with details about which extra columns
    were found and the configured drift handling mode.
    """

    def __init__(
        self,
        message: str,
        cause: Exception | None = None,
        thread_name: str | None = None,
        drift_report: DriftReport | None = None,
    ) -> None:
        """Initialize SchemaDriftError.

        Args:
            message: Human-readable error message.
            cause: Optional underlying exception.
            thread_name: Name of the thread where the error occurred.
            drift_report: DriftReport instance with drift details.
        """
        super().__init__(message, cause=cause, thread_name=thread_name)
        self.drift_report = drift_report

    def __str__(self) -> str:
        """Return string representation with drift info."""
        parts = [self.message]
        if self.thread_name:
            parts.append(f"in thread '{self.thread_name}'")
        if self.drift_report is not None:
            extra = getattr(self.drift_report, "extra_columns", [])
            if extra:
                parts.append(f"({len(extra)} extra column(s))")
        base_msg = " ".join(parts)
        if self.cause:
            return f"{base_msg} (caused by: {self.cause})"
        return base_msg


class DataValidationError(WeevError):
    """Raised when a fatal-severity validation rule has failing rows.

    When any row fails a validation rule declared with ``severity: fatal``,
    the thread aborts immediately without writing data. Downgrade to
    ``severity: error`` to quarantine failing rows instead of aborting.
    """

    pass
