"""Execution configuration model."""

from enum import StrEnum

from weevr.model.base import FrozenBase


class LogLevel(StrEnum):
    """Configurable log level for weevr execution.

    Controls the verbosity of structured logging output during pipeline
    execution. Maps to Python logging levels internally.
    """

    MINIMAL = "minimal"
    STANDARD = "standard"
    VERBOSE = "verbose"
    DEBUG = "debug"


class ExecutionConfig(FrozenBase):
    """Runtime execution settings that cascade through loom/weave/thread.

    Attributes:
        log_level: Logging verbosity for execution output.
        trace: Whether to collect execution spans for telemetry.
    """

    log_level: LogLevel = LogLevel.STANDARD
    trace: bool = True
