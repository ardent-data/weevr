"""Error handling for weevr."""

from weevr.errors.exceptions import (
    ConfigError,
    ConfigParseError,
    ConfigSchemaError,
    ConfigVersionError,
    DataValidationError,
    ExecutionError,
    HookError,
    InheritanceError,
    LookupResolutionError,
    ModelValidationError,
    ReferenceResolutionError,
    SparkError,
    StateError,
    VariableResolutionError,
    WeevError,
)

__all__ = [
    "WeevError",
    "ConfigError",
    "ConfigParseError",
    "ConfigSchemaError",
    "ConfigVersionError",
    "VariableResolutionError",
    "ReferenceResolutionError",
    "InheritanceError",
    "ModelValidationError",
    "ExecutionError",
    "SparkError",
    "StateError",
    "HookError",
    "LookupResolutionError",
    "DataValidationError",
]
