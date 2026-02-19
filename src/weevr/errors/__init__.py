"""Error handling for weevr."""

from weevr.errors.exceptions import (
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
    "DataValidationError",
]
