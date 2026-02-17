"""Variable interpolation and reference resolution."""

import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

from weevr.errors import ConfigSchemaError, VariableResolutionError


def build_param_context(
    runtime_params: dict[str, Any] | None = None,
    param_file_data: dict[str, Any] | None = None,
    config_defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build parameter context with proper priority layering.

    Priority order (highest to lowest):
    1. runtime_params
    2. param_file_data
    3. config_defaults

    Args:
        runtime_params: Runtime parameter overrides
        param_file_data: Parameters from param file
        config_defaults: Default parameters from config

    Returns:
        Merged parameter context dictionary with dotted key access support
    """
    context: dict[str, Any] = {}

    # Layer 3: Config defaults (lowest priority)
    if config_defaults:
        context.update(config_defaults)

    # Layer 2: Param file
    if param_file_data:
        context.update(param_file_data)

    # Layer 1: Runtime params (highest priority)
    if runtime_params:
        context.update(runtime_params)

    return context


def _get_dotted_value(context: dict[str, Any], key: str) -> Any:
    """Get value from context using dotted key notation.

    Args:
        context: Parameter context dictionary
        key: Dotted key path (e.g., 'env.lakehouse')

    Returns:
        Value at the dotted path

    Raises:
        KeyError: If the dotted path doesn't exist
    """
    parts = key.split(".")
    value = context

    for part in parts:
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            raise KeyError(key)

    return value


# Regex pattern for ${var} and ${var:-default}
VARIABLE_PATTERN = re.compile(r"\$\{([^}:]+?)(?::-(.*?))?\}")


def resolve_variables(
    config: dict[str, Any] | list[Any] | str | Any,
    context: dict[str, Any],
) -> Any:
    """Recursively resolve variable references in config.

    Supports:
    - ${var} - simple variable reference (error if not found)
    - ${var:-default} - variable with fallback default

    Args:
        config: Config structure to resolve (dict, list, str, or primitive)
        context: Parameter context for variable lookup

    Returns:
        Config with all variables resolved

    Raises:
        VariableResolutionError: If variable not found and no default provided
    """
    if isinstance(config, dict):
        return {k: resolve_variables(v, context) for k, v in config.items()}

    elif isinstance(config, list):
        return [resolve_variables(item, context) for item in config]

    elif isinstance(config, str):
        # Find all variable references in the string
        def replace_var(match: re.Match[str]) -> str:
            var_name = match.group(1).strip()
            default_value = match.group(2)

            try:
                # Try dotted key access first
                value = _get_dotted_value(context, var_name)
                return str(value)
            except KeyError:
                if default_value is not None:
                    # Use default if provided
                    return default_value
                else:
                    raise VariableResolutionError(
                        f"Unresolved variable '${{{var_name}}}' with no default value",
                        config_key=var_name,
                    )

        return VARIABLE_PATTERN.sub(replace_var, config)

    else:
        # Primitives (int, bool, None, etc.) pass through unchanged
        return config


def validate_params(
    param_specs: dict[str, Any] | None,
    context: dict[str, Any],
) -> None:
    """Validate parameters against their type specifications.

    Args:
        param_specs: Parameter specifications from config
        context: Actual parameter values

    Raises:
        ConfigSchemaError: If required params missing or type mismatches
    """
    if not param_specs:
        return

    for param_name, spec in param_specs.items():
        # Handle both dict specs and ParamSpec objects
        if hasattr(spec, "required"):
            required = spec.required
            param_type = spec.type
            default = spec.default
        else:
            required = spec.get("required", True)
            param_type = spec.get("type", "string")
            default = spec.get("default")

        # Check required params
        if required and param_name not in context:
            if default is not None:
                # Use default if provided
                context[param_name] = default
                continue
            else:
                raise ConfigSchemaError(
                    f"Required parameter '{param_name}' is missing",
                    config_key=param_name,
                )

        # Skip validation if param not present (and not required)
        if param_name not in context:
            continue

        value = context[param_name]

        # Type validation
        if param_type == "string":
            if not isinstance(value, str):
                raise ConfigSchemaError(
                    f"Parameter '{param_name}' expected string, got {type(value).__name__}",
                    config_key=param_name,
                )

        elif param_type == "int":
            if not isinstance(value, int) or isinstance(value, bool):
                raise ConfigSchemaError(
                    f"Parameter '{param_name}' expected int, got {type(value).__name__}",
                    config_key=param_name,
                )

        elif param_type == "float":
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise ConfigSchemaError(
                    f"Parameter '{param_name}' expected float, got {type(value).__name__}",
                    config_key=param_name,
                )

        elif param_type == "bool":
            if not isinstance(value, bool):
                raise ConfigSchemaError(
                    f"Parameter '{param_name}' expected bool, got {type(value).__name__}",
                    config_key=param_name,
                )

        elif param_type == "date":
            if isinstance(value, str):
                try:
                    datetime.strptime(value, "%Y-%m-%d")
                except ValueError as e:
                    raise ConfigSchemaError(
                        f"Parameter '{param_name}' invalid date format, expected YYYY-MM-DD",
                        cause=e,
                        config_key=param_name,
                    ) from e
            elif not isinstance(value, date):
                raise ConfigSchemaError(
                    f"Parameter '{param_name}' expected date or ISO date string",
                    config_key=param_name,
                )

        elif param_type == "timestamp":
            if isinstance(value, str):
                try:
                    datetime.fromisoformat(value)
                except ValueError as e:
                    raise ConfigSchemaError(
                        f"Parameter '{param_name}' invalid timestamp format, "
                        "expected ISO format (YYYY-MM-DDTHH:MM:SS)",
                        cause=e,
                        config_key=param_name,
                    ) from e
            elif not isinstance(value, datetime):
                raise ConfigSchemaError(
                    f"Parameter '{param_name}' expected timestamp or ISO timestamp string",
                    config_key=param_name,
                )

        elif param_type == "list[string]":
            if not isinstance(value, list):
                raise ConfigSchemaError(
                    f"Parameter '{param_name}' expected list, got {type(value).__name__}",
                    config_key=param_name,
                )
            if not all(isinstance(item, str) for item in value):
                raise ConfigSchemaError(
                    f"Parameter '{param_name}' expected list of strings",
                    config_key=param_name,
                )
