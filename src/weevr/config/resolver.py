"""Variable interpolation and reference resolution."""

import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

from weevr.config.parser import TYPED_EXTENSIONS
from weevr.errors import ConfigError, ConfigSchemaError, VariableResolutionError


def build_param_context(
    runtime_params: dict[str, Any] | None = None,
    config_defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build parameter context with proper priority layering.

    Priority order (highest to lowest):
    1. runtime_params
    2. config_defaults

    Args:
        runtime_params: Runtime parameter overrides.
        config_defaults: Default parameters from config.

    Returns:
        Merged parameter context dictionary with dotted key access support.
    """
    context: dict[str, Any] = {}

    # Layer 2: Config defaults (lowest priority)
    if config_defaults:
        context.update(config_defaults)

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
            except KeyError as exc:
                if default_value is not None:
                    return default_value
                else:
                    raise VariableResolutionError(
                        f"Unresolved variable '${{{var_name}}}' with no default value",
                        config_key=var_name,
                    ) from exc

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


def resolve_ref_path(ref: str, project_root: Path) -> Path:
    """Resolve a path-based reference to an absolute file path.

    Args:
        ref: Path-based reference with typed extension, relative to project
            root (e.g., ``'dimensions/dim_customer.thread'``).
        project_root: Absolute path to the ``.weevr`` project directory.

    Returns:
        Resolved absolute path to the referenced config file.

    Raises:
        ReferenceResolutionError: If the referenced file does not exist.
        ConfigError: If the reference does not have a typed extension.
    """
    from weevr.errors import ReferenceResolutionError

    suffix = Path(ref).suffix.lower()
    if suffix not in TYPED_EXTENSIONS:
        raise ConfigError(
            f"Unsupported extension '{suffix}'. Expected .thread, .weave, or .loom",
            file_path=ref,
        )

    resolved = (project_root / ref).resolve()
    if not resolved.exists():
        raise ReferenceResolutionError(
            f"Referenced file not found: {ref}",
            file_path=str(resolved),
        )
    return resolved


def resolve_references(
    config: dict[str, Any],
    config_type: str,
    project_root: Path,
    runtime_params: dict[str, Any] | None = None,
    visited: set[str] | None = None,
) -> dict[str, Any]:
    """Resolve references to other config files.

    Handles both external references (``ref`` key) and inline definitions
    (``name`` + body keys). Recursively loads referenced configs with
    circular reference detection.

    Args:
        config: Config dict to resolve references in.
        config_type: Type of this config (``'weave'`` or ``'loom'``).
        project_root: Absolute path to the ``.weevr`` project directory.
        runtime_params: Runtime parameters to pass to child configs.
        visited: Set of already-visited ref strings (for cycle detection).

    Returns:
        Config dict with resolved child configs attached under
        ``'_resolved_threads'`` or ``'_resolved_weaves'`` keys.

    Raises:
        ReferenceResolutionError: If referenced file not found or circular
            reference detected.
        ConfigError: If an inline definition is missing a ``name`` field.
    """
    from weevr.config.parser import (
        detect_config_type_from_extension,
        extract_config_version,
        parse_yaml,
        validate_config_version,
    )
    from weevr.config.validation import validate_schema
    from weevr.errors import ReferenceResolutionError

    if visited is None:
        visited = set()

    result = config.copy()

    # Resolve weaves in loom
    if config_type == "loom" and "weaves" in config:
        resolved_weaves = []

        for entry in config["weaves"]:
            # Normalize: string entries become {"name": string}
            if isinstance(entry, str):
                entry = {"name": entry}

            if isinstance(entry, dict) and "ref" in entry:
                # External reference
                ref = entry["ref"]
                if ref in visited:
                    cycle = " -> ".join(visited) + f" -> {ref}"
                    raise ReferenceResolutionError(
                        f"Circular reference detected: {cycle}",
                        file_path=ref,
                    )

                weave_path = resolve_ref_path(ref, project_root)
                visited.add(ref)
                try:
                    raw = parse_yaml(weave_path)
                    version = extract_config_version(raw)
                    ext_type = detect_config_type_from_extension(weave_path)
                    child_type = ext_type if ext_type else "weave"
                    validate_config_version(version, child_type)
                    validated = validate_schema(raw, child_type)
                    child_dict = validated.model_dump(exclude_unset=True)

                    # Inject name from filename stem
                    if not child_dict.get("name"):
                        child_dict["name"] = weave_path.stem

                    # Set qualified key
                    child_dict["qualified_key"] = ref

                    context = build_param_context(runtime_params, child_dict.get("defaults"))
                    resolved_child = resolve_variables(child_dict, context)

                    resolved_child = resolve_references(
                        resolved_child,
                        child_type,
                        project_root,
                        runtime_params,
                        visited.copy(),
                    )

                    resolved_weaves.append(resolved_child)
                finally:
                    visited.discard(ref)
            else:
                # Inline definition — must have name
                if isinstance(entry, dict) and not entry.get("name"):
                    raise ConfigError(
                        "Inline weave definition requires a 'name' field",
                    )
                # Inline entries are passed through as-is (already complete dicts)
                if isinstance(entry, dict):
                    inline = entry.copy()
                    inline["qualified_key"] = entry.get("name", "")
                    resolved_weaves.append(inline)

        result["_resolved_weaves"] = resolved_weaves

    # Resolve threads in weave
    elif config_type == "weave" and "threads" in config:
        resolved_threads = []

        for entry in config["threads"]:
            # Normalize: string entries become {"name": string}
            if isinstance(entry, str):
                entry = {"name": entry}

            if isinstance(entry, dict) and "ref" in entry:
                # External reference
                ref = entry["ref"]
                if ref in visited:
                    cycle = " -> ".join(visited) + f" -> {ref}"
                    raise ReferenceResolutionError(
                        f"Circular reference detected: {cycle}",
                        file_path=ref,
                    )

                thread_path = resolve_ref_path(ref, project_root)
                visited.add(ref)
                try:
                    raw = parse_yaml(thread_path)
                    version = extract_config_version(raw)
                    ext_type = detect_config_type_from_extension(thread_path)
                    child_type = ext_type if ext_type else "thread"
                    validate_config_version(version, child_type)
                    validated = validate_schema(raw, child_type)
                    child_dict = validated.model_dump(exclude_unset=True)

                    # Inject name from filename stem
                    if not child_dict.get("name"):
                        child_dict["name"] = thread_path.stem

                    # Set qualified key
                    child_dict["qualified_key"] = ref

                    context = build_param_context(runtime_params, child_dict.get("defaults"))
                    resolved_child = resolve_variables(child_dict, context)

                    # Preserve entry-level overrides (dependencies, condition)
                    for key in ("dependencies", "condition"):
                        if key in entry:
                            resolved_child[f"_entry_{key}"] = entry[key]

                    resolved_threads.append(resolved_child)
                finally:
                    visited.discard(ref)
            else:
                # Inline definition — must have name
                if isinstance(entry, dict) and not entry.get("name"):
                    raise ConfigError(
                        "Inline thread definition requires a 'name' field",
                    )
                if isinstance(entry, dict):
                    inline = entry.copy()
                    inline["qualified_key"] = entry.get("name", "")
                    resolved_threads.append(inline)

        result["_resolved_threads"] = resolved_threads

    return result
