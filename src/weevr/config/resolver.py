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
    fabric_context: dict[str, Any] | None = None,
    entry_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build parameter context with proper priority layering.

    Priority order (highest to lowest):
    1. runtime_params
    2. entry_params (nested under ``param`` key for ``${param.x}`` access)
    3. config_defaults
    4. fabric_context

    Args:
        runtime_params: Runtime parameter overrides.
        config_defaults: Default parameters from config.
        fabric_context: Fabric environment values keyed as ``fabric.<field>``
            (e.g. ``fabric.workspace_id``). None values are omitted. Lowest
            priority — overridden by config_defaults and runtime_params.
        entry_params: ThreadEntry-level parameters injected under the
            ``param`` namespace for ``${param.x}`` dotted-key resolution.

    Returns:
        Merged parameter context dictionary with dotted key access support.
    """
    context: dict[str, Any] = {}

    # Layer 4: Fabric context (lowest priority)
    if fabric_context:
        fabric_dict = {
            k.split(".", 1)[1]: v
            for k, v in fabric_context.items()
            if v is not None and k.startswith("fabric.")
        }
        if fabric_dict:
            context["fabric"] = fabric_dict

    # Layer 3: Config defaults
    if config_defaults:
        context.update(config_defaults)

    # Layer 2: Entry params (under "param" namespace)
    if entry_params:
        context["param"] = entry_params

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

# Matches a string that is entirely a single ${var} or ${var:-default} reference
WHOLE_VALUE_PATTERN = re.compile(r"^\$\{([^}:]+?)(?::-(.*?))?\}$")


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
        # Whole-value check: if the entire string is a single ${param} reference,
        # return the native Python type rather than stringifying it.
        whole_match = WHOLE_VALUE_PATTERN.match(config.strip())
        if whole_match:
            var_name = whole_match.group(1).strip()
            default_value = whole_match.group(2)

            if not var_name.startswith(("var.", "run.")):
                try:
                    return _get_dotted_value(context, var_name)
                except KeyError as exc:
                    if default_value is not None:
                        return default_value
                    else:
                        raise VariableResolutionError(
                            f"Unresolved variable '${{{var_name}}}' with no default value",
                            config_key=var_name,
                        ) from exc
            # var.* / run.* fall through to the existing sub() path below

        # Find all variable references in the string
        def replace_var(match: re.Match[str]) -> str:
            var_name = match.group(1).strip()
            default_value = match.group(2)

            # Skip runtime variable namespaces — resolved later at execution time
            if var_name.startswith(("var.", "run.")):
                return match.group(0)

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
    if not resolved.is_relative_to(project_root.resolve()):
        raise ReferenceResolutionError(
            f"Reference '{ref}' resolves outside project root",
            file_path=str(resolved),
        )
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
                # Name-only entry — resolve by convention to {name}.weave
                if isinstance(entry, dict) and not entry.get("name"):
                    raise ConfigError(
                        "Inline weave definition requires a 'name' field",
                    )
                if isinstance(entry, dict):
                    weave_name = entry["name"]
                    convention_ref = f"{weave_name}.weave"
                    convention_path = project_root / convention_ref
                    if convention_path.exists():
                        if convention_ref in visited:
                            cycle = " -> ".join(visited) + f" -> {convention_ref}"
                            raise ReferenceResolutionError(
                                f"Circular reference detected: {cycle}",
                                file_path=convention_ref,
                            )

                        # Load the weave file by convention
                        visited.add(convention_ref)
                        try:
                            conv_raw = parse_yaml(convention_path)
                            conv_version = extract_config_version(conv_raw)
                            conv_type = detect_config_type_from_extension(convention_path)
                            child_type = conv_type if conv_type else "weave"
                            validate_config_version(conv_version, child_type)
                            validated = validate_schema(conv_raw, child_type)
                            child_dict = validated.model_dump(exclude_unset=True)
                            if not child_dict.get("name"):
                                child_dict["name"] = weave_name
                            child_dict["qualified_key"] = convention_ref

                            # Preserve entry-level overrides (condition)
                            if "condition" in entry:
                                child_dict["condition"] = entry["condition"]

                            context = build_param_context(
                                runtime_params, child_dict.get("defaults")
                            )
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
                            visited.discard(convention_ref)
                    else:
                        # Truly inline — pass through as-is
                        inline = entry.copy()
                        inline["qualified_key"] = entry.get("name", "")
                        resolved_weaves.append(inline)

        result["_resolved_weaves"] = resolved_weaves

    # Resolve threads in weave
    elif config_type == "weave" and "threads" in config:
        resolved_threads = []

        # Validate: duplicate refs without aliases
        from collections import Counter

        ref_entries = [e for e in config["threads"] if isinstance(e, dict) and e.get("ref")]
        ref_counts = Counter(e["ref"] for e in ref_entries)
        for ref_val, count in ref_counts.items():
            if count > 1:
                aliases = [e.get("as") for e in ref_entries if e["ref"] == ref_val]
                if not all(aliases):
                    raise ConfigError(
                        f"Thread ref '{ref_val}' appears multiple times without "
                        f"'as' aliases. Use 'as' to give each instance a unique name."
                    )

        seen_effective_names: set[str] = set()

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

                    # Two-phase param resolution: resolve expressions
                    # in entry param values against parent context first,
                    # then inject resolved values into thread's param context.
                    raw_entry_params = entry.get("params")
                    resolved_entry_params = None
                    if raw_entry_params:
                        parent_context = build_param_context(runtime_params, config.get("defaults"))
                        resolved_entry_params = resolve_variables(raw_entry_params, parent_context)

                    context = build_param_context(
                        runtime_params,
                        child_dict.get("defaults"),
                        entry_params=resolved_entry_params,
                    )
                    resolved_child = resolve_variables(child_dict, context)

                    # Preserve entry-level overrides
                    for key in ("dependencies", "condition", "as", "params"):
                        if key in entry:
                            resolved_child[f"_entry_{key}"] = entry[key]

                    # Track effective name uniqueness
                    effective_name = (
                        entry.get("as") or resolved_child.get("name") or thread_path.stem
                    )
                    if effective_name in seen_effective_names:
                        raise ConfigError(
                            f"Duplicate effective thread name '{effective_name}' "
                            f"in weave. Use 'as' to give each instance a unique name."
                        )
                    seen_effective_names.add(effective_name)

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
                    if "as" in entry:
                        inline["_entry_as"] = entry["as"]

                    # Track effective name uniqueness
                    effective_name = entry.get("as") or entry.get("name", "")
                    if effective_name in seen_effective_names:
                        raise ConfigError(
                            f"Duplicate effective thread name '{effective_name}' "
                            f"in weave. Use 'as' to give each instance a unique name."
                        )
                    seen_effective_names.add(effective_name)

                    resolved_threads.append(inline)

        result["_resolved_threads"] = resolved_threads

    return result
