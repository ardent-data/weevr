"""Variable interpolation: layered ``${...}`` contexts and their resolution.

:func:`build_param_context` layers the sources a variable can read from, and
:func:`resolve_variables` substitutes ``${...}`` references against that context.
"""

import re
from typing import Any

from weevr.config.audit_templates import HIERARCHY_CONTEXT_KEYS
from weevr.diagnostics import diagnostic
from weevr.diagnostics.positions import json_pointer
from weevr.errors import VariableResolutionError


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


def _unresolved_variable_message(var_name: str, context: dict[str, Any]) -> str:
    """Compose an unresolved-variable error message with sibling-name hints.

    For ``${param.x}`` references, list the declared param names currently in
    scope so a typo is obvious. For all other references, fall back to the
    plain message.
    """
    base = f"Unresolved variable '${{{var_name}}}' with no default value"
    if var_name.startswith("param."):
        param_scope = context.get("param")
        if isinstance(param_scope, dict) and param_scope:
            available = ", ".join(sorted(param_scope.keys()))
            return f"{base}. Available declared params: [{available}]"
        return f"{base}. No declared params are in scope at this resolution site"
    return base


# Regex pattern for ${var} and ${var:-default}
VARIABLE_PATTERN = re.compile(r"\$\{([^}:]+?)(?::-(.*?))?\}")

# Matches a string that is entirely a single ${var} or ${var:-default} reference
WHOLE_VALUE_PATTERN = re.compile(r"^\$\{([^}:]+?)(?::-(.*?))?\}$")


def _resolved_at_execution(var_name: str) -> bool:
    """Whether an unresolved ``${var_name}`` is left in place for execution time.

    True for the hierarchy variables the execution-time resolver supplies
    (``HIERARCHY_CONTEXT_KEYS``). Callers apply it only after the load-context
    lookup and any ``:-default`` have missed, so configs that resolve at load
    are unaffected.
    """
    return var_name in HIERARCHY_CONTEXT_KEYS


def _unresolved(
    var_name: str, context: dict[str, Any], at: tuple[str | int, ...]
) -> VariableResolutionError:
    """The error for a ``${var}`` with no value, pointing at the string that holds it."""
    from weevr.config.validation import authored_path

    message = _unresolved_variable_message(var_name, context)
    return VariableResolutionError(
        message,
        config_key=var_name,
        diagnostics=[diagnostic("P110", message, pointer=json_pointer(authored_path(at)))],
    )


def resolve_variables(
    config: dict[str, Any] | list[Any] | str | Any,
    context: dict[str, Any],
    consumed_keys: set[str] | None = None,
    at: tuple[str | int, ...] = (),
) -> Any:
    """Recursively resolve variable references in config.

    Supports:
    - ${var} - simple variable reference (error if not found)
    - ${var:-default} - variable with fallback default

    Left in place for execution time: ``${var.*}`` and ``${run.*}``, and the known
    hierarchy variables (``${thread.name}``, ``${weave.name}``, ``${loom.name}``, and
    the rest of ``HIERARCHY_CONTEXT_KEYS``) when the context doesn't supply them and no
    default is given.

    Args:
        config: Config structure to resolve (dict, list, str, or primitive)
        context: Parameter context for variable lookup
        consumed_keys: Optional set to track which context keys were consumed
            during resolution. When provided, each resolved dotted key is
            added to the set for post-resolution unused-param analysis.
        at: Where ``config`` sits in its file, so an unresolved variable can
            point at the value that holds it.

    Returns:
        Config with all variables resolved

    Raises:
        VariableResolutionError: If variable not found and no default provided
    """
    if isinstance(config, dict):
        return {
            k: resolve_variables(v, context, consumed_keys, (*at, k)) for k, v in config.items()
        }

    elif isinstance(config, list):
        return [
            resolve_variables(item, context, consumed_keys, (*at, i))
            for i, item in enumerate(config)
        ]

    elif isinstance(config, str):
        # Whole-value check: if the entire string is a single ${param} reference,
        # return the native Python type rather than stringifying it.
        whole_match = WHOLE_VALUE_PATTERN.match(config.strip())
        if whole_match:
            var_name = whole_match.group(1).strip()
            default_value = whole_match.group(2)

            if not var_name.startswith(("var.", "run.")):
                try:
                    value = _get_dotted_value(context, var_name)
                    if consumed_keys is not None:
                        consumed_keys.add(var_name)
                    return value
                except KeyError as exc:
                    if default_value is not None:
                        return default_value
                    if _resolved_at_execution(var_name):
                        return config
                    raise _unresolved(var_name, context, at) from exc
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
                if consumed_keys is not None:
                    consumed_keys.add(var_name)
                return str(value)
            except KeyError as exc:
                if default_value is not None:
                    return default_value
                if _resolved_at_execution(var_name):
                    return match.group(0)
                raise _unresolved(var_name, context, at) from exc

        return VARIABLE_PATTERN.sub(replace_var, config)

    else:
        # Primitives (int, bool, None, etc.) pass through unchanged
        return config
