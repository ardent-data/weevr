"""Variable interpolation and reference resolution."""

import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

from weevr.config.audit_templates import HIERARCHY_CONTEXT_KEYS
from weevr.config.collect import skip_on_error
from weevr.config.locations import ConfigLocation
from weevr.config.parser import TYPED_EXTENSIONS
from weevr.diagnostics import Diagnostic, diagnostic
from weevr.diagnostics.positions import json_pointer
from weevr.errors import (
    ConfigError,
    ConfigSchemaError,
    ReferenceResolutionError,
    VariableResolutionError,
)

logger = logging.getLogger(__name__)


@dataclass
class ResolvedChild:
    """A child config pulled in by one entry of its weave or loom.

    Reference resolution returns these instead of attaching children to the
    parent's dict; the cascade, the pre-cascade checks and hydration walk them.

    Attributes:
        entry: The parent's entry for this child, as written (a bare string
            entry becomes ``{"name": ...}``).
        data: The child's resolved config dict. The inheritance cascade
            replaces it with the cascaded dict.
        source_path: The child's own file, or ``None`` for an inline child,
            which lives in its parent's file.
        authored: The child's config before variable substitution.
        children: A weave's threads (for a loom's weave children).
        at: Where an inline child sits in its parent's file
            (``("threads", 2)``); empty for a child in its own file.
    """

    entry: dict[str, Any]
    data: dict[str, Any]
    source_path: str | None = None
    authored: dict[str, Any] | None = None
    children: list["ResolvedChild"] = field(default_factory=list)
    at: tuple[str | int, ...] = ()


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


def _spec_field(spec: Any, name: str, default: Any = None) -> Any:
    """Read a field from either a ParamSpec instance or its model_dump dict."""
    if hasattr(spec, name):
        return getattr(spec, name)
    if isinstance(spec, dict):
        return spec.get(name, default)
    return default


def resolve_declared_params(
    param_specs: dict[str, Any] | None,
    runtime_params: dict[str, Any] | None,
    *,
    file_path: str | None = None,
) -> dict[str, Any]:
    """Resolve loom/weave-level declared ``params:`` to a flat ``{name: value}`` dict.

    Precedence per declared param:

    1. Value from ``runtime_params`` if supplied
    2. ``ParamSpec.default`` if set on the spec
    3. ``ConfigSchemaError`` if the param is required
    4. Omitted from the result if optional with no default

    The returned dict is intended to bind under the ``param.*`` namespace via
    :func:`build_param_context`'s ``entry_params`` argument, so that
    ``${param.x}`` expressions resolve to the layered value.

    Runtime keys not declared in ``param_specs`` are ignored — declared scope
    is the only contract honored at this layer.

    Args:
        param_specs: Mapping of declared params (``ParamSpec`` instances or
            their ``model_dump`` dicts). ``None`` or empty returns ``{}``.
        runtime_params: Caller-supplied values keyed by param name.
        file_path: Optional originating config file path for error messages.

    Returns:
        Resolved ``{name: value}`` dict ready for ``entry_params``.

    Raises:
        ConfigSchemaError: A required declared param has no runtime value and
            no spec default. The message includes the file path and param name.
    """
    if not param_specs:
        return {}

    for name, spec in param_specs.items():
        if not isinstance(spec, dict) and not hasattr(spec, "required"):
            message = (
                f"Parameter '{name}' must be a mapping with 'name' and 'type', "
                f"got {type(spec).__name__}"
            )
            raise ConfigSchemaError(
                message,
                file_path=file_path,
                config_key=name,
                diagnostics=[
                    diagnostic(
                        "C102",
                        message,
                        file=file_path,
                        pointer=json_pointer(("params", name)),
                        expected=["mapping"],
                    )
                ],
            )

    runtime = runtime_params or {}
    resolved: dict[str, Any] = {}
    for name, spec in param_specs.items():
        # Treat an explicit None runtime value as absent so that the declared
        # default and required-check still apply. A literal None almost always
        # signals an unbound notebook variable rather than an intentional null.
        if name in runtime and runtime[name] is not None:
            resolved[name] = runtime[name]
            continue

        default = _spec_field(spec, "default", default=None)
        if default is not None:
            resolved[name] = default
            continue

        required = _spec_field(spec, "required", default=True)
        if required:
            message = f"Required parameter '{name}' is missing (no runtime value and no default)"
            raise ConfigSchemaError(
                message,
                file_path=file_path,
                config_key=name,
                diagnostics=[_missing_parameter(name, message, file_path)],
            )
        # Optional with no default → omit
    return resolved


def _missing_parameter(name: str, message: str, file: str | None) -> Diagnostic:
    """The diagnostic for a required parameter with no value, at its declaration."""
    return diagnostic(
        "P101",
        message,
        file=file,
        pointer=json_pointer(("params", name)),
        hint=f"Pass a value for '{name}' at run time, or give it a default.",
    )


def _warn_unused_params(
    consumed: set[str],
    context: dict[str, Any],
    entry_params: dict[str, Any] | None = None,
    thread_name: str = "",
) -> None:
    """Emit warnings for param-namespace keys not consumed during resolution."""
    if not entry_params:
        return

    # Build the set of param keys that should have been consumed
    expected = {f"param.{k}" for k in entry_params}
    unused = expected - consumed
    for key in sorted(unused):
        param_name = key.split(".", 1)[1] if "." in key else key
        logger.warning(
            "Thread '%s': entry param '%s' was not consumed by the "
            "thread config (source: entry_params)",
            thread_name,
            param_name,
        )


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


def _is_inline_weave(entry: Any) -> bool:
    """Whether a loom weave entry carries an inline weave body."""
    return (
        isinstance(entry, dict) and "ref" not in entry and bool(set(entry) - {"name", "condition"})
    )


def resolve_config_variables(
    config: dict[str, Any],
    config_type: str,
    runtime_params: dict[str, Any] | None = None,
    fabric_context: dict[str, Any] | None = None,
    entry_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve variable references in a top-level config.

    Builds the parameter context from the config's own ``defaults`` and
    resolves every ``${...}`` reference. An inline weave in a loom is
    resolved with its own ``defaults`` layered over the loom's, so it sees
    the same variables it would as a ``.weave`` file plus the loom's.

    Args:
        config: Schema-validated config dict.
        config_type: The config kind (``"thread"``, ``"weave"``, ...).
        runtime_params: Runtime parameter overrides.
        fabric_context: Optional Fabric environment values.
        entry_params: Resolved declared params, exposed as ``${param.x}``.

    Returns:
        The config dict with variables resolved.

    Raises:
        VariableResolutionError: If a variable has no value and no default.
    """
    defaults = config.get("defaults")

    def resolve(
        target: Any, scope_defaults: dict[str, Any] | None, where: tuple[str | int, ...] = ()
    ) -> Any:
        context = build_param_context(
            runtime_params,
            scope_defaults,
            fabric_context=fabric_context,
            entry_params=entry_params,
        )
        return resolve_variables(target, context, at=where)

    weaves = config.get("weaves")
    if config_type != "loom" or not isinstance(weaves, list):
        return resolve(config, defaults)

    inline = {i: entry for i, entry in enumerate(weaves) if _is_inline_weave(entry)}
    if not inline:
        return resolve(config, defaults)

    shell = {**config, "weaves": [None if i in inline else e for i, e in enumerate(weaves)]}
    resolved = resolve(shell, defaults)
    for i, entry in inline.items():
        own = entry.get("defaults")
        weave_defaults = {**(defaults or {}), **(own if isinstance(own, dict) else {})}
        resolved["weaves"][i] = resolve(entry, weave_defaults, ("weaves", i))
    return resolved


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
                message = f"Required parameter '{param_name}' is missing"
                raise ConfigSchemaError(
                    message,
                    config_key=param_name,
                    diagnostics=[_missing_parameter(param_name, message, None)],
                )

        # Skip validation if param not present (and not required)
        if param_name not in context:
            continue

        value = context[param_name]

        # Type validation
        if param_type == "string":
            if not isinstance(value, str):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected string, got {type(value).__name__}",
                )

        elif param_type == "int":
            if not isinstance(value, int) or isinstance(value, bool):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected int, got {type(value).__name__}",
                )

        elif param_type == "float":
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected float, got {type(value).__name__}",
                )

        elif param_type == "bool":
            if not isinstance(value, bool):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected bool, got {type(value).__name__}",
                )

        elif param_type == "date":
            if isinstance(value, str):
                try:
                    datetime.strptime(value, "%Y-%m-%d")
                except ValueError as e:
                    raise _parameter_error(
                        "P103",
                        param_name,
                        f"Parameter '{param_name}' invalid date format, expected YYYY-MM-DD",
                        cause=e,
                    ) from e
            elif not isinstance(value, date):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected date or ISO date string",
                )

        elif param_type == "timestamp":
            if isinstance(value, str):
                try:
                    datetime.fromisoformat(value)
                except ValueError as e:
                    raise _parameter_error(
                        "P103",
                        param_name,
                        f"Parameter '{param_name}' invalid timestamp format, "
                        "expected ISO format (YYYY-MM-DDTHH:MM:SS)",
                        cause=e,
                    ) from e
            elif not isinstance(value, datetime):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected timestamp or ISO timestamp string",
                )

        elif param_type == "list[string]":
            if not isinstance(value, list):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected list, got {type(value).__name__}",
                )
            if not all(isinstance(item, str) for item in value):
                raise _parameter_error(
                    "P102",
                    param_name,
                    f"Parameter '{param_name}' expected list of strings",
                )


def _parameter_error(
    code: str, name: str, message: str, cause: Exception | None = None
) -> ConfigSchemaError:
    """A parameter value that has the wrong type or format, pointed at its declaration."""
    return ConfigSchemaError(
        message,
        cause=cause,
        config_key=name,
        diagnostics=[
            diagnostic(code, message, pointer=json_pointer(("params", name)), origin=__name__)
        ],
    )


def _reference_error(
    code: str,
    message: str,
    *,
    pointer: str | None = None,
    file_path: str | None = None,
    expected: list[str] | None = None,
    hint: str | None = None,
) -> ReferenceResolutionError:
    """A reference failure, located at the entry that holds it.

    ``file_path`` stays on the error as the file the reference points at; the
    diagnostic names no file, so the config being loaded is attached to it.
    """
    return ReferenceResolutionError(
        message,
        file_path=file_path,
        diagnostics=[
            diagnostic(
                code,
                message,
                pointer=pointer,
                expected=expected,
                hint=hint,
                origin=__name__,
            )
        ],
    )


def _entry_error(
    code: str, message: str, pointer: str | None = None, hint: str | None = None
) -> ConfigError:
    """A malformed weave or thread entry, located at the entry."""
    return ConfigError(
        message,
        diagnostics=[diagnostic(code, message, pointer=pointer, hint=hint, origin=__name__)],
    )


def _circular_reference(
    ref: str, visited: dict[str, None], pointer: str
) -> ReferenceResolutionError:
    """The error for a ref that leads back to a file already being loaded."""
    chain = list(visited)
    cycle = [*chain[chain.index(ref) :], ref] if ref in chain else [*chain, ref]
    return _reference_error(
        "C302",
        f"Circular reference detected: {' -> '.join([*chain, ref])}",
        pointer=pointer,
        file_path=ref,
        expected=cycle,
    )


def _check_entry_refs(entries: list[Any], at: tuple[str | int, ...] = ()) -> None:
    """Reject a child entry whose ``ref`` isn't a string, before any ref is used as a key.

    Raises:
        ReferenceResolutionError: Naming no file of its own; the enclosing
            config's file is attached by the caller.
    """
    for index, entry in enumerate(entries):
        if isinstance(entry, dict) and "ref" in entry and not isinstance(entry["ref"], str):
            ref = entry["ref"]
            raise _reference_error(
                "C304",
                f"Reference must be a path string, got {type(ref).__name__}: {ref!r}",
                pointer=json_pointer((*at, index, "ref")),
            )


def resolve_ref_path(
    ref: str, project_root: ConfigLocation | Path, pointer: str | None = None
) -> ConfigLocation:
    """Resolve a path-based reference to a :class:`ConfigLocation`.

    Args:
        ref: Path-based reference with typed extension, relative to project
            root (e.g., ``'dimensions/dim_customer.thread'``).
        project_root: The ``.weevr`` project directory. A bare
            :class:`pathlib.Path` is wrapped in a
            :class:`LocalConfigLocation` for backward compatibility.
        pointer: Where the ``ref`` sits in the referencing file, for the
            diagnostic of any failure.

    Returns:
        Resolved location for the referenced config file.

    Raises:
        ReferenceResolutionError: If the reference isn't a string, or the
            referenced file does not exist or resolves outside the project
            root.
        ConfigError: If the reference does not have a typed extension.
    """
    from weevr.config.locations import LocalConfigLocation

    # Child entries reach here as unvalidated dicts, so the YAML may hold
    # any type. No file is named: the enclosing config's file is attached.
    if not isinstance(ref, str):
        raise _reference_error(
            "C304",
            f"Reference must be a path string, got {type(ref).__name__}: {ref!r}",
            pointer=pointer,
        )

    if isinstance(project_root, Path):
        project_root = LocalConfigLocation(project_root)

    suffix = Path(ref).suffix.lower()
    if suffix not in TYPED_EXTENSIONS:
        message = f"Unsupported extension '{suffix}'. Expected .thread, .weave, or .loom"
        raise ConfigError(
            message,
            file_path=ref,
            diagnostics=[
                diagnostic(
                    "C007",
                    message,
                    pointer=pointer,
                    expected=sorted(TYPED_EXTENSIONS),
                    origin=__name__,
                )
            ],
        )

    try:
        resolved = project_root.join(ref)
    except ValueError as exc:
        raise _reference_error(
            "C303",
            f"Reference '{ref}' resolves outside project root",
            pointer=pointer,
            file_path=ref,
        ) from exc

    if not resolved.is_relative_to(project_root):
        raise _reference_error(
            "C303",
            f"Reference '{ref}' resolves outside project root",
            pointer=pointer,
            file_path=str(resolved),
        )
    if not resolved.exists():
        raise _reference_error(
            "C301",
            f"Referenced file not found: {ref}",
            pointer=pointer,
            file_path=str(resolved),
        )
    return resolved


def resolve_references(
    config: dict[str, Any],
    config_type: str,
    project_root: ConfigLocation | Path,
    runtime_params: dict[str, Any] | None = None,
    visited: dict[str, None] | None = None,
    authored: dict[str, Any] | None = None,
    at: tuple[str | int, ...] = (),
) -> list[ResolvedChild]:
    """Resolve references to other config files.

    Handles both external references (``ref`` key) and inline definitions
    (``name`` + body keys). Recursively loads referenced configs with
    circular reference detection. ``config`` itself is not modified.

    Args:
        config: Config dict to resolve references in.
        config_type: Type of this config (``'weave'`` or ``'loom'``).
        project_root: The ``.weevr`` project directory. A bare
            :class:`pathlib.Path` is wrapped in a
            :class:`LocalConfigLocation` for backward compatibility.
        runtime_params: Runtime parameters to pass to child configs.
        visited: Refs already being loaded, in load order (for cycle detection).
        authored: ``config`` before variable substitution, so inline
            children can keep their authored form.
        at: Where ``config`` sits in its file; empty unless it is an
            inline weave in a loom.

    Returns:
        The resolved children in entry order: a loom's weaves (each with its
        threads) or a weave's threads. Empty for other config types.

    Raises:
        ReferenceResolutionError: If referenced file not found or circular
            reference detected.
        ConfigError: If an inline definition is missing a ``name`` field.
    """
    from weevr.config.attribution import attribute_errors
    from weevr.config.locations import LocalConfigLocation
    from weevr.config.parser import (
        detect_config_type_from_extension,
        extract_config_version,
        parse_yaml,
        validate_config_version,
    )
    from weevr.config.validation import validate_inline_thread_types, validate_schema

    if isinstance(project_root, Path):
        project_root = LocalConfigLocation(project_root)

    if visited is None:
        visited = {}

    def authored_entry(key: str, index: int) -> dict[str, Any] | None:
        entries = (authored or {}).get(key)
        if isinstance(entries, list) and index < len(entries):
            entry = entries[index]
            return entry if isinstance(entry, dict) else None
        return None

    children: list[ResolvedChild] = []

    # Resolve weaves in loom
    if config_type == "loom" and "weaves" in config:
        _check_entry_refs(config["weaves"], (*at, "weaves"))
        for index, entry in enumerate(config["weaves"]):
            with skip_on_error():
                # Normalize: string entries become {"name": string}
                if isinstance(entry, str):
                    entry = {"name": entry}

                if isinstance(entry, dict) and "ref" in entry:
                    # External reference
                    ref = entry["ref"]
                    ref_pointer = json_pointer((*at, "weaves", index, "ref"))
                    if ref in visited:
                        raise _circular_reference(ref, visited, ref_pointer)

                    weave_path = resolve_ref_path(ref, project_root, ref_pointer)
                    visited[ref] = None
                    try:
                        with attribute_errors(weave_path):
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

                            context = build_param_context(
                                runtime_params, child_dict.get("defaults")
                            )
                            resolved_child = resolve_variables(child_dict, context)

                            grandchildren = resolve_references(
                                resolved_child,
                                child_type,
                                project_root,
                                runtime_params,
                                visited.copy(),
                                authored=child_dict,
                            )
                        children.append(
                            ResolvedChild(
                                entry=entry,
                                data=resolved_child,
                                source_path=str(weave_path),
                                authored=child_dict,
                                children=grandchildren,
                            )
                        )
                    finally:
                        visited.pop(ref, None)
                else:
                    # Name-only entry — resolve by convention to {name}.weave
                    if isinstance(entry, dict) and not entry.get("name"):
                        raise _entry_error(
                            "C305",
                            "Inline weave definition requires a 'name' field",
                            json_pointer((*at, "weaves", index)),
                        )
                    if isinstance(entry, dict):
                        weave_name = entry["name"]
                        convention_ref = f"{weave_name}.weave"
                        # Held to the same containment rule as a ref entry.
                        try:
                            convention_path = project_root.join(convention_ref)
                        except ValueError as exc:
                            raise _reference_error(
                                "C303",
                                f"Weave name '{weave_name}' resolves outside project root",
                                pointer=json_pointer((*at, "weaves", index, "name")),
                                file_path=convention_ref,
                            ) from exc
                        if not convention_path.is_relative_to(project_root):
                            raise _reference_error(
                                "C303",
                                f"Weave name '{weave_name}' resolves outside project root",
                                pointer=json_pointer((*at, "weaves", index, "name")),
                                file_path=str(convention_path),
                            )
                        if convention_path.exists():
                            if convention_ref in visited:
                                raise _circular_reference(
                                    convention_ref,
                                    visited,
                                    json_pointer((*at, "weaves", index, "name")),
                                )

                            # Load the weave file by convention
                            visited[convention_ref] = None
                            try:
                                with attribute_errors(convention_path):
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
                                    grandchildren = resolve_references(
                                        resolved_child,
                                        child_type,
                                        project_root,
                                        runtime_params,
                                        visited.copy(),
                                        authored=child_dict,
                                    )
                                children.append(
                                    ResolvedChild(
                                        entry=entry,
                                        data=resolved_child,
                                        source_path=str(convention_path),
                                        authored=child_dict,
                                        children=grandchildren,
                                    )
                                )
                            finally:
                                visited.pop(convention_ref, None)
                        elif _is_inline_weave(entry):
                            # Inline weave body. resolve_config_variables already
                            # resolved its variables with the weave's own defaults;
                            # validate and resolve its threads the way a weave
                            # file is.
                            validated = validate_schema(entry, "weave", at=(*at, "weaves", index))
                            inline = validated.model_dump(exclude_unset=True)
                            inline["qualified_key"] = weave_name
                            if "condition" in entry:
                                inline["condition"] = entry["condition"]
                            inline_authored = authored_entry("weaves", index)
                            inline_at = (*at, "weaves", index)
                            children.append(
                                ResolvedChild(
                                    entry=entry,
                                    data=inline,
                                    authored=inline_authored,
                                    children=resolve_references(
                                        inline,
                                        "weave",
                                        project_root,
                                        runtime_params,
                                        visited.copy(),
                                        authored=inline_authored,
                                        at=inline_at,
                                    ),
                                    at=inline_at,
                                )
                            )
                        else:
                            raise _reference_error(
                                "C301",
                                f"Weave '{weave_name}' has no definition: expected "
                                f"'{convention_ref}' in the project root or an inline "
                                f"weave body (threads) in the loom",
                                pointer=json_pointer((*at, "weaves", index)),
                                hint=f"Add {convention_ref} or give the weave an inline body.",
                            )

    # Resolve threads in weave
    elif config_type == "weave" and "threads" in config:
        # Validate: duplicate refs without aliases
        _check_entry_refs(config["threads"], (*at, "threads"))
        ref_entries = [e for e in config["threads"] if isinstance(e, dict) and e.get("ref")]
        ref_counts = Counter(e["ref"] for e in ref_entries)
        for ref_val, count in ref_counts.items():
            if count > 1:
                aliases = [e.get("as") for e in ref_entries if e["ref"] == ref_val]
                if not all(aliases):
                    first = next(
                        i
                        for i, e in enumerate(config["threads"])
                        if isinstance(e, dict) and e.get("ref") == ref_val and not e.get("as")
                    )
                    raise _entry_error(
                        "C307",
                        f"Thread ref '{ref_val}' appears multiple times without "
                        f"'as' aliases. Use 'as' to give each instance a unique name.",
                        json_pointer((*at, "threads", first)),
                        hint="Add an 'as' alias to each instance.",
                    )

        seen_effective_names: set[str] = set()

        for index, entry in enumerate(config["threads"]):
            with skip_on_error():
                # Normalize: string entries become {"name": string}
                if isinstance(entry, str):
                    entry = {"name": entry}

                if isinstance(entry, dict) and "ref" in entry:
                    # External reference
                    ref = entry["ref"]
                    ref_pointer = json_pointer((*at, "threads", index, "ref"))
                    if ref in visited:
                        raise _circular_reference(ref, visited, ref_pointer)

                    thread_path = resolve_ref_path(ref, project_root, ref_pointer)
                    visited[ref] = None
                    try:
                        # Two-phase param resolution: resolve expressions
                        # in entry param values against parent context first,
                        # then inject resolved values into thread's param context.
                        # Entry params belong to this weave, so they resolve
                        # outside the referenced thread's attribution scope.
                        raw_entry_params = entry.get("params")
                        resolved_entry_params = None
                        if raw_entry_params:
                            parent_context = build_param_context(
                                runtime_params, config.get("defaults")
                            )
                            resolved_entry_params = resolve_variables(
                                raw_entry_params,
                                parent_context,
                                at=(*at, "threads", index, "params"),
                            )

                        with attribute_errors(thread_path):
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

                            context = build_param_context(
                                runtime_params,
                                child_dict.get("defaults"),
                                entry_params=resolved_entry_params,
                            )
                            consumed: set[str] = set()
                            resolved_child = resolve_variables(
                                child_dict, context, consumed_keys=consumed
                            )

                        # Warn on unused params with source attribution
                        _warn_unused_params(
                            consumed,
                            context,
                            entry_params=resolved_entry_params,
                            thread_name=entry.get("as") or child_dict.get("name", ref),
                        )

                        # Track effective name uniqueness
                        effective_name = (
                            entry.get("as") or resolved_child.get("name") or thread_path.stem
                        )
                        if effective_name in seen_effective_names:
                            raise _entry_error(
                                "C306",
                                f"Duplicate effective thread name '{effective_name}' "
                                f"in weave. Use 'as' to give each instance a unique name.",
                                json_pointer((*at, "threads", index)),
                                hint="Use 'as' to give each instance a unique name.",
                            )
                        seen_effective_names.add(effective_name)

                        children.append(
                            ResolvedChild(
                                entry=entry,
                                data=resolved_child,
                                source_path=str(thread_path),
                                authored=child_dict,
                            )
                        )
                    finally:
                        visited.pop(ref, None)
                else:
                    # Inline definition — must have name
                    if isinstance(entry, dict) and not entry.get("name"):
                        raise _entry_error(
                            "C305",
                            "Inline thread definition requires a 'name' field",
                            json_pointer((*at, "threads", index)),
                        )
                    if isinstance(entry, dict):
                        # The cascade and load checks read the body before
                        # hydration, so its value types are checked here.
                        validate_inline_thread_types(entry, at=(*at, "threads", index))
                        inline = entry.copy()
                        inline["qualified_key"] = entry.get("name", "")

                        # Track effective name uniqueness
                        effective_name = entry.get("as") or entry.get("name", "")
                        if effective_name in seen_effective_names:
                            raise _entry_error(
                                "C306",
                                f"Duplicate effective thread name '{effective_name}' "
                                f"in weave. Use 'as' to give each instance a unique name.",
                                json_pointer((*at, "threads", index)),
                                hint="Use 'as' to give each instance a unique name.",
                            )
                        seen_effective_names.add(effective_name)

                        children.append(
                            ResolvedChild(
                                entry=entry,
                                data=inline,
                                authored=authored_entry("threads", index),
                                at=(*at, "threads", index),
                            )
                        )

    return children
