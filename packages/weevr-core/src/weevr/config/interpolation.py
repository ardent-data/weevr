"""Variable interpolation: namespaced ``${...}`` contexts and their resolution.

Every reference names its namespace: ``${param.x}`` and ``${fabric.x}`` resolve
at load, while ``${var.x}``, ``${run.x}`` and the hierarchy variables
(``${thread.name}``, ``${weave.name}``, ``${loom.name}``, ...) are left in place
for execution. Each namespace resolves on its own; nothing falls back from one
to another. A bare ``${x}`` fails ``P111`` with the reference rewritten into its
namespace, and an unknown namespace fails ``P112``.

:func:`load_context` assembles the load-time namespaces, and
:func:`resolve_variables` substitutes ``${...}`` references against them.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from weevr.config.audit_templates import HIERARCHY_CONTEXT_KEYS
from weevr.diagnostics import diagnostic
from weevr.diagnostics.positions import json_pointer
from weevr.errors import VariableResolutionError

NAMESPACES: tuple[str, ...] = ("param", "fabric", "var", "run", "thread", "weave", "loom")
"""Every ``${namespace.name}`` namespace: two resolved at load, five at execution."""

# Namespaces left in place whole, for the execution-time resolver.
_DEFERRED_NAMESPACES = ("var", "run")

# Hierarchy namespaces: only their known keys (``HIERARCHY_CONTEXT_KEYS``) exist.
_HIERARCHY_NAMESPACES = ("thread", "weave", "loom")

FABRIC_KEYS: frozenset[str] = frozenset({"workspace_id", "lakehouse_id", "workspace_name"})
"""The keys of the ``fabric`` namespace."""


@dataclass(frozen=True)
class BareNames:
    """What a bare ``${x}`` reference is classified by, to give its rewrite.

    Attributes:
        declared: Param names declared in the reference's scope chain.
        defaults: The ``defaults:`` block in force where the reference sits.
    """

    declared: frozenset[str] = frozenset()
    defaults: Mapping[str, Any] = field(default_factory=dict)


def fabric_namespace(
    runtime_params: Mapping[str, Any] | None = None,
    fabric_context: Mapping[str, Any] | None = None,
) -> Any:
    """The ``fabric`` namespace a ``${fabric.x}`` reference reads.

    Args:
        runtime_params: Runtime parameters. A ``fabric`` key among them stands
            in for the Fabric context in loads with no session, and wins.
        fabric_context: Fabric environment values keyed as ``fabric.<field>``
            (e.g. ``fabric.workspace_id``). None values are omitted.

    Returns:
        The namespace's values; ``None`` when there are none.
    """
    if runtime_params and "fabric" in runtime_params:
        return runtime_params["fabric"]
    fabric = {
        k.split(".", 1)[1]: v
        for k, v in (fabric_context or {}).items()
        if v is not None and k.startswith("fabric.")
    }
    return fabric or None


def load_context(
    params: Mapping[str, Any] | None,
    fabric: Any = None,
) -> dict[str, Any]:
    """The load-time namespaces a ``${...}`` reference reads.

    Args:
        params: The scope's bound params, the ``param`` namespace.
        fabric: The ``fabric`` namespace (:func:`fabric_namespace`).

    Returns:
        ``{"param": ..., "fabric": ...}``, each present when it has values.
    """
    context: dict[str, Any] = {}
    if fabric:
        context["fabric"] = fabric
    if params:
        context["param"] = dict(params)
    return context


def _get_dotted_value(context: dict[str, Any], key: str) -> Any:
    """Get value from context using dotted key notation.

    Args:
        context: Parameter context dictionary
        key: Dotted key path (e.g., 'param.env.lakehouse')

    Returns:
        Value at the dotted path

    Raises:
        KeyError: If the dotted path doesn't exist
    """
    value: Any = context
    for part in key.split("."):
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


# Regex pattern for ${name} and ${name:-default}
VARIABLE_PATTERN = re.compile(r"\$\{([^}:]+?)(?::-(.*?))?\}")

# Matches a string that is entirely a single ${name} or ${name:-default} reference. The
# default stops at the first ``}``, as in VARIABLE_PATTERN, so a string holding several
# references is never one whole value.
WHOLE_VALUE_PATTERN = re.compile(r"^\$\{([^}:]+?)(?::-([^}]*))?\}$")


def _pointer(at: tuple[str | int, ...]) -> str:
    from weevr.config.validation import authored_path

    return json_pointer(authored_path(at))


def _error(
    code: str,
    message: str,
    var_name: str,
    at: tuple[str | int, ...],
    *,
    expected: list[str] | None = None,
    hint: str | None = None,
) -> VariableResolutionError:
    """A resolution error for ``${var_name}`` with its one diagnostic, at the string at ``at``."""
    return VariableResolutionError(
        message,
        config_key=var_name,
        diagnostics=[diagnostic(code, message, pointer=_pointer(at), expected=expected, hint=hint)],
    )


def _unresolved(
    var_name: str, context: dict[str, Any], at: tuple[str | int, ...]
) -> VariableResolutionError:
    """The ``P110`` error for a reference with no value, at the string that holds it."""
    return _error("P110", _unresolved_variable_message(var_name, context), var_name, at)


def _with_fallback(reference: str, default_value: str | None) -> str:
    """``${reference}``, keeping a ``:-`` fallback when there is one."""
    fallback = "" if default_value is None else f":-{default_value}"
    return f"${{{reference}{fallback}}}"


def _bare(
    var_name: str,
    default_value: str | None,
    context: dict[str, Any],
    names: BareNames,
    at: tuple[str | int, ...],
) -> VariableResolutionError:
    """The error for a reference whose first segment is not a namespace.

    The first segment is classified, first match wins: a declared param, a
    ``defaults:`` key, a Fabric key, else a param. A dotted reference whose
    first segment is none of a declared, supplied or ``defaults:`` name is an
    unknown namespace instead (``P112``).
    """
    head, dotted, _ = var_name.partition(".")
    params = context.get("param")
    supplied = isinstance(params, dict) and head in params
    original = _with_fallback(var_name, default_value)
    hint_extra = ""
    if head in names.declared or supplied:
        namespace = "param"
    elif head in names.defaults:
        namespace = "param"
        hint_extra = (
            f" '{head}' is a defaults: key, and defaults are not variables: declare it as a "
            f"param with default {names.defaults[head]!r}."
        )
    elif dotted:
        return _unknown_namespace(var_name, original, at)
    elif head in FABRIC_KEYS:
        namespace = "fabric"
    else:
        namespace = "param"
    rewrite = _with_fallback(f"{namespace}.{var_name}", default_value)
    return _error(
        "P111",
        f"Reference '{original}' names no namespace; write '{rewrite}'",
        var_name,
        at,
        expected=[rewrite],
        hint=f"Write {rewrite}.{hint_extra}",
    )


def _unknown_namespace(
    var_name: str, original: str, at: tuple[str | int, ...]
) -> VariableResolutionError:
    """The error for a reference in a namespace that does not exist."""
    namespace = var_name.partition(".")[0]
    return _error(
        "P112",
        f"Reference '{original}' uses unknown namespace '{namespace}'",
        var_name,
        at,
        expected=list(NAMESPACES),
        hint="Start the reference with one of: " + ", ".join(NAMESPACES) + ".",
    )


def _unknown_hierarchy_key(
    var_name: str, original: str, at: tuple[str | int, ...]
) -> VariableResolutionError:
    """The error for an unknown key in a hierarchy namespace (``${thread.nme}``)."""
    namespace = var_name.partition(".")[0]
    known = sorted(f"${{{k}}}" for k in HIERARCHY_CONTEXT_KEYS if k.startswith(f"{namespace}."))
    return _error(
        "P112",
        f"Reference '{original}' names no {namespace} variable",
        var_name,
        at,
        expected=known,
        hint="Use one of: " + ", ".join(known) + ".",
    )


def _namespace_without_key(
    var_name: str, original: str, at: tuple[str | int, ...]
) -> VariableResolutionError:
    """The error for a reference that is only a namespace name (``${param}``)."""
    if var_name in _HIERARCHY_NAMESPACES:
        return _unknown_hierarchy_key(var_name, original, at)
    form = f"${{{var_name}.<name>}}"
    return _error(
        "P112",
        f"Reference '{original}' names the {var_name} namespace but no key in it",
        var_name,
        at,
        expected=[form],
        hint=f"Write {form}.",
    )


# Returned by _lookup for a reference left in place for execution.
_KEEP = object()


def _lookup(
    var_name: str,
    default_value: str | None,
    context: dict[str, Any],
    names: BareNames,
    consumed_keys: set[str] | None,
    at: tuple[str | int, ...],
) -> Any:
    """The value of one reference, or ``_KEEP`` to leave it in place."""
    namespace, dotted, _ = var_name.partition(".")
    if not dotted and namespace in NAMESPACES:
        raise _namespace_without_key(var_name, _with_fallback(var_name, default_value), at)
    if not dotted or namespace not in NAMESPACES:
        raise _bare(var_name, default_value, context, names, at)
    if namespace in _DEFERRED_NAMESPACES:
        return _KEEP
    if namespace in _HIERARCHY_NAMESPACES:
        if var_name not in HIERARCHY_CONTEXT_KEYS:
            raise _unknown_hierarchy_key(var_name, _with_fallback(var_name, default_value), at)
        return _KEEP if default_value is None else default_value
    try:
        value = _get_dotted_value(context, var_name)
    except KeyError as exc:
        if default_value is not None:
            return default_value
        raise _unresolved(var_name, context, at) from exc
    if consumed_keys is not None:
        consumed_keys.add(var_name)
    return value


def resolve_variables(
    config: dict[str, Any] | list[Any] | str | Any,
    context: dict[str, Any],
    consumed_keys: set[str] | None = None,
    at: tuple[str | int, ...] = (),
    *,
    names: BareNames | None = None,
) -> Any:
    """Recursively resolve variable references in config.

    Supports ``${namespace.name}`` and ``${namespace.name:-default}``. The
    ``param`` and ``fabric`` namespaces resolve from ``context``, falling back
    to the default; ``param`` values can be traversed (``${param.env.lakehouse}``).
    Left in place for execution time: ``${var.*}`` and ``${run.*}``, and the known
    hierarchy variables (``HIERARCHY_CONTEXT_KEYS``) when no default is given.

    Args:
        config: Config structure to resolve (dict, list, str, or primitive)
        context: The load-time namespaces (:func:`load_context`).
        consumed_keys: Optional set to track which context keys were consumed
            during resolution. When provided, each resolved dotted key is
            added to the set for post-resolution unused-param analysis.
        at: Where ``config`` sits in its file, so an unresolved variable can
            point at the value that holds it.
        names: What a bare reference is classified by for its rewrite.

    Returns:
        Config with all variables resolved

    Raises:
        VariableResolutionError: A ``param`` or ``fabric`` reference has no
            value and no default (``P110``), a reference names no namespace
            (``P111``), or its namespace or hierarchy key is unknown (``P112``).
    """
    if names is None:
        names = BareNames()
    if isinstance(config, dict):
        return {
            k: resolve_variables(v, context, consumed_keys, (*at, k), names=names)
            for k, v in config.items()
        }

    elif isinstance(config, list):
        return [
            resolve_variables(item, context, consumed_keys, (*at, i), names=names)
            for i, item in enumerate(config)
        ]

    elif isinstance(config, str):
        # Whole-value check: if the entire string is a single ${param.x} reference,
        # return the native Python type rather than stringifying it.
        whole_match = WHOLE_VALUE_PATTERN.match(config.strip())
        if whole_match:
            var_name = whole_match.group(1).strip()
            value = _lookup(var_name, whole_match.group(2), context, names, consumed_keys, at)
            return config if value is _KEEP else value

        def replace_var(match: re.Match[str]) -> str:
            var_name = match.group(1).strip()
            value = _lookup(var_name, match.group(2), context, names, consumed_keys, at)
            return match.group(0) if value is _KEEP else str(value)

        return VARIABLE_PATTERN.sub(replace_var, config)

    else:
        # Primitives (int, bool, None, etc.) pass through unchanged
        return config
