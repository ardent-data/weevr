"""Variable interpolation: namespaced ``${...}`` contexts and their resolution.

Every reference names its namespace: ``${param.x}`` and ``${fabric.x}`` resolve
at load, while ``${var.x}``, ``${run.x}`` and the hierarchy variables
(``${thread.name}``, ``${weave.name}``, ``${loom.name}``, ...) are left in place
for execution. Each namespace resolves on its own; nothing falls back from one
to another. A bare ``${x}`` fails ``P111`` with the reference rewritten into its
namespace, and an unknown namespace fails ``P112``. A load with no Fabric
context leaves ``${fabric.x}`` in place, warning ``P113``.

:func:`load_context` assembles the load-time namespaces, and
:func:`resolve_variables` substitutes ``${...}`` references against them; the
diagnostics a reference fails with are built in
:mod:`weevr.config.interpolation_errors`.
"""

import re
from collections.abc import Mapping
from typing import Any

from weevr.config.audit_templates import HIERARCHY_CONTEXT_KEYS
from weevr.config.interpolation_errors import (
    FABRIC_KEYS,
    HIERARCHY_NAMESPACES,
    NAMESPACES,
    ReferenceScope,
    bare,
    deferred_fabric,
    namespace_without_key,
    pointer,
    unknown_fabric_context_key,
    unknown_key,
    unresolved,
)
from weevr.diagnostics import Diagnostic

__all__ = [
    "VARIABLE_PATTERN",
    "WHOLE_VALUE_PATTERN",
    "check_fabric_context",
    "fabric_namespace",
    "load_context",
    "resolve_variables",
]

# Namespaces left in place whole, for the execution-time resolver.
_DEFERRED_NAMESPACES = ("var", "run")


def check_fabric_context(fabric_context: Mapping[str, Any] | None) -> None:
    """Reject a ``fabric_context`` key that names no Fabric value.

    Each key, after an optional ``fabric.`` prefix, must be one of
    ``workspace_id``, ``lakehouse_id`` and ``workspace_name``, whatever the
    config references.

    Args:
        fabric_context: The load's Fabric values; ``None`` when it has none.

    Raises:
        VariableResolutionError: ``P112`` for the first unknown key.
    """
    for key in fabric_context or {}:
        if key.removeprefix("fabric.") not in FABRIC_KEYS:
            raise unknown_fabric_context_key(key)


def fabric_namespace(fabric_context: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """The ``fabric`` namespace a ``${fabric.x}`` reference reads.

    Args:
        fabric_context: The load's Fabric values, keyed ``workspace_id``,
            ``lakehouse_id`` and ``workspace_name`` (a ``fabric.`` prefix,
            as ``fabric.workspace_id``, is accepted). ``None`` means the load
            has no Fabric context. A mapping is the context even when empty;
            a key it lacks or maps to ``None`` has no value.

    Returns:
        The namespace's values; ``None`` when the load has no Fabric context.
    """
    if fabric_context is None:
        return None
    return {k.removeprefix("fabric."): v for k, v in fabric_context.items() if v is not None}


def load_context(
    params: Mapping[str, Any] | None,
    fabric: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The load-time namespaces a ``${...}`` reference reads.

    Args:
        params: The scope's bound params, the ``param`` namespace.
        fabric: The ``fabric`` namespace (:func:`fabric_namespace`); ``None``
            when the load has no Fabric context.

    Returns:
        ``{"param": ..., "fabric": ...}``: ``param`` present when it has
        values, ``fabric`` whenever the load has a Fabric context. Without
        ``fabric``, a ``${fabric.x}`` reference is left in place.
    """
    context: dict[str, Any] = {}
    if fabric is not None:
        context["fabric"] = dict(fabric)
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


# Regex pattern for ${name} and ${name:-default}
VARIABLE_PATTERN = re.compile(r"\$\{([^}:]+?)(?::-(.*?))?\}")

# Matches a string that is entirely a single ${name} or ${name:-default} reference. The
# default stops at the first ``}``, as in VARIABLE_PATTERN, so a string holding several
# references is never one whole value.
WHOLE_VALUE_PATTERN = re.compile(r"^\$\{([^}:]+?)(?::-([^}]*))?\}$")


# Returned by _lookup for a reference left in place for execution.
_KEEP = object()


def _lookup(
    var_name: str,
    default_value: str | None,
    context: dict[str, Any],
    scope: ReferenceScope,
    consumed_keys: set[str] | None,
    at: tuple[str | int, ...],
    read_at: dict[str, str] | None = None,
    deferred: list[Diagnostic] | None = None,
) -> Any:
    """The value of one reference, or ``_KEEP`` to leave it in place."""
    namespace, dotted, key = var_name.partition(".")
    if not dotted and namespace in NAMESPACES:
        raise namespace_without_key(var_name, default_value, at)
    if not dotted or namespace not in NAMESPACES:
        raise bare(var_name, default_value, context, scope, at)
    if namespace in _DEFERRED_NAMESPACES:
        return _KEEP
    if namespace in HIERARCHY_NAMESPACES:
        if var_name not in HIERARCHY_CONTEXT_KEYS:
            raise unknown_key(var_name, default_value, at)
        return _KEEP if default_value is None else default_value
    if namespace == "fabric":
        if key not in FABRIC_KEYS:
            raise unknown_key(var_name, default_value, at)
        if "fabric" not in context and default_value is None:
            if deferred is not None:
                deferred.append(deferred_fabric(var_name, scope, at))
            return _KEEP
    try:
        value = _get_dotted_value(context, var_name)
    except KeyError as exc:
        if default_value is not None:
            return default_value
        raise unresolved(var_name, context, scope, at) from exc
    if consumed_keys is not None:
        consumed_keys.add(var_name)
    if read_at is not None and var_name not in read_at:
        read_at[var_name] = pointer(at)
    return value


def resolve_variables(
    config: dict[str, Any] | list[Any] | str | Any,
    context: dict[str, Any],
    consumed_keys: set[str] | None = None,
    at: tuple[str | int, ...] = (),
    *,
    scope: ReferenceScope | None = None,
    read_at: dict[str, str] | None = None,
    deferred: list[Diagnostic] | None = None,
) -> Any:
    """Recursively resolve variable references in config.

    Supports ``${namespace.name}`` and ``${namespace.name:-default}``. The
    ``param`` and ``fabric`` namespaces resolve from ``context``, falling back
    to the default; ``param`` values can be traversed (``${param.env.lakehouse}``).
    Left in place for execution time: ``${var.*}`` and ``${run.*}``, and the known
    hierarchy variables (``HIERARCHY_CONTEXT_KEYS``) when no default is given.
    A ``${fabric.x}`` with no default is left in place too when ``context``
    has no ``fabric`` namespace (the load has no Fabric context), noted in
    ``deferred`` (``P113``).

    Args:
        config: Config structure to resolve (dict, list, str, or primitive)
        context: The load-time namespaces (:func:`load_context`).
        consumed_keys: Optional set to track which context keys were consumed
            during resolution. When provided, each resolved dotted key is
            added to the set for post-resolution unused-param analysis.
        at: Where ``config`` sits in its file, so an unresolved variable can
            point at the value that holds it.
        scope: The scope the references resolve in, which a bare
            reference's rewrite and an unresolved reference's hint describe.
        read_at: Optional mapping filled with each resolved dotted key and
            the JSON pointer of the first string that read it.
        deferred: Optional list filled with a ``P113`` diagnostic for each
            ``${fabric.x}`` left in place, an error where ``scope`` is strict.
            Without it the references are still left in place but their
            ``P113`` diagnostics are dropped, so a caller resolving for a
            load must pass a list and report what it collects.

    Returns:
        Config with all variables resolved

    Raises:
        VariableResolutionError: A ``param`` or ``fabric`` reference has no
            value and no default (``P110``), a reference names no namespace
            (``P111``), or its namespace, hierarchy key or fabric key is
            unknown (``P112``).
    """
    if scope is None:
        scope = ReferenceScope()
    if isinstance(config, dict):
        return {
            k: resolve_variables(
                v,
                context,
                consumed_keys,
                (*at, k),
                scope=scope,
                read_at=read_at,
                deferred=deferred,
            )
            for k, v in config.items()
        }

    elif isinstance(config, list):
        return [
            resolve_variables(
                item,
                context,
                consumed_keys,
                (*at, i),
                scope=scope,
                read_at=read_at,
                deferred=deferred,
            )
            for i, item in enumerate(config)
        ]

    elif isinstance(config, str):
        # Whole-value check: if the entire string is a single ${param.x} reference,
        # return the native Python type rather than stringifying it.
        whole_match = WHOLE_VALUE_PATTERN.match(config.strip())
        if whole_match:
            var_name = whole_match.group(1).strip()
            value = _lookup(
                var_name, whole_match.group(2), context, scope, consumed_keys, at, read_at, deferred
            )
            return config if value is _KEEP else value

        def replace_var(match: re.Match[str]) -> str:
            var_name = match.group(1).strip()
            value = _lookup(
                var_name, match.group(2), context, scope, consumed_keys, at, read_at, deferred
            )
            return match.group(0) if value is _KEEP else str(value)

        return VARIABLE_PATTERN.sub(replace_var, config)

    else:
        # Primitives (int, bool, None, etc.) pass through unchanged
        return config
