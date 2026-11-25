"""The diagnostics a ``${...}`` reference fails or is deferred with.

:mod:`weevr.config.interpolation` resolves references; the errors it raises
and the ``P113`` warning for a deferred ``${fabric.x}`` are built here: what
each names in ``expected``, the hint that says how to fix it, and the nearest
name in scope when one is close enough to suggest.
"""

import difflib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import PurePath
from typing import Any

from weevr.config.audit_templates import HIERARCHY_CONTEXT_KEYS
from weevr.config.param_provenance import ParamDeclaration
from weevr.config.params import declaration_block
from weevr.diagnostics import Diagnostic, Severity, diagnostic
from weevr.diagnostics.positions import json_pointer
from weevr.errors import VariableResolutionError

NAMESPACES: tuple[str, ...] = ("param", "fabric", "var", "run", "thread", "weave", "loom")
"""Every ``${namespace.name}`` namespace: two resolved at load, five at execution."""

HIERARCHY_NAMESPACES: tuple[str, ...] = ("thread", "weave", "loom")
"""Hierarchy namespaces: only their known keys (``HIERARCHY_CONTEXT_KEYS``) exist."""

FABRIC_KEYS: frozenset[str] = frozenset({"workspace_id", "lakehouse_id", "workspace_name"})
"""The keys of the ``fabric`` namespace."""


def param_name(reference: str) -> str | None:
    """The param a ``${...}`` reference's name reads; ``None`` outside the ``param`` namespace.

    A dotted reference reads its first key: ``param.ws.x`` reads ``ws``.

    Args:
        reference: The reference's name, without ``${`` or any ``:-`` default.

    Returns:
        The param name, or ``None``.
    """
    namespace, _, rest = reference.strip().partition(".")
    if namespace != "param" or not rest:
        return None
    return rest.split(".", 1)[0]


@dataclass(frozen=True)
class ReferenceScope:
    """The scope a reference resolves in, as its errors describe it.

    A bare ``${x}`` is classified by it for its rewrite (``P111``), and an
    unresolved ``${param.x}`` names what it declares and where (``P110``).

    Attributes:
        declared: The params declared in the reference's scope chain, by
            name, each with where its effective spec is declared.
        defaults: The ``defaults:`` blocks in force where the reference sits,
            the enclosing configs' included, the nearest's keys winning.
        trail: The files from the load's root down to the referencing file.
        strict: Whether the scope is strict about params, which makes a
            deferred ``${fabric.x}`` an error.
    """

    declared: Mapping[str, ParamDeclaration] = field(default_factory=dict)
    defaults: Mapping[str, Any] = field(default_factory=dict)
    trail: tuple[str, ...] = ()
    strict: bool = False

    @property
    def file(self) -> str | None:
        """The referencing file: the last of :attr:`trail`; ``None`` when unknown."""
        return self.trail[-1] if self.trail else None

    def trail_from(self, file: str | None) -> tuple[str, ...]:
        """The trail from ``file`` to the referencing file; empty when ``file`` is off it."""
        if file in self.trail:
            return self.trail[self.trail.index(file) :]
        return ()

    def shown(self, file: str) -> str:
        """``file`` relative to the root file's folder when it sits under it, else as given."""
        if not self.trail:
            return file
        path, root = PurePath(file), PurePath(self.trail[0]).parent
        return str(path.relative_to(root)) if path.is_relative_to(root) else file


def pointer(at: tuple[str | int, ...]) -> str:
    """The JSON pointer of the authored value at ``at``."""
    from weevr.config.validation import authored_path

    return json_pointer(authored_path(at))


def with_fallback(reference: str, default_value: str | None) -> str:
    """``${reference}``, keeping a ``:-`` fallback when there is one."""
    fallback = "" if default_value is None else f":-{default_value}"
    return f"${{{reference}{fallback}}}"


# How close a name must be to one in scope to be suggested (difflib's ratio).
_SUGGESTION_CUTOFF = 0.8


def _nearest(name: str, candidates: Iterable[str]) -> str | None:
    """The candidate closest to ``name``, when one is close enough to suggest."""
    found = difflib.get_close_matches(name, sorted(candidates), n=1, cutoff=_SUGGESTION_CUTOFF)
    return found[0] if found else None


def _did_you_mean(suggestion: str | None) -> str:
    """The hint's opening suggestion; empty without one."""
    return "" if suggestion is None else f"Did you mean {suggestion}? "


def _error(
    code: str,
    message: str,
    var_name: str,
    at: tuple[str | int, ...],
    *,
    file: str | None = None,
    expected: list[str] | None = None,
    hint: str | None = None,
) -> VariableResolutionError:
    """A resolution error for ``${var_name}`` with its one diagnostic, at the string at ``at``."""
    return VariableResolutionError(
        message,
        config_key=var_name,
        diagnostics=[
            diagnostic(code, message, file=file, pointer=pointer(at), expected=expected, hint=hint)
        ],
    )


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


def unresolved(
    var_name: str, context: dict[str, Any], scope: ReferenceScope, at: tuple[str | int, ...]
) -> VariableResolutionError:
    """The ``P110`` error for a reference with no value, at the string that holds it.

    A ``${param.x}`` error lists the names its scope binds and declares in
    ``expected``, and its hint says how to bind ``x``: with a ``params:``
    block to paste and a runtime value when nothing declares it, or with the
    trail of files from its declaration down to the reference when something
    does (none when the declaring file is unknown or off the walk's trail). A
    name in scope close to ``x`` is suggested first.

    Args:
        var_name: The reference's name.
        context: The load-time namespaces it was looked up in.
        scope: The scope it resolves in.
        at: Where the string holding it sits.

    Returns:
        The error.
    """
    message = _unresolved_variable_message(var_name, context)
    name = param_name(var_name)
    if name is None:
        return _error("P110", message, var_name, at, file=scope.file)
    params = context.get("param")
    bound = set(params) if isinstance(params, dict) else set()
    in_scope = bound | set(scope.declared)
    expected = [f"${{param.{n}}}" for n in sorted(in_scope)] or None
    hint = None
    if name not in bound:
        close = _nearest(name, in_scope - {name})
        suggestion = _did_you_mean(None if close is None else f"${{param.{close}}}")
        if name in scope.declared:
            trail = " -> ".join(scope.shown(f) for f in scope.trail_from(scope.declared[name].file))
            where = f" (scope: {trail})" if trail else ""
            hint = (
                f"{suggestion}'{name}' is declared with no default and nothing supplies it"
                f"{where}: supply a value at run time or give it a default."
            )
        else:
            hint = (
                f"{suggestion}Declare it:\n{declaration_block({name: None})}\n"
                f'or supply it at run time: Context(spark, project, params={{"{name}": ...}}).'
            )
    return _error("P110", message, var_name, at, file=scope.file, expected=expected, hint=hint)


def bare(
    var_name: str,
    default_value: str | None,
    context: dict[str, Any],
    scope: ReferenceScope,
    at: tuple[str | int, ...],
) -> VariableResolutionError:
    """The error for a reference whose first segment is not a namespace.

    The first segment is classified, first match wins: a declared param, a
    ``defaults:`` key, a Fabric key, else a param. A dotted reference whose
    first segment is none of a declared, supplied or ``defaults:`` name is an
    unknown namespace instead (``P112``).

    Args:
        var_name: The reference's name.
        default_value: Its ``:-`` fallback, if any.
        context: The load-time namespaces.
        scope: The scope it resolves in.
        at: Where the string holding it sits.

    Returns:
        The ``P111`` (or ``P112``) error.
    """
    head, dotted, _ = var_name.partition(".")
    params = context.get("param")
    supplied = isinstance(params, dict) and head in params
    original = with_fallback(var_name, default_value)
    hint_extra = ""
    if head in scope.declared or supplied:
        namespace = "param"
    elif head in scope.defaults:
        namespace = "param"
        hint_extra = (
            f" '{head}' is a defaults: key, and defaults are not variables: declare it as a "
            f"param with default {scope.defaults[head]!r}."
        )
    elif dotted:
        return _unknown_namespace(var_name, original, at)
    elif head in FABRIC_KEYS:
        namespace = "fabric"
    else:
        namespace = "param"
    rewrite = with_fallback(f"{namespace}.{var_name}", default_value)
    return _error(
        "P111",
        f"Reference '{original}' names no namespace; write '{rewrite}'",
        var_name,
        at,
        file=scope.file,
        expected=[rewrite],
        hint=f"Write {rewrite}.{hint_extra}",
    )


def _unknown_namespace(
    var_name: str, original: str, at: tuple[str | int, ...]
) -> VariableResolutionError:
    """The error for a reference in a namespace that does not exist.

    The nearest namespace, when one is close, is suggested in the hint as the
    rewritten reference (``${params.x}`` suggests ``${param.x}``).
    """
    namespace = var_name.partition(".")[0]
    close = _nearest(namespace, NAMESPACES)
    rewrite = None if close is None else "${" + close + original[2 + len(namespace) :]
    return _error(
        "P112",
        f"Reference '{original}' uses unknown namespace '{namespace}'",
        var_name,
        at,
        expected=list(NAMESPACES),
        hint=_did_you_mean(rewrite)
        + "Start the reference with one of: "
        + ", ".join(NAMESPACES)
        + ".",
    )


def unknown_key(
    var_name: str, default_value: str | None, at: tuple[str | int, ...]
) -> VariableResolutionError:
    """The ``P112`` error for an unknown key of a namespace with fixed keys.

    For the hierarchy namespaces (``${thread.nme}``) and ``fabric``
    (``${fabric.workspce_id}``). ``expected`` lists the namespace's known
    keys, the nearest first when one is close.

    Args:
        var_name: The reference's name.
        default_value: Its ``:-`` fallback, if any.
        at: Where the string holding it sits.

    Returns:
        The error.
    """
    namespace = var_name.partition(".")[0]
    if namespace == "fabric":
        keys = [f"fabric.{k}" for k in FABRIC_KEYS]
    else:
        keys = [k for k in HIERARCHY_CONTEXT_KEYS if k.startswith(f"{namespace}.")]
    close = _nearest(var_name, keys)
    suggestion = None if close is None else f"${{{close}}}"
    known = _nearest_first([f"${{{k}}}" for k in keys], suggestion)
    return _error(
        "P112",
        f"Reference '{with_fallback(var_name, default_value)}' names no {namespace} variable",
        var_name,
        at,
        expected=known,
        hint=_did_you_mean(suggestion) + "Use one of: " + ", ".join(known) + ".",
    )


def _nearest_first(known: Iterable[str], nearest: str | None) -> list[str]:
    """``known`` sorted, with ``nearest`` moved to the front when there is one."""
    ordered = sorted(known)
    if nearest is None:
        return ordered
    return [nearest, *(k for k in ordered if k != nearest)]


def unknown_fabric_context_key(key: str) -> VariableResolutionError:
    """The ``P112`` error for a ``fabric_context`` key that names no Fabric value.

    Raised for the loader's argument, not for a reference, so it names no
    position. ``expected`` lists the valid keys, the nearest first when one
    is close.

    Args:
        key: The supplied key, as given.

    Returns:
        The error.
    """
    close = _nearest(key.removeprefix("fabric."), FABRIC_KEYS)
    known = _nearest_first(FABRIC_KEYS, close)
    message = f"fabric_context key '{key}' names no Fabric value"
    return VariableResolutionError(
        message,
        config_key=key,
        diagnostics=[
            diagnostic(
                "P112",
                message,
                expected=known,
                hint=_did_you_mean(close) + "Use one of: " + ", ".join(known) + ".",
            )
        ],
    )


def namespace_without_key(
    var_name: str, default_value: str | None, at: tuple[str | int, ...]
) -> VariableResolutionError:
    """The error for a reference that is only a namespace name (``${param}``).

    Args:
        var_name: The reference's name: a namespace.
        default_value: Its ``:-`` fallback, if any.
        at: Where the string holding it sits.

    Returns:
        The ``P112`` error.
    """
    if var_name in HIERARCHY_NAMESPACES:
        return unknown_key(var_name, default_value, at)
    form = f"${{{var_name}.<name>}}"
    return _error(
        "P112",
        f"Reference '{with_fallback(var_name, default_value)}' names the {var_name} namespace "
        "but no key in it",
        var_name,
        at,
        expected=[form],
        hint=f"Write {form}.",
    )


def deferred_fabric(var_name: str, scope: ReferenceScope, at: tuple[str | int, ...]) -> Diagnostic:
    """The ``P113`` diagnostic for a ``${fabric.x}`` left in place: the load had no Fabric context.

    A warning, or an error where the referencing scope is strict.

    Args:
        var_name: The reference's name.
        scope: The scope it sits in.
        at: Where the string holding it sits.

    Returns:
        The diagnostic.
    """
    return diagnostic(
        "P113",
        f"Reference '${{{var_name}}}' is left unresolved: the load has no Fabric context",
        severity=Severity.ERROR if scope.strict else None,
        file=scope.file,
        pointer=pointer(at),
        expected=["fabric_context"],
        hint=(
            "Pass the Fabric values to the loader, e.g. "
            f'load_resolved(path, fabric_context={{"{var_name.partition(".")[2]}": ...}}), '
            "or run under Context, which reads them from the Spark session."
        ),
    )
