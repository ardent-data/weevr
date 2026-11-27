"""Config inheritance cascade logic."""

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from pydantic import TypeAdapter, ValidationError

from weevr.config.audit_templates import AuditTier, merge_tiered_audit_columns
from weevr.diagnostics import diagnostic
from weevr.diagnostics.positions import json_pointer
from weevr.errors import InheritanceError

logger = logging.getLogger(__name__)

_BOOL = TypeAdapter(bool)


def cascade_registry[V](
    outer: Mapping[str, V] | None,
    inner: Mapping[str, V] | None,
    resource_type: str,
    outer_level: str,
    inner_level: str,
    *,
    log_shadowing: bool = True,
) -> dict[str, V] | None:
    """Merge a named registry (lookups, column sets, variables) into an inner scope.

    The one cascade rule for registries: inner names win, outer names are
    inherited. Both absent gives ``None``; an absent or empty inner registry
    inherits the outer one. Values are kept, not copied, so an inherited
    entry is the same object at every scope.

    Args:
        outer: The enclosing scope's effective registry.
        inner: The registry the inner scope declares.
        resource_type: What the registry holds, for log messages.
        outer_level: The enclosing scope's level (``"loom"``, ``"weave"``).
        inner_level: The inner scope's level (``"weave"``, ``"thread"``).
        log_shadowing: Log each inner name that shadows an outer one at
            DEBUG.

    Returns:
        The inner scope's effective registry, or ``None`` if neither scope
        declares one.
    """
    if outer is None and inner is None:
        return None
    if not inner:
        return dict(outer) if outer else None
    if not outer:
        return dict(inner)
    if log_shadowing:
        for key in inner:
            if key in outer:
                logger.debug(
                    "Resource '%s' in %s shadows %s-level definition",
                    key,
                    inner_level,
                    outer_level,
                )
    return {**outer, **inner}


def cascade(parent: dict[str, Any], child: dict[str, Any]) -> dict[str, Any]:
    """Merge parent config into child with child values taking precedence.

    Inheritance rules:
    - Scalars (str, int, float, bool, None): child replaces parent
    - Lists: child replaces parent entirely (no merge)
    - Dicts: child replaces parent entirely (no deep merge)
    - Keys in parent not in child: inherited from parent
    - Keys in child not in parent: kept from child
    - Keys prefixed with '_' (internal metadata): preserved from child as-is

    Args:
        parent: Parent config dict
        child: Child config dict

    Returns:
        Merged config dict with child values taking precedence
    """
    result = parent.copy()

    for key, child_value in child.items():
        result[key] = child_value

    return result


@dataclass(frozen=True)
class CascadeTier:
    """What one enclosing config contributes to a thread's cascade.

    Attributes:
        level: The tier's kind (``"project"``, ``"loom"``, ``"weave"``), for
            error messages.
        defaults: Its ``defaults:`` block.
        audit_templates: Its top-level audit template definitions.
        connections: Its top-level connection definitions.
    """

    level: str
    defaults: dict[str, Any] | None = None
    audit_templates: dict[str, Any] | None = None
    connections: dict[str, Any] | None = None


def apply_tiers(tiers: Sequence[CascadeTier], thread_config: dict[str, Any]) -> dict[str, Any]:
    """Cascade the enclosing tiers into a thread's config, outermost first.

    ``defaults:`` keys replace whole-value, the nearest tier winning and the
    thread's own keys beating every tier, except for the keys merged by their
    own rule: ``naming`` (the nearest block, into the target), audit columns
    (additive, with the thread's inherit and exclude controls), ``exports``
    (by name) and the target policies (key by key). Connection and audit
    template definitions merge by name, the nearest winning.

    Args:
        tiers: The enclosing tiers, outermost first (project, loom, weave);
            a tier may declare nothing.
        thread_config: The thread's config.

    Returns:
        The cascaded config.

    Raises:
        InheritanceError: If a ``defaults`` block holds a key the cascade
            merges with the wrong shape.
    """
    for tier in tiers:
        check_defaults_shape(tier.defaults, tier.level)
    defaults = [tier.defaults for tier in tiers if tier.defaults]

    result: dict[str, Any] = {}
    for block in defaults:
        result = cascade(result, block)
    result = cascade(result, thread_config) if result else thread_config.copy()

    _cascade_naming(defaults, result)
    _cascade_audit(tiers, result)
    _cascade_exports(defaults, result)
    _cascade_connections([tier.connections for tier in tiers], result)
    _cascade_target_policies(defaults, result)
    return result


def check_defaults_shape(defaults: dict[str, Any] | None, scope: str) -> None:
    """Reject ``defaults`` keys the cascade merges but no schema has typed yet.

    ``defaults`` is free-form until the cascade copies it into each thread,
    so a wrong shape here would otherwise surface as a builtin error.

    Raises:
        InheritanceError: Naming the scope and the key.
    """
    if not defaults:
        return

    def fail(key: str, expected: str, value: Any, at: tuple[str | int, ...] = ()) -> None:
        message = f"{scope} defaults.{key} must be {expected}, got {type(value).__name__}"
        raise InheritanceError(
            message,
            config_key=f"defaults.{key}",
            diagnostics=[
                diagnostic(
                    "C102",
                    message,
                    pointer=json_pointer(("defaults", *at)),
                    expected=[expected.removeprefix("a ")],
                    origin=__name__,
                )
            ],
        )

    if "target" in defaults:
        target = defaults["target"]
        if not isinstance(target, dict):
            fail("target", "a mapping", target, ("target",))
        columns = target.get("audit_columns")
        if columns is not None and not isinstance(columns, dict):
            fail("target.audit_columns", "a mapping", columns, ("target", "audit_columns"))
    if "exports" in defaults:
        exports = defaults["exports"]
        if not isinstance(exports, list):
            fail("exports", "a list", exports, ("exports",))
        for index, export in enumerate(exports):
            if not isinstance(export, dict):
                fail("exports[]", "a mapping", export, ("exports", index))


def _cascade_naming(defaults: Sequence[dict[str, Any]], thread_config: dict[str, Any]) -> None:
    """Cascade the nearest tier's ``defaults.naming`` into the thread's target.

    The most specific block wins entirely (no field-by-field merge), and a
    thread whose target sets ``naming`` keeps its own.
    """
    parent_naming = None
    for block in defaults:
        if "naming" in block:
            parent_naming = block["naming"]

    if parent_naming is None:
        return

    target = thread_config.get("target")
    if isinstance(target, dict) and "naming" not in target:
        target["naming"] = parent_naming


def _extract_audit_templates(
    raw: dict[str, Any] | None,
) -> dict[str, dict[str, str]] | None:
    """Extract and flatten user-defined audit templates into name→columns mapping.

    The raw audit_templates structure at each level is expected to be a dict of
    ``{name: {"columns": {col: expr, ...}}}`` or already ``{name: {col: expr}}``.
    Both forms are supported.

    Args:
        raw: Raw audit_templates dict from a config level, or None.

    Returns:
        Flattened ``{name: {col: expr}}`` dict, or None if input is empty/None.
    """
    if not raw:
        return None
    result: dict[str, dict[str, str]] = {}
    for name, value in raw.items():
        if isinstance(value, dict) and "columns" in value:
            result[name] = value["columns"]
        elif isinstance(value, dict):
            result[name] = value
    return result or None


def _to_template_refs(val: Any) -> list[str] | None:
    """Normalize a raw audit_template value to a list of names.

    Handles both the string sugar form (``"fabric"``) and the list form
    (``["fabric", "custom"]``).  Returns ``None`` for falsy input.
    """
    if not val:
        return None
    return val if isinstance(val, list) else [val]


def _tier_inline_columns(defaults: dict[str, Any] | None) -> dict[str, str]:
    """A tier's inline audit columns: ``defaults.target.audit_columns``."""
    if not defaults:
        return {}
    return dict(defaults.get("target", {}).get("audit_columns") or {})


def _cascade_audit(tiers: Sequence[CascadeTier], thread_config: dict[str, Any]) -> None:
    """Cascade audit columns with template resolution, inheritance, and exclusion.

    Reads ``defaults.target.audit_columns`` and ``defaults.target.audit_template``
    at every tier.

    Args:
        tiers: The enclosing tiers, outermost first.
        thread_config: Fully cascaded thread config (mutated in place).
    """
    outer = [
        AuditTier(
            _extract_audit_templates(tier.audit_templates),
            _to_template_refs((tier.defaults or {}).get("target", {}).get("audit_template")),
            _tier_inline_columns(tier.defaults) or None,
        )
        for tier in tiers
    ]
    raw_target = thread_config.get("target")
    target: dict[str, Any] = raw_target if isinstance(raw_target, dict) else {}
    thread = AuditTier(
        _extract_audit_templates(thread_config.get("audit_templates")),
        _to_template_refs(target.get("audit_template")),
        target.get("audit_columns"),
    )

    # Skip entirely if nothing is defined at any level
    if not any(tier.templates or tier.template_refs or tier.inline for tier in (*outer, thread)):
        return

    merged = merge_tiered_audit_columns(
        outer,
        thread,
        thread_inherit=target.get("audit_template_inherit", True),
        thread_exclude=target.get("audit_columns_exclude"),
    )

    # Write result into thread's target block
    if not isinstance(thread_config.get("target"), dict):
        thread_config["target"] = {}

    if merged:
        thread_config["target"]["audit_columns"] = merged
    else:
        thread_config["target"].pop("audit_columns", None)

    # Clean up intermediate keys from the cascaded thread_config
    thread_config.pop("audit_templates", None)

    target = thread_config["target"]
    target.pop("audit_template", None)
    target.pop("audit_template_inherit", None)
    target.pop("audit_columns_exclude", None)


_TARGET_POLICY_KEYS = ("schema_drift", "on_drift", "warp_enforcement")


def _cascade_target_policies(
    defaults: Sequence[dict[str, Any]], thread_config: dict[str, Any]
) -> None:
    """Cascade target-boundary policies from each tier's ``defaults.target`` into the thread.

    Handles schema_drift, on_drift, and warp_enforcement keys, key by key:
    the nearest tier setting a key wins, and the thread's own target beats
    every tier.

    Args:
        defaults: The enclosing tiers' ``defaults:`` blocks, outermost first.
        thread_config: Fully cascaded thread config (mutated in place).
    """
    effective: dict[str, Any] = {}
    targets = [block.get("target", {}) for block in defaults]
    for target in (*targets, thread_config.get("target") or {}):
        if not isinstance(target, dict):
            continue
        for key in _TARGET_POLICY_KEYS:
            if key in target:
                effective[key] = target[key]

    if not effective:
        return

    # Write resolved policies into thread's target block
    if not isinstance(thread_config.get("target"), dict):
        thread_config["target"] = {}

    for key, value in effective.items():
        thread_config["target"][key] = value


def _cascade_exports(defaults: Sequence[dict[str, Any]], thread_config: dict[str, Any]) -> None:
    """Cascade exports from each tier's defaults into thread config.

    Like audit columns, exports use additive merge: each level extends the
    export set. Same-named exports at a lower level override the definition
    from a higher level. Exports whose ``enabled`` the model reads as false
    (``false``, ``"no"``, ``0``, ...) are removed from the final list, so a
    name-only override can switch off an inherited export.

    Cascade order: each tier's defaults, outermost first, then the thread.
    """
    merged: dict[str, dict[str, Any]] = {}

    for level in defaults:
        if "exports" in level:
            for export in level["exports"]:
                name = export.get("name")
                if name:
                    merged[name] = export

    # Thread-level exports
    if "exports" in thread_config:
        for export in thread_config["exports"]:
            name = export.get("name")
            if name:
                merged[name] = export

    if not merged:
        return

    # Filter out disabled exports and write the resolved list
    thread_config["exports"] = [e for e in merged.values() if not _export_disabled(e)]


def _export_disabled(export: dict[str, Any]) -> bool:
    """Return whether an export's ``enabled`` value reads as false.

    Uses the same bool coercion as ``Export.enabled``, so the cascade and the
    model agree on string and numeric forms. A value the model can't read as a
    bool counts as enabled here and fails model validation instead.
    """
    try:
        return _BOOL.validate_python(export.get("enabled", True)) is False
    except ValidationError:
        return False


def _cascade_connections(
    tiers: Sequence[dict[str, Any] | None], thread_config: dict[str, Any]
) -> None:
    """Cascade connections from each tier into thread config.

    Additive merge: each tier extends the connections dict. Same-named
    connections at a lower tier override the definition from a higher one
    (child wins); the thread's own are the lowest of all.

    Args:
        tiers: The enclosing tiers' connections, outermost first.
        thread_config: Fully cascaded thread config (mutated in place).
    """
    merged: dict[str, Any] = {}
    for connections in (*tiers, thread_config.get("connections")):
        if connections:
            merged.update(connections)

    if merged:
        thread_config["connections"] = merged
