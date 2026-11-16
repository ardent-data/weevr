"""Config inheritance cascade logic."""

import logging
from collections.abc import Mapping
from typing import Any

from weevr.config.audit_templates import merge_audit_columns_with_templates
from weevr.errors import InheritanceError

logger = logging.getLogger(__name__)


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


def apply_inheritance(
    loom_defaults: dict[str, Any] | None,
    weave_defaults: dict[str, Any] | None,
    thread_config: dict[str, Any],
    *,
    loom_audit_templates: dict[str, Any] | None = None,
    weave_audit_templates: dict[str, Any] | None = None,
    loom_connections: dict[str, Any] | None = None,
    weave_connections: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Apply multi-level inheritance cascade.

    Cascade order (lowest to highest priority):
    1. loom_defaults (lowest)
    2. weave_defaults
    3. thread_config (highest)

    Args:
        loom_defaults: Defaults from loom level
        weave_defaults: Defaults from weave level
        thread_config: Thread-specific config
        loom_audit_templates: User-defined audit template definitions from loom
        weave_audit_templates: User-defined audit template definitions from weave
        loom_connections: Named connection definitions from loom top-level
        weave_connections: Named connection definitions from weave top-level

    Returns:
        Fully merged config with thread values taking precedence

    Raises:
        InheritanceError: If a ``defaults`` block holds a key the cascade
            merges with the wrong shape.
    """
    _check_defaults_shape(loom_defaults, "loom")
    _check_defaults_shape(weave_defaults, "weave")
    result = {}

    # Start with loom defaults
    if loom_defaults:
        result = loom_defaults.copy()

    # Cascade weave defaults over loom
    if weave_defaults:
        result = cascade(result, weave_defaults) if result else weave_defaults.copy()

    # Cascade thread config over everything
    result = cascade(result, thread_config) if result else thread_config.copy()

    # Naming config cascade: loom.naming → weave.naming → thread.target.naming
    # The most specific (thread-level) wins entirely.
    _cascade_naming(loom_defaults, weave_defaults, result)

    # Audit cascade: templates, inline columns, inherit flag, and exclusions.
    _cascade_audit(
        loom_defaults, weave_defaults, result, loom_audit_templates, weave_audit_templates
    )

    # Exports cascade: loom → weave → thread with additive merge by name.
    _cascade_exports(loom_defaults, weave_defaults, result)

    # Connections cascade: loom → weave → thread with additive merge by name.
    _cascade_connections(loom_connections, weave_connections, result)

    # Target policy cascade: schema_drift, on_drift, warp_enforcement.
    _cascade_target_policies(loom_defaults, weave_defaults, result)

    return result


def _check_defaults_shape(defaults: dict[str, Any] | None, scope: str) -> None:
    """Reject ``defaults`` keys the cascade merges but no schema has typed yet.

    ``defaults`` is free-form until the cascade copies it into each thread,
    so a wrong shape here would otherwise surface as a builtin error.

    Raises:
        InheritanceError: Naming the scope and the key.
    """
    if not defaults:
        return

    def fail(key: str, expected: str, value: Any) -> None:
        raise InheritanceError(
            f"{scope} defaults.{key} must be {expected}, got {type(value).__name__}",
            config_key=f"defaults.{key}",
        )

    if "target" in defaults:
        target = defaults["target"]
        if not isinstance(target, dict):
            fail("target", "a mapping", target)
        columns = target.get("audit_columns")
        if columns is not None and not isinstance(columns, dict):
            fail("target.audit_columns", "a mapping", columns)
    legacy = defaults.get("audit_columns")
    if legacy is not None and not isinstance(legacy, dict):
        fail("audit_columns", "a mapping", legacy)
    if "exports" in defaults:
        exports = defaults["exports"]
        if not isinstance(exports, list):
            fail("exports", "a list", exports)
        for export in exports:
            if not isinstance(export, dict):
                fail("exports[]", "a mapping", export)


def _cascade_naming(
    loom_defaults: dict[str, Any] | None,
    weave_defaults: dict[str, Any] | None,
    thread_config: dict[str, Any],
) -> None:
    """Cascade naming config from loom/weave defaults into thread target.

    Naming config inheritance: loom.naming → weave.naming → thread.target.naming.
    The most specific non-None value wins entirely (no field-by-field merge).
    """
    # Resolve effective naming from parent levels
    parent_naming = None
    if loom_defaults and "naming" in loom_defaults:
        parent_naming = loom_defaults["naming"]
    if weave_defaults and "naming" in weave_defaults:
        parent_naming = weave_defaults["naming"]

    if parent_naming is None:
        return

    # Apply to thread's target block if thread doesn't already have naming
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


def _cascade_audit(
    loom_defaults: dict[str, Any] | None,
    weave_defaults: dict[str, Any] | None,
    thread_config: dict[str, Any],
    loom_audit_templates: dict[str, Any] | None = None,
    weave_audit_templates: dict[str, Any] | None = None,
) -> None:
    """Cascade audit columns with template resolution, inheritance, and exclusion.

    Handles both the new path (``defaults.target.audit_columns``,
    ``defaults.target.audit_template``) and the legacy path
    (``defaults.audit_columns``). The legacy path is still supported but
    triggers a deprecation warning. When both are present, the new path wins
    on key collision.

    Args:
        loom_defaults: Defaults dict from loom level.
        weave_defaults: Defaults dict from weave level.
        thread_config: Fully cascaded thread config (mutated in place).
        loom_audit_templates: User-defined template definitions from loom top-level.
        weave_audit_templates: User-defined template definitions from weave top-level.
    """
    # --- template definitions ---
    loom_templates = _extract_audit_templates(loom_audit_templates)
    weave_templates = _extract_audit_templates(weave_audit_templates)
    thread_templates = _extract_audit_templates(thread_config.get("audit_templates"))

    # --- template refs ---
    loom_target = (loom_defaults or {}).get("target", {})
    weave_target = (weave_defaults or {}).get("target", {})
    thread_target = thread_config.get("target") or {}

    loom_template_ref = loom_target.get("audit_template")
    weave_template_ref = weave_target.get("audit_template")
    thread_template_ref = (
        thread_target.get("audit_template") if isinstance(thread_target, dict) else None
    )

    loom_template_refs = _to_template_refs(loom_template_ref)
    weave_template_refs = _to_template_refs(weave_template_ref)
    thread_template_refs = _to_template_refs(thread_template_ref)

    # --- inherit flag ---
    thread_inherit = True
    if isinstance(thread_target, dict):
        thread_inherit = thread_target.get("audit_template_inherit", True)

    # --- inline audit_columns: legacy and new paths ---
    loom_inline: dict[str, str] = {}
    if loom_defaults:
        legacy = loom_defaults.get("audit_columns")
        if legacy:
            logger.warning(
                "defaults.audit_columns is deprecated; use defaults.target.audit_columns instead"
            )
            loom_inline.update(legacy)
        new_path = loom_target.get("audit_columns")
        if new_path:
            loom_inline.update(new_path)

    weave_inline: dict[str, str] = {}
    if weave_defaults:
        legacy = weave_defaults.get("audit_columns")
        if legacy:
            logger.warning(
                "defaults.audit_columns is deprecated; use defaults.target.audit_columns instead"
            )
            weave_inline.update(legacy)
        new_path = weave_target.get("audit_columns")
        if new_path:
            weave_inline.update(new_path)

    thread_inline: dict[str, str] | None = None
    if isinstance(thread_target, dict):
        thread_inline = thread_target.get("audit_columns")

    # --- exclude ---
    thread_exclude: list[str] | None = None
    if isinstance(thread_target, dict):
        thread_exclude = thread_target.get("audit_columns_exclude")

    # Skip entirely if nothing is defined at any level
    has_anything = any(
        [
            loom_templates,
            weave_templates,
            thread_templates,
            loom_template_refs,
            weave_template_refs,
            thread_template_refs,
            loom_inline,
            weave_inline,
            thread_inline,
        ]
    )
    if not has_anything:
        thread_config.pop("audit_columns", None)
        return

    merged = merge_audit_columns_with_templates(
        loom_templates=loom_templates,
        loom_template_refs=loom_template_refs,
        loom_inline=loom_inline or None,
        weave_templates=weave_templates,
        weave_template_refs=weave_template_refs,
        weave_inline=weave_inline or None,
        thread_templates=thread_templates,
        thread_template_refs=thread_template_refs,
        thread_inline=thread_inline,
        thread_inherit=thread_inherit,
        thread_exclude=thread_exclude,
    )

    # Write result into thread's target block
    if not isinstance(thread_config.get("target"), dict):
        thread_config["target"] = {}

    if merged:
        thread_config["target"]["audit_columns"] = merged
    else:
        thread_config["target"].pop("audit_columns", None)

    # Clean up intermediate keys from the cascaded thread_config
    thread_config.pop("audit_columns", None)
    thread_config.pop("audit_templates", None)

    target = thread_config["target"]
    target.pop("audit_template", None)
    target.pop("audit_template_inherit", None)
    target.pop("audit_columns_exclude", None)


_TARGET_POLICY_KEYS = ("schema_drift", "on_drift", "warp_enforcement")


def _cascade_target_policies(
    loom_defaults: dict[str, Any] | None,
    weave_defaults: dict[str, Any] | None,
    thread_config: dict[str, Any],
) -> None:
    """Cascade target-boundary policies from defaults.target.* into thread target.

    Handles schema_drift, on_drift, and warp_enforcement keys. These
    cascade from loom → weave → thread target with child-wins semantics.

    Args:
        loom_defaults: Defaults dict from loom level.
        weave_defaults: Defaults dict from weave level.
        thread_config: Fully cascaded thread config (mutated in place).
    """
    # Build cascaded policy values: loom < weave < thread
    effective: dict[str, Any] = {}

    loom_target = (loom_defaults or {}).get("target", {})
    weave_target = (weave_defaults or {}).get("target", {})
    thread_target = thread_config.get("target") or {}

    for key in _TARGET_POLICY_KEYS:
        if isinstance(loom_target, dict) and key in loom_target:
            effective[key] = loom_target[key]
        if isinstance(weave_target, dict) and key in weave_target:
            effective[key] = weave_target[key]
        if isinstance(thread_target, dict) and key in thread_target:
            effective[key] = thread_target[key]

    if not effective:
        return

    # Write resolved policies into thread's target block
    if not isinstance(thread_config.get("target"), dict):
        thread_config["target"] = {}

    for key, value in effective.items():
        thread_config["target"][key] = value


def _cascade_exports(
    loom_defaults: dict[str, Any] | None,
    weave_defaults: dict[str, Any] | None,
    thread_config: dict[str, Any],
) -> None:
    """Cascade exports from loom/weave defaults into thread config.

    Like audit columns, exports use additive merge: each level extends the
    export set. Same-named exports at a lower level override the definition
    from a higher level. Exports with ``enabled: false`` are removed from
    the final list.

    Cascade order: loom defaults → weave defaults → thread config.
    """
    merged: dict[str, dict[str, Any]] = {}

    for level in (loom_defaults, weave_defaults):
        if level and "exports" in level:
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
    thread_config["exports"] = [e for e in merged.values() if e.get("enabled", True) is not False]


def _cascade_connections(
    loom_connections: dict[str, Any] | None,
    weave_connections: dict[str, Any] | None,
    thread_config: dict[str, Any],
) -> None:
    """Cascade connections from loom/weave into thread config.

    Additive merge: each level extends the connections dict.
    Same-named connections at a lower level override the definition
    from a higher level (child wins).

    Cascade order: loom → weave → thread.
    """
    merged: dict[str, Any] = {}

    if loom_connections:
        merged.update(loom_connections)
    if weave_connections:
        merged.update(weave_connections)

    # Thread-level connections (highest priority)
    thread_connections = thread_config.get("connections")
    if thread_connections:
        merged.update(thread_connections)

    if merged:
        thread_config["connections"] = merged
