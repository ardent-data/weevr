"""Config inheritance cascade logic."""

from typing import Any


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

    Returns:
        Fully merged config with thread values taking precedence
    """
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

    # Audit columns cascade: loom → weave → thread.target with additive merge.
    _cascade_audit_columns(loom_defaults, weave_defaults, result)

    # Exports cascade: loom → weave → thread with additive merge by name.
    _cascade_exports(loom_defaults, weave_defaults, result)

    return result


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


def _cascade_audit_columns(
    loom_defaults: dict[str, Any] | None,
    weave_defaults: dict[str, Any] | None,
    thread_config: dict[str, Any],
) -> None:
    """Cascade audit columns from loom/weave defaults into thread target.

    Unlike standard cascade (child replaces parent), audit columns use
    additive merge: each level extends the column set. Same-named columns
    at a lower level override the value from a higher level.

    Cascade order: loom defaults → weave defaults → thread target.
    """
    merged: dict[str, str] = {}

    if loom_defaults and "audit_columns" in loom_defaults:
        merged.update(loom_defaults["audit_columns"])
    if weave_defaults and "audit_columns" in weave_defaults:
        merged.update(weave_defaults["audit_columns"])

    # Thread-level audit_columns live under target.audit_columns
    target = thread_config.get("target")
    if isinstance(target, dict) and "audit_columns" in target:
        merged.update(target["audit_columns"])

    if not merged:
        return

    # Write the merged set into thread's target block
    if not isinstance(target, dict):
        thread_config.setdefault("target", {})
        target = thread_config["target"]
    target["audit_columns"] = merged

    # Remove top-level audit_columns inherited from defaults (not a Thread field)
    thread_config.pop("audit_columns", None)


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
