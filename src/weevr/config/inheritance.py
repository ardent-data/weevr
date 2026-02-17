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

    return result
