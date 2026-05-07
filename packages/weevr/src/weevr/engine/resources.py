"""Runtime resource merge helpers for cascade across loom → weave → thread levels."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def merge_resource_dicts(
    parent: dict[str, Any] | None,
    child: dict[str, Any] | None,
    resource_type: str,
    parent_level: str,
    child_level: str,
) -> dict[str, Any] | None:
    """Merge two resource dicts, with child winning on key conflicts.

    Emits a DEBUG log entry for each key in child that shadows a key in parent.

    Args:
        parent: Resource dict from the outer (parent) level, or None.
        child: Resource dict from the inner (child) level, or None.
        resource_type: Human-readable label for the resource type (used in logs).
        parent_level: Name of the parent scope level (e.g. ``"loom"``).
        child_level: Name of the child scope level (e.g. ``"weave"``).

    Returns:
        Merged dict where child keys win, or None if both inputs are None.
        An empty child dict is treated as no override — parent is returned.
    """
    if parent is None and child is None:
        return None

    if not child:
        return dict(parent) if parent else None

    if not parent:
        return dict(child)

    for key in child:
        if key in parent:
            logger.debug(
                "Resource '%s' in %s shadows %s-level definition",
                key,
                child_level,
                parent_level,
            )

    return {**parent, **child}


def merge_hook_lists(
    parent: list[Any] | None,
    child: list[Any] | None,
) -> list[Any] | None:
    """Resolve the effective hook list, with child replacing parent entirely.

    If child is not None, it wins outright — hook lists do not merge element-wise.

    Args:
        parent: Hook list from the parent level, or None.
        child: Hook list from the child level, or None.

    Returns:
        Child list when present, otherwise parent list, or None if both are None.
    """
    if child is not None:
        return list(child)
    return list(parent) if parent is not None else None
