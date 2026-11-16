"""Hook list resolution across loom → weave → thread levels.

Named registries (lookups, column sets, variables) cascade at load; see
:func:`weevr.config.inheritance.cascade_registry`.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


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
