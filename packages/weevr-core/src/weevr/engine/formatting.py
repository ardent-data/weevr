"""Shared formatting utilities for the engine and result modules."""

from __future__ import annotations

from typing import Any


def format_duration(ms: int | float) -> str:
    """Format milliseconds as a human-readable duration string.

    Args:
        ms: Duration in milliseconds.

    Returns:
        Formatted string like ``"42ms"``, ``"1.2s"``, or ``"2m 30.0s"``.
    """
    ms = int(ms)
    if ms < 1000:
        return f"{ms}ms"
    seconds = ms / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    remaining = seconds % 60
    return f"{minutes}m {remaining:.1f}s"


def format_project(project: Any) -> str:
    """Describe a project by name and id, for run output.

    Args:
        project: A :class:`~weevr.model.Project`, or ``None``.

    Returns:
        ``"<name> (<id>)"``, or ``"(none)"`` for a load without a ``weevr.yaml``.
    """
    if project is None:
        return "(none)"
    return f"{project.name} ({project.id})"
