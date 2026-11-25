"""Resolve a config dict's references the way the loader does, for resolver tests."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

from weevr.config.locations import ConfigLocation
from weevr.config.params import ScopePositions
from weevr.config.resolver import ResolvedChild, resolve_references
from weevr.config.scoping import WalkState, resolve_config_variables
from weevr.diagnostics import Diagnostic


def resolve_tree(
    config: dict[str, Any],
    config_type: str,
    project_root: ConfigLocation | Path,
    runtime_params: dict[str, Any] | None = None,
    *,
    visited: dict[str, None] | None = None,
    warnings: list[Diagnostic] | None = None,
) -> list[ResolvedChild]:
    """Run the variable pass over ``config``, then resolve its references.

    Args:
        config: The config dict, as written.
        config_type: Its kind (``"weave"`` or ``"loom"``).
        project_root: The root references resolve against.
        runtime_params: The invoker's runtime params.
        visited: Refs already being loaded, for cycle detection.
        warnings: Receives the load warnings (``P123``) when given.

    Returns:
        The resolved children.
    """
    walk = WalkState.start(config, file=None, kind=config_type, runtime_params=runtime_params)
    if warnings is not None:
        walk = replace(walk, warnings=warnings)
    scopes: ScopePositions = {}
    resolved = resolve_config_variables(config, config_type, walk, inline_scopes=scopes)
    return resolve_references(
        resolved, config_type, project_root, walk, visited, authored=config, inline_scopes=scopes
    )
