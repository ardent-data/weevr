"""Resolve and load configs the way the loader does, for resolver and load tests."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from weevr.config import load_resolved
from weevr.config.document import ResolvedDocument
from weevr.config.loader import collect_resolved_config
from weevr.config.locations import ConfigLocation, LocalConfigLocation
from weevr.config.params import ScopePositions
from weevr.config.resolver import ResolvedChild, resolve_references
from weevr.config.scoping import WalkState, resolve_config_variables
from weevr.diagnostics import Diagnostic
from weevr.errors import ConfigError


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


def load_at(path: Path, runtime: dict[str, Any] | None = None, **kwargs: Any) -> Any:
    """Load ``path`` with its directory as the project root.

    Args:
        path: The config file.
        runtime: The runtime params.
        **kwargs: Further :func:`~weevr.config.load_resolved` keywords.

    Returns:
        The loaded config.
    """
    return load_resolved(path, runtime_params=runtime, project_root=path.parent, **kwargs)


def load_failure(path: Path, runtime: dict[str, Any] | None = None, **kwargs: Any) -> ConfigError:
    """The error loading ``path`` (:func:`load_at`) fails with."""
    with pytest.raises(ConfigError) as info:
        load_at(path, runtime, **kwargs)
    return info.value


def collected_at(
    path: Path, runtime: dict[str, Any] | None = None, **kwargs: Any
) -> tuple[ResolvedDocument | None, list[Diagnostic], list[Diagnostic]]:
    """Load ``path`` collecting every error, with its directory as the project root.

    Returns:
        The document (``None`` on error), the errors, and the warnings.
    """
    return collect_resolved_config(
        LocalConfigLocation(path),
        runtime_params=runtime,
        project_root=LocalConfigLocation(path.parent),
        **kwargs,
    )


def with_code(diagnostics: list[Diagnostic], code: str) -> list[Diagnostic]:
    """The diagnostics in ``diagnostics`` with ``code``."""
    return [d for d in diagnostics if d.code == code]


def leaf_node(loaded: Any) -> ResolvedDocument:
    """The first leaf of a loaded tree: its thread node, whatever the root kind.

    Args:
        loaded: A load result (``load_at`` or ``load_resolved``).

    Returns:
        The leaf :class:`ResolvedDocument`, found by following first children.
    """
    document: ResolvedDocument = loaded.document
    while document.children:
        document = next(iter(document.children.values()))
    return document
