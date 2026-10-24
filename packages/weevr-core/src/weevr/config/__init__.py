"""Configuration loading and validation."""

import logging
from pathlib import Path
from typing import Any

from weevr.config.attribution import attribute_errors
from weevr.config.loader import COMPONENT_TYPES as _COMPONENT_TYPES
from weevr.config.loader import hydrate_resolved as _hydrate_resolved
from weevr.config.loader import load_resolved
from weevr.config.loader import resolve_config as _resolve_config
from weevr.config.locations import ConfigLocation, LocalConfigLocation, make_location
from weevr.model import Loom, Thread, Weave

logger = logging.getLogger(__name__)

__all__ = ["load_config", "load_resolved"]


def load_config(
    path: str | Path | ConfigLocation,
    runtime_params: dict[str, Any] | None = None,
    project_root: Path | ConfigLocation | None = None,
) -> Thread | Weave | Loom | dict[str, Any]:
    """Load and validate a weevr configuration file.

    Runs the shared config load (:mod:`weevr.config.loader`), the same one
    ``Context`` uses:
    1. Parse YAML file
    2. Extract and validate config_version
    3. Detect config type (extension-based for components, content-based for params)
    4. Validate schema with Pydantic
    5. Build parameter context (runtime > defaults)
    6. Resolve variable references (${var} and ${var:-default})
    7. Resolve references to child configs (threads, weaves)
    8. Apply inheritance cascade (loom -> weave -> thread)
    9. Validate name against filename stem; set ``qualified_key``
    10. Resolve relative file-Source paths against the project root
    11. Hydrate into typed domain model (thread, weave, loom only), validating
        every child thread and weave

    Execution-scope and trace-composition warnings are logged on this
    module's logger.

    Args:
        path: Path or :class:`ConfigLocation` for the config file (thread,
            weave, or loom). Local paths are wrapped automatically; remote
            URIs must be supplied as a :class:`RemoteConfigLocation` so the
            caller can attach a SparkSession.
        runtime_params: Optional runtime parameter overrides.
        project_root: The ``.weevr`` project directory. Child references,
            relative file-Source paths, and ``qualified_key`` resolve against
            it; defaults to the file's directory. Accepts a
            :class:`ConfigLocation` or a bare :class:`pathlib.Path`.

    Returns:
        A frozen, typed domain model instance (Thread, Weave, or Loom) for
        thread/weave/loom config types. Returns a plain dict for params configs.

    Raises:
        ConfigParseError: YAML syntax errors, file not found
        ConfigVersionError: Unsupported config version
        ConfigSchemaError: Schema validation failures
        ConfigError: Extension or name validation failures
        VariableResolutionError: Unresolved variables without defaults
        ReferenceResolutionError: Missing referenced files, circular dependencies
        ModelValidationError: Semantic validation failures during model hydration
    """
    file_path = make_location(path)
    if project_root is None or isinstance(project_root, ConfigLocation):
        root = project_root
    else:
        root = LocalConfigLocation(Path(project_root))
    with attribute_errors(file_path):
        resolved = _resolve_config(file_path, runtime_params=runtime_params, project_root=root)
        if resolved.config_type not in _COMPONENT_TYPES:
            return resolved.data
        loaded = _hydrate_resolved(resolved)
    for warning in loaded.warnings:
        logger.warning(warning)
    return loaded.model


# Federated namespace: weevr.config splits across two wheels — pure
# parser/resolver/validation/inheritance modules in weevr-core (which
# ships this __init__.py) and Spark-bound config helpers (fabric.py,
# paths.py) contributed by the weevr engine wheel. In editable workspace
# installs each wheel's src/ is a separate sys.path entry, so we extend
# __path__ to pull engine-side contributions into the import-time view.
# In a real install both wheels' files merge into a single
# site-packages/weevr/config/ directory and the loop is a no-op. The
# basename(p) == "src" gate scopes the scan to src-layout package roots.
def _extend_namespace_path() -> None:
    """Append sibling weevr.config/ directories on sys.path to ``__path__``."""
    import os.path as osp
    import sys

    components = __name__.split(".")
    src_entries_seen = 0
    candidates_added = 0
    for entry in sys.path:
        # Only weevr's own src-layout roots count: an unrelated src directory
        # on the path (another editable package, PYTHONPATH=src) says nothing
        # about whether this workspace is partially installed.
        if not entry or osp.basename(entry) != "src" or not osp.isdir(osp.join(entry, "weevr")):
            continue
        src_entries_seen += 1
        candidate = osp.join(entry, *components)
        if osp.isdir(candidate) and candidate not in __path__:
            __path__.append(candidate)
            candidates_added += 1
    if src_entries_seen > 0 and candidates_added == 0:
        import warnings

        warnings.warn(
            f"weevr federated namespace {__name__!r}: no sibling "
            "contributor found on sys.path. If this is an editable "
            "workspace install, run `uv sync --dev` from the repo root "
            "to install both wheels.",
            stacklevel=2,
        )


_extend_namespace_path()
del _extend_namespace_path
