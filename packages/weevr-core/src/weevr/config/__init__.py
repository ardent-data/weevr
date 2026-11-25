"""Configuration loading and validation."""

import logging
from pathlib import Path
from typing import Any

from weevr.config.attribution import attribute_errors
from weevr.config.collect import collecting
from weevr.config.discovery import DiscoveredDocument, discover_documents
from weevr.config.document import EntryMeta, Provenance, ResolvedDocument, ScopeRegistries
from weevr.config.loader import COMPONENT_TYPES as _COMPONENT_TYPES
from weevr.config.loader import hydrate_resolved as _hydrate_resolved
from weevr.config.loader import load_resolved
from weevr.config.loader import resolve_config as _resolve_config
from weevr.config.locations import ConfigLocation, LocalConfigLocation, make_location
from weevr.config.manifest import param_manifest
from weevr.config.manifest_types import ManifestParam, ParamManifest
from weevr.config.param_provenance import (
    ParamDeclaration,
    ParamPin,
    ParamProvenance,
    ParamSource,
    ParamTier,
)
from weevr.config.root import find_project_root
from weevr.config.run_store import (
    RUN_STORE_TABLES,
    RunStoreLocation,
    TableLocation,
    resolve_run_store,
)
from weevr.config.validation import render_diagnostics
from weevr.model import Loom, Thread, Weave

logger = logging.getLogger(__name__)

__all__ = [
    "RUN_STORE_TABLES",
    "DiscoveredDocument",
    "EntryMeta",
    "ManifestParam",
    "ParamDeclaration",
    "ParamManifest",
    "ParamPin",
    "ParamProvenance",
    "ParamSource",
    "ParamTier",
    "Provenance",
    "ResolvedDocument",
    "RunStoreLocation",
    "ScopeRegistries",
    "TableLocation",
    "discover_documents",
    "find_project_root",
    "load_config",
    "load_resolved",
    "param_manifest",
    "resolve_run_store",
]


def load_config(
    path: str | Path | ConfigLocation,
    runtime_params: dict[str, Any] | None = None,
    project_root: Path | ConfigLocation | None = None,
    *,
    strict_params: bool | None = None,
    fabric_context: dict[str, Any] | None = None,
) -> Thread | Weave | Loom | dict[str, Any]:
    """Load and validate a weevr configuration file.

    Runs the shared config load (:mod:`weevr.config.loader`), the same one
    ``Context`` uses:
    1. Parse YAML file
    2. Extract and validate config_version
    3. Take the config kind from the document's ``kind`` (checked against a typed
       extension), else from the extension or content
    4. Validate schema with Pydantic
    5. Bind params per scope (pin > runtime > declared default)
    6. Resolve variable references (``${namespace.name}`` and ``${namespace.name:-default}``);
       ``${fabric.x}`` without a Fabric context is left in place (``P113``)
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
        project_root: The project directory. Child references, relative
            file-Source paths, and ``qualified_key`` resolve against it. When
            omitted, the nearest folder above the file holding ``weevr.yaml``
            is the root; with none, the file's directory is. Accepts a
            :class:`ConfigLocation` or a bare :class:`pathlib.Path`.
        strict_params: Overrides every document's ``strict_params:``. ``True``
            fails the load on a ``${param.x}`` no declaration in its chain
            covers (``P120``) and on a runtime param nothing declares or
            references (``P121``); ``False`` keeps both warnings even where a
            document sets ``strict_params: true``. ``None`` (the default)
            leaves each document's setting, most specific wins, absent is
            lenient.
        fabric_context: The Fabric values ``${fabric.x}`` resolves from, keyed
            ``workspace_id``, ``lakehouse_id`` and ``workspace_name`` (a
            ``fabric.`` prefix is accepted). ``None`` (the default) means no
            Fabric context: each ``${fabric.x}`` without a ``:-`` default is
            left in place and warns ``P113``, an error under strict params. A
            mapping, even an empty one, is the context: a key it lacks or
            maps to ``None`` fails ``P110``. ``Context`` always passes the
            values of its Spark session. Runtime params never supply them.

    Returns:
        A frozen, typed domain model instance (Thread, Weave, or Loom) for
        thread/weave/loom config types. Returns a plain dict for params configs.

    Raises:
        ConfigParseError: YAML syntax errors, file not found
        ConfigVersionError: Unsupported config version
        ConfigSchemaError: Schema validation failures
        ConfigError: Extension or name validation failures
        VariableResolutionError: Unresolved variables without defaults, and
            undeclared or unused params or deferred ``${fabric.x}`` references
            under strict params
        ReferenceResolutionError: Missing referenced files, circular dependencies
        ModelValidationError: Semantic validation failures during model hydration
    """
    file_path = make_location(path)
    if project_root is None or isinstance(project_root, ConfigLocation):
        root = project_root
    else:
        root = LocalConfigLocation(Path(project_root))
    with collecting(), attribute_errors(file_path):
        resolved = _resolve_config(
            file_path,
            runtime_params=runtime_params,
            project_root=root,
            fabric_context=fabric_context,
            strict_params=strict_params,
        )
        if resolved.config_type not in _COMPONENT_TYPES:
            return resolved.data
        document, warnings = _hydrate_resolved(resolved)
    for warning in render_diagnostics(warnings):
        logger.warning(warning)
    return document.model


# Federated namespace: weevr.config splits across two wheels — pure
# parser/resolver/validation/inheritance modules in weevr-core (which
# ships this __init__.py) and Spark-bound config helpers (fabric.py,
# paths.py) contributed by the weevr engine wheel. In editable workspace
# installs each wheel's src/ is a separate sys.path entry, so we extend
# __path__ to pull engine-side contributions into the import-time view.
# In a real install both wheels' files merge into a single
# site-packages/weevr/config/ directory and the loop is a no-op. Only
# src-layout roots that hold a weevr/ package count; other src entries
# on sys.path are ignored.
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
        own_root = (
            bool(entry) and osp.basename(entry) == "src" and osp.isdir(osp.join(entry, "weevr"))
        )
        if not own_root:
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
