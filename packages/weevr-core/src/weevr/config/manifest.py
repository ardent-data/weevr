"""The param manifest: what running a config file asks of its invoker.

:func:`param_manifest` answers "what params does this file take?" without
any param values and without a Spark session. It reads the authored
documents only — the file and every child it pulls in, by ``ref:`` or
inline — and binds nothing: declared specs cascade by name the way a load's
param scopes do (a child's re-declaration shadows its ancestors'), an
entry's ``params:`` pins satisfy the child's names for that subtree, and
``${param.x}`` references are scanned where they are written. No value is
substituted or coerced, so a config whose required params nobody supplied
still yields its manifest.

The project's ``weevr.yaml`` params are the outermost tier, as in a load.
The manifest is one aggregate for the file being run: the declarations
anywhere in its tree and in the project, the names the invoker must supply, and the names used
without a declaration. Per-child manifests are not reported.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from weevr.config.attribution import attribute_errors
from weevr.config.document_load import load_document
from weevr.config.interpolation import param_references
from weevr.config.loader import COMPONENT_TYPES, resolve_project_root
from weevr.config.locations import ConfigLocation, LocalConfigLocation, make_location
from weevr.config.manifest_types import ManifestParam, ParamManifest
from weevr.config.param_provenance import ParamTier
from weevr.config.params import DeclaredSpec
from weevr.config.resolver import resolve_ref_path
from weevr.config.root import PROJECT_FILE
from weevr.config.scoping import (
    CHILD_LISTS,
    entry_pins,
    is_inline_thread,
    is_inline_weave,
    param_tier,
    project_blocks,
)
from weevr.diagnostics.positions import json_pointer
from weevr.model.params import ParamSpec
from weevr.model.project import Project

__all__ = ["param_manifest"]


@dataclass(frozen=True)
class _Scope:
    """The specs in force in one scope and the names an entry pin above it binds."""

    specs: Mapping[str, DeclaredSpec] = field(default_factory=dict)
    pinned: frozenset[str] = frozenset()


class _Walk:
    """The manifest's records, filled while the config tree is walked."""

    def __init__(self, root: ConfigLocation) -> None:
        """Start a walk resolving ``ref:`` entries against ``root``."""
        self.root = root
        self.declared: dict[tuple[str, str, str], ManifestParam] = {}
        self.required: dict[str, None] = {}
        self.undeclared: dict[str, None] = {}

    def document(
        self,
        document: Mapping[str, Any],
        kind: str,
        parent: _Scope,
        *,
        file: str,
        at: tuple[str | int, ...] = (),
        trail: tuple[str, ...] = (),
    ) -> None:
        """Walk one document (in its own file or inline) and its children."""
        scope = self._open(document.get("params"), parent, file, at, param_tier(kind))
        key, child_kind, entry_keys = CHILD_LISTS.get(kind, ("", "", frozenset()))
        entries = document.get(key) if key else None
        if not isinstance(entries, list):
            self._scan(document, scope)
            return
        # Inline children's bodies sit in their own scope; their entry keys,
        # and a ref: entry's pins, belong to this one.
        parent_keys = entry_keys - {"params"}
        is_inline = is_inline_weave if kind == "loom" else is_inline_thread
        shell = {k: v for k, v in document.items() if k != key}
        self._scan(shell, scope)
        for index, entry in enumerate(entries):
            where = (*at, key, index)
            if not isinstance(entry, dict):
                continue
            if "ref" in entry:
                self._scan(entry, scope)
                self._ref(entry, scope, file, where, trail)
                continue
            self._scan({k: v for k, v in entry.items() if k in parent_keys}, scope)
            if is_inline(entry):
                body = {k: v for k, v in entry.items() if k not in parent_keys}
                self.document(body, child_kind, scope, file=file, at=where, trail=trail)

    def _ref(
        self,
        entry: Mapping[str, Any],
        scope: _Scope,
        file: str,
        at: tuple[str | int, ...],
        trail: tuple[str, ...],
    ) -> None:
        """Walk the child a ``ref:`` entry loads, with the entry's pins bound below it."""
        ref = entry["ref"]
        if ref in trail:
            # A cycle; the load reports it. The manifest stops following it.
            return
        with attribute_errors(file):
            location = resolve_ref_path(ref, self.root, json_pointer((*at, "ref")))
        with attribute_errors(location):
            loaded_kind, model = load_document(location)
        pins = entry_pins(dict(entry)) or {}
        below = _Scope(scope.specs, scope.pinned | frozenset(pins))
        child = model.model_dump(exclude_unset=True)
        self.document(
            child,
            loaded_kind,
            below,
            file=str(location),
            trail=(*trail, ref),
        )

    def project(self, project: Project | None, file: str) -> _Scope:
        """Open the project's scope, the outermost tier: record its declarations.

        The references in the project's cascading blocks read this scope.
        """
        if project is None:
            return _Scope()
        scope = self._open(project.params, _Scope(), file, (), "project")
        self._scan(project_blocks(project), scope)
        return scope

    def _open(
        self,
        specs: Any,
        parent: _Scope,
        file: str,
        at: tuple[str | int, ...],
        tier: ParamTier | None,
    ) -> _Scope:
        """Open a document's scope: record its declarations and what they leave unbound."""
        declared: dict[str, DeclaredSpec] = {}
        for name, spec in (specs if isinstance(specs, dict) else {}).items():
            declared[name] = DeclaredSpec.read(name, spec, file, at, tier)
            self._declare(name, spec, declared[name], file, at)
        effective = {**parent.specs, **declared}
        for name, spec in effective.items():
            if name not in parent.pinned and spec.needs_supply:
                self.required.setdefault(name, None)
        return _Scope(effective, parent.pinned)

    def _declare(
        self, name: str, spec: Any, read: DeclaredSpec, file: str, at: tuple[str | int, ...]
    ) -> None:
        """Record one declaration, once per place it is written."""
        pointer = json_pointer((*at, "params", name))
        if isinstance(spec, ParamSpec):
            spec = spec.model_dump(exclude_unset=True)
        description = spec.get("description") if isinstance(spec, dict) else None
        self.declared.setdefault(
            (name, file, pointer),
            ManifestParam(
                name=name,
                type=read.type,
                required=read.required,
                default=read.default,
                description=description if isinstance(description, str) else None,
                file=file,
                pointer=pointer,
                tier=read.tier,
            ),
        )

    def _scan(self, value: Any, scope: _Scope) -> None:
        """Note each ``${param.x}`` in ``value`` that ``scope`` neither declares nor pins."""
        for name in param_references(value):
            if name not in scope.specs and name not in scope.pinned:
                self.undeclared.setdefault(name, None)


def param_manifest(
    path: str | Path | ConfigLocation,
    *,
    project_root: Path | ConfigLocation | None = None,
) -> ParamManifest:
    """The params running ``path`` takes, read from the authored documents alone.

    No param values are needed and none are bound: a required param nobody
    supplied, or a default its type rejects, never fails here. ``ref:``
    children resolve against the project root as a load's do; a reference
    cycle is not followed (the load reports it).

    Args:
        path: The config file (thread, weave or loom). A local path is
            wrapped automatically.
        project_root: The project directory ``ref:`` entries resolve
            against. When omitted, the nearest folder above the file holding
            ``weevr.yaml`` is the root; with none, the file's directory is.

    Returns:
        The manifest for ``path``. A document that is not a thread, weave or
        loom has an empty one.

    Raises:
        ConfigError: A document cannot be read or fails its envelope or
            schema checks, a ``ref:`` target is missing or invalid, or the
            project's ``weevr.yaml`` fails to load.
    """
    location = make_location(path)
    if project_root is None or isinstance(project_root, ConfigLocation):
        given = project_root
    else:
        given = LocalConfigLocation(Path(project_root))
    with attribute_errors(location):
        root, project = resolve_project_root(location, given)
        kind, model = load_document(location)
    document = model.model_dump(exclude_unset=True)
    name = document.get("name") or location.stem
    if kind not in COMPONENT_TYPES:
        return ParamManifest(executable=name, kind=kind, file=str(location))
    walk = _Walk(root)
    outermost = walk.project(project, str(root.join(PROJECT_FILE)))
    walk.document(document, kind, outermost, file=str(location))
    return ParamManifest(
        executable=name,
        kind=kind,
        file=str(location),
        declared=tuple(walk.declared.values()),
        invoker_required=tuple(walk.required),
        undeclared_used=tuple(walk.undeclared),
    )
