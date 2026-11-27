"""The shared config load: parse, resolve, cascade, and hydrate one file.

Every entry point — ``load_config``, ``load_resolved``, and the engine's
``Context`` — runs these functions, so a config loads the same way whichever
one is used. Engine-bound input (the Fabric context) is passed in, never
imported.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter, ValidationError

from weevr.config.attribution import attribute_errors
from weevr.config.collect import collecting, diagnostics_of, errors_recorded
from weevr.config.document import (
    ConfigKind,
    EntryMeta,
    Provenance,
    ResolvedDocument,
    ScopeRegistries,
)
from weevr.config.document_load import load_document
from weevr.config.hydration import (
    check_column_set_references,
    check_resolve_lookups,
    child_name,
    hydrate_children,
    hydrate_model,
)
from weevr.config.inheritance import CascadeTier, apply_tiers, check_defaults_shape
from weevr.config.interpolation import check_fabric_context
from weevr.config.locations import (
    ConfigLocation,
    LocalConfigLocation,
    RemoteConfigLocation,
    location_like,
    make_location,
)
from weevr.config.macros import expand_foreach, reject_foreach_in_with
from weevr.config.param_provenance import ParamProvenance
from weevr.config.params import ScopePositions
from weevr.config.parser import detect_config_type_from_extension
from weevr.config.resolver import (
    ResolvedChild,
    resolve_references,
)
from weevr.config.root import PROJECT_FILE, find_project_root, read_project
from weevr.config.scoping import WalkState, param_use_diagnostics, resolve_config_variables
from weevr.config.validation import (
    dimension_override_diagnostics,
    execution_scope_diagnostics,
    in_thread,
    incremental_thread_diagnostics,
    partition_diagnostics,
    render_diagnostics,
    trace_composition_diagnostics,
)
from weevr.config.warp import discover_warp, find_warp
from weevr.diagnostics import Diagnostic, diagnostic
from weevr.diagnostics.positions import fill_lines, json_pointer, parse_pointer, place_all
from weevr.diagnostics.pydantic import from_validation_error
from weevr.errors import ConfigError, ModelValidationError, VariableResolutionError
from weevr.model import Loom, Thread, Weave
from weevr.model.column_set import ColumnSet
from weevr.model.connection import OneLakeConnection
from weevr.model.lookup import Lookup
from weevr.model.project import Project
from weevr.model.warp import WarpBinding

logger = logging.getLogger("weevr.config")

COMPONENT_TYPES = ("thread", "weave", "loom")

_KINDS: dict[type, ConfigKind] = {Thread: "thread", Weave: "weave", Loom: "loom"}


@dataclass
class ResolvedConfig:
    """A config file resolved to a dict, before hydration.

    Attributes:
        location: The file the config came from.
        project_root: The root child references and Source paths resolved against.
        config_type: Config kind (``"thread"``, ``"weave"``, ``"loom"``, ``"params"``).
        config_name: Name of the config (declared, else the filename stem).
        data: The resolved config dict.
        children: The configs it pulls in: a loom's weaves (each with its
            threads) or a weave's threads, cascaded.
        authored: The config dict before variable substitution.
        warnings: Load warnings collected so far.
        project: The project document at ``project_root``; ``None`` when it
            has no ``weevr.yaml``.
        provenance: Where each of the config's own params came from.
        project_scope: The project's lookups and column sets, resolved in
            its own scope; ``None`` when it declares neither.
        project_connections: The project's connections, resolved in its own
            scope, for the lookups and column sets a loom or weave
            materializes; ``None`` when it declares none.
    """

    location: ConfigLocation
    project_root: ConfigLocation
    config_type: str
    config_name: str
    data: dict[str, Any]
    children: list[ResolvedChild] = field(default_factory=list)
    authored: dict[str, Any] | None = None
    warnings: list[Diagnostic] = field(default_factory=list)
    project: Project | None = None
    provenance: Mapping[str, ParamProvenance] = field(default_factory=dict)
    project_scope: ScopeRegistries | None = None
    project_connections: dict[str, OneLakeConnection] | None = None


def resolve_project_root(
    location: ConfigLocation,
    project_root: ConfigLocation | None,
    project: Project | None = None,
) -> tuple[ConfigLocation, Project | None]:
    """The root a load of ``location`` resolves against, and its project.

    With no ``project_root``, the nearest folder above the file holding
    ``weevr.yaml`` is the root; with none, the file's folder is, and there
    is no project. A given ``project_root`` is used as it is, with
    ``project`` when the caller already loaded it, else the root's own
    ``weevr.yaml`` when it has one.

    Args:
        location: The config file being loaded.
        project_root: The root the caller chose, if any.
        project: The root's project document, when the caller loaded it.

    Returns:
        ``(root, project)``.

    Raises:
        ConfigError: The ``weevr.yaml`` found fails to load.
    """
    if project_root is not None:
        return project_root, project if project is not None else read_project(project_root)
    found = find_project_root(location.parent)
    if found is None:
        return location.parent, None
    return found, read_project(found)


def resolve_config(
    location: ConfigLocation,
    *,
    runtime_params: dict[str, Any] | None = None,
    project_root: ConfigLocation | None = None,
    fabric_context: dict[str, Any] | None = None,
    require_typed_extension: bool = False,
    project: Project | None = None,
    strict_params: bool | None = None,
) -> ResolvedConfig:
    """Resolve one config file to a dict ready for hydration.

    Parses and validates the file, resolves params, variables, and child
    references, applies the inheritance cascade, expands ``foreach`` macros,
    checks the name, sets ``qualified_key``, and resolves relative file-Source
    paths. Execution-scope warnings are collected before the cascade.

    Args:
        location: The config file.
        runtime_params: Runtime parameter overrides.
        project_root: Root for child references and Source paths. Defaults to
            the nearest folder above the file holding ``weevr.yaml``, else
            the file's directory.
        fabric_context: The Fabric values for ``${fabric.x}``; ``None`` when
            the load has none (each reference is then left in place, ``P113``).
            See :func:`load_resolved`.
        require_typed_extension: Reject files without a ``.thread``, ``.weave``,
            or ``.loom`` extension instead of reading the kind from the envelope.
        project: The ``project_root``'s project document, when the caller
            already loaded it; else it is read from the root.
        strict_params: Overrides every document's ``strict_params:``; see
            :func:`load_resolved`.

    Returns:
        The resolved config.

    Raises:
        ConfigError: Unsupported extension or a name/filename mismatch.
        VariableResolutionError: A ``fabric_context`` key names no Fabric
            value (``P112``).
        ConfigSchemaError: A declared param has no value (``P101``) or a value
            its type rejects (``P102``, ``P103``).
        VariableResolutionError: A variable has no value and no default, or
            (raised or recorded) a strict scope references an undeclared
            param (``P120``) or the strict root is given an unused runtime
            param (``P121``).
    """
    check_fabric_context(fabric_context)
    root, project = resolve_project_root(location, project_root, project)
    config_type, validated = load_document(
        location, require_typed_extension=require_typed_extension
    )
    config_dict = validated.model_dump(exclude_unset=True)

    errors_before = errors_recorded()
    project_file = None if project is None else root.join(PROJECT_FILE)
    walk = WalkState.start(
        config_dict,
        file=str(location),
        kind=config_type,
        runtime_params=runtime_params,
        fabric_context=fabric_context,
        strict_override=strict_params,
        project=project,
        project_file=None if project_file is None else str(project_file),
    )
    project_tier, project_scope, project_connections = _project_layers(
        project, project_file, walk, root
    )

    inline_scopes: ScopePositions = {}
    try:
        resolved = resolve_config_variables(
            config_dict, config_type, walk, inline_scopes=inline_scopes
        )
    except VariableResolutionError as exc:
        unresolved = [d.code for d in exc.diagnostics] == ["P110"]
        reference = getattr(exc, "config_key", None) or ""
        if config_type == "thread" and unresolved and reference.startswith("param."):
            message = (
                f"{exc}. This thread may be a template designed for "
                f"use with 'as' and 'params' on a ThreadEntry in a weave."
            )
            raise VariableResolutionError(
                message,
                config_key=getattr(exc, "config_key", None),
                diagnostics=[replace(d, message=message) for d in exc.diagnostics],
            ) from exc
        raise

    data = resolved
    # Child files resolve with the root's Fabric context: one load, one session.
    children = resolve_references(
        resolved,
        config_type,
        root,
        walk,
        authored=config_dict,
        inline_scopes=inline_scopes,
    )

    # Execution-scope warnings come from the PRE-cascade dicts: after the
    # cascade an authored execution block is indistinguishable from one
    # injected by defaults.execution.
    warnings = list(walk.warnings)
    if config_type in COMPONENT_TYPES:
        walk_complete = errors_recorded() == errors_before
        warnings.extend(param_use_diagnostics(walk, walk_complete=walk_complete))
        warnings.extend(
            collect_execution_warnings(config_type, data, children, project_tier=project_tier)
        )
    # Dimension override rules apply to what each thread declares itself, so
    # they also run before the cascade adds loom/weave write and keys defaults.
    if config_type in COMPONENT_TYPES:
        check_dimension_overrides(config_type, data, children, location)
    data = _apply_cascade(config_type, data, children, project_tier)

    if config_type == "thread":
        reject_foreach_in_with(data)
        if isinstance(data.get("steps"), list):
            data["steps"] = expand_foreach(data["steps"])

    if detect_config_type_from_extension(location) is not None:
        declared_name = data.get("name", "")
        if declared_name and declared_name != location.stem:
            message = (
                f"Declared name '{declared_name}' does not match filename stem '{location.stem}'"
            )
            raise ConfigError(
                message,
                file_path=str(location),
                diagnostics=[
                    diagnostic(
                        "C205",
                        message,
                        file=str(location),
                        pointer="/name",
                        expected=[location.stem],
                    )
                ],
            )
        if not declared_name:
            data["name"] = location.stem
        data["qualified_key"] = compute_qualified_key(location, root)
    elif not data.get("name"):
        data["name"] = location.stem

    # Children are joined during hydration (hydrate_children's prepare hook),
    # so only the top-level config is joined here.
    resolve_source_paths(data, root)

    return ResolvedConfig(
        location=location,
        project_root=root,
        config_type=config_type,
        config_name=data["name"],
        data=data,
        children=children,
        authored=config_dict,
        warnings=warnings,
        project=project,
        provenance=walk.provenance,
        project_scope=project_scope,
        project_connections=project_connections,
    )


def hydrate_resolved(resolved: ResolvedConfig) -> tuple[ResolvedDocument, list[Diagnostic]]:
    """Hydrate a resolved thread, weave, or loom and the children it pulls in.

    Hydration consumes ``resolved``: child dicts in ``resolved.data`` are
    prepared in place, so hydrate each resolved config once.

    Args:
        resolved: The output of :func:`resolve_config` for a component config.

    Returns:
        ``(document, warnings)``: the resolved config tree, and the load
        warnings in emission order.

    Raises:
        ModelValidationError: If the config or any child fails model validation.
    """
    lookup_warnings: list[Diagnostic] = []
    project_scope = resolved.project_scope or ScopeRegistries()
    if resolved.config_type == "thread":
        # A standalone thread sees its own lookups over the project's at run time
        check_resolve_lookups(
            resolved.data,
            project_scope.lookups,
            resolved.location,
            resolved.config_name,
            lookup_warnings,
            standalone=True,
        )
        check_column_set_references(
            resolved.data,
            project_scope.column_sets,
            resolved.location,
            resolved.config_name,
            lookup_warnings,
            standalone=True,
        )
    model = hydrate_model(resolved.data, resolved.config_type, resolved.location)
    root = resolved.project_root
    if isinstance(model, Thread):
        bind_thread_warp(model, resolved.location, root)
    weaves, threads = hydrate_children(
        resolved.config_type,
        resolved.data,
        resolved.children,
        resolved.location,
        resolved.config_name,
        prepare=lambda child_dict: resolve_source_paths(child_dict, root),
        warnings=lookup_warnings,
        bind=lambda thread, source: bind_thread_warp(thread, location_like(source, root), root),
        outer=project_scope,
    )

    warnings = list(resolved.warnings)
    # Trace composition needs hydrated models (model_fields_set): a weave's
    # explicit trace: true under a traceless loom is a no-op.
    if isinstance(model, Loom):
        warnings.extend(
            trace_composition_diagnostics(
                model.execution,
                {name: w.execution for name, w in weaves.items()},
            )
        )

    warnings.extend(lookup_warnings)

    document = _assemble_document(resolved, model, weaves, threads)
    check_incremental(resolved, document, warnings)
    return document, warnings


def check_incremental(
    resolved: ResolvedConfig, document: ResolvedDocument, warnings: list[Diagnostic]
) -> None:
    """Run the incremental-load checks on every thread the config pulls in.

    Each thread is checked as the engine runs it (hydrated, after the
    cascade), except that the ``${param.*}`` rule reads its steps and
    sources as written, before substitution.

    Raises:
        ConfigError: For an incremental setting that can never run, naming
            the thread and the file it is written in.
    """
    for thread, authored in _threads_with_authored(resolved, document):
        model = thread.model
        if not isinstance(model, Thread) or model.load is None or model.load.mode == "full":
            continue
        errors, advisories = partition_diagnostics(incremental_thread_diagnostics(model, authored))
        if errors:
            located = in_thread(errors, thread.name, file=thread.source_ref or None)
            raise ConfigError(
                "; ".join(d.message for d in located),
                file_path=thread.source_ref,
                diagnostics=located,
            )
        warnings.extend(in_thread(advisories, thread.name))


def _threads_with_authored(
    resolved: ResolvedConfig, document: ResolvedDocument
) -> list[tuple[ResolvedDocument, dict[str, Any] | None]]:
    """Each thread node of the tree with its dict as written."""
    if document.kind == "thread":
        return [(document, resolved.authored)]
    if document.kind == "weave":
        weaves = [(document, resolved.children)]
    else:
        weave_nodes = {child_name(node): node for node in resolved.children}
        weaves = [
            (weave, weave_nodes[name].children)
            for name, weave in document.children.items()
            if name in weave_nodes
        ]
    pairs: list[tuple[ResolvedDocument, dict[str, Any] | None]] = []
    for weave, nodes in weaves:
        by_name = {child_name(node): node for node in nodes}
        pairs.extend(
            (thread, by_name[name].authored)
            for name, thread in weave.children.items()
            if name in by_name
        )
    return pairs


def _assemble_document(
    resolved: ResolvedConfig,
    model: Thread | Weave | Loom,
    weaves: dict[str, Weave],
    threads: dict[str, dict[str, Thread]],
) -> ResolvedDocument:
    """Build the document tree from the hydrated models and the resolved children.

    Children are paired with the parent's typed entries by effective name;
    a child a collecting load skipped after it failed gets no node.
    """
    top_file = str(resolved.location)

    def thread_docs(
        weave: Weave, by_name: dict[str, Thread], nodes: list[ResolvedChild], enclosing: str
    ) -> dict[str, ResolvedDocument]:
        node_by_name = {child_name(node): node for node in nodes}
        docs: dict[str, ResolvedDocument] = {}
        for entry in weave.threads:
            name = entry.effective_name
            thread, node = by_name.get(name), node_by_name.get(name)
            if thread is None or node is None:
                continue  # skipped by a collecting load after it failed
            docs[name] = ResolvedDocument(
                model=thread,
                kind="thread",
                name=name,
                entry=EntryMeta.from_entry(entry),
                source_ref=node.source_path or enclosing,
                provenance=Provenance(params=node.provenance),
            )
        return docs

    children: dict[str, ResolvedDocument] = {}
    if isinstance(model, Weave):
        children = thread_docs(
            model, threads.get(resolved.config_name, {}), resolved.children, top_file
        )
    elif isinstance(model, Loom):
        weave_nodes = {child_name(node): node for node in resolved.children}
        for entry in model.weaves:
            name = entry.effective_name
            weave, node = weaves.get(name), weave_nodes.get(name)
            if weave is None or node is None:
                continue  # skipped by a collecting load after it failed
            weave_file = node.source_path or top_file
            children[name] = ResolvedDocument(
                model=weave,
                kind="weave",
                name=name,
                entry=EntryMeta.from_entry(entry),
                children=thread_docs(weave, threads.get(name, {}), node.children, weave_file),
                source_ref=weave_file,
                provenance=Provenance(params=node.provenance),
            )

    return ResolvedDocument(
        model=model,
        kind=_KINDS[type(model)],
        name=resolved.config_name,
        children=children,
        source_ref=top_file,
        project=resolved.project,
        project_root=str(resolved.project_root),
        project_scope=resolved.project_scope,
        project_connections=resolved.project_connections,
        provenance=Provenance(params=resolved.provenance),
    ).with_scopes()


def bind_thread_warp(thread: Thread, thread_file: ConfigLocation, root: ConfigLocation) -> None:
    """Resolve a thread's warp from its file's directory and the project root.

    An explicit ``target.warp`` is searched by name; with no ``target.warp``
    a warp named after the target is discovered; ``warp: false`` binds none.

    The warp search runs at load so a missing explicit warp fails before any
    data is read, and so the engine never has to guess config locations
    from the target path.

    Args:
        thread: The hydrated thread; receives a :class:`WarpBinding`.
        thread_file: The thread's config file (the enclosing file for inline
            threads).
        root: The project root.

    Raises:
        ConfigError: If an explicit ``target.warp`` cannot be found.
    """
    thread_dir = thread_file.parent
    found = None
    with attribute_errors(thread_file):
        if isinstance(thread.target.warp, str):
            found = find_warp(thread.target.warp, thread_dir, root)
        elif thread.target.warp is None:
            found = discover_warp(thread.target, thread_dir, root)
    thread.bind_warp(
        WarpBinding(
            warp=found[0] if found else None,
            warp_file=str(found[1]) if found else None,
            thread_dir=str(thread_dir),
            writable=isinstance(thread_dir, LocalConfigLocation),
        )
    )


def load_resolved_config(
    location: ConfigLocation,
    *,
    runtime_params: dict[str, Any] | None = None,
    project_root: ConfigLocation | None = None,
    fabric_context: dict[str, Any] | None = None,
    project: Project | None = None,
    strict_params: bool | None = None,
) -> tuple[ResolvedDocument, list[Diagnostic]]:
    """Load a typed thread, weave, or loom file with its resolved children.

    The engine-facing entry point: ``Context`` calls it with its project root
    and Fabric context. Errors name the file they were found in; every
    problem in the project is raised together in one error. Warnings are
    returned, not logged.

    Args:
        location: The config file (``.thread``, ``.weave``, or ``.loom``).
        runtime_params: Runtime parameter overrides.
        project_root: Root for child references and Source paths. Defaults to
            the nearest folder above the file holding ``weevr.yaml``, else
            the file's directory.
        fabric_context: The Fabric values for ``${fabric.x}``; ``None`` when
            the load has none (each reference is then left in place, ``P113``).
            See :func:`load_resolved`.
        project: The ``project_root``'s project document, when the caller
            already loaded it.
        strict_params: Overrides every document's ``strict_params:``; see
            :func:`load_resolved`.

    Returns:
        ``(document, warnings)``: the resolved config tree and the load
        warnings.
    """
    with collecting(), attribute_errors(location):
        return hydrate_resolved(
            resolve_config(
                location,
                runtime_params=runtime_params,
                project_root=project_root,
                fabric_context=fabric_context,
                require_typed_extension=True,
                project=project,
                strict_params=strict_params,
            )
        )


def collect_resolved_config(
    location: ConfigLocation,
    *,
    runtime_params: dict[str, Any] | None = None,
    project_root: ConfigLocation | None = None,
    fabric_context: dict[str, Any] | None = None,
    project: Project | None = None,
    strict_params: bool | None = None,
) -> tuple[ResolvedDocument | None, list[Diagnostic], list[Diagnostic]]:
    """Load like :func:`load_resolved_config`, returning problems instead of raising.

    For validate mode, which reports every problem. Files that fail are left
    out of the tree; the rest load as usual.

    Args:
        location: The config file (``.thread``, ``.weave``, or ``.loom``).
        runtime_params: Runtime parameter overrides.
        project_root: Root for child references and Source paths. Defaults to
            the nearest folder above the file holding ``weevr.yaml``, else
            the file's directory.
        fabric_context: The Fabric values for ``${fabric.x}``; ``None`` when
            the load has none (each reference is then left in place, ``P113``).
            See :func:`load_resolved`.
        project: The ``project_root``'s project document, when the caller
            already loaded it.
        strict_params: Overrides every document's ``strict_params:``; see
            :func:`load_resolved`.

    Returns:
        ``(document, warnings, errors)``: the tree, ``None`` when the file
        itself failed; the load warnings; and the error diagnostics, ordered
        by file and position.
    """
    errors: list[ConfigError] = []
    loaded: tuple[ResolvedDocument, list[Diagnostic]] | None = None
    with collecting(into=errors), attribute_errors(location):
        loaded = hydrate_resolved(
            resolve_config(
                location,
                runtime_params=runtime_params,
                project_root=project_root,
                fabric_context=fabric_context,
                require_typed_extension=True,
                project=project,
                strict_params=strict_params,
            )
        )
    found = diagnostics_of(errors)
    if loaded is None:
        return None, [], found
    return loaded[0], loaded[1], found


def load_resolved(
    path: str | Path | ConfigLocation,
    runtime_params: dict[str, Any] | None = None,
    project_root: Path | ConfigLocation | None = None,
    *,
    strict_params: bool | None = None,
    fabric_context: dict[str, Any] | None = None,
) -> Any:  # weevr.result.LoadedConfig; importing it here would close an import cycle
    """Load a thread, weave, or loom with its resolved child threads and weaves.

    Runs the same load as :func:`load_config` and ``Context.load`` — params,
    variables, references, the inheritance cascade, ``foreach`` expansion,
    and model validation of every child — without Spark. Use it to inspect or
    plan a config: the result exposes the hydrated model, each weave's
    threads, the load warnings, and the execution plan.

    Only ``.thread``, ``.weave``, and ``.loom`` files are accepted.

    Args:
        path: The config file. Local paths are wrapped automatically; remote
            URIs must be supplied as a :class:`RemoteConfigLocation`.
        runtime_params: Optional runtime parameter overrides.
        project_root: The project directory. Child references, relative
            file-Source paths, and ``qualified_key`` resolve against it. When
            omitted, the nearest folder above the file holding ``weevr.yaml``
            is the root; with none, the file's directory is, and the result's
            ``project`` is ``None``.
        strict_params: Overrides every document's ``strict_params:`` and the
            project's. ``True`` fails the load on a ``${param.x}`` no
            declaration in its chain covers (``P120``) and on a runtime param
            nothing declares or references (``P121``); ``False`` keeps both
            warnings even where a document sets ``strict_params: true``.
            ``None`` (the default) leaves each document's setting, most
            specific wins; a document that sets none inherits, at the root
            from ``weevr.yaml``'s ``strict_params``, and absent everywhere is
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
        A :class:`~weevr.result.LoadedConfig` with ``model``, ``document``
        (the resolved config tree), ``project`` and ``project_root``,
        ``warnings``, and ``execution_plan``.
        Warnings are also logged on the ``weevr.config`` logger.

    Raises:
        ConfigError: Unsupported extension or a name/filename mismatch, plus
            every error :func:`load_config` raises.
    """
    # Imported here, not at module level: weevr.result reaches the engine
    # wheel's weevr.engine package, which needs weevr.config fully imported.
    from weevr.result import LoadedConfig

    if project_root is None or isinstance(project_root, ConfigLocation):
        root = project_root
    else:
        root = LocalConfigLocation(Path(project_root))
    document, warnings = load_resolved_config(
        make_location(path),
        runtime_params=runtime_params,
        project_root=root,
        fabric_context=fabric_context,
        strict_params=strict_params,
    )
    for warning in render_diagnostics(warnings):
        logger.warning(warning)
    return LoadedConfig.from_document(document, warnings)


_LOOKUPS: TypeAdapter[dict[str, Lookup]] = TypeAdapter(dict[str, Lookup])
_COLUMN_SETS: TypeAdapter[dict[str, ColumnSet]] = TypeAdapter(dict[str, ColumnSet])
_CONNECTIONS: TypeAdapter[dict[str, OneLakeConnection]] = TypeAdapter(dict[str, OneLakeConnection])


def _project_layers(
    project: Project | None,
    project_file: ConfigLocation | None,
    walk: WalkState,
    root: ConfigLocation,
) -> tuple[CascadeTier | None, ScopeRegistries | None, dict[str, OneLakeConnection] | None]:
    """The project's cascade tier and registry layer, its blocks resolved in its own scope.

    Its ``defaults`` are checked under ``weevr.yaml``, so a malformed block is
    reported there rather than against the config being loaded. Relative
    file paths in its lookups and column sets join ``root``, as a loom's do.

    Returns:
        ``(tier, registries, connections)``: the cascade tier from its
        ``defaults``, ``audit_templates`` and ``connections``; the registry
        layer from its ``lookups`` and ``column_sets``; and its connections
        hydrated. Each is ``None`` when the project declares none of its
        blocks.
    """
    if project is None or project_file is None:
        return None, None, None
    file = str(project_file)
    with attribute_errors(project_file):
        blocks = walk.project_tier(project, file)
        check_defaults_shape(blocks.get("defaults"), "project")
    resolve_source_paths(blocks, root)
    tier = None
    if blocks.keys() & {"defaults", "audit_templates", "connections"}:
        tier = CascadeTier(
            "project",
            defaults=blocks.get("defaults"),
            audit_templates=blocks.get("audit_templates"),
            connections=blocks.get("connections"),
        )
    lookups = _project_block(blocks, "lookups", _LOOKUPS, file)
    column_sets = _project_block(blocks, "column_sets", _COLUMN_SETS, file)
    registries = None
    if lookups or column_sets:
        registries = ScopeRegistries(lookups=lookups or None, column_sets=column_sets or None)
    connections = _project_block(blocks, "connections", _CONNECTIONS, file)
    return tier, registries, connections or None


def _project_block[V](
    blocks: dict[str, Any], block: str, adapter: TypeAdapter[dict[str, V]], file: str
) -> dict[str, V] | None:
    """Hydrate one resolved project registry block; ``None`` when the project has none.

    Raises:
        ModelValidationError: A substituted value the block's model rejects,
            reported under ``weevr.yaml``.
    """
    body = blocks.get(block)
    if body is None:
        return None
    try:
        return adapter.validate_python(body)
    except ValidationError as exc:
        raise ModelValidationError(
            f"Project {block} failed validation: {exc}",
            cause=exc,
            file_path=file,
            diagnostics=place_all(
                from_validation_error(exc, body, file=file), file, body, (block,)
            ),
        ) from exc


def _tier(level: str, data: dict[str, Any]) -> CascadeTier:
    """The cascade tier a loom's or weave's resolved dict contributes."""
    return CascadeTier(
        level,
        defaults=data.get("defaults"),
        audit_templates=data.get("audit_templates"),
        connections=data.get("connections"),
    )


def _apply_cascade(
    config_type: str,
    data: dict[str, Any],
    children: list[ResolvedChild],
    project_tier: CascadeTier | None = None,
) -> dict[str, Any]:
    """Cascade project, loom and weave defaults and registries into each thread's dict.

    A referenced weave's ``defaults`` are checked under its own file, so a
    malformed block is reported there rather than against the loom. A
    standalone thread is cascaded under the project's tier alone, or under
    none, so its own audit settings apply the same either way.

    Returns:
        ``data``, cascaded itself when it is a thread.
    """
    outer = [] if project_tier is None else [project_tier]
    if config_type == "loom":
        for weave in children:
            if weave.source_path is not None:
                with attribute_errors(weave.source_path):
                    check_defaults_shape(weave.data.get("defaults"), "weave")
            tiers = [*outer, _tier("loom", data), _tier("weave", weave.data)]
            for thread in weave.children:
                thread.data = apply_tiers(tiers, thread.data)

    elif config_type == "weave":
        tiers = [*outer, _tier("weave", data)]
        for thread in children:
            thread.data = apply_tiers(tiers, thread.data)

    elif config_type == "thread":
        return apply_tiers(outer, data)
    return data


def check_dimension_overrides(
    config_type: str,
    resolved: dict[str, Any],
    children: list[ResolvedChild],
    location: ConfigLocation,
) -> None:
    """Reject forbidden write/keys fields on dimension threads, from pre-cascade dicts.

    Raises:
        ConfigError: Naming the field and the thread's own file (the enclosing
            file for an inline thread).
    """
    for thread, thread_file, at in _pre_cascade_threads(
        config_type, resolved, children, str(location)
    ):
        errors = [d for d in dimension_override_diagnostics(thread) if d.is_error]
        if errors:
            located = fill_lines(
                [
                    replace(
                        d,
                        file=thread_file,
                        node=thread.get("name") or None,
                        pointer=json_pointer(at + parse_pointer(d.resolved_path or "")),
                    )
                    for d in errors
                ],
                thread_file,
            )
            raise ConfigError(
                "; ".join(d.message for d in located),
                file_path=thread_file,
                diagnostics=located,
            )


def collect_execution_warnings(
    config_type: str,
    resolved: dict[str, Any],
    children: list[ResolvedChild],
    *,
    project_tier: CascadeTier | None = None,
) -> list[Diagnostic]:
    """Gather thread-scope execution warnings from pre-cascade dicts and the project's defaults."""
    defaults_by_scope: list[tuple[str, dict[str, Any] | None]] = []
    if project_tier is not None:
        defaults_by_scope.append(("project", project_tier.defaults))
    if config_type == "loom":
        loom_label = f"loom '{resolved.get('name') or '(unnamed)'}'"
        defaults_by_scope.append((loom_label, resolved.get("defaults")))
        for i, weave in enumerate(children):
            weave_label = f"weave '{weave.data.get('name') or f'#{i}'}'"
            defaults_by_scope.append((weave_label, weave.data.get("defaults")))
    elif config_type == "weave":
        weave_label = f"weave '{resolved.get('name') or '(unnamed)'}'"
        defaults_by_scope.append((weave_label, resolved.get("defaults")))

    threads = [thread for thread, _, _ in _pre_cascade_threads(config_type, resolved, children, "")]
    return execution_scope_diagnostics(defaults_by_scope, threads)


def _pre_cascade_threads(
    config_type: str,
    resolved: dict[str, Any],
    children: list[ResolvedChild],
    enclosing: str,
) -> list[tuple[dict[str, Any], str, tuple[str | int, ...]]]:
    """Every thread dict a config pulls in, with its file and where it sits there.

    The file is the thread's own, else the referenced weave file that holds
    it inline, else ``enclosing`` (the top-level config). The position is
    empty for a thread in its own file, else the path to the inline thread
    (through an inline weave, when the weave is inline too).
    """
    if config_type == "loom":
        return [
            (
                thread.data,
                thread.source_path or weave.source_path or enclosing,
                () if thread.source_path else (() if weave.source_path else weave.at) + thread.at,
            )
            for weave in children
            for thread in weave.children
        ]
    if config_type == "weave":
        return [
            (thread.data, thread.source_path or enclosing, () if thread.source_path else thread.at)
            for thread in children
        ]
    return [(resolved, enclosing, ())]


def compute_qualified_key(file_path: ConfigLocation, project_root: ConfigLocation) -> str:
    """Return ``file_path`` expressed relative to ``project_root``.

    Falls back to the absolute string when the file lies outside the
    project root.
    """
    if isinstance(file_path, LocalConfigLocation) and isinstance(project_root, LocalConfigLocation):
        try:
            rel = file_path.path.resolve().relative_to(project_root.path.resolve())
            return str(rel)
        except (OSError, ValueError):
            return str(file_path)
    if isinstance(file_path, RemoteConfigLocation) and isinstance(
        project_root, RemoteConfigLocation
    ):
        file_str = str(file_path)
        root_str = str(project_root).rstrip("/")
        if file_str.startswith(root_str + "/"):
            return file_str[len(root_str) + 1 :]
        return file_str
    return str(file_path)


def resolve_source_paths(
    config_dict: dict[str, Any],
    project_root: ConfigLocation | Path | str,
) -> None:
    """Resolve relative source file paths against the project root.

    Modifies the config dict in place, prepending the project root to
    any relative ``path`` values in file-based sources (csv, json,
    parquet): the entries under ``sources``, the ``source`` of
    each entry under ``lookups``, and the ``source`` of each YAML entry
    under ``column_sets``. Absolute paths and URIs are left as they are.

    Args:
        config_dict: Thread, weave, or loom configuration dict
            (pre-hydration).
        project_root: Resolved project root. Accepts a
            :class:`ConfigLocation`, :class:`pathlib.Path`, or string
            for backward compatibility with internal callers.
    """
    # Normalize legacy Path/str inputs into ConfigLocation form so the
    # join logic only has one branch. Strings that look like URIs are
    # left as plain strings — only the local Path branch needs the FUSE
    # post-processing below.
    if isinstance(project_root, ConfigLocation):
        location: ConfigLocation | None = project_root
        legacy_str_root: str | None = None
    elif isinstance(project_root, Path):
        location = LocalConfigLocation(project_root)
        legacy_str_root = None
    else:
        # Plain string — preserve the original abfss-style concat path
        # for callers that still hand in a raw URI string.
        location = None
        legacy_str_root = project_root

    source_cfgs: list[Any] = []
    sources = config_dict.get("sources")
    if isinstance(sources, dict):
        source_cfgs.extend(sources.values())
    lookups = config_dict.get("lookups")
    if isinstance(lookups, dict):
        source_cfgs.extend(
            lookup_cfg.get("source")
            for lookup_cfg in lookups.values()
            if isinstance(lookup_cfg, dict)
        )
    # Only a YAML column set has a path; Delta ones name a table and are skipped below.
    column_sets = config_dict.get("column_sets")
    if isinstance(column_sets, dict):
        source_cfgs.extend(
            column_set_cfg.get("source")
            for column_set_cfg in column_sets.values()
            if isinstance(column_set_cfg, dict)
        )

    for source_cfg in source_cfgs:
        if not isinstance(source_cfg, dict):
            continue
        path = source_cfg.get("path")
        # Only string paths are joined; any other type fails model validation.
        if not isinstance(path, str):
            continue
        # Skip already-absolute paths and URIs
        if os.path.isabs(path) or "://" in path:
            continue

        if location is not None:
            joined = location.join(path)
            resolved = str(joined)
            # Fabric mount paths (/lakehouse/default/...) are valid for
            # Python file I/O but not for Spark reads which bypass the
            # FUSE mount. Strip the mount prefix so Spark gets a
            # lakehouse-relative path (e.g. "Files/project.weevr/...").
            if isinstance(location, LocalConfigLocation) and resolved.startswith(
                "/lakehouse/default/"
            ):
                resolved = resolved[len("/lakehouse/default/") :]
            source_cfg["path"] = resolved
        else:
            assert legacy_str_root is not None
            source_cfg["path"] = f"{legacy_str_root.rstrip('/')}/{path}"
