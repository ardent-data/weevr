"""Which project lookups and column sets the threads of a run use.

A project registry entry is shared by every loom in the project, so the
scope that materializes it reads only the entries a running thread names.
Spark-free: the engine calls these to decide what to read.
"""

from __future__ import annotations

from collections.abc import Mapping

from weevr.config.document import ResolvedDocument, ScopeRegistries
from weevr.engine.planner import resolve_lookup_names
from weevr.model.column_set import ColumnSet
from weevr.model.lookup import Lookup
from weevr.model.pipeline import RenameStep
from weevr.model.thread import Thread


def _lookup_names_used(thread: Thread) -> set[str]:
    """Lookup names a thread reads: its lookup-bound sources and its resolve steps.

    A resolve name that is one of its lookup-bound source aliases is that
    source's lookup, already counted.
    """
    bound = {alias for alias, src in thread.sources.items() if src.lookup is not None}
    names = {src.lookup for src in thread.sources.values() if src.lookup is not None}
    return names | (resolve_lookup_names(thread) - bound)


def _column_set_names_used(thread: Thread) -> set[str]:
    """Column set names a thread's rename steps read, in every pipeline."""
    steps = list(thread.steps)
    for sub in (thread.with_ or {}).values():
        steps.extend(sub.steps)
    return {
        step.rename.column_set
        for step in steps
        if isinstance(step, RenameStep) and step.rename.column_set is not None
    }


def _project_names_used[V](
    project: Mapping[str, V] | None, effective: Mapping[str, V] | None, names: set[str]
) -> dict[str, V]:
    """The project definitions among ``names`` that ``effective`` resolves to.

    A name a lower scope declares resolves to that declaration, not the
    project's, so it is not a use of the project's.
    """
    found: dict[str, V] = {}
    for name in names:
        definition = (project or {}).get(name)
        if definition is not None and (effective or {}).get(name) is definition:
            found[name] = definition
    return found


def used_project_registries(
    document: ResolvedDocument, threads: Mapping[str, Mapping[str, Thread]] | None = None
) -> ScopeRegistries:
    """The project lookups and column sets the threads of a run use.

    A thread uses a project definition when it names it (in a lookup-bound
    source, a resolve step or a rename step) and the name resolves to the
    project's, rather than to one a loom, weave or thread declares. Only
    these are read where the run materializes them: a project lookup or
    column set no thread uses is never read.

    Args:
        document: The resolved config tree of the run; its root carries the
            project's registries.
        threads: The threads that run, by weave name then effective name;
            ``None`` counts every thread in the tree.

    Returns:
        The used project lookups and column sets; empty without a project
        registry layer.
    """
    project = document.project_scope
    if project is None:
        return ScopeRegistries()
    if document.kind == "thread":
        nodes = [document]
    else:
        weaves = {document.name: document} if document.kind == "weave" else dict(document.children)
        nodes = [
            node
            for weave_name, weave in weaves.items()
            for name, node in weave.children.items()
            if threads is None or name in threads.get(weave_name, {})
        ]
    lookups: dict[str, Lookup] = {}
    column_sets: dict[str, ColumnSet] = {}
    for node in nodes:
        if not isinstance(node.model, Thread):
            continue
        lookups.update(
            _project_names_used(
                project.lookups, node.effective.lookups, _lookup_names_used(node.model)
            )
        )
        column_sets.update(
            _project_names_used(
                project.column_sets, node.effective.column_sets, _column_set_names_used(node.model)
            )
        )
    return ScopeRegistries(lookups=lookups or None, column_sets=column_sets or None)


def without_unused_project_column_sets(
    column_sets: Mapping[str, ColumnSet] | None,
    project: ScopeRegistries | None,
    used: ScopeRegistries,
) -> dict[str, ColumnSet] | None:
    """A scope's effective column sets less the project's that no thread of the run uses.

    The scope that materializes column sets resolves each one it is handed,
    so an unused project column set is left out to keep it from being read.
    """
    if column_sets is None:
        return None
    project_sets = (project.column_sets if project is not None else None) or {}
    used_sets = used.column_sets or {}
    return {
        name: column_set
        for name, column_set in column_sets.items()
        if name in used_sets or project_sets.get(name) is not column_set
    } or None
