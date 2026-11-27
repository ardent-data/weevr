"""The resolved config tree a load returns.

A load produces one :class:`ResolvedDocument` for the file it was given, with
a child document for each config that file pulls in: a loom's weaves, and
each weave's threads. Every node holds the hydrated model, every child
records the parent entry that pulled it in as an :class:`EntryMeta`, and every
node records where its param values came from as its :class:`Provenance`.

It imports only :mod:`weevr.model`, the registry cascade and the provenance
records, so it loads without PySpark or the engine wheel.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, Literal

from weevr.config.inheritance import cascade_registry
from weevr.config.param_provenance import ParamProvenance
from weevr.model import Loom, Thread, Weave
from weevr.model.column_set import ColumnSet
from weevr.model.connection import OneLakeConnection
from weevr.model.lookup import Lookup
from weevr.model.loom import WeaveEntry
from weevr.model.project import Project
from weevr.model.variable import VariableSpec
from weevr.model.weave import ConditionSpec, ThreadEntry

ConfigKind = Literal["thread", "weave", "loom"]
_KINDS: dict[str, ConfigKind] = {"thread": "thread", "weave": "weave", "loom": "loom"}


@dataclass(frozen=True)
class EntryMeta:
    """How a weave or loom entry pulled in a child config.

    Attributes:
        ref: The referenced file, relative to the project root; ``None`` for
            an inline child or a weave found by name.
        alias: The entry's ``as`` name, which becomes the child's effective
            name (threads only).
        params: The entry's ``params``: values a ``ref:`` entry pins for its
            subtree (an inline child's own declarations).
        dependencies: Threads this one waits for, as declared on the entry
            (threads only).
        condition: The entry's run condition.
    """

    ref: str | None = None
    alias: str | None = None
    params: Mapping[str, Any] | None = None
    dependencies: tuple[str, ...] | None = None
    condition: ConditionSpec | None = None

    def __post_init__(self) -> None:
        """Freeze ``params`` so the entry can't be changed through it."""
        if self.params is not None:
            object.__setattr__(self, "params", MappingProxyType(dict(self.params)))

    @classmethod
    def from_entry(cls, entry: ThreadEntry | WeaveEntry) -> EntryMeta:
        """Build from a hydrated weave's thread entry or a loom's weave entry."""
        if isinstance(entry, WeaveEntry):
            return cls(ref=entry.ref, params=entry.params, condition=entry.condition)
        return cls(
            ref=entry.ref,
            alias=entry.as_,
            params=entry.params,
            dependencies=tuple(entry.dependencies) if entry.dependencies is not None else None,
            condition=entry.condition,
        )


def registry_as_dict[V](registry: Mapping[str, V] | None) -> dict[str, V] | None:
    """A registry as a plain, mutable dict; ``None`` (no registry) stays ``None``.

    For engine code that hands registries to functions taking ``dict``.
    """
    return dict(registry) if registry is not None else None


def _freeze(registry: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    return MappingProxyType(dict(registry)) if registry is not None else None


@dataclass(frozen=True)
class ScopeRegistries:
    """The named lookups, column sets and variables one scope has.

    Each is ``None`` when the scope has none. Read-only.

    Attributes:
        lookups: Lookups by name.
        column_sets: Column sets by name.
        variables: Variables by name.
    """

    lookups: Mapping[str, Lookup] | None = None
    column_sets: Mapping[str, ColumnSet] | None = None
    variables: Mapping[str, VariableSpec] | None = None

    def __post_init__(self) -> None:
        """Freeze the registries."""
        object.__setattr__(self, "lookups", _freeze(self.lookups))
        object.__setattr__(self, "column_sets", _freeze(self.column_sets))
        object.__setattr__(self, "variables", _freeze(self.variables))

    @classmethod
    def declared_by(cls, model: Thread | Weave | Loom) -> ScopeRegistries:
        """The registries a thread, weave or loom declares itself."""
        return cls(
            lookups=model.lookups,
            column_sets=model.column_sets,
            variables=model.variables,
        )

    def under(self, outer: ScopeRegistries, outer_level: str, level: str) -> ScopeRegistries:
        """These registries cascaded under an enclosing scope's: inner names win."""
        return ScopeRegistries(
            lookups=cascade_registry(outer.lookups, self.lookups, "lookup", outer_level, level),
            column_sets=cascade_registry(
                outer.column_sets, self.column_sets, "column_set", outer_level, level
            ),
            variables=cascade_registry(
                outer.variables, self.variables, "variable", outer_level, level
            ),
        )


def _frozen_children() -> Mapping[str, ResolvedDocument]:
    return MappingProxyType({})


def _frozen_params() -> Mapping[str, ParamProvenance]:
    return MappingProxyType({})


@dataclass(frozen=True)
class Provenance:
    """Where one config's resolved values came from, read at load. Read-only.

    Holds names and source kinds only, never the values themselves.

    Attributes:
        params: By param name, where this config's value came from: each
            name its scope binds, and each unbound name a ``${param.x:-d}``
            reference here read through its fallback. Each record
            (:class:`weevr.config.param_provenance.ParamProvenance`) gives the value
            source (``runtime``, ``pin``, ``inherited``, ``default`` or
            ``fallback``), the winning declaration's tier, file and pointer
            (``None`` for an undeclared name), and the pinning entry when the
            value traces to a pin. Empty for a tree built from models.
    """

    params: Mapping[str, ParamProvenance] = field(default_factory=_frozen_params)

    def __post_init__(self) -> None:
        """Freeze ``params``."""
        object.__setattr__(self, "params", MappingProxyType(dict(self.params)))


@dataclass(frozen=True)
class ResolvedDocument:
    """One loaded config and the configs it pulls in, fully resolved and hydrated.

    Attributes:
        model: The hydrated Thread, Weave, or Loom.
        kind: ``"thread"``, ``"weave"``, or ``"loom"``.
        name: The config's name: its effective name for a child (``as`` >
            name > file stem), else its declared name or file stem.
        entry: The parent entry that pulled this config in; ``None`` for the
            file that was loaded.
        children: A loom's weaves or a weave's threads, by effective name, in
            entry order. Read-only; empty for a thread.
        source_ref: The file the config is written in: its own file, or for
            an inline child, the file that holds it.
        effective: The lookups, column sets and variables this config sees:
            its own merged over every enclosing scope's, the project's
            outermost, its own names winning. Computed at load.
        project: The project the load ran under: the ``weevr.yaml`` at the
            project root; ``None`` when the root has none. Set on the loaded
            file's node only.
        project_root: The project root the load resolved references and
            Source paths against; ``None`` when unknown. Set on the loaded
            file's node only.
        project_scope: The project's lookups and column sets, resolved in
            the project's own scope: the outermost layer of every node's
            ``effective``. ``None`` when the project declares neither (or
            there is no project). Set on the loaded file's node only.
        project_connections: The project's connections, resolved in its own
            scope. A loom or weave materializes lookups and column sets with
            its own connections over these. ``None`` when the project
            declares none. Set on the loaded file's node only.
        provenance: Where this config's values came from: per param, its
            value source and winning declaration (:class:`Provenance`).
    """

    model: Thread | Weave | Loom
    kind: ConfigKind
    name: str
    entry: EntryMeta | None = None
    children: Mapping[str, ResolvedDocument] = field(default_factory=_frozen_children)
    source_ref: str = ""
    effective: ScopeRegistries = field(default_factory=ScopeRegistries)
    project: Project | None = None
    project_root: str | None = None
    project_scope: ScopeRegistries | None = None
    project_connections: Mapping[str, OneLakeConnection] | None = None
    provenance: Provenance = field(default_factory=Provenance)

    def __post_init__(self) -> None:
        """Freeze ``children`` so the tree can't be changed through it."""
        object.__setattr__(self, "children", MappingProxyType(dict(self.children)))

    @property
    def declared(self) -> ScopeRegistries:
        """The lookups, column sets and variables this config declares itself."""
        return ScopeRegistries.declared_by(self.model)

    def with_scopes(
        self, outer: ScopeRegistries | None = None, outer_level: str = ""
    ) -> ResolvedDocument:
        """Return this tree with every node's effective registries computed.

        Args:
            outer: The enclosing scope's effective registries; ``None`` for
                the loaded file itself, which sits under its
                ``project_scope`` when it has one.
            outer_level: The enclosing scope's kind, for shadowing logs.

        Returns:
            A copy of the tree with ``effective`` set on every node.
        """
        if outer is None and self.project_scope is not None:
            outer, outer_level = self.project_scope, "project"
        declared = self.declared
        effective = declared if outer is None else declared.under(outer, outer_level, self.kind)
        children = {
            name: child.with_scopes(effective, self.kind) for name, child in self.children.items()
        }
        return replace(self, children=children, effective=effective)

    @classmethod
    def from_models(
        cls,
        model: Thread | Weave | Loom,
        kind: str,
        name: str,
        *,
        threads: Mapping[str, Mapping[str, Any]] | None = None,
        weaves: Mapping[str, Any] | None = None,
    ) -> ResolvedDocument:
        """Assemble a tree from a model and its children by name.

        For callers that hold hydrated models rather than a load's tree.
        Children are matched to the model's entries by effective name: a
        child no entry names is left out, and an entry with no child gets no
        node. Thread and weave values may be models or their dicts. Effective
        registries are computed as a load computes them; ``source_ref`` is
        empty.

        Args:
            model: The top-level Thread, Weave, or Loom.
            kind: ``"thread"``, ``"weave"``, or ``"loom"``.
            name: The top-level config's name; a weave's threads are found
                under it in ``threads``.
            threads: Thread models by weave name, then effective name.
            weaves: A loom's weave models by name.

        Returns:
            The tree, with effective registries.
        """
        if kind not in _KINDS:
            raise ValueError(f"Unknown config kind {kind!r}")

        def as_thread(value: Any) -> Thread:
            return value if isinstance(value, Thread) else Thread.model_validate(value)

        def thread_docs(weave: Any, by_name: Mapping[str, Any]) -> dict[str, ResolvedDocument]:
            docs = {}
            for entry in getattr(weave, "threads", None) or []:
                if entry.effective_name in by_name:
                    docs[entry.effective_name] = cls(
                        model=as_thread(by_name[entry.effective_name]),
                        kind="thread",
                        name=entry.effective_name,
                        entry=EntryMeta.from_entry(entry),
                    )
            return docs

        children: dict[str, ResolvedDocument] = {}
        if kind == "weave":
            children = thread_docs(model, (threads or {}).get(name, {}))
        elif kind == "loom":
            for entry in getattr(model, "weaves", None) or []:
                weave = (weaves or {}).get(entry.effective_name)
                if weave is None:
                    continue
                weave = weave if isinstance(weave, Weave) else Weave.model_validate(weave)
                children[entry.effective_name] = cls(
                    model=weave,
                    kind="weave",
                    name=entry.effective_name,
                    entry=EntryMeta.from_entry(entry),
                    children=thread_docs(weave, (threads or {}).get(entry.effective_name, {})),
                )
        return cls(
            model=model,
            kind=_KINDS[kind],
            name=name,
            children=children,
        ).with_scopes()

    def weave_threads(self) -> dict[str, dict[str, Thread]]:
        """Each weave's thread models, by weave name then effective thread name.

        A weave gives one entry (itself); a loom one per weave; a thread none.
        """
        if self.kind == "weave":
            return {self.name: _thread_models(self)}
        if self.kind == "loom":
            return {name: _thread_models(weave) for name, weave in self.children.items()}
        return {}

    def weaves(self) -> dict[str, Weave]:
        """A loom's weave models by name; empty for a thread or weave."""
        if self.kind != "loom":
            return {}
        weaves: dict[str, Weave] = {}
        for name, weave in self.children.items():
            if isinstance(weave.model, Weave):
                weaves[name] = weave.model
        return weaves


def _thread_models(weave: ResolvedDocument) -> dict[str, Thread]:
    threads: dict[str, Thread] = {}
    for name, thread in weave.children.items():
        if isinstance(thread.model, Thread):
            threads[name] = thread.model
    return threads
