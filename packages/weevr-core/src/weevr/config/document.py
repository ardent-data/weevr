"""The resolved config tree a load returns.

A load produces one :class:`ResolvedDocument` for the file it was given, with
a child document for each config that file pulls in: a loom's weaves, and
each weave's threads. Every node holds the hydrated model, and every child
records the parent entry that pulled it in as an :class:`EntryMeta`.

This module imports only :mod:`weevr.model`, so it loads without PySpark or
the engine wheel.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal

from weevr.model import Loom, Thread, Weave
from weevr.model.loom import WeaveEntry
from weevr.model.weave import ConditionSpec, ThreadEntry

ConfigKind = Literal["thread", "weave", "loom"]


@dataclass(frozen=True)
class EntryMeta:
    """How a weave or loom entry pulled in a child config.

    Attributes:
        ref: The referenced file, relative to the project root; ``None`` for
            an inline child or a weave found by name.
        alias: The entry's ``as`` name, which becomes the child's effective
            name (threads only).
        params: Values the entry passes to a template thread's ``params``
            (threads only).
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
            return cls(ref=entry.ref, condition=entry.condition)
        return cls(
            ref=entry.ref,
            alias=entry.as_,
            params=entry.params,
            dependencies=tuple(entry.dependencies) if entry.dependencies is not None else None,
            condition=entry.condition,
        )


def _frozen_children() -> Mapping[str, ResolvedDocument]:
    return MappingProxyType({})


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
    """

    model: Thread | Weave | Loom
    kind: ConfigKind
    name: str
    entry: EntryMeta | None = None
    children: Mapping[str, ResolvedDocument] = field(default_factory=_frozen_children)
    source_ref: str = ""

    def __post_init__(self) -> None:
        """Freeze ``children`` so the tree can't be changed through it."""
        object.__setattr__(self, "children", MappingProxyType(dict(self.children)))

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
