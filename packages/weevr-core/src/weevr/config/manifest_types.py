"""The param manifest's types: what running a config file asks of its invoker.

Kept apart from :mod:`weevr.config.manifest`, which builds them, so the
result types can name them without importing the config load.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = ["ManifestParam", "ParamManifest"]


@dataclass(frozen=True)
class ManifestParam:
    """One declared param spec, as written.

    Attributes:
        name: The param name.
        type: The declared type name; ``None`` when the declaration names none.
        required: Whether the spec is required (``True`` when not set).
        default: The declared default, as written; ``None`` when it has none.
        description: The spec's ``description``; ``None`` when not set.
        file: The declaring file.
        pointer: Where the declaration sits in ``file``, as a JSON pointer
            (``/params/region``, or ``/threads/0/params/region`` for an
            inline thread).
    """

    name: str
    type: str | None
    required: bool
    default: Any
    description: str | None
    file: str
    pointer: str


@dataclass(frozen=True)
class ParamManifest:
    """The params running one config file takes.

    Attributes:
        executable: The config's name (declared, else the file stem).
        kind: The config kind (``"thread"``, ``"weave"``, ``"loom"``).
        file: The config file.
        declared: Every param declaration in the file and the children it
            pulls in, in walk order. A file referenced more than once
            reports its declarations once; a re-declaration that shadows an
            ancestor's is reported beside it.
        invoker_required: Names the invoker must supply: a required spec in
            force in some scope with no default there and no entry pin above
            it binding the name. A child scope re-derives defaults from its
            own effective spec, so an ancestor's default does not satisfy a
            child that re-declares the name without one.
        undeclared_used: Names a ``${param.x}`` reference reads where no
            declaration in its scope chain covers the name and no entry pin
            above it binds it. A reference with a ``:-`` default counts.
    """

    executable: str
    kind: str
    file: str
    declared: tuple[ManifestParam, ...] = ()
    invoker_required: tuple[str, ...] = ()
    undeclared_used: tuple[str, ...] = ()
