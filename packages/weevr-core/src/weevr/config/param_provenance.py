"""Where a config scope's values came from, recorded at load without values.

The records a load attaches to each node of its tree
(:class:`weevr.config.document.Provenance`): per param name, the value source,
the winning declaration, and the pinning entry; per registry entry and
``defaults:`` key, the declaring tier and file. They hold names, kinds, files
and pointers only, never the values themselves.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from weevr.diagnostics.positions import json_pointer

ParamTier = Literal["project", "loom", "weave", "thread"]
"""The kind of config a param, registry entry or ``defaults:`` key is declared in."""

ParamSource = Literal["pin", "runtime", "inherited", "default", "fallback"]
"""Where one scope's value for a param came from; see :class:`ParamProvenance`."""


@dataclass(frozen=True)
class ParamDeclaration:
    """Where a param is declared.

    Attributes:
        tier: The kind of the declaring config; ``None`` when unknown.
        file: The declaring file; ``None`` when unknown.
        pointer: The JSON pointer of the declaration in ``file``
            (``/params/<name>``, under an inline child's position).
    """

    tier: ParamTier | None
    file: str | None
    pointer: str


@dataclass(frozen=True)
class ParamPin:
    """The entry that pinned a param's value.

    Attributes:
        entry: The pinning entry's effective name: its ``as`` (threads) or
            ``name``, else the child's own name.
        file: The file holding the entry; ``None`` when unknown.
        pointer: The JSON pointer of the pin in ``file``
            (``/.../params/<name>``).
    """

    entry: str
    file: str | None
    pointer: str


@dataclass(frozen=True)
class PinEntry:
    """A ``ref:`` entry whose ``params:`` pin values for its child.

    Attributes:
        name: The entry's effective name.
        file: The file holding the entry; ``None`` when unknown.
        at: Where the entry sits in ``file``.
    """

    name: str
    file: str | None
    at: tuple[str | int, ...] = ()

    def pin(self, param: str) -> ParamPin:
        """The pin of ``param`` on this entry."""
        return ParamPin(self.name, self.file, json_pointer((*self.at, "params", param)))


@dataclass(frozen=True)
class ParamProvenance:
    """Where one config scope's value for a param came from. Holds no value.

    Sources:

    - ``runtime``: supplied by the invoker's runtime params, at the run root
      (the loaded config's own scope).
    - ``pin``: pinned by the entry that pulled this config in.
    - ``inherited``: supplied above this scope (a runtime value or an
      ancestor entry's pin) and flowing down unchanged; coercion by this
      scope's own declaration still counts as inherited. :attr:`pin` tells
      the two apart: it is set when the value traces to a pin.
    - ``default``: the nearest declared default, re-derived in each scope.
    - ``fallback``: nothing bound the name here, and a ``${param.x:-d}``
      reference in this config used its fallback. A bound name read with a
      fallback keeps its binding's source.

    Attributes:
        source: Where the value came from.
        spec: The declaration whose spec won here (the nearest one); ``None``
            when no config in the scope chain declares the name.
        pin: The pinning entry when the value traces to a pin (``pin``, or
            ``inherited`` from an ancestor's pin); else ``None``.
    """

    source: ParamSource
    spec: ParamDeclaration | None = None
    pin: ParamPin | None = None


@dataclass(frozen=True)
class RegistryDeclaration:
    """Where a registry entry or ``defaults:`` key a config sees is declared. Holds no value.

    Attributes:
        tier: The kind of the declaring config (``project``, ``loom``,
            ``weave`` or ``thread``).
        file: The declaring file: ``weevr.yaml`` for the project, else the
            config's own file, or the file holding it when it is inline;
            ``None`` when unknown.
        pointer: The JSON pointer of the declaration in ``file``
            (``/connections/<name>``, ``/defaults/<key>``, under an inline
            child's position).
    """

    tier: ParamTier
    file: str | None
    pointer: str
