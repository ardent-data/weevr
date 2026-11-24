"""Per-scope param binding: declared specs cascade, supplied values flow down.

Each config scope (a thread, weave, or loom document, inline or in its own file)
opens a :class:`ParamScope` from its parent's. Declared ``params:`` specs merge
by name, the child's declaration shadowing its ancestors'. Values bind
closure-style; per name, highest first:

1. A pin: the referencing entry's ``params:`` value.
2. The inherited binding: a value supplied above this scope. At the run root
   that is the runtime params.
3. The nearest declared default: the effective spec's ``default``.

A required spec left with none of these fails ``P101`` at its declaration. An
optional spec with no default, like an undeclared name nobody supplied, stays
unbound: a ``${param.x}`` reference to it falls back to its ``:-`` default or
fails at load. A binding never changes after its scope opens; a child scope
re-derives defaults from its own effective specs, so a nearer default wins over
an ancestor's, while supplied values always beat defaults.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal

from weevr.diagnostics import Diagnostic, diagnostic
from weevr.diagnostics.positions import json_pointer
from weevr.errors import ConfigSchemaError

BindingSource = Literal["runtime", "pin", "default"]
"""Where a binding's value came from: supplied (``runtime``, ``pin``) or ``default``."""


@dataclass(frozen=True)
class Binding:
    """One bound param value.

    Attributes:
        value: The bound value.
        source: Where the value came from. Supplied values (``runtime``,
            ``pin``) flow into child scopes; a ``default`` is re-derived in
            each scope from its effective spec.
    """

    value: Any
    source: BindingSource

    @property
    def supplied(self) -> bool:
        """Whether the value was supplied rather than taken from a spec default."""
        return self.source != "default"


@dataclass(frozen=True)
class DeclaredSpec:
    """A ParamSpec and where it was declared.

    Attributes:
        spec: The ``ParamSpec`` or its ``model_dump`` mapping.
        file: The declaring file; ``None`` when unknown.
        at: Where the declaring document sits in ``file`` (empty for a
            document in its own file).
    """

    spec: Any
    file: str | None
    at: tuple[str | int, ...] = ()

    @property
    def default(self) -> Any:
        """The spec's default; ``None`` when it has none."""
        return spec_field(self.spec, "default")

    @property
    def required(self) -> bool:
        """Whether the spec is required (the ParamSpec default is ``True``)."""
        return bool(spec_field(self.spec, "required", True))


def _frozen(mapping: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
    return MappingProxyType(dict(mapping or {}))


@dataclass(frozen=True)
class ParamScope:
    """The param bindings and effective specs one config scope sees.

    Open the run root with :meth:`root` and each nested scope with
    :meth:`child`; read the bound values with :meth:`values`.

    Attributes:
        bindings: Bound values by name.
        specs: Effective declared specs by name (nearest declaration wins).
    """

    bindings: Mapping[str, Binding] = field(default_factory=_frozen)
    specs: Mapping[str, DeclaredSpec] = field(default_factory=_frozen)

    def __post_init__(self) -> None:
        """Freeze the mappings so a binding never changes after its scope opens."""
        object.__setattr__(self, "bindings", _frozen(self.bindings))
        object.__setattr__(self, "specs", _frozen(self.specs))

    @classmethod
    def root(cls, runtime_params: Mapping[str, Any] | None = None) -> ParamScope:
        """The scope above the run root, binding the invoker's runtime params.

        An explicit ``None`` value is treated as absent, so the declared
        default and the required check still apply: a literal ``None``
        almost always signals an unbound notebook variable.

        Args:
            runtime_params: Caller-supplied values keyed by param name.

        Returns:
            A scope with no specs, binding each non-``None`` runtime value.
        """
        return cls(
            bindings={
                name: Binding(value, "runtime")
                for name, value in (runtime_params or {}).items()
                if value is not None
            }
        )

    def child(
        self,
        specs: Mapping[str, Any] | None,
        *,
        file: str | None = None,
        at: tuple[str | int, ...] = (),
        pins: Mapping[str, Any] | None = None,
    ) -> ParamScope:
        """Open a nested scope declaring ``specs``.

        Args:
            specs: The document's own ``params:`` (name to ``ParamSpec`` or
                its mapping); ``None`` when it declares none.
            file: The document's file, named by any error at a declaration.
            at: Where the document sits in ``file`` (an inline child's
                ``("weaves", 0)``); empty for a document in its own file.
            pins: Values the referencing entry binds for this scope, already
                resolved in the parent scope. They beat inherited values.

        Returns:
            The child scope.

        Raises:
            ConfigSchemaError: A declared spec is not a mapping (``C102``), or
                a required spec has no value (``P101``, at its declaration).
        """
        declared = {
            name: DeclaredSpec(_checked(name, spec, file, at), file, at)
            for name, spec in (specs or {}).items()
        }
        effective = {**self.specs, **declared}
        bindings = {name: b for name, b in self.bindings.items() if b.supplied}
        bindings.update({name: Binding(value, "pin") for name, value in (pins or {}).items()})
        for name, spec in effective.items():
            if name in bindings:
                continue
            if spec.default is not None:
                bindings[name] = Binding(spec.default, "default")
            elif spec.required:
                message = (
                    f"Required parameter '{name}' is missing (no runtime value and no default)"
                )
                raise ConfigSchemaError(
                    message,
                    file_path=spec.file,
                    config_key=name,
                    diagnostics=[missing_parameter(name, message, spec.file, spec.at)],
                )
        return ParamScope(bindings=bindings, specs=effective)

    def values(self) -> dict[str, Any]:
        """The bound values by name, for the ``param`` namespace."""
        return {name: binding.value for name, binding in self.bindings.items()}


def _checked(name: str, spec: Any, file: str | None, at: tuple[str | int, ...]) -> Any:
    """``spec`` when it is a ParamSpec or mapping; else a ``C102`` error."""
    if isinstance(spec, dict) or hasattr(spec, "required"):
        return spec
    message = (
        f"Parameter '{name}' must be a mapping with 'name' and 'type', got {type(spec).__name__}"
    )
    raise ConfigSchemaError(
        message,
        file_path=file,
        config_key=name,
        diagnostics=[
            diagnostic(
                "C102",
                message,
                file=file,
                pointer=json_pointer((*at, "params", name)),
                expected=["mapping"],
            )
        ],
    )


def spec_field(spec: Any, name: str, default: Any = None) -> Any:
    """Read a field from either a ParamSpec instance or its ``model_dump`` dict.

    Args:
        spec: A ``ParamSpec`` or its mapping.
        name: The field name.
        default: The value when the field is absent.

    Returns:
        The field's value, else ``default``.
    """
    if hasattr(spec, name):
        return getattr(spec, name)
    if isinstance(spec, dict):
        return spec.get(name, default)
    return default


def missing_parameter(
    name: str, message: str, file: str | None, at: tuple[str | int, ...] = ()
) -> Diagnostic:
    """The diagnostic for a required parameter with no value, at its declaration.

    Args:
        name: The param name.
        message: The error message.
        file: The declaring file, when known.
        at: Where the declaring document sits in ``file``.

    Returns:
        A ``P101`` diagnostic pointing at ``/params/<name>`` in the declaring document.
    """
    return diagnostic(
        "P101",
        message,
        file=file,
        pointer=json_pointer((*at, "params", name)),
        hint=f"Pass a value for '{name}' at run time, or give it a default.",
    )
