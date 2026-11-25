"""Per-scope param binding: declared specs cascade, supplied values flow down.

Each config scope (a thread, weave, or loom document, inline or in its own file)
opens a :class:`ParamScope` from its parent's. Declared ``params:`` specs merge
by name, the child's declaration shadowing its ancestors'. Values bind
closure-style; per name, highest first:

1. A pin: the referencing entry's ``params:`` value.
2. The inherited binding: a value supplied above this scope. At the run root
   that is the runtime params.
3. The nearest declared default: the effective spec's ``default``.

Each value bound for a declared name is checked and coerced against the
effective spec's ``type`` by :func:`coerce_param`; a value it rejects fails
``P102`` (``P103`` for a malformed date or timestamp string) at that spec's
declaration. A child scope coerces the value as supplied, not an ancestor's
coerced value, so its own declaration decides.

A required spec left with none of these fails ``P101`` at its declaration. An
optional spec with no default, like an undeclared name nobody supplied, stays
unbound: a ``${param.x}`` reference to it falls back to its ``:-`` default or
fails at load. A binding never changes after its scope opens; a child scope
re-derives defaults from its own effective specs, so a nearer default wins over
an ancestor's, while supplied values always beat defaults.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from types import MappingProxyType
from typing import Any, Literal

from weevr.diagnostics import Diagnostic, Severity, diagnostic
from weevr.diagnostics.positions import json_pointer
from weevr.errors import ConfigSchemaError
from weevr.model.params import ParamSpec

BindingSource = Literal["runtime", "pin", "default"]
"""Where a binding's value came from: supplied (``runtime``, ``pin``) or ``default``."""


@dataclass(frozen=True)
class Binding:
    """One bound param value.

    Attributes:
        value: The bound value, coerced to its declared type when it has one.
        source: Where the value came from. Supplied values (``runtime``,
            ``pin``) flow into child scopes; a ``default`` is re-derived in
            each scope from its effective spec.
        origin: The supplied binding this one coerces; ``None`` when the
            value is as supplied. Reading a coerced binding reads its origin.
    """

    value: Any
    source: BindingSource
    origin: Binding | None = field(default=None, compare=False, repr=False)

    @property
    def supplied_binding(self) -> Binding:
        """The binding as supplied: :attr:`origin`, else this binding."""
        return self.origin or self

    @property
    def supplied(self) -> bool:
        """Whether the value was supplied rather than taken from a spec default."""
        return self.source != "default"


@dataclass(frozen=True)
class DeclaredSpec:
    """A declared param spec, read once into one shape, and where it was declared.

    Attributes:
        type: The declared type name (``ParamSpec.type``); ``None`` when the
            declaration names none or a non-string, which the model rejects
            at hydration. Such a spec coerces nothing.
        required: Whether the param must be bound (the ParamSpec default is ``True``).
        default: The spec's default; ``None`` when it has none.
        file: The declaring file; ``None`` when unknown.
        at: Where the declaring document sits in ``file`` (empty for a
            document in its own file).
    """

    type: str | None
    required: bool
    default: Any
    file: str | None
    at: tuple[str | int, ...] = ()

    @classmethod
    def read(
        cls, name: str, spec: Any, file: str | None, at: tuple[str | int, ...] = ()
    ) -> DeclaredSpec:
        """Read a ``ParamSpec`` or its mapping into a declared spec.

        Args:
            name: The param name.
            spec: The ``ParamSpec`` or its mapping, as declared.
            file: The declaring file.
            at: Where the declaring document sits in ``file``.

        Returns:
            The declared spec.

        Raises:
            ConfigSchemaError: ``spec`` is neither a ParamSpec nor a mapping (``C102``).
        """
        if isinstance(spec, ParamSpec):
            fields: Mapping[str, Any] = spec.model_dump()
        elif isinstance(spec, dict):
            fields = spec
        else:
            raise _not_a_mapping(name, spec, file, at)
        declared_type = fields.get("type")
        required = fields.get("required", True)
        return cls(
            type=declared_type if isinstance(declared_type, str) else None,
            required=required if isinstance(required, bool) else True,
            default=fields.get("default"),
            file=file,
            at=at,
        )

    def coerce(self, name: str, value: Any) -> Any:
        """``value`` coerced to this spec's type, or an error at this declaration.

        Args:
            name: The param name.
            value: The value to bind.

        Returns:
            The coerced value (see :func:`coerce_param`).

        Raises:
            ConfigSchemaError: The value does not fit the type (``P102``), or
                is a malformed date or timestamp string (``P103``).
        """
        if self.type is None:
            return value
        try:
            return coerce_param(value, self.type)
        except (TypeError, ValueError) as exc:
            # Only a date or timestamp string can be malformed; any other
            # failure is a type mismatch.
            if isinstance(exc, ValueError) and self.type in _ISO_TYPES:
                code = "P103"
                message = f"Parameter '{name}' is not a valid ISO {self.type}: {value!r}"
            else:
                code = "P102"
                message = f"Parameter '{name}' expected {self.type}, got {type(value).__name__}"
            cause = exc
        raise ConfigSchemaError(
            message,
            cause=cause,
            file_path=self.file,
            config_key=name,
            diagnostics=[
                diagnostic(
                    code,
                    message,
                    file=self.file,
                    pointer=json_pointer((*self.at, "params", name)),
                    expected=[self.type],
                )
            ],
        )

    def bind(self, name: str, binding: Binding) -> Binding:
        """``binding``'s supplied value coerced to this spec's type.

        Args:
            name: The param name.
            binding: The binding in force, possibly coerced by an ancestor's spec.

        Returns:
            The supplied binding itself when coercion keeps its value, else a
            binding of the coerced value whose :attr:`Binding.origin` is it.

        Raises:
            ConfigSchemaError: See :meth:`coerce`.
        """
        supplied = binding.supplied_binding
        value = self.coerce(name, supplied.value)
        if value is supplied.value:
            return supplied
        return Binding(value, supplied.source, origin=supplied)


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
            ConfigSchemaError: A declared spec is not a mapping (``C102``), a
                required spec has no value (``P101``, at its declaration), or a
                bound value does not fit its effective spec's type (``P102`` or
                ``P103``, at that spec's declaration).
        """
        declared = {
            name: DeclaredSpec.read(name, spec, file, at) for name, spec in (specs or {}).items()
        }
        effective = {**self.specs, **declared}
        supplied = {name: b for name, b in self.bindings.items() if b.supplied}
        supplied.update({name: Binding(value, "pin") for name, value in (pins or {}).items()})
        bindings = {
            name: effective[name].bind(name, b) if name in effective else b
            for name, b in supplied.items()
        }
        for name, spec in effective.items():
            if name in bindings:
                continue
            if spec.default is not None:
                bindings[name] = Binding(spec.coerce(name, spec.default), "default")
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

    def open_child(
        self,
        specs: Mapping[str, Any] | None,
        *,
        file: str | None,
        at: tuple[str | int, ...] = (),
        pins: Mapping[str, Any] | None = None,
        reads: BindingReads | None = None,
    ) -> ParamScope:
        """Open the child scope declaring ``specs``, with ``pins`` already resolved.

        Like :meth:`child`, and each declared name's binding counts as read in
        ``reads``: a pin that satisfies a declaration is consumed.

        Args:
            specs: The document's own ``params:``; ``None`` when it declares none.
            file: The document's file, named by any error at a declaration.
            at: Where the document sits in ``file``.
            pins: Values the referencing entry binds, already resolved in this scope.
            reads: Receives the declared bindings; ``None`` to record nothing.

        Returns:
            The child scope.

        Raises:
            ConfigSchemaError: See :meth:`child`.
        """
        opened = self.child(specs, file=file, at=at, pins=pins)
        if reads is not None:
            for name in specs or {}:
                binding = opened.bindings.get(name)
                if binding is not None:
                    reads.note(name, binding)
        return opened

    def values(self) -> dict[str, Any]:
        """The bound values by name, for the ``param`` namespace."""
        return {name: binding.value for name, binding in self.bindings.items()}


ScopePositions = dict[tuple[str | int, ...], ParamScope]
"""The param scope opened for each inline child, by where the child sits in its file."""


class BindingReads:
    """The bindings a load read, by identity, and the names they were read under.

    A pin is consumed when some binding read in its subtree is the very
    binding the pin created, or a coercion of it: a nearer pin for the same
    name makes a new binding, so it does not count as reading the outer one.
    The names are tracked too, for ``P121``: a supplied name is used when any
    binding was read under it, whichever scope bound it. A declaration counts
    as use: a scope that declares the name reads its binding when it opens
    (:meth:`ParamScope.open_child`), as a ``${param.x}`` reference does.
    """

    def __init__(self) -> None:
        """Start with nothing read."""
        self._read: dict[int, Binding] = {}
        self._names: set[str] = set()

    def note(self, name: str, binding: Binding) -> None:
        """Record that ``binding`` was read under ``name``.

        Args:
            name: The param name the binding is bound to.
            binding: The binding a reference or declaration used.
        """
        supplied = binding.supplied_binding
        # Holding the binding keeps its id from being reused by a later one.
        self._read[id(supplied)] = supplied
        self._names.add(name)

    def was_read(self, binding: Binding) -> bool:
        """Whether ``binding`` was read.

        Args:
            binding: A binding from some scope.

        Returns:
            ``True`` when :meth:`note` recorded this binding or a coercion of
            the binding it coerces.
        """
        supplied = binding.supplied_binding
        return self._read.get(id(supplied)) is supplied

    def name_used(self, name: str) -> bool:
        """Whether any binding was read under ``name``.

        Args:
            name: A param name.

        Returns:
            ``True`` when a reference or a declaration anywhere in the load
            read a binding of ``name``.
        """
        return name in self._names


@dataclass
class _Undeclared:
    """The first reference to one undeclared name in a file."""

    value: Any
    pointer: str | None
    strict: bool


class UndeclaredParams:
    """Bound ``${param.x}`` references no declaration in their chain covers.

    Kept by file and name, so each name warns once per file (``P120``) at its
    first reference; the warning is an error when any of the file's
    references to the name sits in a strict scope.
    """

    def __init__(self) -> None:
        """Start with none found."""
        self._by_file: dict[str | None, dict[str, _Undeclared]] = {}

    def note(
        self, name: str, value: Any, *, file: str | None, pointer: str | None, strict: bool
    ) -> None:
        """Record a reference to the undeclared ``name``.

        Args:
            name: The param name.
            value: The value the reference read, to suggest the declared type.
            file: The referencing file.
            pointer: Where the reference sits in ``file``.
            strict: Whether the reference's scope is strict.
        """
        names = self._by_file.setdefault(file, {})
        found = names.get(name)
        if found is None:
            names[name] = _Undeclared(value, pointer, strict)
        elif strict:
            found.strict = True

    def diagnostics(self) -> list[Diagnostic]:
        """One ``P120`` per file and name, in the order they were found.

        Each one's hint is a ``params:`` block declaring every undeclared
        name the file references, ready to paste.

        Returns:
            The diagnostics: warnings, or errors for names referenced from a
            strict scope.
        """
        found: list[Diagnostic] = []
        for file, names in self._by_file.items():
            block = declaration_block({name: ref.value for name, ref in names.items()})
            found.extend(
                diagnostic(
                    "P120",
                    f"Parameter '{name}' is referenced but declared nowhere in its scope chain",
                    severity=Severity.ERROR if ref.strict else None,
                    file=file,
                    pointer=ref.pointer,
                    hint=block,
                )
                for name, ref in names.items()
            )
        return found


def _type_of(value: Any) -> str:
    """The ``ParamSpec.type`` a value binds as; ``string`` when no other type fits."""
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, datetime):
        return "timestamp"
    if isinstance(value, date):
        return "date"
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return "list[string]"
    return "string"


def declaration_block(values: Mapping[str, Any]) -> str:
    """A ``params:`` block declaring each name, typed by its bound value.

    Args:
        values: The bound value by param name.

    Returns:
        YAML text, one ``name``/``type`` spec per name.
    """
    lines = ["params:"]
    for name, value in values.items():
        lines += [f"  {name}:", f"    name: {name}", f"    type: {_type_of(value)}"]
    return "\n".join(lines)


def unused_runtime_params(
    runtime_params: Mapping[str, Any] | None,
    reads: BindingReads,
    *,
    file: str | None,
    strict: bool,
) -> list[Diagnostic]:
    """``P121`` for each runtime key nothing in the load declared or referenced.

    A ``None`` value is treated as absent, as when it binds. A ``fabric`` key
    is a param like any other: runtime params never supply ``${fabric.x}``.

    Args:
        runtime_params: The invoker's runtime params.
        reads: The bindings the whole load read.
        file: The root config's file, which the diagnostics name.
        strict: Whether the root config is strict, making each an error.

    Returns:
        One diagnostic per unused key, in the order supplied.
    """
    return [
        diagnostic(
            "P121",
            f"Runtime parameter '{name}' is supplied but nothing declares or references it",
            severity=Severity.ERROR if strict else None,
            file=file,
            expected=[name],
            hint=f"Stop passing '{name}', or reference it as ${{param.{name}}}.",
        )
        for name, value in (runtime_params or {}).items()
        if value is not None and not reads.name_used(name)
    ]


def _not_a_mapping(
    name: str, spec: Any, file: str | None, at: tuple[str | int, ...]
) -> ConfigSchemaError:
    """The ``C102`` error for a declared spec that is not a mapping."""
    message = (
        f"Parameter '{name}' must be a mapping with 'name' and 'type', got {type(spec).__name__}"
    )
    return ConfigSchemaError(
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


_INT = re.compile(r"[+-]?[0-9]+")
_FLOAT = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")


def _mismatch(value: Any, type_name: str) -> TypeError:
    return TypeError(f"{type(value).__name__} is not a {type_name}")


def _to_string(value: Any) -> str:
    if isinstance(value, str):
        return value
    raise _mismatch(value, "string")


def _to_int(value: Any) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and _INT.fullmatch(value):
        try:
            return int(value)
        except ValueError:  # more digits than int() converts
            pass
    raise _mismatch(value, "int")


def _to_float(value: Any) -> float:
    numeric = isinstance(value, (int, float)) and not isinstance(value, bool)
    if numeric or (isinstance(value, str) and _FLOAT.fullmatch(value)):
        try:
            result = float(value)
        except OverflowError:  # an int too large for a float
            result = math.inf
        if math.isfinite(result):
            return value if isinstance(value, float) else result
    raise _mismatch(value, "float")


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ("true", "false"):
        return value.lower() == "true"
    raise _mismatch(value, "bool")


def _to_date(value: Any) -> str:
    """A date as its ISO string: a ``YYYY-MM-DD`` string as written, a date's ``isoformat()``.

    A string is checked with ``strptime("%Y-%m-%d")``, so an unpadded
    ``2024-1-5`` is accepted as in v1; values bind as written. Binding keeps
    dates as strings; native date objects are left to later field-type
    coercion.
    """
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        datetime.strptime(value, "%Y-%m-%d")
        return value
    raise _mismatch(value, "date")


def _to_timestamp(value: Any) -> str:
    """A timestamp as its ISO string: an ISO string as written, a datetime's ``isoformat()``.

    A string is checked with ``datetime.fromisoformat``, so a date-only
    ``2024-01-05``, a space separator, and the compact (``20240105T1030``)
    and week (``2024-W01-1``) forms are accepted as in v1; values bind as
    written. Binding keeps timestamps as strings, so ``T`` and ``Z`` survive
    in embedded references; native datetime objects are left to later
    field-type coercion.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        datetime.fromisoformat(value)
        return value
    raise _mismatch(value, "timestamp")


def _to_string_list(value: Any) -> list[str]:
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    raise _mismatch(value, "list[string]")


_ISO_TYPES = frozenset({"date", "timestamp"})
"""The types whose malformed strings fail ``P103`` rather than ``P102``."""

_COERCIONS: dict[str, Callable[[Any], Any]] = {
    "string": _to_string,
    "int": _to_int,
    "float": _to_float,
    "bool": _to_bool,
    "date": _to_date,
    "timestamp": _to_timestamp,
    "list[string]": _to_string_list,
}


def coerce_param(value: Any, type_name: str) -> Any:
    """Coerce ``value`` to the param type ``type_name``, by the narrow binding table.

    - ``string``: a string.
    - ``int``: an int (not a bool), or a base-10 string (``"42"``, ``"-3"``).
    - ``float``: a finite float, an int (not a bool), or a numeric string.
    - ``bool``: a bool, or ``"true"`` / ``"false"`` in any case.
    - ``date``: a ``YYYY-MM-DD`` string (bound as written), or a date
      (a datetime's date part), bound as its ``isoformat()``.
    - ``timestamp``: an ISO 8601 string (bound as written), or a datetime,
      bound as its ``isoformat()``.

    Date and timestamp strings are accepted as in v1; values bind as written.
    A date may be unpadded (``2024-1-5``). A timestamp may be date-only, use a
    space separator, or take the compact (``20240105T1030``) or week
    (``2024-W01-1``) forms ``datetime.fromisoformat`` reads on Python 3.12
    and 3.13.
    - ``list[string]``: a list of strings.

    A type name outside the table passes the value through unchanged.

    Args:
        value: The value to coerce.
        type_name: A ``ParamSpec.type`` name.

    Returns:
        The value as the type's native Python value; for ``date`` and
        ``timestamp``, its ISO string. A value already of the bound form is
        returned as the same object.

    Raises:
        TypeError: The value is not one the type accepts.
        ValueError: A date or timestamp string is not in ISO format.
    """
    convert = _COERCIONS.get(type_name)
    return value if convert is None else convert(value)


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


def pin_warnings(
    pins: Mapping[str, Any] | None,
    scope: ParamScope,
    reads: BindingReads,
    *,
    entry_name: str,
    file: str | None,
    at: tuple[str | int, ...],
    upstream_readers: Mapping[str, Sequence[str]] | None = None,
) -> list[Diagnostic]:
    """The warnings for the entry's pins: each pin gets at most one of P122 or P123, never both.

    A pin that a registry asset declared above the entry reads warns ``P122``
    naming those assets, whether or not the subtree also reads the pin: the
    asset resolved before the pin existed, so the pin does not retarget it,
    and the subtree's own fields would then disagree with the asset. The
    warning may fire on a deliberate pin; it is never an error.

    Any other pin warns ``P123`` when nothing beneath the entry read it. A pin
    is consumed when its binding is read by a ``${param.x}`` reference in the
    entry's subtree (a nested entry's pin value included) or satisfies a
    ``params:`` declaration there. A nearer pin for the same name makes its
    own binding, so a pin every reader shadows is unconsumed.

    Args:
        pins: The entry's resolved pins.
        scope: The child scope the entry opened, holding the pins' bindings.
        reads: The bindings read beneath the entry.
        entry_name: The entry's name, for the message.
        file: The file holding the entry.
        at: Where the entry sits in ``file``.
        upstream_readers: By pin name, the upstream assets visible beneath
            the entry that read it, each described for ``expected``.

    Returns:
        The warnings, in pin order.
    """
    found: list[Diagnostic] = []
    for name in pins or {}:
        pointer = json_pointer((*at, "params", name))
        assets = (upstream_readers or {}).get(name)
        if assets:
            found.append(
                diagnostic(
                    "P122",
                    f"Entry '{entry_name}' pins parameter '{name}', which registry assets "
                    "declared above the entry also read; they resolved where they are "
                    "declared, so the pin does not retarget them",
                    file=file,
                    pointer=pointer,
                    expected=list(assets),
                    hint="To specialize an asset for this entry, re-declare it under the "
                    "same name below the entry.",
                )
            )
            continue
        if reads.was_read(scope.bindings[name]):
            continue
        found.append(
            diagnostic(
                "P123",
                f"Entry '{entry_name}' pins parameter '{name}', which nothing beneath it reads "
                "or declares",
                file=file,
                pointer=pointer,
                hint=f"Remove the pin, or reference it as ${{param.{name}}} below the entry.",
            )
        )
    return found
