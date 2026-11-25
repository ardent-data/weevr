"""Scoped variable resolution: each config opens its param scope, each entry its child's.

A load walks its config tree carrying a :class:`WalkState`: the param scope
in force, the file being resolved, whether that scope is strict, and the
records the whole walk shares (bindings read, undeclared references, load
warnings), and the registry assets the config sees (:mod:`weevr.config.assets`).
:func:`resolve_config_variables` resolves a config's ``${...}``
references with inline children in scopes of their own. :func:`enter_child`
is the one place a ``ref:`` entry's child scope is opened and its misfiring
pins reported, for both ``ref:`` weaves and ``ref:`` threads.
"""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from functools import cached_property
from typing import Any

from weevr.config.assets import AssetKey, DeclaredAsset, upstream_readers, visible_assets
from weevr.config.collect import errors_recorded, raise_or_record
from weevr.config.interpolation import (
    BareNames,
    fabric_namespace,
    load_context,
    param_name,
    resolve_variables,
)
from weevr.config.params import (
    BindingReads,
    ParamScope,
    ScopePositions,
    UndeclaredParams,
    pin_warnings,
    unused_runtime_params,
)
from weevr.diagnostics import Diagnostic
from weevr.errors import VariableResolutionError

# Keys an entry carries about its place in the parent, not about the child's body.
WEAVE_ENTRY_KEYS = frozenset({"name", "params", "condition"})
THREAD_ENTRY_KEYS = frozenset({"name", "as", "params", "dependencies", "condition"})


def has_inline_body(entry: Any, entry_keys: frozenset[str]) -> bool:
    """Whether a child entry carries an inline body: no ``ref`` and a key beyond ``entry_keys``."""
    return isinstance(entry, dict) and "ref" not in entry and bool(set(entry) - entry_keys)


def is_inline_weave(entry: Any) -> bool:
    """Whether a loom weave entry carries an inline weave body."""
    return has_inline_body(entry, WEAVE_ENTRY_KEYS)


def is_inline_thread(entry: Any) -> bool:
    """Whether a weave thread entry carries an inline thread body."""
    return has_inline_body(entry, THREAD_ENTRY_KEYS)


def strictness(document: Mapping[str, Any], inherited: bool, override: bool | None) -> bool:
    """Whether a document's scope is strict about params.

    The load's override wins; else the document's own ``strict_params:``,
    else the enclosing scope's setting. A ``strict_params`` that is not a bool
    is left for the model to reject and does not count.

    Args:
        document: The document (or inline body) opening the scope.
        inherited: The enclosing scope's setting; ``False`` at the root.
        override: The load's ``strict_params`` argument; ``None`` when not given.

    Returns:
        The scope's setting.
    """
    if override is not None:
        return override
    own = document.get("strict_params")
    return own if isinstance(own, bool) else inherited


@dataclass(frozen=True, eq=False)
class WalkState:
    """What one config's resolution carries down the walk.

    A child gets its own state from :meth:`inline` or :meth:`in_file`; the
    shared records are the same objects in every state of one load. Frozen
    means a state's fields can't be rebound; the records they hold are
    mutable and shared by design, so states compare by identity.

    Attributes:
        scope: The param scope in force.
        file: The file being resolved; ``None`` when unknown.
        strict: Whether the scope is strict (:func:`strictness`): undeclared
            references and unused runtime params are errors rather than
            warnings.
        strict_override: The load's ``strict_params`` argument, which beats
            every document; ``None`` when not given.
        runtime_params: The invoker's runtime params; their ``fabric`` key
            stands in for the Fabric context.
        fabric_context: Fabric environment values for ``${fabric.x}``.
        reads: The bindings the load read, shared by the whole walk.
        undeclared: Undeclared references found, shared by the whole walk.
        warnings: Load warnings found, shared by the whole walk.
        assets: The registry assets this config sees, by kind and name: its
            own declarations over every enclosing config's (a lower
            declaration shadows a higher one of the same kind and name).
    """

    scope: ParamScope
    file: str | None = None
    strict: bool = False
    strict_override: bool | None = None
    runtime_params: Mapping[str, Any] | None = None
    fabric_context: Mapping[str, Any] | None = None
    reads: BindingReads = field(default_factory=BindingReads)
    undeclared: UndeclaredParams = field(default_factory=UndeclaredParams)
    warnings: list[Diagnostic] = field(default_factory=list)
    assets: Mapping[AssetKey, DeclaredAsset] = field(default_factory=dict)

    @classmethod
    def start(
        cls,
        document: Mapping[str, Any],
        *,
        file: str | None,
        runtime_params: Mapping[str, Any] | None = None,
        fabric_context: Mapping[str, Any] | None = None,
        strict_override: bool | None = None,
    ) -> "WalkState":
        """The state at the root of a load: the root config's scope opened.

        Args:
            document: The root config; its ``params:`` declare the root scope
                and its ``strict_params`` applies.
            file: The root config's file.
            runtime_params: The invoker's runtime params, bound above the root.
            fabric_context: Fabric environment values.
            strict_override: The load's ``strict_params`` argument.

        Returns:
            The root state, with fresh shared records.

        Raises:
            ConfigSchemaError: See :meth:`ParamScope.child`.
        """
        reads = BindingReads()
        scope = ParamScope.root(runtime_params).open_child(
            document.get("params"), file=file, reads=reads
        )
        return cls(
            scope=scope,
            file=file,
            strict=strictness(document, False, strict_override),
            strict_override=strict_override,
            runtime_params=runtime_params,
            fabric_context=fabric_context,
            reads=reads,
            assets=visible_assets({}, document, file),
        )

    @cached_property
    def context(self) -> dict[str, Any]:
        """The load-time namespaces a reference in this scope reads."""
        return scope_context(
            self.scope, runtime_params=self.runtime_params, fabric_context=self.fabric_context
        )

    def inline(
        self,
        scope: ParamScope,
        body: Mapping[str, Any],
        *,
        at: tuple[str | int, ...] = (),
        authored: Mapping[str, Any] | None = None,
    ) -> "WalkState":
        """The state of an inline child of this config, in the same file.

        Args:
            scope: The child's param scope.
            body: The child's body, whose ``strict_params`` applies.
            at: Where the child sits in the file.
            authored: The body before variable substitution, when ``body``
                has been substituted; its registry assets are read from it.

        Returns:
            The child's state.
        """
        written = body if authored is None else authored
        return replace(
            self,
            scope=scope,
            strict=strictness(body, self.strict, self.strict_override),
            assets=visible_assets(self.assets, written, self.file, at),
        )

    def in_file(self, scope: ParamScope, document: Mapping[str, Any], file: str) -> "WalkState":
        """The state of a child config in its own file.

        Args:
            scope: The child's param scope.
            document: The child's document, whose ``strict_params`` applies.
            file: The child's file.

        Returns:
            The child's state.
        """
        return replace(
            self,
            scope=scope,
            file=file,
            strict=strictness(document, self.strict, self.strict_override),
            assets=visible_assets(self.assets, document, file),
        )


def resolve_config_variables(
    config: dict[str, Any],
    config_type: str,
    walk: WalkState,
    *,
    inline_scopes: ScopePositions | None = None,
) -> dict[str, Any]:
    """Resolve variable references in a top-level config.

    ``${param.x}`` resolves from ``walk``'s scope, the config's own. An
    inline child (a loom's inline weave, a weave's inline thread) opens its
    own scope from its parent's, so its declared ``params:`` bind for it the
    way a child file's do. The child's entry keys (``name``, ``condition``,
    and a thread's ``as`` and ``dependencies``) belong to the parent and
    resolve in the parent's scope, as do a ``ref:`` entry's pins. A bare
    ``${x}`` fails with its rewrite, classified by the scope's declarations
    and the config's ``defaults`` (an inline weave's own ``defaults``
    layered over the loom's).

    Args:
        config: Schema-validated config dict.
        config_type: The config kind (``"thread"``, ``"weave"``, ...).
        walk: The load's walk state at this config (:meth:`WalkState.start`).
        inline_scopes: Filled with the scope opened for each inline child, so
            :func:`resolve_references` reuses it rather than opening another.

    Returns:
        The config dict with variables resolved.

    Raises:
        VariableResolutionError: A variable has no value and no default, or
            a reference is bare or in an unknown namespace.
        ConfigSchemaError: A required param declared by an inline child has
            no value.
    """
    return resolve_scoped(
        config, config_type, walk, config.get("defaults"), inline_scopes=inline_scopes
    )


def scope_context(
    scope: ParamScope,
    *,
    runtime_params: Mapping[str, Any] | None = None,
    fabric_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The load-time namespaces a reference in ``scope`` reads.

    Args:
        scope: The param scope; its bound values are the ``param`` namespace.
        runtime_params: Runtime parameters; their ``fabric`` key stands in for
            the Fabric context.
        fabric_context: Optional Fabric environment values.

    Returns:
        The context for :func:`resolve_variables`.
    """
    return load_context(scope.values(), fabric_namespace(runtime_params, fabric_context))


# The list each kind holds its children in, and the kind of those children.
_CHILD_LISTS = {"loom": ("weaves", "weave"), "weave": ("threads", "thread")}


def substitute(
    config: Any,
    walk: WalkState,
    at: tuple[str | int, ...] = (),
    defaults: Mapping[str, Any] | None = None,
) -> Any:
    """Resolve ``config`` in ``walk``'s scope, noting each binding it read.

    A bare reference's rewrite is classified by the scope's declared params
    and ``defaults``, the ``defaults:`` block in force. A bound reference to a
    name no declaration in the scope chain covers is noted as undeclared.
    """
    scope = walk.scope
    read_at: dict[str, str] = {}
    names = BareNames(declared=frozenset(scope.specs), defaults=defaults or {})
    resolved = resolve_variables(config, walk.context, at=at, names=names, read_at=read_at)
    for key, pointer in read_at.items():
        name = param_name(key)
        if name is None:
            continue
        binding = scope.bindings.get(name)
        if binding is None:
            continue
        walk.reads.note(name, binding)
        if name not in scope.specs:
            walk.undeclared.note(
                name, binding.value, file=walk.file, pointer=pointer, strict=walk.strict
            )
    return resolved


def resolve_scoped(
    config: dict[str, Any],
    config_type: str,
    walk: WalkState,
    defaults: dict[str, Any] | None,
    *,
    at: tuple[str | int, ...] = (),
    inline_scopes: ScopePositions | None = None,
) -> dict[str, Any]:
    """Resolve ``config`` in ``walk``'s scope, each inline child in its own scope opened from it.

    An inline child's entry keys resolve in ``walk``'s scope, its body in the child's.
    """
    key, child_type = _CHILD_LISTS.get(config_type, ("", ""))
    entries = config.get(key) if key else None
    if not isinstance(entries, list):
        return substitute(config, walk, at, defaults)
    is_inline = is_inline_weave if config_type == "loom" else is_inline_thread
    inline = {i: entry for i, entry in enumerate(entries) if is_inline(entry)}
    if not inline:
        return substitute(config, walk, at, defaults)

    # An inline child's own params: are its declarations, so they stay with the body.
    entry_keys = (WEAVE_ENTRY_KEYS if config_type == "loom" else THREAD_ENTRY_KEYS) - {"params"}
    shell = {**config, key: [None if i in inline else e for i, e in enumerate(entries)]}
    resolved = substitute(shell, walk, at, defaults)
    for i, entry in inline.items():
        where = (*at, key, i)
        child_defaults = defaults
        if config_type == "loom":
            own = entry.get("defaults")
            child_defaults = {**(defaults or {}), **(own if isinstance(own, dict) else {})}
        child_scope = walk.scope.open_child(
            entry.get("params"), file=walk.file, at=where, reads=walk.reads
        )
        if inline_scopes is not None:
            inline_scopes[where] = child_scope
        placement = substitute(
            {k: v for k, v in entry.items() if k in entry_keys}, walk, where, defaults
        )
        body = resolve_scoped(
            {k: v for k, v in entry.items() if k not in entry_keys},
            child_type,
            walk.inline(child_scope, entry, at=where),
            child_defaults,
            at=where,
            inline_scopes=inline_scopes,
        )
        resolved[key][i] = {k: placement[k] if k in placement else body[k] for k in entry}
    return resolved


def entry_pins(entry: dict[str, Any]) -> dict[str, Any] | None:
    """A ``ref:`` entry's pins, as the variable pass resolved them; ``None`` when it pins none.

    The pins resolve in the parent's scope with the rest of the parent
    (:func:`resolve_config_variables`, :func:`resolve_scoped`). Entries reach
    here unvalidated: a ``params:`` that is not a mapping is left for the
    entry model to reject when the parent hydrates.
    """
    pins = entry.get("params")
    if not pins or not isinstance(pins, dict):
        return None
    return pins


class ChildEntry:
    """One ``ref:`` entry being entered: its resolved pins and, once opened, its state.

    Attributes:
        pins: The entry's pin values, resolved in the parent's scope; ``None``
            when it pins none.
    """

    def __init__(self, pins: dict[str, Any] | None, parent: WalkState) -> None:
        """Hold the entry's ``pins`` for opening a scope beneath ``parent``."""
        self.pins = pins
        self._parent = parent
        self._opened: tuple[str, WalkState] | None = None

    def open(self, child: dict[str, Any], path: str) -> WalkState:
        """Open the child's scope: its declared ``params:`` bound with the entry's pins.

        Args:
            child: The child's document, as loaded.
            path: The child's file.

        Returns:
            The child's walk state, also kept as :attr:`opened`.
        """
        scope = self._parent.scope.open_child(
            child.get("params"), file=path, pins=self.pins, reads=self._parent.reads
        )
        state = self._parent.in_file(scope, child, path)
        self._opened = (child["name"], state)
        return state

    def _require_opened(self) -> tuple[str, WalkState]:
        """The child's name and walk state, once :meth:`open` has run.

        Raises:
            RuntimeError: :meth:`open` was not called.
        """
        if self._opened is None:
            raise RuntimeError("ChildEntry.open() was never called for this entry")
        return self._opened

    @property
    def opened(self) -> WalkState:
        """The child's walk state; raises ``RuntimeError`` before :meth:`open`."""
        return self._require_opened()[1]

    @property
    def name(self) -> str:
        """The child's own name; raises ``RuntimeError`` before :meth:`open`."""
        return self._require_opened()[0]


@contextmanager
def enter_child(
    entry: dict[str, Any],
    pins: dict[str, Any] | None,
    walk: WalkState,
    at: tuple[str | int, ...],
    *,
    alias_key: str,
) -> Iterator[ChildEntry]:
    """Enter a ``ref:`` entry with its resolved ``pins``, then report the pins that misfire.

    The caller loads the child and calls :meth:`ChildEntry.open` inside the
    block, resolves the child's subtree there, and leaves the block normally.
    Then the pins are checked, unless the subtree recorded an error, which
    hides whether its pins were read. If the block raises, nothing is
    reported. Each pin warns at most once (:func:`pin_warnings`):

    - ``P122`` when a registry asset upstream of the entry reads the pinned
      name, whether or not the subtree reads it too. The asset is declared in
      ``walk``'s config or one enclosing it, so it resolved before the pin
      existed, and the child does not shadow it (the child sees the very
      asset ``walk`` sees).
    - Else ``P123`` when nothing beneath the entry read the pin.

    Args:
        entry: The ``ref:`` entry.
        pins: The entry's pins, resolved in the parent's scope
            (:func:`entry_pins`); ``None`` when it pins none.
        walk: The parent's walk state; the warnings name its file.
        at: Where the entry sits in the parent's file.
        alias_key: The entry key that renames the child (``as`` for a thread,
            ``name`` for a weave); the child's own name stands in without it.

    Yields:
        The entry, to open the child's scope from.

    Raises:
        RuntimeError: The block left normally without opening the child.
    """
    errors_before = errors_recorded()
    child = ChildEntry(pins, walk)
    yield child
    opened = child.opened
    if errors_recorded() == errors_before:
        walk.warnings.extend(
            pin_warnings(
                child.pins,
                opened.scope,
                walk.reads,
                entry_name=entry.get(alias_key) or child.name,
                file=walk.file,
                at=at,
                upstream_readers=upstream_readers(child.pins, walk.assets, opened.assets),
            )
        )


def param_use_diagnostics(walk: WalkState, *, walk_complete: bool) -> list[Diagnostic]:
    """The load's undeclared-reference (``P120``) and unused-param (``P121``) warnings.

    Each is an error when its scope is strict: an undeclared reference by the
    strictness where it sits, an unused runtime param by the root's. Errors
    are raised, or recorded in a collecting load (:func:`raise_or_record`).

    Args:
        walk: The root config's walk state, after the whole tree was walked.
        walk_complete: Whether the walk recorded no error. A skipped subtree
            hides what it would have read, so unused runtime params are only
            reported for a complete walk.

    Returns:
        The warnings.

    Raises:
        VariableResolutionError: Outside a collecting load, for any of them
            that is an error.
    """
    found = walk.undeclared.diagnostics()
    if walk_complete:
        found += unused_runtime_params(
            walk.runtime_params, walk.reads, file=walk.file, strict=walk.strict
        )
    return raise_or_record(found, file=walk.file, error_type=VariableResolutionError)
