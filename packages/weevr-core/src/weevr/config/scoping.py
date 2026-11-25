"""Scoped variable resolution: each config opens its param scope, each entry its child's.

:func:`resolve_config_variables` resolves a config's ``${...}`` references with
inline children in scopes of their own. :func:`enter_child` is the one place a
``ref:`` entry's pins are resolved, its child scope opened, and its unconsumed
pins reported, for both ``ref:`` weaves and ``ref:`` threads.
"""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from weevr.config.collect import recorded
from weevr.config.interpolation import BareNames, build_param_context, resolve_variables
from weevr.config.params import BindingReads, ParamScope, ScopePositions, unconsumed_pins
from weevr.diagnostics import Diagnostic

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


def resolve_config_variables(
    config: dict[str, Any],
    config_type: str,
    runtime_params: dict[str, Any] | None = None,
    fabric_context: dict[str, Any] | None = None,
    entry_params: dict[str, Any] | None = None,
    *,
    scope: ParamScope | None = None,
    file: str | None = None,
    inline_scopes: ScopePositions | None = None,
) -> dict[str, Any]:
    """Resolve variable references in a top-level config.

    ``${param.x}`` resolves from ``scope``, the config's own param scope. An
    inline child (a loom's inline weave, a weave's inline thread) opens its
    own scope from its parent's, so its declared ``params:`` bind for it the
    way a child file's do. The child's entry keys (``name``, ``condition``,
    and a thread's ``as`` and ``dependencies``) belong to the parent and
    resolve in the parent's scope. A bare ``${x}`` fails with its rewrite,
    classified by the scope's declarations and the config's ``defaults`` (an
    inline weave's own ``defaults`` layered over the loom's).

    Args:
        config: Schema-validated config dict.
        config_type: The config kind (``"thread"``, ``"weave"``, ...).
        runtime_params: Runtime parameter overrides; their ``fabric`` key
            stands in for the Fabric context.
        fabric_context: Optional Fabric environment values.
        entry_params: Values bound for ``${param.x}`` when no ``scope`` is
            given (over the runtime params).
        scope: The config's param scope. Defaults to the runtime params'
            root scope with ``entry_params`` bound over it.
        file: The config's file, named by errors at inline declarations.
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
    if scope is None:
        scope = ParamScope.root(runtime_params).child(None, pins=entry_params)
    return resolve_scoped(
        config,
        config_type,
        scope,
        config.get("defaults"),
        runtime_params=runtime_params,
        fabric_context=fabric_context,
        file=file,
        inline_scopes=inline_scopes,
    )


# The list each kind holds its children in, and the kind of those children.
_CHILD_LISTS = {"loom": ("weaves", "weave"), "weave": ("threads", "thread")}


def substitute(
    config: Any,
    context: dict[str, Any],
    scope: ParamScope,
    reads: BindingReads | None,
    at: tuple[str | int, ...] = (),
    defaults: Mapping[str, Any] | None = None,
) -> Any:
    """Resolve ``config`` in ``context``, noting each ``scope`` binding it read.

    A bare reference's rewrite is classified by ``scope``'s declared params
    and ``defaults``, the ``defaults:`` block in force.
    """
    consumed: set[str] = set()
    names = BareNames(declared=frozenset(scope.specs), defaults=defaults or {})
    resolved = resolve_variables(config, context, consumed, at, names=names)
    if reads is not None:
        for key in consumed:
            namespace, _, rest = key.partition(".")
            binding = scope.bindings.get(rest.split(".", 1)[0]) if namespace == "param" else None
            if binding is not None:
                reads.note(binding)
    return resolved


def resolve_scoped(
    config: dict[str, Any],
    config_type: str,
    scope: ParamScope,
    defaults: dict[str, Any] | None,
    *,
    runtime_params: dict[str, Any] | None,
    fabric_context: dict[str, Any] | None,
    file: str | None,
    at: tuple[str | int, ...] = (),
    reads: BindingReads | None = None,
    inline_scopes: ScopePositions | None = None,
) -> dict[str, Any]:
    """Resolve ``config`` in ``scope``, each inline child in its own scope opened from it.

    An inline child's entry keys resolve in ``scope``, its body in the child's.
    """
    context = build_param_context(
        runtime_params, fabric_context=fabric_context, entry_params=scope.values()
    )
    key, child_type = _CHILD_LISTS.get(config_type, ("", ""))
    entries = config.get(key) if key else None
    if not isinstance(entries, list):
        return substitute(config, context, scope, reads, at, defaults)
    is_inline = is_inline_weave if config_type == "loom" else is_inline_thread
    inline = {i: entry for i, entry in enumerate(entries) if is_inline(entry)}
    if not inline:
        return substitute(config, context, scope, reads, at, defaults)

    # An inline child's own params: are its declarations, so they stay with the body.
    entry_keys = (WEAVE_ENTRY_KEYS if config_type == "loom" else THREAD_ENTRY_KEYS) - {"params"}
    shell = {**config, key: [None if i in inline else e for i, e in enumerate(entries)]}
    resolved = substitute(shell, context, scope, reads, at, defaults)
    for i, entry in inline.items():
        where = (*at, key, i)
        child_defaults = defaults
        if config_type == "loom":
            own = entry.get("defaults")
            child_defaults = {**(defaults or {}), **(own if isinstance(own, dict) else {})}
        child_scope = scope.open_child(entry.get("params"), file=file, at=where, reads=reads)
        if inline_scopes is not None:
            inline_scopes[where] = child_scope
        placement = substitute(
            {k: v for k, v in entry.items() if k in entry_keys},
            context,
            scope,
            reads,
            where,
            defaults,
        )
        body = resolve_scoped(
            {k: v for k, v in entry.items() if k not in entry_keys},
            child_type,
            child_scope,
            child_defaults,
            runtime_params=runtime_params,
            fabric_context=fabric_context,
            file=file,
            at=where,
            reads=reads,
            inline_scopes=inline_scopes,
        )
        resolved[key][i] = {k: placement[k] if k in placement else body[k] for k in entry}
    return resolved


def _entry_pins(
    entry: dict[str, Any],
    parent_context: dict[str, Any] | None,
    scope: ParamScope,
    reads: BindingReads,
    at: tuple[str | int, ...],
    defaults: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """A ``ref:`` entry's pins, resolved in its parent's scope; ``None`` when it pins none.

    With no ``parent_context`` the pins were already resolved, with the rest of
    the parent, by the variable pass, and are returned as they are.

    Entries reach here unvalidated: a ``params:`` that is not a mapping is left
    for the entry model to reject when the parent hydrates.
    """
    pins = entry.get("params")
    if not pins or not isinstance(pins, dict):
        return None
    if parent_context is None:
        return pins
    return substitute(pins, parent_context, scope, reads, (*at, "params"), defaults)


def _errors_recorded() -> int:
    """How many errors the active collecting load has recorded; 0 outside one."""
    errors = recorded()
    return len(errors) if errors is not None else 0


class ChildEntry:
    """One ``ref:`` entry being entered: its resolved pins and, once opened, its scope.

    Attributes:
        pins: The entry's pin values, resolved in the parent's scope; ``None``
            when it pins none.
        scope: The child's param scope; set by :meth:`open`.
        name: The child's own name; set by :meth:`open`.
    """

    def __init__(
        self, pins: dict[str, Any] | None, parent: ParamScope, reads: BindingReads
    ) -> None:
        """Hold the entry's ``pins`` for opening a scope beneath ``parent``."""
        self.pins = pins
        self.scope: ParamScope = parent
        self.name = ""
        self._parent = parent
        self._reads = reads

    def open(self, child: dict[str, Any], path: str) -> ParamScope:
        """Open the child's scope: its declared ``params:`` bound with the entry's pins.

        Args:
            child: The child's document, as loaded.
            path: The child's file.

        Returns:
            The child scope, also kept as :attr:`scope`.
        """
        self.name = child["name"]
        self.scope = self._parent.open_child(
            child.get("params"), file=path, pins=self.pins, reads=self._reads
        )
        return self.scope


@contextmanager
def enter_child(
    entry: dict[str, Any],
    parent_context: dict[str, Any] | None,
    scope: ParamScope,
    reads: BindingReads,
    at: tuple[str | int, ...],
    warnings: list[Diagnostic],
    *,
    file: str | None,
    alias_key: str,
    defaults: Mapping[str, Any] | None = None,
) -> Iterator[ChildEntry]:
    """Enter a ``ref:`` entry: resolve its pins, then report those nothing read.

    The caller loads the child and calls :meth:`ChildEntry.open` inside the
    block, resolves the child's subtree there, and leaves the block normally.
    Then each pin nothing beneath the entry read warns ``P123``, unless the
    subtree recorded an error, which hides whether its pins were read. If the
    block raises, nothing is reported.

    Args:
        entry: The ``ref:`` entry.
        parent_context: The parent's variable context for resolving the pins;
            ``None`` when the variable pass already resolved them.
        scope: The parent's param scope.
        reads: The bindings read so far, shared down the walk.
        at: Where the entry sits in the parent's file.
        warnings: Receives the ``P123`` warnings.
        file: The parent's file, which the warnings name.
        alias_key: The entry key that renames the child (``as`` for a thread,
            ``name`` for a weave); the child's own name stands in without it.
        defaults: The parent's ``defaults:``, which classify a bare reference
            in a pin.

    Yields:
        The entry, to open the child's scope from.
    """
    errors_before = _errors_recorded()
    pins = _entry_pins(entry, parent_context, scope, reads, at, defaults)
    child = ChildEntry(pins, scope, reads)
    yield child
    if _errors_recorded() == errors_before:
        warnings.extend(
            unconsumed_pins(
                child.pins,
                child.scope,
                reads,
                entry_name=entry.get(alias_key) or child.name,
                file=file,
                at=at,
            )
        )
