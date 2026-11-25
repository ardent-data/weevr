"""Registry assets as their declaring configs wrote them, for pin checks.

A connection, lookup, column set, audit template or variable whose fields
hold ``${param.x}`` resolves once, in the scope of the config that declares
it; a pin on an entry below never retargets it. This module records which
params each asset references, which assets a config sees (its own over its
enclosing configs', by kind and name), and which of those a pin would have
needed to retarget (``P122``).
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from weevr.config.interpolation import VARIABLE_PATTERN, param_name

# The registry blocks a config can declare; every one is substituted with the rest of
# its declaring config, so its ${param.x} references resolve in that config's scope.
REGISTRY_KINDS = ("connections", "lookups", "column_sets", "audit_templates", "variables")

AssetKey = tuple[str, str]
"""A registry asset's kind and name: the key a lower declaration shadows it by."""


@dataclass(frozen=True)
class DeclaredAsset:
    """One registry asset declaration, as written.

    Two declarations are equal only when they are the same declaration: the
    same kind and name at the same place in the same file. A re-declaration
    with an identical body elsewhere is a different asset.

    Attributes:
        kind: The registry block (``"connections"``, ``"lookups"``, ...).
        name: The asset's name in the block.
        file: The declaring config's file; ``None`` when unknown.
        at: Where the declaring config sits in ``file`` (an inline weave's
            ``("weaves", 0)``); empty for a config in its own file.
        params: The names its fields reference as ``${param.x}``.
    """

    kind: str
    name: str
    file: str | None
    at: tuple[str | int, ...]
    params: frozenset[str]

    def describe(self) -> str:
        """``kind.name (file)``, for a diagnostic's ``expected``."""
        label = f"{self.kind}.{self.name}"
        return f"{label} ({self.file})" if self.file is not None else label


def _referenced_params(value: Any) -> set[str]:
    """The names ``value`` references as ``${param.x}``, with or without a ``:-`` default."""
    if isinstance(value, Mapping):
        return {name for item in value.values() for name in _referenced_params(item)}
    if isinstance(value, list):
        return {name for item in value for name in _referenced_params(item)}
    if not isinstance(value, str):
        return set()
    names = (param_name(match.group(1)) for match in VARIABLE_PATTERN.finditer(value))
    return {name for name in names if name is not None}


def declared_assets(
    document: Mapping[str, Any], file: str | None, at: tuple[str | int, ...] = ()
) -> dict[AssetKey, DeclaredAsset]:
    """The registry assets ``document`` declares itself, as written.

    Read before substitution, so each asset records every param it
    references whether or not the name is bound.

    Args:
        document: A config or inline body, before variable substitution.
        file: The file it is written in.
        at: Where it sits in ``file``.

    Returns:
        The assets by kind and name.
    """
    assets: dict[AssetKey, DeclaredAsset] = {}
    for kind in REGISTRY_KINDS:
        block = document.get(kind)
        if not isinstance(block, Mapping):
            continue
        for name, body in block.items():
            params = frozenset(_referenced_params(body))
            assets[(kind, str(name))] = DeclaredAsset(kind, str(name), file, at, params)
    return assets


def visible_assets(
    outer: Mapping[AssetKey, DeclaredAsset],
    document: Mapping[str, Any],
    file: str | None,
    at: tuple[str | int, ...] = (),
) -> dict[AssetKey, DeclaredAsset]:
    """The assets a config sees: its own declarations over its enclosing config's.

    A declaration shadows an outer one of the same kind and name, as the load
    cascades do for the resolved values (``_cascade_connections`` and
    ``cascade_registry`` in :mod:`weevr.config.inheritance`, and
    ``ScopeRegistries.under``).

    Args:
        outer: The assets the enclosing config sees; empty at the root.
        document: The config or inline body, before variable substitution.
        file: The file it is written in.
        at: Where it sits in ``file``.

    Returns:
        The assets by kind and name.
    """
    return {**outer, **declared_assets(document, file, at)}


def upstream_readers(
    pins: Mapping[str, Any] | None,
    parent: Mapping[AssetKey, DeclaredAsset],
    child: Mapping[AssetKey, DeclaredAsset],
) -> dict[str, list[str]]:
    """By pin name, the upstream assets the pinned child still sees that read it.

    An asset is upstream of an entry when the entry's config sees it
    (``parent``): it is declared there or in a config enclosing it, so it
    resolved before the pin existed. The child still sees it when the child's
    asset of that kind and name is the same declaration; one the child
    declares itself shadows it.

    Args:
        pins: The entry's pins; ``None`` when it pins none.
        parent: The assets the entry's config sees.
        child: The assets the pinned child config sees.

    Returns:
        Each pin some such asset reads, with those assets described.
    """
    readers: dict[str, list[str]] = {}
    for name in pins or {}:
        found = [
            asset.describe()
            for key, asset in parent.items()
            if name in asset.params and child.get(key) == asset
        ]
        if found:
            readers[name] = found
    return readers
