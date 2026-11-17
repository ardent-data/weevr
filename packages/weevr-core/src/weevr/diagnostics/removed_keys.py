"""The removed and renamed key table: v1 keys v2 no longer reads, and what replaced them.

A key in the table found in an authored document fails the load with a
dedicated code naming its replacement (``C112``) or why it was removed
(``C111``), whether or not the schema rejects unknown keys. The same
table drives the migration guide and weevr-cli's ``migrate``.

Each topic that removes or renames a key adds its row when it lands.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from weevr.diagnostics.positions import Segments, json_pointer, parse_pointer
from weevr.diagnostics.record import Diagnostic
from weevr.diagnostics.registry import diagnostic

WILDCARD = "*"
"""A path segment matching any one key or list index."""


@dataclass(frozen=True)
class RemovedKey:
    """One removed or renamed key.

    Attributes:
        kind: The config kind the key was read in (``thread``, ``weave``,
            ``loom``, ``params`` or ``warp``).
        path: JSON pointer to the old key; ``*`` matches any one segment.
        replacement: Pointer to the key that replaced it, in the same form,
            for a rename. ``*`` segments take the matched values.
        reason: Why it was removed, for a key with no replacement.
        removed_in: The version that stopped reading it.
    """

    kind: str
    path: str
    replacement: str | None = None
    reason: str | None = None
    removed_in: str = ""

    def __post_init__(self) -> None:
        """Require exactly one of ``replacement`` and ``reason``."""
        if (self.replacement is None) == (self.reason is None):
            raise ValueError(f"Removed key '{self.path}' needs a replacement or a reason")


REMOVED_KEYS: tuple[RemovedKey, ...] = ()
"""Every removed or renamed key, by the topic that removed it. Rows are only added."""


def removed_key_diagnostics(
    document: Mapping[str, Any],
    kind: str,
    *,
    at: Segments = (),
    table: Sequence[RemovedKey] | None = None,
) -> list[Diagnostic]:
    """Find removed or renamed keys in an authored document.

    Args:
        document: The document as written, before references and the cascade.
        kind: Its config kind.
        at: Where the document sits in its file, for an inline body;
            prefixes each pointer.
        table: The rows to check; defaults to :data:`REMOVED_KEYS`.

    Returns:
        One diagnostic per key found, with its authored ``pointer``.
    """
    rows = REMOVED_KEYS if table is None else table
    found: list[Diagnostic] = []
    for row in rows:
        if row.kind != kind:
            continue
        for matched in _matches(document, parse_pointer(row.path), ()):
            found.append(_diagnostic(row, matched, at))
    return found


def _matches(node: Any, pattern: Segments, at: Segments) -> Iterator[Segments]:
    """Every concrete path under ``node`` that ``pattern`` matches."""
    if not pattern:
        yield at
        return
    head, rest = str(pattern[0]), pattern[1:]
    if isinstance(node, Mapping):
        keys = list(node) if head == WILDCARD else [head] if head in node else []
        for key in keys:
            yield from _matches(node[key], rest, (*at, key))
    elif isinstance(node, list):
        indexes = (
            range(len(node))
            if head == WILDCARD
            else [int(head)]
            if head.isdigit() and int(head) < len(node)
            else []
        )
        for index in indexes:
            yield from _matches(node[index], rest, (*at, index))


def _diagnostic(row: RemovedKey, matched: Segments, at: Segments) -> Diagnostic:
    """The removed-key or renamed-key diagnostic for one match."""
    pointer = json_pointer((*at, *matched))
    if row.replacement is not None:
        replacement = json_pointer(
            (*at, *_fill(parse_pointer(row.path), parse_pointer(row.replacement), matched))
        )
        return diagnostic(
            "C112",
            f"'{pointer}' was renamed to '{replacement}' in weevr {row.removed_in}",
            pointer=pointer,
            expected=[replacement],
            hint=f"Rename '{pointer}' to '{replacement}'.",
        )
    return diagnostic(
        "C111",
        f"'{pointer}' was removed in weevr {row.removed_in}",
        pointer=pointer,
        hint=row.reason,
    )


def _fill(old: Segments, replacement: Segments, matched: Segments) -> Segments:
    """The replacement path, its ``*`` segments taking what the old path's matched, in order."""
    values = iter(m for p, m in zip(old, matched, strict=True) if str(p) == WILDCARD)
    return tuple(next(values, s) if str(s) == WILDCARD else s for s in replacement)
