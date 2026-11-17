"""Where each value of a YAML document was written: JSON pointer → line and column.

The map is built from the composed YAML node graph, so it reflects exactly
what the author typed:

- A mapping entry is placed at its key; a list item at its first character.
- An alias is placed where the alias is written, not at its anchor.
- Keys merged in with ``<<`` are placed in the anchored mapping they came from.

Positions are 1-based. Building a map reads and composes the file again, so
it is done only when a diagnostic needs placing, never on a successful load.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import yaml

from weevr.diagnostics.record import Diagnostic

Segments = tuple[str | int, ...]


def json_pointer(segments: Sequence[str | int]) -> str:
    """Render path segments as a JSON pointer (``""`` for the root).

    Args:
        segments: Keys and list indexes from the root down.

    Returns:
        The pointer, with ``~`` and ``/`` escaped.
    """
    return "".join("/" + str(s).replace("~", "~0").replace("/", "~1") for s in segments)


def parse_pointer(pointer: str) -> Segments:
    """Split a JSON pointer into segments; digit-only segments become indexes.

    Args:
        pointer: A JSON pointer (``""`` or starting with ``/``).

    Returns:
        The segments.
    """
    if not pointer:
        return ()
    segments: list[str | int] = []
    for part in pointer[1:].split("/"):
        text = part.replace("~1", "/").replace("~0", "~")
        segments.append(int(text) if text.isdigit() else text)
    return tuple(segments)


@dataclass(frozen=True)
class PositionMap:
    """Positions of one YAML document's values.

    Attributes:
        positions: JSON pointer → (line, column), both 1-based.
        lengths: JSON pointer of each list → its number of items.
        substituted: Pointers of string values that hold a ``${…}``
            reference, so the value used at run time came from elsewhere.
    """

    positions: Mapping[str, tuple[int, int]] = field(default_factory=dict)
    lengths: Mapping[str, int] = field(default_factory=dict)
    substituted: frozenset[str] = frozenset()


class _PositionLoader(yaml.SafeLoader):
    """A safe loader that gives each alias its own node, marked where it is written."""

    def compose_node(self, parent: yaml.Node | None, index: Any) -> yaml.Node | None:
        """Compose a node; an alias becomes a copy of its anchor, marked at the alias."""
        if self.check_event(yaml.AliasEvent):
            event = self.peek_event()
            anchored = super().compose_node(parent, index)
            if anchored is None:  # pragma: no cover - an alias always resolves or raises
                return None
            alias = copy.copy(anchored)
            alias.start_mark = event.start_mark
            alias.end_mark = event.end_mark
            return alias
        return super().compose_node(parent, index)


def build_position_map(text: str) -> PositionMap:
    """Build the position map of a YAML document.

    Args:
        text: The document's text.

    Returns:
        Its position map.

    Raises:
        yaml.YAMLError: If the text is not valid YAML.
    """
    loader = _PositionLoader(text)
    try:
        node = loader.get_single_node()
        if node is None:
            return PositionMap()
        # Constructing flattens merge keys in place, so the walk below sees
        # merged entries with the marks of the anchored mapping they came from.
        loader.construct_document(node)
    finally:
        loader.dispose()
    positions: dict[str, tuple[int, int]] = {}
    lengths: dict[str, int] = {}
    substituted: set[str] = set()
    _walk(node, "", positions, lengths, substituted)
    positions[""] = _position(node)
    return PositionMap(positions, lengths, frozenset(substituted))


def _position(node: yaml.Node) -> tuple[int, int]:
    """A node's 1-based (line, column)."""
    return (node.start_mark.line + 1, node.start_mark.column + 1)


def _walk(
    node: yaml.Node,
    pointer: str,
    positions: dict[str, tuple[int, int]],
    lengths: dict[str, int],
    substituted: set[str],
) -> None:
    """Record the position of every entry under ``node``, keyed by pointer."""
    if isinstance(node, yaml.MappingNode):
        for key_node, value_node in node.value:
            if not isinstance(key_node, yaml.ScalarNode):
                continue
            # Merged entries come first; a later explicit key overwrites them,
            # as it does in the constructed mapping.
            child = pointer + json_pointer([key_node.value])
            positions[child] = _position(key_node)
            _walk(value_node, child, positions, lengths, substituted)
    elif isinstance(node, yaml.SequenceNode):
        lengths[pointer] = len(node.value)
        for index, item in enumerate(node.value):
            child = f"{pointer}/{index}"
            positions[child] = _position(item)
            _walk(item, child, positions, lengths, substituted)
    elif isinstance(node, yaml.ScalarNode) and "${" in str(node.value):
        substituted.add(pointer)


def read_position_map(file: str) -> PositionMap | None:
    """Build the position map of a local config file.

    Args:
        file: Path of the file.

    Returns:
        The map, or ``None`` when the file can't be read or parsed (remote
        files included); diagnostics then keep their pointer without a line.
    """
    try:
        return build_position_map(Path(file).read_text(encoding="utf-8"))
    except (OSError, ValueError, yaml.YAMLError):
        return None


ABSENT = object()
"""What :func:`child_of` returns when there is no such child."""


def child_of(node: Any, segment: str | int) -> Any:
    """The value at ``segment`` under a mapping or list, or :data:`ABSENT`.

    Args:
        node: A mapping, a list, or anything else (which has no children).
        segment: A key, or a list index.

    Returns:
        The child value, or :data:`ABSENT`.
    """
    if isinstance(node, Mapping) and segment in node:
        return node[segment]
    if isinstance(node, list) and isinstance(segment, int) and 0 <= segment < len(node):
        return node[segment]
    return ABSENT


def _value_at(data: Any, segments: Segments) -> Any:
    """The value at ``segments`` under ``data``, or :data:`ABSENT`."""
    node = data
    for segment in segments:
        node = child_of(node, segment)
        if node is ABSENT:
            break
    return node


def place(
    diagnostic: Diagnostic,
    positions: PositionMap | None,
    resolved: Any = None,
    at: Segments = (),
) -> Diagnostic:
    """Give a diagnostic its authored pointer, line and column.

    ``diagnostic.resolved_path`` points into ``resolved``, the validated
    document; ``at`` is where that document sits in the authored file (an
    inline thread's ``("threads", 2)``). A diagnostic that already has a
    pointer keeps it and only gains a line and column.

    - A value written in the file gets its own position.
    - A missing key gets the position of the mapping it is missing from.
    - A value the file doesn't hold (it came from a cascade) keeps its
      pointer with no line, flagged ``location_fallback``.
    - Under a list whose length differs from the authored one (a ``foreach``
      expanded it), the diagnostic is placed at that list, flagged
      ``location_fallback``, because item indexes no longer match the file.

    Args:
        diagnostic: The diagnostic to place.
        positions: The authored file's position map; ``None`` when unknown.
        resolved: The validated document, to compare list lengths with.
        at: The validated document's location in the authored file.

    Returns:
        The placed diagnostic.
    """
    fallback = diagnostic.location_fallback
    if diagnostic.pointer is not None:
        segments = parse_pointer(diagnostic.pointer)
    elif diagnostic.resolved_path is not None:
        full = at + parse_pointer(diagnostic.resolved_path)
        segments = _corresponding_prefix(full, at, resolved, positions)
        fallback = fallback or segments != full
    else:
        return diagnostic
    pointer = json_pointer(segments)
    if positions is None:
        return replace(diagnostic, pointer=pointer, location_fallback=fallback)
    line_column = _line_column(diagnostic.code, segments, positions)
    if pointer in positions.positions:
        fallback = fallback or pointer in positions.substituted
    elif line_column is None or diagnostic.code != "C101":
        fallback = True
    line, column = line_column if line_column is not None else (None, None)
    return replace(
        diagnostic, pointer=pointer, line=line, column=column, location_fallback=fallback
    )


def _corresponding_prefix(
    segments: Segments, at: Segments, resolved: Any, positions: PositionMap | None
) -> Segments:
    """The longest prefix of ``segments`` whose list indexes match the authored file."""
    if positions is None or resolved is None:
        return segments
    for depth in range(len(at), len(segments)):
        if not isinstance(segments[depth], int):
            continue
        authored = positions.lengths.get(json_pointer(segments[:depth]))
        value = _value_at(resolved, segments[len(at) : depth])
        if isinstance(value, list) and authored is not None and authored != len(value):
            return segments[:depth]
    return segments


def place_all(
    diagnostics: Sequence[Diagnostic],
    file: str | None,
    resolved: Any = None,
    at: Segments = (),
) -> list[Diagnostic]:
    """Place every diagnostic from one validated document in its authored file.

    Args:
        diagnostics: Diagnostics whose ``resolved_path`` points into ``resolved``.
        file: The authored file; its map is read once. ``None`` places
            pointers only.
        resolved: The validated document.
        at: The validated document's location in ``file``.

    Returns:
        The placed diagnostics.
    """
    positions = read_position_map(file) if file else None
    return [place(d, positions, resolved, at) for d in diagnostics]


def pointed(diagnostics: Sequence[Diagnostic], at: Segments = ()) -> list[Diagnostic]:
    """Set each diagnostic's authored pointer from its resolved path.

    For a document validated as written (before references and the cascade
    change its shape), the resolved path is the authored path below ``at``.

    Args:
        diagnostics: Diagnostics with ``resolved_path`` set.
        at: Where the validated document sits in its file.

    Returns:
        The diagnostics with ``pointer`` set.
    """
    return [
        replace(d, pointer=json_pointer(at + parse_pointer(d.resolved_path)))
        if d.pointer is None and d.resolved_path is not None
        else d
        for d in diagnostics
    ]


def fill_lines(diagnostics: Sequence[Diagnostic], file: str) -> list[Diagnostic]:
    """Add line and column to diagnostics in ``file`` that have a pointer but no line.

    Args:
        diagnostics: Any diagnostics; others pass through unchanged.
        file: The authored file whose map is read, at most once.

    Returns:
        The diagnostics, placed where possible.
    """
    pending = [
        d.file == file and d.pointer is not None and d.line is None and not d.location_fallback
        for d in diagnostics
    ]
    if not any(pending):
        return list(diagnostics)
    positions = read_position_map(file)
    if positions is None:
        return list(diagnostics)
    return [
        _with_line(d, positions) if todo else d
        for d, todo in zip(diagnostics, pending, strict=True)
    ]


def _with_line(diagnostic: Diagnostic, positions: PositionMap) -> Diagnostic:
    """Add the line and column of the diagnostic's pointer, when the map has it."""
    segments = parse_pointer(diagnostic.pointer or "")
    line_column = _line_column(diagnostic.code, segments, positions)
    if line_column is None:
        return diagnostic
    return replace(diagnostic, line=line_column[0], column=line_column[1])


def _line_column(code: str, segments: Segments, positions: PositionMap) -> tuple[int, int] | None:
    """Where ``segments`` was written; a missing key (``C101``) is placed at its mapping."""
    line_column = positions.positions.get(json_pointer(segments))
    if line_column is None and code == "C101" and segments:
        line_column = positions.positions.get(json_pointer(segments[:-1]))
    return line_column
