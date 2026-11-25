"""Reference implementation of the v1 → v2 migration rules.

Implements ``docs/reference/migration-rules.md`` exactly, as line edits on
the document text, so the rules can be proven against the fixture golden.
It is test code: neither wheel ships migration code, and weevr-cli
implements the same rules independently.
"""

from __future__ import annotations

import copy
import json
import os
import re
import shutil
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml

# Deliberately independent of weevr.config.parser: a standalone reference for weevr-cli.
KIND_BY_SUFFIX = {".thread": "thread", ".weave": "weave", ".loom": "loom", ".warp": "warp"}
YAML_SUFFIXES = (".yaml", ".yml")
KINDS = ("thread", "weave", "loom", "warp", "params")
BOM = "\ufeff"

_LINE = re.compile(r"(?P<body>[^\r\n]*)(?P<eol>\r\n|\n|\r|\Z)")
_ROOT_KEY = re.compile(r"^(?P<key>[A-Za-z_][\w-]*)\s*:(?:\s|$)")
_V1_VERSION = r"\"1\.\d+\"|'1\.\d+'|1\.\d+"
_VERSION_LINE = re.compile(
    rf"^(?P<lead>\s*(?:-\s+)?config_version\s*:\s*)(?P<value>{_V1_VERSION})(?P<rest>(?:\s+#.*)?\s*)$"
)
_DASH = re.compile(r"^(?P<indent> *)-(?P<gap>\s+|$)")
_SCALAR_ITEM = re.compile(
    r"^(?P<lead> *-\s+)(?P<value>\"[^\"]*\"|'[^']*'|[^#\s][^#]*?)(?P<rest>(?:\s+#.*)?)$"
)
_NAME_KEY = re.compile(
    r"^name(?P<sep>\s*:\s*)(?P<value>\"[^\"]*\"|'[^']*'|[^#\s][^#]*?)(?P<rest>(?:\s+#.*)?)$"
)
_BLOCK_SCALAR = re.compile(
    r"^(?P<lead> *(?:-\s+)*)(?P<key>[^\s#-][^#]*?:\s+)?[|>][-+0-9]*\s*(?:#.*)?$"
)
_MARKER = re.compile(r"^---(\s|$)")
_PLAIN_REF = re.compile(r"^[A-Za-z0-9_./-]+$")
_NAME_UNSAFE = re.compile(r"[^A-Za-z0-9_]")

NAMESPACES = ("param", "fabric", "var", "run", "thread", "weave", "loom")
FABRIC_KEYS = ("workspace_id", "lakehouse_id", "workspace_name")
_REFERENCE = re.compile(r"\$\{([^}:]+?)(?::-(.*?))?\}")
_DEFAULTS_HEADER = re.compile(r"^defaults\s*:\s*(#.*)?$")
_PARAMS_HEADER = re.compile(r"^params\s*:\s*(#.*)?$")
_DEFAULT_ENTRY = re.compile(
    r"^(?P<key>[A-Za-z_][\w-]*)\s*:\s+"
    r"(?P<value>\"[^\"]*\"|'[^']*'|\[[^\]#]*\]|[^#\s\[{|>&*!\"'][^#]*?)"
    r"(?P<rest>(?:\s+#.*)?)\s*$"
)


def _split(text: str) -> list[tuple[str, str]]:
    """Split text into (body, line ending) pairs on ``\\r\\n``, ``\\n`` and ``\\r`` only."""
    return [
        (match["body"], match["eol"])
        for match in _LINE.finditer(text)
        if match["body"] or match["eol"]
    ]


def _is_v1_version(value: Any) -> bool:
    if isinstance(value, str):
        return re.fullmatch(r"1\.\d+", value) is not None
    return isinstance(value, float) and int(value) == 1


def _yaml_kind(raw: dict[str, Any]) -> str | None:
    """Detect a .yaml/.yml document's kind in v1's order (rule 1)."""
    if raw.get("type") in KINDS:
        return raw["type"]
    if "weaves" in raw:
        return "loom"
    if "threads" in raw:
        return "weave"
    if "sources" in raw or "target" in raw:
        return "thread"
    if "config_version" in raw and len(raw) > 1:
        return "params"
    return None


def _is_preamble(body: str) -> bool:
    """A blank, comment, directive or ``---`` line above the first root key."""
    stripped = body.strip()
    return not stripped or stripped.startswith(("#", "%")) or _MARKER.match(body) is not None


def _is_comment_or_blank(body: str) -> bool:
    stripped = body.strip()
    return not stripped or stripped.startswith("#")


def _ref_text(name: str) -> str:
    ref = f"{name}.weave"
    return ref if _PLAIN_REF.match(ref) else json.dumps(ref)


def _rewrite_weave_entries(
    lines: list[tuple[str, str]], entries: list[Any], weave_exists: Callable[[str], bool]
) -> set[int]:
    """Rule 4: rewrite plain-string and name-only loom entries in place.

    Returns:
        The indices of the entries rewritten.
    """
    rewritten: set[int] = set()
    start = next(
        (i for i, (body, _) in enumerate(lines) if re.match(r"^weaves\s*:\s*(#.*)?$", body)),
        None,
    )
    if start is None:
        return rewritten
    end = start + 1
    while end < len(lines):
        body = lines[end][0]
        if body[:1] not in ("", " ", "-") and not _is_comment_or_blank(body):
            break
        end += 1
    dashes = [
        (i, len(match["indent"]))
        for i in range(start + 1, end)
        if not _is_comment_or_blank(lines[i][0]) and (match := _DASH.match(lines[i][0]))
    ]
    if not dashes:
        return rewritten
    item_indent = dashes[0][1]
    items = [i for i, indent in dashes if indent == item_indent]
    if len(items) != len(entries):
        return rewritten
    bounds = [*items[1:], end]
    for position, (entry, first, stop) in enumerate(zip(entries, items, bounds, strict=True)):
        if isinstance(entry, str):
            if not weave_exists(entry):
                continue
            body, eol = lines[first]
            match = _SCALAR_ITEM.match(body)
            if match:
                lines[first] = (f"{match['lead']}ref: {_ref_text(entry)}{match['rest']}", eol)
                rewritten.add(position)
            continue
        if not (
            isinstance(entry, dict)
            and isinstance(entry.get("name"), str)
            and "ref" not in entry
            and set(entry) <= {"name", "condition"}
            and weave_exists(entry["name"])
        ):
            continue
        dash = _DASH.match(lines[first][0])
        assert dash is not None
        key_col = item_indent + 1 + len(dash["gap"])
        if not lines[first][0][key_col:].strip():
            # "-" alone on its line: the keys start on the next content line.
            content = [b for b, _ in lines[first + 1 : stop] if not _is_comment_or_blank(b)]
            if not content:
                continue
            key_col = len(content[0]) - len(content[0].lstrip(" "))
        for index in range(first, stop):
            body, eol = lines[index]
            if index != first and body[:key_col].strip():
                continue
            match = _NAME_KEY.match(body[key_col:])
            if match:
                ref = _ref_text(entry["name"])
                lines[index] = (f"{body[:key_col]}ref{match['sep']}{ref}{match['rest']}", eol)
                rewritten.add(position)
                break
    return rewritten


def _scalar_content(lines: list[tuple[str, str]]) -> set[int]:
    """The indices of lines inside block scalars (``key: |`` / ``key: >`` content)."""
    inside: set[int] = set()
    opener: int | None = None
    for index, (body, _) in enumerate(lines):
        indent = len(body) - len(body.lstrip(" "))
        if opener is not None and (not body.strip() or indent > opener):
            inside.add(index)
            continue
        opener = None
        match = _BLOCK_SCALAR.match(body)
        if match:
            lead = match["lead"]
            opener = len(lead) if match["key"] else lead.rstrip().rfind("-")
    return inside


def _bump(node: Any) -> None:
    if isinstance(node, dict) and _is_v1_version(node.get("config_version")):
        node["config_version"] = "2.0"


def _expected(
    raw: dict[str, Any],
    kind: str,
    rewritten: set[int],
    tiers: Sequence[Tier],
    converted: tuple[list[tuple[str, str]], set[str]] | None,
) -> dict[str, Any]:
    """The parsed document the rules must produce, to check the line edits against."""
    expected = _rewrite_tree(copy.deepcopy(raw), tiers)
    if converted:
        _convert_tree(expected, *converted)
    if raw.get("type") in KINDS:
        del expected["type"]
    expected.setdefault("kind", kind)
    _bump(expected)
    threads = expected.get("threads")
    for thread in threads if isinstance(threads, list) else ():
        _bump(thread)
    weaves = expected.get("weaves")
    entries: list[Any] = weaves if isinstance(weaves, list) else []
    for position, weave in enumerate(entries):
        if position in rewritten:
            if isinstance(weave, str):
                entries[position] = {"ref": f"{weave}.weave"}
            else:
                body = {key: value for key, value in weave.items() if key != "name"}
                entries[position] = {**body, "ref": f"{weave['name']}.weave"}
            continue
        _bump(weave)
        inline_threads = weave.get("threads") if isinstance(weave, dict) else None
        for thread in inline_threads if isinstance(inline_threads, list) else ():
            _bump(thread)
    return expected


@dataclass(frozen=True)
class Tier:
    """What one document of a scope chain contributes to classifying a reference.

    Attributes:
        file: The document's path relative to the project root.
        declared: Names declared in a ``params:`` block, at the root or inline.
        defaults: Root ``defaults:`` keys rule 8 can convert: a one-line scalar
            (or one-line list of strings) in a block ``defaults:``, in a document
            whose root ``params:`` is absent or a block mapping.
    """

    file: str
    declared: frozenset[str] = frozenset()
    defaults: frozenset[str] = frozenset()


@dataclass(frozen=True)
class Rewrite:
    """One bare reference and the rule that rewrote it.

    Attributes:
        file: The document's path relative to the project root.
        line: The 1-based line of the reference in the v1 document.
        reference: The reference as written.
        result: The reference as rewritten.
        rule: The page's rule that classified the name: 7 (declared), 8 (a
            ``defaults:`` key), 9 (a Fabric key) or 10 (fallback). A dotted name
            is classified by its first segment, so it never has rule 9.
        dotted: Whether the name was dotted (the page's "Dotted names").
    """

    file: str
    line: int
    reference: str
    result: str
    rule: int
    dotted: bool


@dataclass
class Report:
    """What a migration did that a reviewer should look at.

    Attributes:
        rewrites: Each bare reference rewritten.
        skipped: The documents left as v1 although they were migratable, because
            the line edits did not mean what the rules say (for example a bare
            reference in a mapping key).
    """

    rewrites: list[Rewrite] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    @property
    def fallbacks(self) -> list[Rewrite]:
        """The references no declaration or default explains (rule 10): declare them."""
        return [r for r in self.rewrites if r.rule == 10]


@dataclass(frozen=True)
class _Ref:
    index: int
    start: int
    end: int
    name: str
    fallback: str | None


@dataclass(frozen=True)
class _Entry:
    index: int
    key: str
    indent: int
    value: str
    rest: str
    parsed: Any


def _comment_start(body: str) -> int:
    """Where a trailing ``#`` comment starts on a line; ``len(body)`` when it has none."""
    quote: str | None = None
    previous = ""
    index = 0
    while index < len(body):
        char = body[index]
        if quote:
            if quote == '"' and char == "\\":
                index += 1
            elif char == quote:
                quote = None
        elif char in "\"'" and previous in ("", ":", "-", "[", "{", ","):
            quote = char
        elif char == "#" and previous in ("", " ", "\t"):
            return index
        if char not in " \t":
            previous = char
        elif previous not in ("", ":", "-", "[", "{", ","):
            previous = " "
        index += 1
    return len(body)


def _find_refs(lines: list[tuple[str, str]]) -> list[_Ref]:
    """The bare references on the lines: outside comments, none in a namespace."""
    scalar = _scalar_content(lines)
    found = []
    for index, (body, _) in enumerate(lines):
        limit = len(body) if index in scalar else _comment_start(body)
        for match in _REFERENCE.finditer(body[:limit]):
            name = match[1].strip()
            if name.partition(".")[0] not in NAMESPACES:
                found.append(_Ref(index, match.start(), match.end(), name, match[2]))
    return found


def _classify(name: str, tiers: Sequence[Tier]) -> tuple[int, str | None]:
    """Classify a bare name by its first segment: ``(rule, converted file)``.

    The rule is 7, 8, 9 or 10, first match wins; the file is the document whose
    ``defaults:`` key rule 8 converts.
    """
    head, dotted, _ = name.partition(".")
    if any(head in tier.declared for tier in tiers):
        return 7, None
    if tier := next((t for t in tiers if head in t.defaults), None):
        return 8, tier.file
    if not dotted and head in FABRIC_KEYS:
        return 9, None
    return 10, None


def _rewrite(ref_name: str, fallback: str | None, rule: int) -> str:
    """The namespaced reference: ``fabric`` for rule 9, else ``param``; ``:-`` kept."""
    namespace = "fabric" if rule == 9 else "param"
    return "${" + f"{namespace}.{ref_name}" + ("" if fallback is None else f":-{fallback}") + "}"


def _rewrite_text(text: str, tiers: Sequence[Tier]) -> str:
    """Rewrite the bare references in one string value."""

    def replace(match: re.Match[str]) -> str:
        name = match[1].strip()
        if name.partition(".")[0] in NAMESPACES:
            return match[0]
        rule, _ = _classify(name, tiers)
        return _rewrite(name, match[2], rule)

    return _REFERENCE.sub(replace, text)


def _rewrite_tree(node: Any, tiers: Sequence[Tier]) -> Any:
    if isinstance(node, str):
        return _rewrite_text(node, tiers)
    if isinstance(node, list):
        return [_rewrite_tree(item, tiers) for item in node]
    if isinstance(node, dict):
        return {key: _rewrite_tree(value, tiers) for key, value in node.items()}
    return node


def _declared_names(raw: dict[str, Any]) -> frozenset[str]:
    """Param names declared in the document's ``params:`` and in its inline scopes."""
    names: set[str] = set()

    def visit(node: Any) -> None:
        if not isinstance(node, dict):
            return
        params = node.get("params")
        if isinstance(params, dict):
            names.update(str(key) for key in params)
        elif isinstance(params, list):
            names.update(p["name"] for p in params if isinstance(p, dict) and "name" in p)
        for key in ("weaves", "threads"):
            children = node.get(key)
            for child in children if isinstance(children, list) else ():
                visit(child)

    visit(raw)
    return frozenset(names)


def _is_content(body: str) -> bool:
    return not _is_comment_or_blank(body)


def _defaults_entries(
    lines: list[tuple[str, str]],
) -> tuple[int, list[_Entry]] | None:
    """The root ``defaults:`` header line and its convertible one-line entries."""
    header = next((i for i, (b, _) in enumerate(lines) if _DEFAULTS_HEADER.match(b)), None)
    if header is None:
        return None
    entries: list[_Entry] = []
    indent: int | None = None
    for index in range(header + 1, len(lines)):
        body = lines[index][0]
        if not _is_content(body):
            continue
        depth = len(body) - len(body.lstrip(" "))
        if depth == 0:
            break
        indent = depth if indent is None else indent
        match = _DEFAULT_ENTRY.match(body[depth:]) if depth == indent else None
        if not match:
            continue
        try:
            parsed = yaml.safe_load(match["value"])
        except yaml.YAMLError:
            continue
        scalar = parsed is not None and not isinstance(parsed, dict | list)
        strings = isinstance(parsed, list) and all(isinstance(item, str) for item in parsed)
        if scalar or strings:
            entries.append(
                _Entry(index, match["key"], depth, match["value"], match["rest"], parsed)
            )
    return header, entries


def _params_block(lines: list[tuple[str, str]]) -> tuple[int, int, int] | str | None:
    """The root ``params:`` block: ``(header, last content line, key indent)``.

    Returns:
        ``None`` when the document has no root ``params``; ``"unsupported"`` when it is
        not a block mapping.
    """
    header = next((i for i, (b, _) in enumerate(lines) if re.match(r"^params\s*:", b)), None)
    if header is None:
        return None
    if not _PARAMS_HEADER.match(lines[header][0]):
        return "unsupported"
    last = indent = None
    for index in range(header + 1, len(lines)):
        body = lines[index][0]
        if not _is_content(body):
            continue
        depth = len(body) - len(body.lstrip(" "))
        if depth == 0:
            break
        if indent is None:
            if body.lstrip().startswith("-"):
                return "unsupported"
            indent = depth
        last = index
    return "unsupported" if last is None or indent is None else (header, last, indent)


def scan_tier(file: str, raw: dict[str, Any], lines: list[tuple[str, str]]) -> Tier:
    """What ``file`` contributes to the scope chain of the references below it."""
    block = _params_block(lines)
    found = _defaults_entries(lines)
    convertible = (
        frozenset(e.key for e in found[1]) if found and block != "unsupported" else frozenset()
    )
    return Tier(file, _declared_names(raw), convertible)


def _param_type(value: Any) -> tuple[str, bool]:
    """The ``ParamSpec`` type a ``defaults:`` value migrates to, and whether to quote it.

    Dates and timestamps stay strings, as v1 substituted their text.
    """
    if isinstance(value, bool):
        return "bool", False
    if isinstance(value, int):
        return "int", False
    if isinstance(value, float):
        return "float", False
    if isinstance(value, list):
        return "list[string]", False
    return "string", isinstance(value, date | datetime)


def _spec_lines(entry: _Entry, key_indent: int, step: int) -> list[str]:
    type_name, quote = _param_type(entry.parsed)
    value = json.dumps(entry.value) if quote else entry.value
    lead = " " * key_indent
    inner = " " * (key_indent + step)
    return [
        f"{lead}{entry.key}:",
        f"{inner}name: {entry.key}",
        f"{inner}type: {type_name}",
        f"{inner}default: {value}{entry.rest}",
    ]


def _convert_defaults(lines: list[tuple[str, str]], keys: set[str]) -> list[tuple[str, str]]:
    """Rule 8: move the named ``defaults:`` keys into ``params:`` as ParamSpecs."""
    found = _defaults_entries(lines)
    block = _params_block(lines)
    assert found is not None and block != "unsupported"
    header, entries = found
    chosen = [entry for entry in entries if entry.key in keys]
    removed = {entry.index for entry in chosen}
    step = chosen[0].indent
    eol = lines[header][1] or "\n"
    end = next(
        (
            i
            for i in range(header + 1, len(lines))
            if _is_content(lines[i][0]) and lines[i][0][:1] != " "
        ),
        len(lines),
    )
    keeps_block = any(_is_content(lines[i][0]) and i not in removed for i in range(header + 1, end))
    out: list[tuple[str, str]] = []
    for index, pair in enumerate(lines):
        if index == header:
            if block is None:
                out.append(("params:", eol))
                for entry in chosen:
                    out.extend((text, eol) for text in _spec_lines(entry, step, step))
                if keeps_block:
                    out.append(("", eol))
            if not keeps_block:
                continue
        if index not in removed:
            out.append(pair)
        if isinstance(block, tuple) and index == block[1]:
            if not pair[1]:
                out[-1] = (pair[0], "\n")
            texts = [t for entry in chosen for t in _spec_lines(entry, block[2], step)]
            out.extend((text, pair[1] or "\n") for text in texts[:-1])
            out.append((texts[-1], pair[1]))
    return out


def _convert_tree(expected: dict[str, Any], lines: list[tuple[str, str]], keys: set[str]) -> None:
    """The parsed result of :func:`_convert_defaults`, for the equivalence check."""
    found = _defaults_entries(lines)
    assert found is not None
    defaults = expected.get("defaults")
    for entry in found[1]:
        if entry.key not in keys:
            continue
        type_name, quote = _param_type(entry.parsed)
        default = entry.value if quote else entry.parsed
        expected.setdefault("params", {})[entry.key] = {
            "name": entry.key,
            "type": type_name,
            "default": default,
        }
        if isinstance(defaults, dict):
            defaults.pop(entry.key, None)
    if isinstance(defaults, dict) and not defaults:
        del expected["defaults"]


def _rewrite_references(
    lines: list[tuple[str, str]], refs: list[_Ref], chain: Sequence[Tier], file: str
) -> list[Rewrite]:
    """Rewrite each bare reference in place on its line, last first.

    Returns:
        The rewrites, in document order.
    """
    rewrites = []
    for ref in reversed(refs):
        rule, _ = _classify(ref.name, chain)
        body, eol = lines[ref.index]
        result = _rewrite(ref.name, ref.fallback, rule)
        dotted = "." in ref.name
        rewrites.append(
            Rewrite(file, ref.index + 1, body[ref.start : ref.end], result, rule, dotted)
        )
        lines[ref.index] = (body[: ref.start] + result + body[ref.end :], eol)
    return rewrites[::-1]


def _means(migrated: str, expected: dict[str, Any]) -> bool:
    """Whether the migrated text parses to the document the rules must produce."""
    try:
        return yaml.safe_load(migrated) == expected
    except yaml.YAMLError:
        return False


def _document_lines(text: str) -> tuple[dict[str, Any], list[tuple[str, str]]] | None:
    """The parsed root and the lines of a block-mapping v1 document; else ``None``."""
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError:
        return None
    if not isinstance(raw, dict) or not _is_v1_version(raw.get("config_version")):
        return None
    lines = _split(text)
    first_key = next((i for i, (b, _) in enumerate(lines) if not _is_preamble(b)), None)
    if first_key is None or not _ROOT_KEY.match(lines[first_key][0]):
        return None
    return raw, lines


def migrate_document(
    text: str,
    suffix: str,
    *,
    weave_exists: Callable[[str], bool] = lambda name: True,
    tiers: Sequence[Tier] | None = None,
    converted: set[str] | None = None,
    report: Report | None = None,
    file: str = "",
) -> str:
    """Apply the rules to one document's text.

    Args:
        text: The document text.
        suffix: Its file extension (``.thread``, ``.yaml``, ...).
        weave_exists: Whether ``<name>.weave`` exists at the project root.
        tiers: The scope chain of the document's references, nearest first, its
            own tier first. By default the chain is the document alone.
        converted: The ``defaults:`` keys of this document that rule 8 converts.
            By default, those its own references resolve to.
        report: Collects each reference rewritten.
        file: The document's path in ``report``.

    Returns:
        The migrated text, or ``text`` unchanged when it is not a v1 document.
    """
    suffix = suffix.lower()
    bom = BOM if text.startswith(BOM) else ""
    body_text = text[len(bom) :]
    try:
        raw = yaml.safe_load(body_text)
    except yaml.YAMLError:
        return text
    if not isinstance(raw, dict) or not _is_v1_version(raw.get("config_version")):
        return text
    kind = KIND_BY_SUFFIX.get(suffix) or (_yaml_kind(raw) if suffix in YAML_SUFFIXES else None)
    if kind is None:
        return text

    lines = _split(body_text)
    first_key = next((i for i, (b, _) in enumerate(lines) if not _is_preamble(b)), None)
    if first_key is None or not _ROOT_KEY.match(lines[first_key][0]):
        # Not a block mapping at column 0 (flow style, JSON, indented): not migrated.
        return text

    own = scan_tier(file, raw, lines)
    chain: Sequence[Tier] = tiers if tiers is not None else [own]
    refs = _find_refs(lines)
    if converted is None:
        converted = {
            ref.name.partition(".")[0] for ref in refs if _classify(ref.name, chain)[1] == file
        }
    converted = converted & own.defaults

    # Rule 2: bump every 1.x config_version outside block scalars.
    scalar = _scalar_content(lines)
    for index, (body, eol) in enumerate(lines):
        match = None if index in scalar else _VERSION_LINE.match(body)
        if match:
            value = "'2.0'" if match["value"].startswith("'") else '"2.0"'
            lines[index] = (f"{match['lead']}{value}{match['rest']}", eol)

    # Rule 4: loom weave entries.
    rewritten: set[int] = set()
    if kind == "loom" and isinstance(raw.get("weaves"), list):
        rewritten = _rewrite_weave_entries(lines, raw["weaves"], weave_exists)

    # Rules 6-10: the bare-reference rewrite.
    rewrites = _rewrite_references(lines, refs, chain, file)
    pre_conversion = list(lines)

    # Rule 3: drop the root type line naming a kind.
    removed: int | None = None
    if raw.get("type") in KINDS:
        removed = next(
            (
                i
                for i, (b, _) in enumerate(lines)
                if (m := _ROOT_KEY.match(b)) and m["key"] == "type"
            ),
            None,
        )

    # Rule 1: insert kind before the first root key line.
    insert: list[tuple[str, str]] = []
    if "kind" not in raw:
        insert = [(f"kind: {kind}", lines[first_key][1] or "\n")]

    # Inserting before the first key, then removing a root type line that was that
    # first key, is the page's "kind takes the place of a dropped root type:".
    out = [
        *(pair for i, pair in enumerate(lines[:first_key]) if i != removed),
        *insert,
        *(pair for i, pair in enumerate(lines[first_key:], first_key) if i != removed),
    ]
    if converted:
        out = _convert_defaults(out, converted)
    migrated = bom + "".join(body + eol for body, eol in out)

    # The line edits must mean exactly what the rules say; otherwise not migrated.
    pending = (pre_conversion, converted) if converted else None
    expected = _expected(raw, kind, rewritten, chain, pending)
    if not _means(migrated[len(bom) :], expected):
        if report is not None:
            report.skipped.append(file)
        return text
    if report is not None:
        report.rewrites.extend(rewrites)
    return migrated


def project_name(folder: str) -> str:
    """Derive the ``weevr.yaml`` name from the project folder's name (rule 5)."""
    stem = folder.removesuffix(".weevr")
    name = _NAME_UNSAFE.sub("_", stem)
    if not name:
        return "project"
    if not ("A" <= name[0] <= "Z" or "a" <= name[0] <= "z"):
        name = f"project_{name}"
    return name[:100]


def _is_migratable(relative: Path) -> bool:
    """Whether a file is handled by the rules: a document extension, not hidden.

    Files under a folder whose name starts with ``.``, and ``.platform`` files,
    are copied unchanged.
    """
    suffix = relative.suffix.lower()
    if any(part.startswith(".") for part in relative.parent.parts) or relative.name == ".platform":
        return False
    return suffix in KIND_BY_SUFFIX or suffix in YAML_SUFFIXES


def _children(raw: dict[str, Any], weave_exists: Callable[[str], bool]) -> set[str]:
    """The files a document references: ``ref:`` paths and loom weaves named by file."""
    found: set[str] = set()

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            ref = node.get("ref")
            if isinstance(ref, str):
                found.add(ref)
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(raw)
    weaves = raw.get("weaves")
    for entry in weaves if isinstance(weaves, list) else ():
        name = (
            entry
            if isinstance(entry, str)
            else entry.get("name")
            if isinstance(entry, dict)
            else None
        )
        if isinstance(name, str) and weave_exists(name):
            found.add(f"{name}.weave")
    return found


def _chains(tiers: dict[str, Tier], parents: dict[str, set[str]]) -> dict[str, list[Tier]]:
    """Each document's scope chain: itself, then the documents above it, nearest first."""
    chains: dict[str, list[Tier]] = {}
    for file in tiers:
        seen, order, frontier = {file}, [file], [file]
        while frontier:
            following = sorted({p for f in frontier for p in parents.get(f, ())} - seen)
            seen.update(following)
            order.extend(following)
            frontier = following
        chains[file] = [tiers[f] for f in order]
    return chains


def migrate_project(source: Path, dest: Path, *, project_id: str) -> Report:
    """Migrate the project folder ``source`` into ``dest``.

    Args:
        source: The v1 project folder (it becomes the v2 project root).
        dest: Where to write the migrated tree.
        project_id: The ``weevr.yaml`` id; tools generate a fresh UUID4.

    Returns:
        The report: every bare reference rewritten, rule-10 fallbacks among them.
    """

    def weave_exists(name: str) -> bool:
        return (source / f"{name}.weave").is_file()

    # Pass 1: read every migratable document and its place in the reference graph.
    docs: dict[str, tuple[str, str]] = {}
    tiers: dict[str, Tier] = {}
    parents: dict[str, set[str]] = {}
    for src in sorted(p for p in source.rglob("*") if p.is_file()):
        relative = src.relative_to(source)
        suffix = src.suffix.lower()
        if not _is_migratable(relative):
            continue
        try:
            text = src.read_bytes().decode("utf-8")
        except UnicodeDecodeError:
            continue
        name = relative.as_posix()
        docs[name] = (text, suffix)
        parsed = _document_lines(text[len(BOM) :] if text.startswith(BOM) else text)
        if parsed is not None:
            tiers[name] = scan_tier(name, *parsed)
            for child in _children(parsed[0], weave_exists):
                parents.setdefault(child, set()).add(name)
    chains = _chains(tiers, parents)

    # Pass 2: which defaults keys rule 8 converts, per declaring document.
    converted: dict[str, set[str]] = {}
    for name, chain in chains.items():
        parsed = _document_lines(docs[name][0].removeprefix(BOM))
        assert parsed is not None
        for ref in _find_refs(parsed[1]):
            declaring = _classify(ref.name, chain)[1]
            if declaring is not None:
                converted.setdefault(declaring, set()).add(ref.name.partition(".")[0])

    report = Report()
    for dirpath, dirnames, filenames in os.walk(source):
        dirnames.sort()
        here = Path(dirpath)
        for filename in sorted(filenames):
            src = here / filename
            out = dest / src.relative_to(source)
            out.parent.mkdir(parents=True, exist_ok=True)
            suffix = src.suffix.lower()
            if not _is_migratable(src.relative_to(source)):
                shutil.copyfile(src, out)
                continue
            data = src.read_bytes()
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                out.write_bytes(data)
                continue
            name = src.relative_to(source).as_posix()
            migrated = migrate_document(
                text,
                suffix,
                weave_exists=weave_exists,
                tiers=chains.get(name),
                converted=converted.get(name, set()),
                report=report,
                file=name,
            )
            out.write_bytes(migrated.encode())

    dest.mkdir(parents=True, exist_ok=True)
    if not (source / "weevr.yaml").exists():
        (dest / "weevr.yaml").write_bytes(
            (
                "kind: project\n"
                'config_version: "2.0"\n'
                f"id: {project_id}\n"
                f"name: {project_name(source.name)}\n"
            ).encode()
        )
    return report
