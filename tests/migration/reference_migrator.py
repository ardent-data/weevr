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
from collections.abc import Callable
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


def _expected(raw: dict[str, Any], kind: str, rewritten: set[int]) -> dict[str, Any]:
    """The parsed document the rules must produce, to check the line edits against."""
    expected = copy.deepcopy(raw)
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


def migrate_document(
    text: str, suffix: str, *, weave_exists: Callable[[str], bool] = lambda name: True
) -> str:
    """Apply rules 1-4 to one document's text.

    Args:
        text: The document text.
        suffix: Its file extension (``.thread``, ``.yaml``, ...).
        weave_exists: Whether ``<name>.weave`` exists at the project root.

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
    migrated = bom + "".join(body + eol for body, eol in out)

    # The line edits must mean exactly what the rules say; otherwise not migrated.
    try:
        result = yaml.safe_load(migrated[len(bom) :])
    except yaml.YAMLError:
        return text
    return migrated if result == _expected(raw, kind, rewritten) else text


def project_name(folder: str) -> str:
    """Derive the ``weevr.yaml`` name from the project folder's name (rule 5)."""
    stem = folder.removesuffix(".weevr")
    name = _NAME_UNSAFE.sub("_", stem)
    if not name:
        return "project"
    if not ("A" <= name[0] <= "Z" or "a" <= name[0] <= "z"):
        name = f"project_{name}"
    return name[:100]


def migrate_project(source: Path, dest: Path, *, project_id: str) -> None:
    """Migrate the project folder ``source`` into ``dest``.

    Args:
        source: The v1 project folder (it becomes the v2 project root).
        dest: Where to write the migrated tree.
        project_id: The ``weevr.yaml`` id; tools generate a fresh UUID4.
    """

    def weave_exists(name: str) -> bool:
        return (source / f"{name}.weave").is_file()

    for dirpath, dirnames, filenames in os.walk(source):
        dirnames.sort()
        here = Path(dirpath)
        hidden = any(part.startswith(".") for part in here.relative_to(source).parts)
        for filename in sorted(filenames):
            src = here / filename
            out = dest / src.relative_to(source)
            out.parent.mkdir(parents=True, exist_ok=True)
            suffix = src.suffix.lower()
            migratable = suffix in KIND_BY_SUFFIX or suffix in YAML_SUFFIXES
            if hidden or filename == ".platform" or not migratable:
                shutil.copyfile(src, out)
                continue
            data = src.read_bytes()
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                out.write_bytes(data)
                continue
            out.write_bytes(migrate_document(text, suffix, weave_exists=weave_exists).encode())

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
