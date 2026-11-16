"""The config load carries children in typed nodes, never in magic dict keys.

Reference resolution used to attach children and entry fields to config dicts
under underscore keys (``_resolved_threads``, ``_resolved_weaves``,
``_entry_as``, ``_source_path``…) that later stages had to pop before
hydration. This scan fails if any such key comes back anywhere in the
packages.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

PACKAGES = Path(__file__).resolve().parents[3] / "packages"
SOURCES = sorted(PACKAGES.glob("*/src/weevr/**/*.py"))

MAGIC_KEYS = {"_resolved_threads", "_resolved_weaves", "_source_path"}
MAGIC_PREFIX = "_entry_"


def _attribute_names(tree: ast.Module) -> set[int]:
    """Ids of string constants that name attributes, not dict keys.

    ``__slots__`` entries and the name argument of ``getattr``/``setattr``/
    ``hasattr`` are attribute names.
    """
    ids: set[int] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in ("getattr", "setattr", "hasattr")
            and len(node.args) >= 2
        ):
            ids.add(id(node.args[1]))
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "__slots__" for t in node.targets)
            and isinstance(node.value, ast.Tuple | ast.List)
        ):
            ids.update(id(elt) for elt in node.value.elts)
    return ids


def magic_strings(source: str) -> list[tuple[int, str]]:
    """Return ``(line, text)`` for every string literal that is a magic key."""
    tree = ast.parse(source)
    attributes = _attribute_names(tree)
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        if id(node) in attributes:
            continue
        text = node.value
        if text in MAGIC_KEYS or text.startswith(MAGIC_PREFIX):
            found.append((node.lineno, text))
    return sorted(found)


def test_sources_found() -> None:
    assert len(SOURCES) > 50


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.relative_to(PACKAGES).as_posix())
def test_no_magic_config_keys(path: Path) -> None:
    """No module reads or writes a magic key on a config dict."""
    assert magic_strings(path.read_text()) == []


class TestScanner:
    """The scanner catches the key shapes the load used to write."""

    def test_subscript_and_get(self) -> None:
        source = 'd["_resolved_threads"] = []\nd.get("_resolved_weaves")\nd.pop("_source_path")\n'
        assert [text for _, text in magic_strings(source)] == [
            "_resolved_threads",
            "_resolved_weaves",
            "_source_path",
        ]

    def test_entry_prefix_in_fstring(self) -> None:
        source = 'd[f"_entry_{key}"] = 1\nd.pop("_entry_as", None)\n'
        assert [text for _, text in magic_strings(source)] == ["_entry_", "_entry_as"]

    def test_slot_names_are_not_keys(self) -> None:
        source = 'class C:\n    __slots__ = ("_resolved_threads",)\n'
        assert magic_strings(source) == []

    def test_getattr_names_are_not_keys(self) -> None:
        assert magic_strings('getattr(result, "_resolved_threads", None)\n') == []

    def test_function_names_are_not_keys(self) -> None:
        assert magic_strings("def _entry_name(entry): ...\n_entry_name(1)\n") == []
