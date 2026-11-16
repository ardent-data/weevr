"""Loaded children travel in typed nodes and attributes, never in magic keys.

Reference resolution used to attach children and entry fields to config dicts
under underscore keys (``_resolved_threads``, ``_resolved_weaves``,
``_entry_as``, ``_source_path``…) that later stages had to pop before
hydration, and results carried an untyped ``_resolved_threads`` attribute.
This scan fails if any such key or attribute comes back in the packages.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

PACKAGES = Path(__file__).resolve().parents[3] / "packages"
SOURCES = sorted(PACKAGES.glob("*/src/weevr/**/*.py"))

MAGIC_KEYS = {"_resolved_threads", "_resolved_weaves", "_source_path"}
MAGIC_PREFIX = "_entry_"


def magic_strings(source: str) -> list[tuple[int, str]]:
    """Return ``(line, text)`` for every magic key or attribute name in ``source``.

    Catches string literals (dict keys, ``getattr`` names, ``__slots__``
    entries, f-string prefixes) and attribute access such as
    ``result._resolved_threads``.
    """
    found = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            text = node.value
        elif isinstance(node, ast.Attribute):
            text = node.attr
        else:
            continue
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

    def test_attribute_names(self) -> None:
        source = (
            'class C:\n    __slots__ = ("_resolved_threads",)\n'
            'getattr(result, "_resolved_threads", None)\n'
            "result._resolved_threads = {}\n"
        )
        assert [text for _, text in magic_strings(source)] == ["_resolved_threads"] * 3

    def test_function_names_are_not_keys(self) -> None:
        assert magic_strings("def _entry_name(entry): ...\n_entry_name(1)\n") == []
