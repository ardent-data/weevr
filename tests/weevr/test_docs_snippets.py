"""Every complete weevr config shown in the docs loads as documented.

A fenced ``yaml`` block under ``docs/`` with a top-level ``config_version``
key is a complete config. A page's valid blocks are written into one scratch
project, so a weave on a page can reference the thread shown next to it: a
block named by a filename comment (``# silver/sap_table.thread``) is written
at that path, any other at ``<name>.<kind>`` in the project root. Each block is
then loaded through ``load_config`` and ``load_resolved``; a warp block is
validated as a ``WarpConfig``. The block's kind comes from, in order: an HTML comment hint on
the line above the fence, the block's own ``kind:``, a filename comment on the block's first line
(``# dim_customer.warp``), or its shape (``weaves:`` → loom, ``threads:`` →
weave, ``columns:`` → warp, ``sources:`` or ``target:`` → thread).

The hint is a YAML flow mapping with any of ``kind``, ``path`` (where the
block lives in the page's project), ``params`` (runtime params for the load)
and ``expect`` (the error class a deliberately invalid example must raise)::

    <!-- weevr-snippet: {path: staging/stg_customers.thread} -->
    <!-- weevr-snippet: {params: {run_date: "2026-01-01"}} -->
    <!-- weevr-snippet: {expect: ReferenceResolutionError} -->

Blocks that are deliberately partial, or that reference files the snippet
doesn't include, are listed with a reason in
``fixtures/docs_snippet_allowlist.yaml``, keyed by page and a hash of the
block's text so an edited block is checked again.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml

import weevr.errors
from weevr.config import load_config, load_resolved
from weevr.model.warp import WarpConfig

REPO = Path(__file__).resolve().parents[2]
DOCS = REPO / "docs"
ALLOWLIST = Path(__file__).parent / "fixtures" / "docs_snippet_allowlist.yaml"

_FENCE = re.compile(r"^(?P<indent> *)```ya?ml[^\n]*\n(?P<body>.*?)^(?P=indent)```", re.S | re.M)
_HINT = re.compile(r"<!--\s*weevr-snippet:\s*(?P<spec>\{.*\})\s*-->")
_FILE_COMMENT = re.compile(
    r"^#\s*(?P<path>(?:[\w.-]+/)*(?P<stem>[\w-]+)\.(?P<kind>thread|weave|loom|warp))(?:\s.*)?$"
)
_TOP_LEVEL_VERSION = re.compile(r"^config_version\s*:", re.M)
_DECLARED_KIND = re.compile(r"^kind\s*:\s*(?P<kind>\w+)\s*$", re.M)
KINDS = ("thread", "weave", "loom", "warp")


@dataclass(frozen=True)
class Snippet:
    """One complete config block in the docs."""

    page: str
    line: int
    body: str
    kind: str | None
    stem: str | None
    path: str | None = None
    params: tuple[tuple[str, Any], ...] = ()
    expect: str | None = None

    @property
    def key(self) -> str:
        """Allowlist key: page and a short hash of the block text."""
        digest = hashlib.sha256(self.body.encode()).hexdigest()[:8]
        return f"{self.page}#{digest}"


def _dedent(body: str, indent: str) -> str:
    return "".join(line.removeprefix(indent) for line in body.splitlines(keepends=True))


def _kind_from_shape(data: object) -> str | None:
    if not isinstance(data, dict):
        return None
    if "weaves" in data:
        return "loom"
    if "threads" in data:
        return "weave"
    if "columns" in data:
        return "warp"
    if "sources" in data or "target" in data:
        return "thread"
    return None


def extract_snippets(page: Path, root: Path = DOCS) -> list[Snippet]:
    """Return the complete config blocks on a docs page."""
    text = page.read_text()
    snippets = []
    for match in _FENCE.finditer(text):
        body = _dedent(match.group("body"), match.group("indent"))
        if not _TOP_LEVEL_VERSION.search(body):
            continue
        line = text.count("\n", 0, match.start()) + 1
        preceding = text[: match.start()].rstrip("\n").rsplit("\n", 1)[-1]
        first = body.lstrip().split("\n", 1)[0].strip()
        kind = stem = path = expect = None
        params: dict[str, Any] = {}
        if hint := _HINT.search(preceding):
            spec = yaml.safe_load(hint.group("spec"))
            kind, path, params = spec.get("kind"), spec.get("path"), spec.get("params") or {}
            expect = spec.get("expect")
            if path and not kind:
                kind = path.rsplit(".", 1)[-1]
        if kind is None and (declared := _DECLARED_KIND.search(body)):
            kind = declared.group("kind")
        if file_comment := _FILE_COMMENT.match(first):
            stem = file_comment.group("stem")
            path = path or file_comment.group("path")
            kind = kind or file_comment.group("kind")
        if kind is None:
            try:
                kind = _kind_from_shape(yaml.safe_load(body))
            except yaml.YAMLError:
                kind = None
        snippets.append(
            Snippet(
                page.relative_to(root).as_posix(),
                line,
                body,
                kind,
                stem,
                path,
                tuple(sorted(params.items())),
                expect,
            )
        )
    return snippets


def snippet_path(snippet: Snippet) -> str:
    """Return where a snippet lives in its page's project."""
    if snippet.path:
        return snippet.path
    data = yaml.safe_load(snippet.body)
    declared = data.get("name") if isinstance(data, dict) else None
    return f"{declared or snippet.stem or 'snippet'}.{snippet.kind}"


def write_project(snippets: list[Snippet], tmp_path: Path) -> Path:
    """Write a page's config snippets into one project; return its root."""
    project = tmp_path / "docs.weevr"
    project.mkdir(parents=True, exist_ok=True)
    for snippet in snippets:
        if snippet.kind in KINDS:
            target = project / snippet_path(snippet)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(snippet.body)
    return project


def load_snippet(snippet: Snippet, tmp_path: Path, page: list[Snippet] | None = None) -> None:
    """Load a snippet through the public load functions, or validate it as a warp.

    ``page`` holds the other snippets on its page, written alongside it.
    Deliberately invalid ones (those with ``expect``) are left out.
    """
    if snippet.kind not in KINDS:
        raise ValueError(f"can't tell the config kind of {snippet.page}:{snippet.line}")
    if snippet.kind == "warp":
        WarpConfig.model_validate(yaml.safe_load(snippet.body))
        return
    others = [s for s in page or [] if s != snippet and s.expect is None]
    # The snippet under test is written last, so it wins a path clash.
    project = write_project([*others, snippet], tmp_path)
    path = project / snippet_path(snippet)
    params = dict(snippet.params) or None
    load_config(path, runtime_params=params, project_root=project)
    load_resolved(path, runtime_params=params, project_root=project)


def _allowlist() -> dict[str, str]:
    entries = yaml.safe_load(ALLOWLIST.read_text()) or []
    return {entry["key"]: entry["reason"] for entry in entries}


# Release notes are history: their configs show the version they shipped in.
HISTORY = DOCS / "release-notes"

SNIPPETS = [
    s
    for page in sorted(DOCS.rglob("*.md"))
    if not page.is_relative_to(HISTORY)
    for s in extract_snippets(page)
]


def test_docs_have_complete_snippets() -> None:
    """The extractor finds complete configs to check."""
    assert len(SNIPPETS) > 10


@pytest.mark.parametrize("snippet", SNIPPETS, ids=lambda s: f"{s.page}:{s.line}")
def test_snippet_loads(snippet: Snippet, tmp_path: Path) -> None:
    """A complete config in the docs loads, unless it's allowlisted with a reason."""
    allowed = _allowlist()
    if snippet.key in allowed:
        pytest.skip(f"allowlisted: {allowed[snippet.key]}")
    page = [s for s in SNIPPETS if s.page == snippet.page]
    if snippet.expect is None:
        load_snippet(snippet, tmp_path, page)
        return
    expected = getattr(weevr.errors, snippet.expect)
    with pytest.raises(expected) as caught:
        load_snippet(snippet, tmp_path, page)
    assert type(caught.value) is expected, f"raised {type(caught.value).__name__}"


def test_allowlist_entries_are_live() -> None:
    """Every allowlist entry still matches a block and gives a reason."""
    keys = {s.key for s in SNIPPETS}
    for key, reason in _allowlist().items():
        assert key in keys, f"stale allowlist entry {key}: the block changed or is gone"
        assert reason.strip(), f"allowlist entry {key} needs a reason"


class TestExtractor:
    """The extractor finds blocks and their kinds."""

    @staticmethod
    def _page(tmp_path: Path, text: str) -> list[Snippet]:
        page = tmp_path / "page.md"
        page.write_text(text)
        return extract_snippets(page, tmp_path)

    def test_skips_fragments(self, tmp_path: Path) -> None:
        assert self._page(tmp_path, "```yaml\nsources: {}\n```\n") == []

    def test_kind_from_shape(self, tmp_path: Path) -> None:
        text = (
            '```yaml\nconfig_version: "2.0"\nweaves: []\n```\n'
            '```yml\nconfig_version: "2.0"\nthreads: []\n```\n'
            '```yaml\nconfig_version: "2.0"\ntarget: {}\n```\n'
        )
        assert [s.kind for s in self._page(tmp_path, text)] == ["loom", "weave", "thread"]

    def test_declared_kind_beats_shape(self, tmp_path: Path) -> None:
        text = '```yaml\nkind: thread\nconfig_version: "2.0"\nthreads: []\n```\n'
        assert [s.kind for s in self._page(tmp_path, text)] == ["thread"]

    def test_hint_beats_shape(self, tmp_path: Path) -> None:
        text = (
            "<!-- weevr-snippet: {kind: thread} -->\n"
            '```yaml\nconfig_version: "2.0"\nthreads: []\n```\n'
        )
        assert [s.kind for s in self._page(tmp_path, text)] == ["thread"]

    def test_hint_gives_path_and_params(self, tmp_path: Path) -> None:
        text = (
            "<!-- weevr-snippet: {path: a/b.loom, params: {d: '2026-01-01'}} -->\n"
            '```yaml\nconfig_version: "2.0"\nweaves: []\n```\n'
        )
        (snippet,) = self._page(tmp_path, text)
        assert (snippet.kind, snippet.path, snippet.params) == (
            "loom",
            "a/b.loom",
            (("d", "2026-01-01"),),
        )

    def test_file_comment_gives_kind_and_stem(self, tmp_path: Path) -> None:
        text = '```yaml\n# warps/dim_customer.warp — the contract\nconfig_version: "2.0"\n```\n'
        (snippet,) = self._page(tmp_path, text)
        assert (snippet.kind, snippet.stem, snippet.path) == (
            "warp",
            "dim_customer",
            "warps/dim_customer.warp",
        )

    def test_nested_version_is_a_fragment(self, tmp_path: Path) -> None:
        text = '```yaml\nthreads:\n  - name: t\n    config_version: "2.0"\n```\n'
        assert self._page(tmp_path, text) == []

    def test_page_snippets_share_a_project(self, tmp_path: Path) -> None:
        text = (
            '```yaml\n# w.weave\nkind: weave\nconfig_version: "2.0"\n'
            "threads:\n  - ref: sub/t.thread\n```\n"
            '```yaml\n# sub/t.thread\nkind: thread\nconfig_version: "2.0"\n'
            "sources:\n  s:\n    type: delta\n    alias: a.b\ntarget:\n  alias: c.d\n```\n"
        )
        weave, thread = self._page(tmp_path, text)
        load_snippet(weave, tmp_path / "out", [weave, thread])

    def test_indented_block_in_tabs(self, tmp_path: Path) -> None:
        text = '=== "Tab"\n\n    ```yaml\n    config_version: "2.0"\n    target: {}\n    ```\n'
        (snippet,) = self._page(tmp_path, text)
        assert snippet.body == 'config_version: "2.0"\ntarget: {}\n'
        assert snippet.line == 3

    def test_unknown_kind_fails_load(self, tmp_path: Path) -> None:
        (snippet,) = self._page(tmp_path, '```yaml\nconfig_version: "2.0"\n```\n')
        with pytest.raises(ValueError, match="config kind"):
            load_snippet(snippet, tmp_path)
