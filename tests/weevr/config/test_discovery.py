"""Tests for document discovery."""

from pathlib import Path

import pytest

from documents import write_document, write_project
from weevr.config import DiscoveredDocument, discover_documents
from weevr.errors import ConfigError

THREAD = 'config_version: "2.0"\nsources: {}\n'


def _project(tmp_path: Path) -> Path:
    root = write_project(tmp_path / "p")
    (root / "threads").mkdir()
    write_document(root / "threads" / "a.thread", THREAD)
    write_document(root / "main.loom", 'config_version: "2.0"\n')
    return root


def test_lists_documents_sorted_with_kinds(tmp_path: Path):
    root = _project(tmp_path)
    assert discover_documents(root) == (
        DiscoveredDocument("main.loom", "loom"),
        DiscoveredDocument("threads/a.thread", "thread"),
        DiscoveredDocument("weevr.yaml", "project"),
    )


def test_skips_tooling_dot_dirs_and_platform(tmp_path: Path):
    root = _project(tmp_path)
    for d in (".weevr", ".git"):
        (root / d).mkdir()
        write_document(root / d / "x.thread", THREAD)
    (root / ".platform").write_text("{}")
    assert [d.path for d in discover_documents(root)] == [
        "main.loom",
        "threads/a.thread",
        "weevr.yaml",
    ]


def test_kindless_yaml_is_inert_but_kindless_typed_file_is_listed(tmp_path: Path):
    root = _project(tmp_path)
    (root / "data.yaml").write_text("a: 1\n")
    (root / "notes.txt").write_text("x")
    (root / "bare.weave").write_text('config_version: "2.0"\n')
    paths = {d.path: d.kind for d in discover_documents(root)}
    assert "data.yaml" not in paths and "notes.txt" not in paths
    assert paths["bare.weave"] is None


def test_yaml_document_with_kind_is_listed(tmp_path: Path):
    root = _project(tmp_path)
    write_document(root / "w.yaml", 'config_version: "2.0"\n', kind="warp")
    assert DiscoveredDocument("w.yaml", "warp") in discover_documents(root)


def test_adding_a_file_changes_only_its_entry(tmp_path: Path):
    root = _project(tmp_path)
    before = discover_documents(root)
    write_document(root / "threads" / "b.thread", THREAD)
    after = discover_documents(root)
    assert set(after) - set(before) == {DiscoveredDocument("threads/b.thread", "thread")}
    assert set(before) <= set(after)


def test_missing_root_raises_instead_of_listing_nothing(tmp_path: Path) -> None:
    """A root that isn't a directory is an error, not an empty project."""
    with pytest.raises(ConfigError) as exc_info:
        discover_documents(tmp_path / "missing")
    assert [d.code for d in exc_info.value.diagnostics] == ["C209"]
