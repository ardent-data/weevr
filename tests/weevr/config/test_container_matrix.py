"""The same project loads identically from a ``.weevr`` and a Git-shaped container."""

import json
from pathlib import Path
from typing import Any

import pytest

from documents import write_document, write_project
from weevr.config import ResolvedDocument, load_config, load_resolved
from weevr.model import Loom

THREAD = """\
config_version: "2.0"
sources:
  main:
    type: csv
    path: data/in.csv
target:
  path: out
"""


def _build(root: Path) -> Path:
    write_project(root)
    (root / "silver" / "deep").mkdir(parents=True)
    write_document(root / "silver" / "a.thread", THREAD)
    write_document(root / "silver" / "deep" / "b.thread", THREAD)
    write_document(
        root / "silver.weave",
        (
            'config_version: "2.0"\nthreads:\n'
            "  - ref: silver/a.thread\n  - ref: silver/deep/b.thread\n"
        ),
    )
    write_document(root / "nightly.loom", 'config_version: "2.0"\nweaves:\n  - ref: silver.weave\n')
    return root


@pytest.mark.parametrize("container", ["x.weevr", "x.WeevrProject"])
def test_loom_loads_from_each_container(tmp_path: Path, container: str) -> None:
    root = _build(tmp_path / container)
    if container.endswith(".WeevrProject"):
        (root / ".platform").write_text('{"metadata": {"type": "WeevrProject"}}')
    loom = load_config(root / "nightly.loom", project_root=root)
    assert isinstance(loom, Loom)
    assert [e.ref for e in loom.weaves] == ["silver.weave"]


def _tree(document: ResolvedDocument) -> dict[str, Any]:
    """The resolved tree as plain data: every node's model, ref and children."""
    return {
        "name": document.name,
        "kind": document.kind,
        "source_ref": document.source_ref,
        "project_root": document.project_root,
        "model": document.model.model_dump(mode="json"),
        "children": {name: _tree(child) for name, child in document.children.items()},
    }


def _root_relative(document: ResolvedDocument, root: Path) -> str:
    """The tree as JSON with the container's own root path replaced by a marker."""
    return json.dumps(_tree(document), sort_keys=True).replace(str(root), "<root>")


def test_resolved_models_are_identical_across_containers(tmp_path: Path) -> None:
    plain = _build(tmp_path / "plain" / "x.weevr")
    git = _build(tmp_path / "git" / "x.WeevrProject")
    (git / ".platform").write_text('{"metadata": {"type": "WeevrProject"}}')
    a = load_config(plain / "nightly.loom", project_root=plain)
    b = load_config(git / "nightly.loom", project_root=git)
    assert isinstance(a, Loom) and isinstance(b, Loom)
    assert a.model_dump() == b.model_dump()

    resolved_a = load_resolved(plain / "nightly.loom", project_root=plain).document
    resolved_b = load_resolved(git / "nightly.loom", project_root=git).document
    assert len(resolved_a.children["silver"].children) == 2
    tree = _root_relative(resolved_a, plain)
    assert "<root>" in tree
    assert tree == _root_relative(resolved_b, git)
