"""The resolved config tree: ``ResolvedDocument`` and ``EntryMeta``."""

from __future__ import annotations

import dataclasses
import shutil
from pathlib import Path

import pytest

from weevr.config import EntryMeta, ResolvedDocument, load_resolved
from weevr.model import Loom, Thread, Weave
from weevr.model.weave import ConditionSpec

FORMS = Path(__file__).parents[1] / "fixtures" / "load_parity" / "forms.weevr"


@pytest.fixture
def project(tmp_path: Path) -> Path:
    root = tmp_path / "forms.weevr"
    shutil.copytree(FORMS, root)
    return root


class TestShape:
    """The tree's nodes are frozen and keyed by effective name."""

    def test_loom_tree(self, project: Path) -> None:
        doc = load_resolved(project / "forms.loom", project_root=project).document
        assert isinstance(doc, ResolvedDocument)
        assert (doc.kind, doc.name, doc.entry) == ("loom", "forms", None)
        assert isinstance(doc.model, Loom)
        assert doc.source_ref == str(project / "forms.loom")
        assert list(doc.children) == ["core", "conv", "inline_w"]
        core = doc.children["core"]
        assert core.kind == "weave"
        assert isinstance(core.model, Weave)
        assert list(core.children) == ["base", "tpl_a", "tpl_b", "inline_core"]
        assert all(isinstance(t.model, Thread) for t in core.children.values())
        assert all(t.children == {} for t in core.children.values())

    def test_source_refs_name_the_file_each_child_is_written_in(self, project: Path) -> None:
        doc = load_resolved(project / "forms.loom", project_root=project).document
        core = doc.children["core"]
        assert core.source_ref == str(project / "weaves" / "core.weave")
        assert core.children["base"].source_ref == str(project / "threads" / "base.thread")
        assert core.children["tpl_a"].source_ref == str(project / "threads" / "template.thread")
        # Inline children live in their parent's file.
        assert core.children["inline_core"].source_ref == core.source_ref
        assert doc.children["inline_w"].source_ref == doc.source_ref
        assert doc.children["inline_w"].children["inline_t"].source_ref == doc.source_ref
        assert doc.children["conv"].source_ref == str(project / "conv.weave")

    def test_frozen(self, project: Path) -> None:
        doc = load_resolved(project / "forms.loom", project_root=project).document
        with pytest.raises(dataclasses.FrozenInstanceError):
            doc.kind = "weave"  # type: ignore[misc]
        with pytest.raises(TypeError):
            doc.children["x"] = doc  # type: ignore[index]
        entry = doc.children["core"].children["tpl_a"].entry
        assert entry is not None
        with pytest.raises(dataclasses.FrozenInstanceError):
            entry.alias = "x"  # type: ignore[misc]

    def test_thread_and_weave_roots(self, project: Path) -> None:
        thread = load_resolved(project / "threads" / "base.thread", project_root=project).document
        assert (thread.kind, thread.name, thread.children) == ("thread", "base", {})
        weave = load_resolved(project / "conv.weave", project_root=project).document
        assert (weave.kind, weave.name, list(weave.children)) == ("weave", "conv", ["conv"])


class TestEntryMeta:
    """Each child records the entry that pulled it in."""

    def test_template_entries(self, project: Path) -> None:
        core = load_resolved(project / "forms.loom", project_root=project).document.children["core"]
        assert core.children["tpl_a"].entry == EntryMeta(
            ref="threads/template.thread",
            alias="tpl_a",
            params={"suffix": "a"},
            dependencies=("base",),
            condition=None,
        )
        tpl_b = core.children["tpl_b"].entry
        assert tpl_b is not None
        assert tpl_b.condition == ConditionSpec(when="table_exists('gold.base')")
        assert tpl_b.dependencies is None

    def test_inline_and_plain_entries(self, project: Path) -> None:
        doc = load_resolved(project / "forms.loom", project_root=project).document
        core = doc.children["core"]
        assert core.children["base"].entry == EntryMeta(ref="threads/base.thread")
        assert core.children["inline_core"].entry == EntryMeta(dependencies=("tpl_a",))

    def test_weave_entries(self, project: Path) -> None:
        doc = load_resolved(project / "forms.loom", project_root=project).document
        core = doc.children["core"].entry
        assert core is not None
        assert core.ref == "weaves/core.weave"
        assert core.condition == ConditionSpec(when="'dev' == 'dev'")
        assert doc.children["conv"].entry == EntryMeta(ref="conv.weave")


class TestViews:
    """Convenience views over the tree."""

    def test_weave_threads_and_weaves(self, project: Path) -> None:
        doc = load_resolved(project / "forms.loom", project_root=project).document
        threads = doc.weave_threads()
        assert list(threads) == ["core", "conv", "inline_w"]
        assert threads["core"]["tpl_a"] is doc.children["core"].children["tpl_a"].model
        assert doc.weaves()["conv"] is doc.children["conv"].model

    def test_weave_root_views(self, project: Path) -> None:
        doc = load_resolved(project / "conv.weave", project_root=project).document
        assert list(doc.weave_threads()) == ["conv"]
        assert doc.weaves() == {}

    def test_thread_root_views(self, project: Path) -> None:
        doc = load_resolved(project / "threads" / "base.thread", project_root=project).document
        assert doc.weave_threads() == {}
        assert doc.weaves() == {}
