"""Lookups, column sets and variables cascade loom → weave → thread at load.

Each node of the resolved tree keeps the registries it declares and the
effective ones it sees: outer scopes merged in, inner names winning. The rules
are the ones the engine applied at run time (``merge_resource_dicts``).
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import pytest

from weevr.config import load_resolved
from weevr.config.document import ScopeRegistries
from weevr.config.inheritance import cascade_registry

FIXTURES = Path(__file__).parents[1] / "fixtures" / "load_parity"
FORMS = FIXTURES / "forms.weevr"


class TestCascadeRegistry:
    """The merge rule, on plain mappings."""

    def test_both_absent(self) -> None:
        assert cascade_registry(None, None, "lookup", "loom", "weave") is None

    @pytest.mark.parametrize("inner", [None, {}])
    def test_absent_or_empty_inner_inherits(self, inner: dict | None) -> None:
        outer = {"a": 1}
        merged = cascade_registry(outer, inner, "lookup", "loom", "weave")
        assert merged == {"a": 1}
        assert merged is not outer

    def test_empty_inner_and_absent_outer(self) -> None:
        assert cascade_registry(None, {}, "lookup", "loom", "weave") is None

    def test_inner_only(self) -> None:
        assert cascade_registry(None, {"b": 2}, "lookup", "loom", "weave") == {"b": 2}

    def test_inner_wins(self) -> None:
        merged = cascade_registry({"a": 1, "s": 1}, {"s": 2, "b": 3}, "lookup", "loom", "weave")
        assert merged == {"a": 1, "s": 2, "b": 3}

    def test_debug_log_per_shadowed_key(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.DEBUG, logger="weevr.config.inheritance"):
            cascade_registry({"a": 1, "s": 1, "t": 1}, {"s": 2, "t": 2}, "lookup", "loom", "weave")
        messages = [r.getMessage() for r in caplog.records]
        assert messages == [
            "Resource 's' in weave shadows loom-level definition",
            "Resource 't' in weave shadows loom-level definition",
        ]

    def test_quiet(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.DEBUG, logger="weevr.config.inheritance"):
            cascade_registry({"s": 1}, {"s": 2}, "lookup", "loom", "weave", log_shadowing=False)
        assert caplog.records == []


@pytest.fixture
def forms(tmp_path: Path):
    root = tmp_path / "forms.weevr"
    shutil.copytree(FORMS, root)
    return load_resolved(root / "forms.loom", project_root=root).document


class TestTreeScopes:
    """Declared and effective registries on every node of the forms project."""

    def test_loom_effective_is_declared(self, forms) -> None:
        assert forms.effective == forms.declared
        assert set(forms.declared.lookups or {}) == {"regions", "shared"}

    def test_weave_shadows_loom(self, forms) -> None:
        core = forms.children["core"]
        assert set(core.declared.lookups or {}) == {"shared", "weave_only"}
        assert set(core.effective.lookups or {}) == {"regions", "shared", "weave_only"}
        assert core.effective.lookups["shared"] is core.declared.lookups["shared"]
        assert core.effective.column_sets["shared_map"].param == "weave_shared_map"
        assert core.effective.column_sets["loom_map"].param == "loom_map"
        assert core.effective.variables["shared_var"].default == 2
        assert core.effective.variables["loom_var"].default == "a"

    def test_thread_shadows_weave(self, forms) -> None:
        core = forms.children["core"]
        base = core.children["base"]
        assert set(base.declared.lookups or {}) == {"shared"}
        assert base.effective.lookups["shared"] is base.declared.lookups["shared"]
        assert base.effective.lookups["shared"].source.alias == "ref.shared_thread"
        assert base.effective.variables["shared_var"].default == 3
        assert set(base.effective.column_sets or {}) == {"loom_map", "shared_map", "thread_map"}

    def test_inherited_entry_is_effective_not_declared_and_same_object(self, forms) -> None:
        core = forms.children["core"]
        tpl_a = core.children["tpl_a"]
        regions = forms.declared.lookups["regions"]
        assert "regions" not in (core.declared.lookups or {})
        assert "regions" not in (tpl_a.declared.lookups or {})
        assert core.effective.lookups["regions"] is regions
        assert tpl_a.effective.lookups["regions"] is regions

    def test_thread_declaring_nothing_sees_its_weave(self, forms) -> None:
        core = forms.children["core"]
        tpl_a = core.children["tpl_a"]
        assert tpl_a.declared == ScopeRegistries()
        assert tpl_a.effective == core.effective

    def test_inline_weave_scope(self, forms) -> None:
        inline_w = forms.children["inline_w"]
        assert inline_w.effective.lookups["shared"].source.alias == "ref.shared_inline"
        assert (
            inline_w.children["inline_t"].effective.lookups["shared"]
            is (inline_w.declared.lookups["shared"])
        )

    def test_standalone_thread(self, tmp_path: Path) -> None:
        root = tmp_path / "forms.weevr"
        shutil.copytree(FORMS, root)
        doc = load_resolved(root / "threads" / "base.thread", project_root=root).document
        assert doc.effective == doc.declared
        assert set(doc.effective.lookups or {}) == {"shared"}
