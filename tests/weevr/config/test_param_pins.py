"""Entry pins scope a value to their subtree.

A loom's weave entry and a weave's thread entry can pin ``params:``. Pin
values resolve in the parent scope, then bind in the child, where they are the
ambient value for the whole subtree: they beat runtime params there, and are
invisible to the parent and to sibling entries. An entry's ``condition``
belongs to the parent and resolves in its scope. A pin no binding in the
subtree ever reads warns ``P123``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest

from documents import write_document
from weevr.config import load_resolved
from weevr.config.loader import collect_resolved_config
from weevr.config.locations import LocalConfigLocation
from weevr.config.params import Binding, ParamScope
from weevr.config.scoping import ChildEntry, WalkState, enter_child
from weevr.errors import ConfigError, ConfigSchemaError
from weevr.model.loom import Loom, WeaveEntry

THREAD: dict[str, Any] = {
    "config_version": "2.0",
    "sources": {"s": {"type": "delta", "alias": "raw.t"}},
    "target": {"alias": "out.t"},
}


def _specs(**specs: dict[str, Any]) -> dict[str, Any]:
    """ParamSpec mappings keyed by name."""
    return {name: {"name": name, "type": "string", **spec} for name, spec in specs.items()}


def _optional(**defaults: str) -> dict[str, Any]:
    """Optional string specs with the given defaults."""
    return _specs(**{name: {"required": False, "default": d} for name, d in defaults.items()})


def _thread_body(alias: str = "out.${param.x}", **extra: Any) -> dict[str, Any]:
    return {**THREAD, "target": {"alias": alias}, **extra}


def _write(root: Path, name: str, body: dict[str, Any]) -> Path:
    return write_document(root / name, body)


def _weave_body(threads: list[Any], **extra: Any) -> dict[str, Any]:
    return {"config_version": "2.0", "threads": threads, **extra}


def _loom(root: Path, weaves: list[Any], **extra: Any) -> Path:
    return _write(root, "l.loom", {"config_version": "2.0", "weaves": weaves, **extra})


def _loaded(path: Path, runtime: dict[str, Any] | None = None) -> Any:
    return load_resolved(path, runtime_params=runtime, project_root=path.parent)


def _alias(doc: Any) -> str:
    return doc.model.target.alias


def _codes(loaded: Any, code: str) -> list[Any]:
    return [d for d in loaded.diagnostics if d.code == code]


@pytest.fixture
def matrix(tmp_path: Path) -> Any:
    """A loom with a pinned weave (ref + inline thread) beside an unpinned sibling.

    Runtime binds ``x=rt``; the first weave entry pins ``x: pin``.
    """
    _write(tmp_path, "t.thread", _thread_body())
    inline = {**_thread_body(), "name": "i"}
    weave = _weave_body([{"ref": "t.thread"}, inline], defaults={"weave_note": "${param.x}"})
    _write(tmp_path, "w1.weave", weave)
    _write(tmp_path, "w2.weave", weave)
    path = _loom(
        tmp_path,
        [{"ref": "w1.weave", "params": {"x": "pin"}}, {"ref": "w2.weave"}],
        defaults={"loom_note": "${param.x}"},
    )
    return _loaded(path, {"x": "rt"})


class TestSubtreeVisibility:
    """A weave-entry pin is the ambient value below it and nowhere else."""

    def test_visible_in_pinned_weave(self, matrix: Any) -> None:
        assert matrix.document.children["w1"].model.defaults["weave_note"] == "pin"

    def test_visible_in_ref_thread_below(self, matrix: Any) -> None:
        assert _alias(matrix.document.children["w1"].children["t"]) == "out.pin"

    def test_visible_in_inline_thread_below(self, matrix: Any) -> None:
        assert _alias(matrix.document.children["w1"].children["i"]) == "out.pin"

    def test_invisible_to_the_loom(self, matrix: Any) -> None:
        assert matrix.document.model.defaults["loom_note"] == "rt"

    def test_invisible_to_sibling_weave(self, matrix: Any) -> None:
        sibling = matrix.document.children["w2"]
        assert sibling.model.defaults["weave_note"] == "rt"
        assert _alias(sibling.children["t"]) == "out.rt"
        assert _alias(sibling.children["i"]) == "out.rt"

    def test_entry_keeps_its_pins(self, matrix: Any) -> None:
        loom = matrix.document.model
        assert isinstance(loom, Loom)
        assert loom.weaves[0].params == {"x": "pin"}
        assert loom.weaves[1].params is None

    def test_pinned_literal_beats_declared_default(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body(params=_optional(x="th")))
        _write(tmp_path, "w.weave", _weave_body([{"ref": "t.thread"}], params=_optional(x="wv")))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"x": "pin"}}])
        assert _alias(_loaded(path, {"x": "rt"}).document.children["w"].children["t"]) == (
            "out.pin"
        )

    def test_nearer_thread_pin_wins_for_its_thread(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body())
        threads = [
            {"ref": "t.thread", "as": "a", "params": {"x": "tp"}},
            {"ref": "t.thread", "as": "b"},
        ]
        _write(tmp_path, "w.weave", _weave_body(threads))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"x": "wp"}}])
        weave = _loaded(path, {"x": "rt"}).document.children["w"]
        assert _alias(weave.children["a"]) == "out.tp"
        assert _alias(weave.children["b"]) == "out.wp"

    def test_thread_pin_invisible_to_sibling_thread(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body())
        threads = [
            {"ref": "t.thread", "as": "a", "params": {"x": "tp"}},
            {"ref": "t.thread", "as": "b"},
        ]
        path = _write(tmp_path, "w.weave", _weave_body(threads))
        weave = _loaded(path, {"x": "rt"}).document
        assert _alias(weave.children["a"]) == "out.tp"
        assert _alias(weave.children["b"]) == "out.rt"


class TestTwoPhase:
    """Pin values resolve in the parent scope, then bind in the child."""

    def test_weave_pin_resolves_in_loom_scope(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body())
        _write(tmp_path, "w.weave", _weave_body([{"ref": "t.thread"}], params=_optional(y="wv")))
        path = _loom(
            tmp_path,
            [{"ref": "w.weave", "params": {"x": "${param.y}_p"}}],
            params=_optional(y="lm"),
        )
        assert _alias(_loaded(path).document.children["w"].children["t"]) == "out.lm_p"

    def test_thread_pin_resolves_in_pinned_weave_scope(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body())
        _write(
            tmp_path, "w.weave", _weave_body([{"ref": "t.thread", "params": {"x": "${param.y}"}}])
        )
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"y": "wp"}}])
        assert _alias(_loaded(path, {"y": "rt"}).document.children["w"].children["t"]) == "out.wp"


class TestEntryCondition:
    """An entry's ``condition`` belongs to the parent and resolves in its scope."""

    WHEN = "'${param.x}' == 'go'"

    def test_ref_weave_condition_ignores_its_own_pin(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body())
        _write(tmp_path, "w.weave", _weave_body([{"ref": "t.thread"}]))
        entry = {"ref": "w.weave", "params": {"x": "pin"}, "condition": {"when": self.WHEN}}
        path = _loom(tmp_path, [entry], params=_optional(x="lm"))
        loom = _loaded(path).document.model
        assert isinstance(loom, Loom)
        assert loom.weaves[0].condition is not None
        assert loom.weaves[0].condition.when == "'lm' == 'go'"

    def test_inline_weave_condition_ignores_its_own_declaration(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body())
        inline = {
            "name": "w",
            "condition": {"when": self.WHEN},
            **_weave_body([{"ref": "t.thread"}], params=_optional(x="wv")),
        }
        path = _loom(tmp_path, [inline], params=_optional(x="lm"))
        loaded = _loaded(path)
        loom = loaded.document.model
        assert isinstance(loom, Loom)
        assert loom.weaves[0].condition is not None
        assert loom.weaves[0].condition.when == "'lm' == 'go'"
        assert _alias(loaded.document.children["w"].children["t"]) == "out.wv"

    def test_inline_thread_condition_ignores_its_own_declaration(self, tmp_path: Path) -> None:
        inline = {
            **_thread_body(params=_optional(x="th")),
            "name": "i",
            "condition": {"when": self.WHEN},
        }
        path = _write(tmp_path, "w.weave", _weave_body([inline], params=_optional(x="wv")))
        loaded = _loaded(path)
        thread = loaded.document.children["i"]
        assert thread.entry.condition.when == "'wv' == 'go'"
        assert _alias(thread) == "out.th"


class TestUnconsumedPin:
    """A pin no binding in its subtree reads warns ``P123`` at the entry."""

    def test_unused_weave_pin_warns_at_the_loom_entry(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body())
        _write(tmp_path, "w.weave", _weave_body([{"ref": "t.thread"}]))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"x": "a", "zz": "b"}}])
        found = _codes(_loaded(path), "P123")
        assert len(found) == 1
        assert found[0].severity == "warning"
        assert found[0].file == str(path)
        assert found[0].pointer == "/weaves/0/params/zz"

    def test_unused_thread_pin_warns_at_the_weave_entry(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body(alias="out.t"))
        weave = _write(
            tmp_path, "w.weave", _weave_body([{"ref": "t.thread", "params": {"zz": "b"}}])
        )
        path = _loom(tmp_path, [{"ref": "w.weave"}])
        found = _codes(_loaded(path), "P123")
        assert len(found) == 1
        assert found[0].file == str(weave)
        assert found[0].pointer == "/threads/0/params/zz"

    def test_unused_pin_in_inline_weave_points_through_it(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body())
        inline = {"name": "w", **_weave_body([{"ref": "t.thread", "params": {"zz": "b"}}])}
        path = _loom(tmp_path, [inline])
        found = _codes(_loaded(path, {"x": "rt"}), "P123")
        assert [(d.file, d.pointer) for d in found] == [
            (str(path), "/weaves/0/threads/0/params/zz")
        ]

    def test_pin_read_deep_in_the_subtree_is_consumed(self, matrix: Any) -> None:
        assert _codes(matrix, "P123") == []

    def test_pin_read_only_by_a_nested_pin_value_is_consumed(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body())
        _write(
            tmp_path, "w.weave", _weave_body([{"ref": "t.thread", "params": {"x": "${param.y}"}}])
        )
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"y": "wp"}}])
        assert _codes(_loaded(path), "P123") == []

    def test_pin_read_by_a_nested_entry_condition_is_consumed(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body(alias="out.t"))
        entry = {"ref": "t.thread", "condition": {"when": "'${param.y}' == 'wp'"}}
        _write(tmp_path, "w.weave", _weave_body([entry]))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"y": "wp"}}])
        assert _codes(_loaded(path), "P123") == []

    def test_pin_read_by_traversing_into_its_value_is_consumed(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body(alias="out.${param.y.k}"))
        _write(tmp_path, "w.weave", _weave_body([{"ref": "t.thread"}]))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"y": {"k": "v"}}}])
        loaded = _loaded(path)
        assert _alias(loaded.document.children["w"].children["t"]) == "out.v"
        assert _codes(loaded, "P123") == []

    def test_pin_satisfying_a_declaration_is_consumed(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body(alias="out.t", params=_specs(x={})))
        _write(tmp_path, "w.weave", _weave_body([{"ref": "t.thread"}]))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"x": "a"}}])
        assert _codes(_loaded(path), "P123") == []

    def test_pin_shadowed_by_nearer_pins_is_unconsumed(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body())
        _write(tmp_path, "w.weave", _weave_body([{"ref": "t.thread", "params": {"x": "tp"}}]))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"x": "wp"}}])
        found = _codes(_loaded(path), "P123")
        assert [d.pointer for d in found] == ["/weaves/0/params/x"]

    def test_no_uncoded_log_line(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        _write(tmp_path, "t.thread", _thread_body(alias="out.t"))
        path = _write(tmp_path, "w.weave", _weave_body([{"ref": "t.thread", "params": {"z": 1}}]))
        with caplog.at_level(logging.WARNING, logger="weevr.config.resolver"):
            loaded = _loaded(path)
        assert len(_codes(loaded, "P123")) == 1
        assert not [r for r in caplog.records if r.name == "weevr.config.resolver"]


class TestUnconsumedPinWhenSubtreeFails:
    """A subtree that failed to load can't show its pins unread: no ``P123``."""

    def test_validate_mode_reports_only_the_failure(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body(params=_specs(need={})))
        _write(tmp_path, "w.weave", _weave_body([{"ref": "t.thread"}]))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"x": "pin"}}])
        _, warnings, errors = collect_resolved_config(
            LocalConfigLocation(path), project_root=LocalConfigLocation(tmp_path)
        )
        assert [d.code for d in errors] == ["P101"]
        assert [d.code for d in warnings if d.code == "P123"] == []


class TestNonMappingPins:
    """A ``params:`` on a ``ref:`` entry that is not a mapping fails ``C102`` at the entry."""

    def _code_at(self, path: Path) -> list[tuple[str, str | None, str | None]]:
        with pytest.raises(ConfigError) as exc:
            _loaded(path)
        return [(d.code, d.file, d.pointer) for d in exc.value.diagnostics]

    def test_on_a_weave_entry(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body(alias="out.t"))
        _write(tmp_path, "w.weave", _weave_body([{"ref": "t.thread"}]))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": "a"}])
        assert self._code_at(path) == [("C102", str(path), "/weaves/0/params")]

    def test_on_a_thread_entry(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body(alias="out.t"))
        path = _write(tmp_path, "w.weave", _weave_body([{"ref": "t.thread", "params": "a"}]))
        assert self._code_at(path) == [("C102", str(path), "/threads/0/params")]


class TestInlineWeaveScope:
    """An inline weave opens one scope, used for its own fields and its threads."""

    def test_weave_and_its_threads_see_one_binding(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body())
        inline = {
            "name": "w",
            **_weave_body(
                [{"ref": "t.thread"}],
                params=_optional(x="${param.y}_w"),
                defaults={"weave_note": "${param.x}"},
            ),
        }
        path = _loom(tmp_path, [inline], params=_optional(y="lm"))
        weave = _loaded(path).document.children["w"]
        # Declared defaults bind as written, so both see the literal default.
        assert weave.model.defaults["weave_note"] == "${param.y}_w"
        assert _alias(weave.children["t"]) == "out.${param.y}_w"

    def test_required_without_value_fails_once(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread_body(alias="out.t"))
        inline = {"name": "w", **_weave_body([{"ref": "t.thread"}], params=_specs(x={}))}
        path = _loom(tmp_path, [inline])
        with pytest.raises(ConfigError) as exc:
            _loaded(path)
        found = [d for d in exc.value.diagnostics if d.code == "P101"]
        assert len(found) == 1
        assert found[0].file == str(path)
        assert found[0].pointer == "/weaves/0/params/x"


class TestDeclaredDefaultsBindLiterally:
    """A declared default binds as written at every tier."""

    def test_root_and_ref_loaded_thread_bind_alike(self, tmp_path: Path) -> None:
        root = _write(tmp_path, "t.thread", _thread_body(params=_optional(x="${param.y}_d")))
        path = _write(tmp_path, "w.weave", _weave_body([{"ref": "t.thread"}]))
        runtime = {"y": "rt"}
        standalone = _alias(_loaded(root, runtime).document)
        assert standalone == _alias(_loaded(path, runtime).document.children["t"])
        assert standalone == "out.${param.y}_d"


class TestWeaveEntryModel:
    """``WeaveEntry.params`` mirrors ``ThreadEntry.params``."""

    def test_pins_carried_on_entry_meta(self, matrix: Any) -> None:
        assert matrix.document.children["w1"].entry.params == {"x": "pin"}

    def test_params_default_none(self) -> None:
        assert WeaveEntry(ref="w.weave").params is None

    def test_params_accepts_any_values(self) -> None:
        assert WeaveEntry(ref="w.weave", params={"n": 1, "s": "a"}).params == {"n": 1, "s": "a"}


class TestParamScope:
    """``ParamScope.root/child`` directly, without a config load."""

    def test_root_binds_runtime(self) -> None:
        assert ParamScope.root({"x": "rt", "n": None}).values() == {"x": "rt"}

    def test_pin_beats_runtime(self) -> None:
        child = ParamScope.root({"x": "rt"}).child(None, pins={"x": "pin"})
        assert child.bindings["x"] == Binding("pin", "pin")

    def test_pin_flows_to_grandchild(self) -> None:
        grandchild = ParamScope.root({"x": "rt"}).child(None, pins={"x": "pin"}).child(None)
        assert grandchild.values() == {"x": "pin"}

    def test_child_default_does_not_override_supplied(self) -> None:
        child = ParamScope.root({"x": "rt"}).child(_optional(x="d"))
        assert child.bindings["x"] == Binding("rt", "runtime")

    def test_nearer_default_rederived(self) -> None:
        parent = ParamScope.root().child(_optional(x="outer"))
        assert parent.child(_optional(x="inner")).values() == {"x": "inner"}

    def test_non_mapping_spec_is_c102(self) -> None:
        with pytest.raises(ConfigSchemaError) as exc:
            ParamScope.root().child({"x": "string"}, file="l.loom", at=("weaves", 0))
        assert [(d.code, d.pointer) for d in exc.value.diagnostics] == [
            ("C102", "/weaves/0/params/x")
        ]

    def test_required_without_value_is_p101(self) -> None:
        with pytest.raises(ConfigSchemaError) as exc:
            ParamScope.root().child(_specs(x={}), file="l.loom")
        assert [(d.code, d.file) for d in exc.value.diagnostics] == [("P101", "l.loom")]

    def test_bindings_reject_mutation(self) -> None:
        scope = ParamScope.root({"x": "rt"})
        with pytest.raises(TypeError):
            scope.bindings["x"] = Binding("y", "pin")  # type: ignore[index]

    def test_specs_reject_mutation(self) -> None:
        scope = ParamScope.root().child(_optional(x="d"))
        with pytest.raises(TypeError):
            del scope.specs["x"]  # type: ignore[attr-defined]


class TestEnterChild:
    """Entering a ``ref:`` entry must open the child's scope before leaving."""

    def test_leaving_without_opening_raises(self) -> None:
        walk = WalkState(scope=ParamScope.root())
        with (
            pytest.raises(RuntimeError),
            enter_child({"ref": "t.thread"}, None, walk, (), alias_key="as"),
        ):
            pass

    def test_name_before_opening_raises(self) -> None:
        entered = ChildEntry(None, WalkState(scope=ParamScope.root()))
        with pytest.raises(RuntimeError):
            _ = entered.name

    def test_opened_state_is_the_childs(self) -> None:
        walk = WalkState(scope=ParamScope.root({"x": "rt"}), file="w.weave")
        entered = ChildEntry({"x": "pin"}, walk)
        opened = entered.open({"name": "t", "strict_params": True}, "t.thread")
        assert (opened.file, opened.strict, opened.scope.values()) == (
            "t.thread",
            True,
            {"x": "pin"},
        )
        assert opened.reads is walk.reads
