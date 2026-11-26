"""Resolution errors say where a binding was expected and how to supply one.

An unresolved ``${param.x}`` fails ``P110`` at the reference, with ``expected``
listing what its scope binds and declares. Its hint depends on the case: an
undeclared, unsupplied name gets a paste-able ``params:`` block and a runtime
supply example; a declared but unbound name gets the trail of files from its
declaration down to the reference; a name close to one in scope gets that name
suggested. An unknown namespace (``P112``) suggests the nearest one.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from documents import loom_body, string_specs, thread_body, weave_body, write_document, write_named
from resolving import load_at
from weevr.config.interpolation import resolve_variables
from weevr.config.interpolation_errors import ReferenceScope
from weevr.config.params import ParamDeclaration, declaration_block
from weevr.diagnostics import Diagnostic
from weevr.errors import VariableResolutionError

THREAD: dict[str, Any] = {
    "config_version": "2.0",
    "sources": {"s": {"type": "delta", "alias": "raw.t"}},
}


def _optional(default: str | None = None) -> dict[str, Any]:
    return {"required": False, "default": default}


def _thread(root: Path, alias: str, **extra: Any) -> Path:
    return write_named(root, "t.thread", thread_body(alias, **extra))


def _tree(root: Path, alias: str, *, loom: dict[str, Any] | None = None, **loom_extra: Any) -> Path:
    """A loom → ref weave → ref thread tree; the loom optionally declares params."""
    _thread(root, alias)
    write_named(root, "w.weave", weave_body([{"ref": "t.thread"}]))
    body = loom_body([{"ref": "w.weave"}], **loom_extra)
    if loom is not None:
        body["params"] = loom
    return write_named(root, "l.loom", body)


def _failure(path: Path, runtime: dict[str, Any] | None = None) -> Diagnostic:
    with pytest.raises(VariableResolutionError) as exc:
        load_at(path, runtime)
    assert len(exc.value.diagnostics) == 1
    return exc.value.diagnostics[0]


def _hint(found: Diagnostic) -> str:
    assert found.hint is not None
    return found.hint


class TestUndeclaredUnsupplied:
    def test_hint_carries_a_params_block_and_a_runtime_example(self, tmp_path: Path) -> None:
        path = _thread(tmp_path, "out.${param.x}")
        found = _failure(path)
        assert found.code == "P110"
        assert found.file == str(path)
        assert found.pointer == "/target/alias"
        hint = _hint(found)
        assert declaration_block({"x": None}) in hint
        assert 'params={"x":' in hint

    def test_expected_lists_bound_and_declared_names(self, tmp_path: Path) -> None:
        params = string_specs(a=_optional("da"), b=_optional())
        path = _thread(tmp_path, "out.${param.x}", params=params)
        found = _failure(path, {"c": "rc"})
        assert found.code == "P110"
        assert {"${param.a}", "${param.b}", "${param.c}"} <= set(found.expected or ())


class TestDeclaredUnbound:
    def test_hint_traces_the_scope_from_declaration_to_reference(self, tmp_path: Path) -> None:
        path = _tree(tmp_path, "out.${param.x}", loom=string_specs(x=_optional()))
        found = _failure(path)
        assert found.code == "P110"
        assert found.file == str(tmp_path / "t.thread")
        hint = _hint(found)
        positions = [hint.index(file) for file in ("l.loom", "w.weave", "t.thread")]
        assert positions == sorted(positions)
        assert str(tmp_path) not in hint
        assert "params:" not in hint
        assert "${param.x}" in (found.expected or ())

    def test_declared_in_the_referencing_file(self, tmp_path: Path) -> None:
        path = _thread(tmp_path, "out.${param.x}", params=string_specs(x=_optional()))
        found = _failure(path)
        assert found.code == "P110"
        assert "t.thread" in _hint(found)

    def test_bound_name_missing_a_key_has_no_hint(self, tmp_path: Path) -> None:
        found = _failure(_thread(tmp_path, "out.${param.ws.x}"), {"ws": {"a": "1"}})
        assert found.code == "P110"
        assert "${param.ws}" in (found.expected or ())
        assert found.hint is None

    def test_declaring_file_off_the_trail_leaves_the_trail_out(self) -> None:
        scope = ReferenceScope(
            declared={"x": ParamDeclaration("weave", "/elsewhere/a.weave", "/params/x")},
            trail=("/p/t.thread",),
        )
        with pytest.raises(VariableResolutionError) as exc:
            resolve_variables("out.${param.x}", {}, scope=scope)
        (found,) = exc.value.diagnostics
        assert found.code == "P110"
        hint = _hint(found)
        assert "t.thread" not in hint
        assert "a.weave" not in hint


class TestReferenceScope:
    TRAIL = ("/p/l.loom", "/p/sub/w.weave", "/p/sub/t.thread")

    def test_trail_from_a_file_on_the_trail(self) -> None:
        scope = ReferenceScope(trail=self.TRAIL)
        assert scope.trail_from("/p/sub/w.weave") == self.TRAIL[1:]

    @pytest.mark.parametrize("file", ["/q/w.weave", None])
    def test_trail_from_a_file_off_the_trail_or_unknown(self, file: str | None) -> None:
        assert ReferenceScope(trail=self.TRAIL).trail_from(file) == ()

    def test_shown_relative_to_the_root_folder(self) -> None:
        assert ReferenceScope(trail=self.TRAIL).shown("/p/sub/t.thread") == "sub/t.thread"

    def test_shown_outside_the_root_folder_stays_absolute(self) -> None:
        assert ReferenceScope(trail=self.TRAIL).shown("/q/t.thread") == "/q/t.thread"


class TestNearestName:
    def test_near_miss_of_a_declared_name_is_suggested(self, tmp_path: Path) -> None:
        path = _thread(tmp_path, "out.${param.regoin}", params=string_specs(region=_optional("eu")))
        found = _failure(path)
        assert found.code == "P110"
        assert "${param.region}" in _hint(found)

    def test_near_miss_of_a_supplied_name_is_suggested(self, tmp_path: Path) -> None:
        found = _failure(_thread(tmp_path, "out.${param.tabel}"), {"table": "orders"})
        assert found.code == "P110"
        assert "${param.table}" in _hint(found)

    def test_far_name_suggests_nothing(self, tmp_path: Path) -> None:
        path = _thread(tmp_path, "out.${param.zone}", params=string_specs(region=_optional("eu")))
        found = _failure(path)
        assert found.code == "P110"
        assert "${param.region}" not in _hint(found)


class TestNearestNamespace:
    @pytest.mark.parametrize(
        ("ref", "suggested"),
        [
            ("${params.x}", "${param.x}"),
            ("${params.x:-d}", "${param.x:-d}"),
            ("${fabrc.workspace_id}", "${fabric.workspace_id}"),
        ],
    )
    def test_near_namespace_is_suggested(self, tmp_path: Path, ref: str, suggested: str) -> None:
        path = _thread(tmp_path, f"out.{ref}")
        found = _failure(path)
        assert found.code == "P112"
        assert found.file == str(path)
        assert suggested in _hint(found)

    def test_far_namespace_suggests_nothing(self, tmp_path: Path) -> None:
        path = _thread(tmp_path, "out.${zzz.x}")
        found = _failure(path)
        assert found.code == "P112"
        assert found.file == str(path)
        assert ".x}" not in _hint(found)

    def test_near_hierarchy_key_is_suggested_first(self, tmp_path: Path) -> None:
        path = _thread(tmp_path, "out.${thread.sourcs}")
        found = _failure(path)
        assert found.code == "P112"
        assert found.file == str(path)
        assert found.expected is not None
        assert found.expected[0] == "${thread.sources}"

    def test_far_hierarchy_key_keeps_the_known_keys_in_order(self, tmp_path: Path) -> None:
        path = _thread(tmp_path, "out.${thread.zzz}")
        found = _failure(path)
        assert found.code == "P112"
        assert found.file == str(path)
        assert list(found.expected or ()) == sorted(found.expected or ())


class TestAncestorDefaults:
    def test_bare_reference_to_a_loom_defaults_key_gets_the_declare_hint(
        self, tmp_path: Path
    ) -> None:
        path = _tree(tmp_path, "out.${region}", defaults={"region": "emea-west"})
        found = _failure(path)
        assert found.code == "P111"
        assert "emea-west" in _hint(found)

    def test_inline_weave_defaults_key_gets_the_declare_hint(self, tmp_path: Path) -> None:
        thread = {**THREAD, "name": "t", "target": {"alias": "out.${region}"}}
        weave = {
            "name": "w",
            "config_version": "2.0",
            "defaults": {"region": "inline-weave"},
            "threads": [thread],
        }
        path = write_document(tmp_path / "l.loom", {"config_version": "2.0", "weaves": [weave]})
        found = _failure(path)
        assert found.code == "P111"
        assert "inline-weave" in _hint(found)

    def test_inline_thread_defaults_key_gets_the_declare_hint(self, tmp_path: Path) -> None:
        thread = {
            **THREAD,
            "name": "t",
            "defaults": {"region": "inline-thread"},
            "target": {"alias": "out.${region}"},
        }
        path = write_document(tmp_path / "w.weave", {"config_version": "2.0", "threads": [thread]})
        found = _failure(path)
        assert found.code == "P111"
        assert "inline-thread" in _hint(found)

    def test_nearer_defaults_block_wins(self, tmp_path: Path) -> None:
        _thread(tmp_path, "out.${region}")
        weave = {
            "config_version": "2.0",
            "defaults": {"region": "weave-value"},
            "threads": [{"ref": "t.thread"}],
        }
        write_document(tmp_path / "w.weave", weave)
        loom = {
            "config_version": "2.0",
            "defaults": {"region": "loom-value"},
            "weaves": [{"ref": "w.weave"}],
        }
        found = _failure(write_document(tmp_path / "l.loom", loom))
        assert found.code == "P111"
        hint = _hint(found)
        assert "weave-value" in hint
        assert "loom-value" not in hint
