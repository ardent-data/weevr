"""Params bind per scope: declared specs cascade, supplied values flow down.

Per name, a scope binds the inherited binding (runtime params at the run root),
else the nearest declared default, else the reference's ``:-`` fallback; a
required spec with no value fails ``P101`` at its declaration and any other
unbound reference fails ``P110`` at load.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from documents import write_document
from weevr.config import load_resolved
from weevr.config.document import ResolvedDocument
from weevr.diagnostics import Diagnostic
from weevr.errors import ConfigError

THREAD: dict[str, Any] = {
    "config_version": "2.0",
    "sources": {"s": {"type": "delta", "alias": "raw.t"}},
    "target": {"alias": "out.t"},
}


def _specs(**specs: dict[str, Any]) -> dict[str, Any]:
    """ParamSpec mappings keyed by name."""
    return {name: {"name": name, "type": "string", **spec} for name, spec in specs.items()}


def _thread(root: Path, alias: str, params: dict[str, Any] | None = None, name: str = "t") -> Path:
    body: dict[str, Any] = {**THREAD, "target": {"alias": alias}}
    if params is not None:
        body["params"] = params
    return write_document(root / f"{name}.thread", body)


def _weave(
    root: Path,
    threads: list[Any],
    params: dict[str, Any] | None = None,
    name: str = "w",
) -> Path:
    body: dict[str, Any] = {"config_version": "2.0", "threads": threads}
    if params is not None:
        body["params"] = params
    return write_document(root / f"{name}.weave", body)


def _loom(root: Path, weaves: list[Any], params: dict[str, Any] | None = None) -> Path:
    body: dict[str, Any] = {"config_version": "2.0", "weaves": weaves}
    if params is not None:
        body["params"] = params
    return write_document(root / "l.loom", body)


def _load(path: Path, runtime: dict[str, Any] | None = None) -> ResolvedDocument:
    return load_resolved(path, runtime_params=runtime, project_root=path.parent).document


def _error(path: Path, runtime: dict[str, Any] | None = None) -> Diagnostic:
    with pytest.raises(ConfigError) as exc:
        _load(path, runtime)
    assert len(exc.value.diagnostics) == 1
    return exc.value.diagnostics[0]


def _alias(doc: ResolvedDocument) -> str:
    model = doc.model
    return model.target.alias  # type: ignore[union-attr]


def _tree(
    root: Path,
    *,
    loom: dict[str, Any] | None = None,
    weave: dict[str, Any] | None = None,
    thread: dict[str, Any] | None = None,
    alias: str = "out.${param.x}",
) -> Path:
    """A loom → ref weave → ref thread tree, each tier optionally declaring params."""
    _thread(root, alias, thread)
    _weave(root, [{"ref": "t.thread"}], weave)
    return _loom(root, [{"ref": "w.weave"}], loom)


def _leaf(doc: ResolvedDocument) -> ResolvedDocument:
    return doc.children["w"].children["t"]


class TestGoldens:
    """The two headline scenarios: zero scaffolding and declare-once."""

    def test_zero_scaffold_lone_thread(self, tmp_path: Path) -> None:
        path = _thread(tmp_path, "out.${param.table}")
        assert not (tmp_path / "weevr.yaml").exists()
        assert _alias(_load(path, {"table": "orders"})) == "out.orders"

    def test_declare_once_in_loom_reaches_ref_thread(self, tmp_path: Path) -> None:
        path = _tree(tmp_path, loom=_specs(x={"required": False, "default": "lm"}))
        assert _alias(_leaf(_load(path))) == "out.lm"


class TestPrecedence:
    """Inherited binding > nearest declared default > ``:-`` fallback > error."""

    def test_runtime_beats_thread_default(self, tmp_path: Path) -> None:
        path = _thread(tmp_path, "out.${param.x}", _specs(x={"required": False, "default": "d"}))
        assert _alias(_load(path, {"x": "rt"})) == "out.rt"

    def test_runtime_beats_thread_default_two_tiers_down(self, tmp_path: Path) -> None:
        path = _tree(
            tmp_path,
            loom=_specs(x={"required": False, "default": "lm"}),
            thread=_specs(x={"required": False, "default": "th"}),
        )
        assert _alias(_leaf(_load(path, {"x": "rt"}))) == "out.rt"

    def test_child_redeclaration_shadows_default(self, tmp_path: Path) -> None:
        path = _tree(
            tmp_path,
            loom=_specs(x={"required": False, "default": "lm"}),
            thread=_specs(x={"required": False, "default": "th"}),
        )
        assert _alias(_leaf(_load(path))) == "out.th"

    def test_nearest_default_is_the_weaves(self, tmp_path: Path) -> None:
        path = _tree(
            tmp_path,
            loom=_specs(x={"required": False, "default": "lm"}),
            weave=_specs(x={"required": False, "default": "wv"}),
        )
        assert _alias(_leaf(_load(path))) == "out.wv"

    def test_weave_declared_default_reaches_ref_thread(self, tmp_path: Path) -> None:
        path = _tree(tmp_path, weave=_specs(x={"required": False, "default": "wv"}))
        assert _alias(_leaf(_load(path))) == "out.wv"

    def test_runtime_undeclared_reaches_ref_thread(self, tmp_path: Path) -> None:
        path = _tree(tmp_path)
        assert _alias(_leaf(_load(path, {"x": "rt"}))) == "out.rt"

    def test_explicit_none_runtime_falls_to_default(self, tmp_path: Path) -> None:
        path = _tree(tmp_path, loom=_specs(x={"required": False, "default": "lm"}))
        assert _alias(_leaf(_load(path, {"x": None}))) == "out.lm"

    def test_fallback_when_unbound(self, tmp_path: Path) -> None:
        path = _tree(tmp_path, alias="out.${param.x:-fb}")
        assert _alias(_leaf(_load(path))) == "out.fb"

    def test_default_beats_fallback(self, tmp_path: Path) -> None:
        path = _tree(
            tmp_path,
            loom=_specs(x={"required": False, "default": "lm"}),
            alias="out.${param.x:-fb}",
        )
        assert _alias(_leaf(_load(path))) == "out.lm"


class TestUnbound:
    """No value anywhere: ``P101`` at a required spec, else ``P110`` at the reference."""

    def test_required_in_ref_thread_names_its_file(self, tmp_path: Path) -> None:
        path = _tree(tmp_path, thread=_specs(x={"required": True}))
        found = _error(path)
        assert found.code == "P101"
        assert found.file == str(tmp_path / "t.thread")
        assert found.pointer == "/params/x"

    def test_required_in_ref_weave_names_its_file(self, tmp_path: Path) -> None:
        path = _tree(tmp_path, weave=_specs(x={"required": True}))
        found = _error(path)
        assert found.code == "P101"
        assert found.file == str(tmp_path / "w.weave")
        assert found.pointer == "/params/x"

    def test_required_in_loom_names_the_loom(self, tmp_path: Path) -> None:
        path = _tree(tmp_path, loom=_specs(x={"required": True}))
        found = _error(path)
        assert found.code == "P101"
        assert found.file == str(path)
        assert found.pointer == "/params/x"

    def test_required_satisfied_by_runtime(self, tmp_path: Path) -> None:
        path = _tree(tmp_path, thread=_specs(x={"required": True}))
        assert _alias(_leaf(_load(path, {"x": "rt"}))) == "out.rt"

    def test_undeclared_unsupplied_in_ref_thread(self, tmp_path: Path) -> None:
        assert _error(_tree(tmp_path)).code == "P110"

    def test_optional_without_default_in_loom(self, tmp_path: Path) -> None:
        path = _tree(tmp_path, loom=_specs(x={"required": False}))
        assert _error(path).code == "P110"


class TestInlineChildren:
    """Inline weaves and threads open their own scope like ``ref:`` children."""

    def test_inline_weave_inherits_loom_default(self, tmp_path: Path) -> None:
        _thread(tmp_path, "out.${param.x}")
        path = _loom(
            tmp_path,
            [{"name": "w", "config_version": "2.0", "threads": [{"ref": "t.thread"}]}],
            _specs(x={"required": False, "default": "lm"}),
        )
        assert _alias(_leaf(_load(path))) == "out.lm"

    def test_inline_weave_declaration_shadows_loom(self, tmp_path: Path) -> None:
        _thread(tmp_path, "out.${param.x}")
        path = _loom(
            tmp_path,
            [
                {
                    "name": "w",
                    "config_version": "2.0",
                    "params": _specs(x={"required": False, "default": "wv"}),
                    "threads": [{"ref": "t.thread"}],
                }
            ],
            _specs(x={"required": False, "default": "lm"}),
        )
        assert _alias(_leaf(_load(path))) == "out.wv"

    def test_inline_thread_declares_its_own_default(self, tmp_path: Path) -> None:
        inline = {
            **THREAD,
            "name": "t",
            "params": _specs(x={"required": False, "default": "th"}),
            "target": {"alias": "out.${param.x}"},
        }
        path = _weave(tmp_path, [inline], _specs(x={"required": False, "default": "wv"}))
        assert _alias(_load(path).children["t"]) == "out.th"

    def test_inline_thread_sees_weave_default(self, tmp_path: Path) -> None:
        inline = {**THREAD, "name": "t", "target": {"alias": "out.${param.x}"}}
        path = _weave(tmp_path, [inline], _specs(x={"required": False, "default": "wv"}))
        assert _alias(_load(path).children["t"]) == "out.wv"


class TestUnchangedBehavior:
    """Entry ``params:`` and bare ``${x}`` keep working until later changes retire them."""

    def test_thread_entry_params_bind_in_thread(self, tmp_path: Path) -> None:
        _thread(tmp_path, "out.${param.x}")
        path = _weave(tmp_path, [{"ref": "t.thread", "params": {"x": "pin"}}])
        assert _alias(_load(path).children["t"]) == "out.pin"

    def test_thread_entry_params_resolve_in_weave_scope(self, tmp_path: Path) -> None:
        _thread(tmp_path, "out.${param.x}")
        path = _weave(
            tmp_path,
            [{"ref": "t.thread", "params": {"x": "${param.y}_p"}}],
            _specs(y={"required": False, "default": "wv"}),
        )
        assert _alias(_load(path).children["t"]) == "out.wv_p"

    def test_bare_runtime_reference_still_resolves_in_ref_thread(self, tmp_path: Path) -> None:
        path = _tree(tmp_path, alias="out.${x}")
        assert _alias(_leaf(_load(path, {"x": "rt"}))) == "out.rt"


def _declaring_thread(tmp_path: Path, params: dict[str, Any], **body: Any) -> Path:
    """Write a thread declaring ``params`` (name -> spec fields) plus ``body`` overrides."""
    declared = {name: {"name": name, **spec} for name, spec in params.items()}
    return write_document(tmp_path / "t.thread", {**THREAD, "params": declared, **body})


def _model(path: Path, runtime: dict[str, Any] | None = None) -> Any:
    return load_resolved(path, runtime_params=runtime, project_root=path.parent).model


def _code(path: Path, runtime: dict[str, Any] | None = None) -> str:
    with pytest.raises(ConfigError) as exc:
        _model(path, runtime)
    assert len(exc.value.diagnostics) == 1
    return exc.value.diagnostics[0].code


class TestExplicitNoneRuntimeValue:
    """An explicit ``None`` runtime value is absent: default or required check apply."""

    def test_declared_default_applies(self, tmp_path: Path) -> None:
        path = _declaring_thread(
            tmp_path,
            {"n": {"type": "string", "required": False, "default": "dflt"}},
            target={"alias": "out.${param.n}"},
        )
        assert _model(path, {"n": None}).target.alias == "out.dflt"

    def test_required_check_raises(self, tmp_path: Path) -> None:
        path = _declaring_thread(
            tmp_path,
            {"n": {"type": "string", "required": True}},
            target={"alias": "out.${param.n}"},
        )
        assert _code(path, {"n": None}) == "P101"


class TestOptionalParamWithoutDefault:
    """An optional param with no default is unbound: its reference fails unless ``:-``."""

    SPEC = {"n": {"type": "string", "required": False}}

    def test_reference_fails(self, tmp_path: Path) -> None:
        path = _declaring_thread(tmp_path, self.SPEC, target={"alias": "out.${param.n}"})
        assert _code(path) == "P110"

    def test_fallback_applies(self, tmp_path: Path) -> None:
        path = _declaring_thread(tmp_path, self.SPEC, target={"alias": "out.${param.n:-fb}"})
        assert _model(path).target.alias == "out.fb"

    def test_fallback_applies_for_explicit_none(self, tmp_path: Path) -> None:
        path = _declaring_thread(tmp_path, self.SPEC, target={"alias": "out.${param.n:-fb}"})
        assert _model(path, {"n": None}).target.alias == "out.fb"


class TestWholeValueReferencesKeepNativeType:
    """A value that is only a reference resolves to the param's native type."""

    @pytest.mark.parametrize(
        ("ptype", "value"),
        [("int", 7), ("bool", True), ("float", 1.5), ("list[string]", ["a", "b"])],
    )
    def test_native_type(self, tmp_path: Path, ptype: str, value: Any) -> None:
        path = _declaring_thread(
            tmp_path,
            {"p": {"type": ptype, "required": True}},
            sources={
                "s": {
                    "type": "delta",
                    "alias": "raw.t",
                    "options": {"probe": "${param.p}"},
                }
            },
        )
        source = _model(path, {"p": value}).sources["s"]
        resolved = source.options["probe"]
        assert resolved == value
        assert type(resolved) is type(value)
