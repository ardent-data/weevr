"""The param manifest: what a config declares, needs supplied, and uses undeclared.

The manifest reads the authored documents only: declared specs cascade by
name, entry pins satisfy the child's names for their subtree, and
``${param.x}`` references are scanned. No value is bound, so a config whose
required params nobody supplied still yields its manifest.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import pytest

from documents import write_document
from weevr.config import ManifestParam, ParamManifest, param_manifest
from weevr.config.loader import collect_resolved_config
from weevr.config.locations import LocalConfigLocation
from weevr.errors import ConfigError

THREAD: dict[str, Any] = {
    "config_version": "2.0",
    "sources": {"s": {"type": "delta", "alias": "raw.t"}},
    "target": {"alias": "out.t"},
}


def _spec(name: str, **spec: Any) -> dict[str, Any]:
    return {"name": name, "type": "string", **spec}


def _specs(*specs: dict[str, Any]) -> dict[str, Any]:
    return {spec["name"]: spec for spec in specs}


def _thread(alias: str = "out.t", **extra: Any) -> dict[str, Any]:
    return {**THREAD, "target": {"alias": alias}, **extra}


def _weave(threads: list[Any], **extra: Any) -> dict[str, Any]:
    return {"config_version": "2.0", "threads": threads, **extra}


def _loom(weaves: list[Any], **extra: Any) -> dict[str, Any]:
    return {"config_version": "2.0", "weaves": weaves, **extra}


def _write(root: Path, name: str, body: dict[str, Any]) -> Path:
    return write_document(root / name, body)


def _manifest(path: Path) -> ParamManifest:
    return param_manifest(path, project_root=path.parent)


def _declared(manifest: ParamManifest, name: str) -> list[ManifestParam]:
    return [p for p in manifest.declared if p.name == name]


class TestShape:
    """The manifest names the executable and is frozen."""

    def test_thread_manifest_names_the_file(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread())
        manifest = _manifest(path)
        assert manifest.executable == "t"
        assert manifest.kind == "thread"
        assert manifest.file == str(path)
        assert manifest.declared == ()
        assert manifest.invoker_required == ()
        assert manifest.undeclared_used == ()

    def test_manifest_is_frozen(self, tmp_path: Path) -> None:
        manifest = _manifest(_write(tmp_path, "t.thread", _thread()))
        with pytest.raises(dataclasses.FrozenInstanceError):
            manifest.kind = "weave"  # type: ignore[misc]

    def test_declared_name_beats_stem(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}], name="w"))
        _write(tmp_path, "t.thread", _thread())
        assert _manifest(path).executable == "w"

    def test_accepts_a_string_path(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread())
        assert param_manifest(str(path)).executable == "t"


class TestDeclared:
    """Each declaration is reported with its spec fields and where it sits."""

    def test_spec_fields(self, tmp_path: Path) -> None:
        params = _specs(
            _spec("region", description="Sales region"),
            _spec("limit", type="int", required=False, default=10),
        )
        path = _write(tmp_path, "t.thread", _thread(params=params))
        manifest = _manifest(path)
        assert manifest.declared == (
            ManifestParam(
                name="region",
                type="string",
                required=True,
                default=None,
                description="Sales region",
                file=str(path),
                pointer="/params/region",
            ),
            ManifestParam(
                name="limit",
                type="int",
                required=False,
                default=10,
                description=None,
                file=str(path),
                pointer="/params/limit",
            ),
        )

    def test_children_declarations_carry_their_file(self, tmp_path: Path) -> None:
        thread = _write(tmp_path, "t.thread", _thread(params=_specs(_spec("x"))))
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}]))
        (declared,) = _declared(_manifest(path), "x")
        assert declared.file == str(thread)
        assert declared.pointer == "/params/x"

    def test_inline_declaration_pointer(self, tmp_path: Path) -> None:
        inline = {**_thread(), "name": "i", "params": _specs(_spec("x"))}
        path = _write(tmp_path, "w.weave", _weave([inline]))
        (declared,) = _declared(_manifest(path), "x")
        assert declared.file == str(path)
        assert declared.pointer == "/threads/0/params/x"

    def test_shadowing_declarations_are_each_reported(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread(params=_specs(_spec("x"))))
        params = _specs(_spec("x", required=False, default="w"))
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}], params=params))
        assert len(_declared(_manifest(path), "x")) == 2

    def test_file_referenced_twice_declares_once(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread(params=_specs(_spec("x"))))
        entries = [{"ref": "t.thread", "as": "a"}, {"ref": "t.thread", "as": "b"}]
        path = _write(tmp_path, "w.weave", _weave(entries))
        assert len(_declared(_manifest(path), "x")) == 1


class TestInvokerRequired:
    """Required specs no pin or default satisfies must come from the invoker."""

    def test_required_without_default(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(params=_specs(_spec("x"))))
        assert _manifest(path).invoker_required == ("x",)

    def test_default_satisfies(self, tmp_path: Path) -> None:
        params = _specs(_spec("x", default="d"))
        path = _write(tmp_path, "t.thread", _thread(params=params))
        assert _manifest(path).invoker_required == ()

    def test_optional_without_default_is_not_required(self, tmp_path: Path) -> None:
        params = _specs(_spec("x", required=False))
        path = _write(tmp_path, "t.thread", _thread(params=params))
        assert _manifest(path).invoker_required == ()

    def test_weave_pin_satisfies_thread_required(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread(params=_specs(_spec("x"))))
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread", "params": {"x": "v"}}]))
        assert _manifest(path).invoker_required == ()

    def test_pin_satisfies_only_its_subtree(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread(params=_specs(_spec("x"))))
        entries = [
            {"ref": "t.thread", "as": "a", "params": {"x": "v"}},
            {"ref": "t.thread", "as": "b"},
        ]
        path = _write(tmp_path, "w.weave", _weave(entries))
        assert _manifest(path).invoker_required == ("x",)

    def test_loom_pin_reaches_grandchild(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread(params=_specs(_spec("x"))))
        _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}]))
        path = _write(tmp_path, "l.loom", _loom([{"ref": "w.weave", "params": {"x": "v"}}]))
        assert _manifest(path).invoker_required == ()

    def test_pin_from_parent_param_moves_the_need_up(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread(params=_specs(_spec("x"))))
        entries = [{"ref": "t.thread", "params": {"x": "${param.y}"}}]
        params = _specs(_spec("y"))
        path = _write(tmp_path, "w.weave", _weave(entries, params=params))
        manifest = _manifest(path)
        assert manifest.invoker_required == ("y",)
        assert manifest.undeclared_used == ()

    def test_ancestor_default_does_not_satisfy_redeclared_child(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread(params=_specs(_spec("x"))))
        params = _specs(_spec("x", default="w"))
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}], params=params))
        assert _manifest(path).invoker_required == ("x",)

    def test_child_default_satisfies_inherited_required(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread(params=_specs(_spec("x", default="t"))))
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}]))
        assert _manifest(path).invoker_required == ()

    def test_inherited_required_spec_counts_in_child(self, tmp_path: Path) -> None:
        # The weave declares x required; its thread inherits the spec but the
        # weave scope already lists it, so it appears once.
        _write(tmp_path, "t.thread", _thread())
        params = _specs(_spec("x"))
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}], params=params))
        assert _manifest(path).invoker_required == ("x",)

    def test_inline_thread_declaration(self, tmp_path: Path) -> None:
        inline = {**_thread(), "name": "i", "params": _specs(_spec("x"))}
        path = _write(tmp_path, "w.weave", _weave([inline]))
        assert _manifest(path).invoker_required == ("x",)

    def test_inline_weave_with_ref_thread_pinned(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread(params=_specs(_spec("x"))))
        inline_weave = {"name": "iw", "threads": [{"ref": "t.thread", "params": {"x": "v"}}]}
        path = _write(tmp_path, "l.loom", _loom([inline_weave]))
        assert _manifest(path).invoker_required == ()


class TestUndeclaredUsed:
    """``${param.x}`` references no declaration in their scope chain covers."""

    def test_undeclared_reference(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(alias="out.${param.region}"))
        assert _manifest(path).undeclared_used == ("region",)

    def test_declared_reference_is_not_listed(self, tmp_path: Path) -> None:
        body = _thread(alias="out.${param.region}", params=_specs(_spec("region")))
        path = _write(tmp_path, "t.thread", body)
        assert _manifest(path).undeclared_used == ()

    def test_reference_with_inline_default_is_listed(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(alias="out.${param.region:-eu}"))
        assert _manifest(path).undeclared_used == ("region",)

    def test_dotted_reference_reads_its_first_key(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(alias="out.${param.ws.name}"))
        assert _manifest(path).undeclared_used == ("ws",)

    def test_other_namespaces_ignored(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "t.thread", _thread(alias="out.${fabric.workspace_id}"))
        assert _manifest(path).undeclared_used == ()

    def test_ancestor_declaration_covers_child_reference(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread(alias="out.${param.x}"))
        params = _specs(_spec("x"))
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}], params=params))
        assert _manifest(path).undeclared_used == ()

    def test_sibling_declaration_does_not_cover(self, tmp_path: Path) -> None:
        _write(tmp_path, "a.thread", _thread(params=_specs(_spec("x", default="d"))))
        _write(tmp_path, "b.thread", _thread(alias="out.${param.x}"))
        path = _write(tmp_path, "w.weave", _weave([{"ref": "a.thread"}, {"ref": "b.thread"}]))
        assert _manifest(path).undeclared_used == ("x",)

    def test_pinned_undeclared_name_is_not_listed(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread(alias="out.${param.x}"))
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread", "params": {"x": "v"}}]))
        assert _manifest(path).undeclared_used == ()

    def test_inline_entry_keys_resolve_in_parent_scope(self, tmp_path: Path) -> None:
        inline = {
            **_thread(alias="out.${param.x}"),
            "name": "i",
            "condition": {"when": "${param.c} == 'y'"},
            "params": _specs(_spec("x", default="d")),
        }
        path = _write(tmp_path, "w.weave", _weave([inline]))
        assert _manifest(path).undeclared_used == ("c",)

    def test_names_listed_once(self, tmp_path: Path) -> None:
        body = _thread(alias="${param.x}.${param.x}", defaults={"n": "${param.x}"})
        path = _write(tmp_path, "t.thread", body)
        assert _manifest(path).undeclared_used == ("x",)


class TestNoValues:
    """Nothing is bound or coerced: missing or ill-typed values never fail."""

    def test_required_unsupplied_does_not_raise(self, tmp_path: Path) -> None:
        params = _specs(_spec("x", type="int"))
        path = _write(tmp_path, "t.thread", _thread(alias="out.${param.x}", params=params))
        assert _manifest(path).invoker_required == ("x",)

    def test_ill_typed_default_is_reported_as_written(self, tmp_path: Path) -> None:
        params = _specs(_spec("x", type="int", default="not-an-int"))
        path = _write(tmp_path, "t.thread", _thread(params=params))
        (declared,) = _manifest(path).declared
        assert declared.default == "not-an-int"


class TestStructuralErrors:
    """A document that cannot be read raises its load error."""

    def test_missing_ref_raises(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "w.weave", _weave([{"ref": "missing.thread"}]))
        with pytest.raises(ConfigError) as info:
            _manifest(path)
        assert [d.code for d in info.value.diagnostics] == ["C301"]

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError):
            _manifest(tmp_path / "absent.thread")

    def test_cycle_is_not_followed(self, tmp_path: Path) -> None:
        """A reference cycle is walked once round and then left; the load reports it."""
        params = _specs(_spec("x"))
        _write(tmp_path, "a.weave", _weave([{"ref": "b.weave"}], params=params))
        path = _write(tmp_path, "b.weave", _weave([{"ref": "a.weave"}]))
        # Each weave lists the other as a thread entry: a ref cycle the walk
        # must stop following.
        manifest = _manifest(path)
        assert [(p.name, Path(p.file).name) for p in manifest.declared] == [("x", "a.weave")]
        assert manifest.invoker_required == ("x",)

    def test_wrong_kind_ref_is_walked_as_loaded(self, tmp_path: Path) -> None:
        """A thread entry naming a weave file is walked as the weave it is.

        The manifest does not check the child's kind; the load reports it.
        """
        _write(tmp_path, "t.thread", _thread(params=_specs(_spec("x"))))
        _write(tmp_path, "inner.weave", _weave([{"ref": "t.thread"}]))
        path = _write(tmp_path, "w.weave", _weave([{"ref": "inner.weave"}]))
        manifest = _manifest(path)
        assert [p.name for p in manifest.declared] == ["x"]
        assert manifest.invoker_required == ("x",)


def _p101_names(path: Path) -> set[str]:
    """The names a load with no runtime params reports ``P101`` for."""
    _, errors, warnings = collect_resolved_config(
        LocalConfigLocation(path), project_root=LocalConfigLocation(path.parent)
    )
    return {str(d.pointer).rsplit("/", 1)[-1] for d in [*errors, *warnings] if d.code == "P101"}


class TestParityWithLoad:
    """What the manifest asks the invoker for is what a bare load reports ``P101`` on."""

    @pytest.mark.parametrize("open_tier", ["loom", "weave", "thread"])
    def test_loom_tree_with_pin_default_and_one_unsatisfied(
        self, tmp_path: Path, open_tier: str
    ) -> None:
        # A failed document ends the load's descent, so one tier is left open per run.
        def open_spec(tier: str) -> dict[str, Any]:
            return _specs(_spec(f"{tier}_open")) if tier == open_tier else {}

        _write(
            tmp_path,
            "t.thread",
            _thread(
                params={
                    **_specs(_spec("pinned"), _spec("defaulted", required=False, default="d")),
                    **open_spec("thread"),
                }
            ),
        )
        _write(
            tmp_path,
            "w.weave",
            _weave([{"ref": "t.thread", "params": {"pinned": "x"}}], params=open_spec("weave")),
        )
        path = _write(tmp_path, "l.loom", _loom([{"ref": "w.weave"}], params=open_spec("loom")))
        expected = {f"{open_tier}_open"}
        assert set(_manifest(path).invoker_required) == expected
        assert _p101_names(path) == expected

    def test_weave_entry_pin_satisfies_the_child(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", _thread(params=_specs(_spec("a"), _spec("b"))))
        path = _write(
            tmp_path,
            "w.weave",
            _weave([{"ref": "t.thread", "params": {"a": "1"}}]),
        )
        assert set(_manifest(path).invoker_required) == {"b"}
        assert _p101_names(path) == {"b"}

    def test_defaults_satisfy_everywhere(self, tmp_path: Path) -> None:
        _write(
            tmp_path,
            "t.thread",
            _thread(params=_specs(_spec("a", required=False, default="1"))),
        )
        path = _write(tmp_path, "w.weave", _weave([{"ref": "t.thread"}]))
        assert _manifest(path).invoker_required == ()
        assert _p101_names(path) == set()
