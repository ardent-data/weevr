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

from documents import loom_body, string_spec, thread_body, weave_body, write_named
from weevr.config import ManifestParam, ParamManifest, param_manifest
from weevr.config.loader import collect_resolved_config
from weevr.config.locations import LocalConfigLocation
from weevr.errors import ConfigError


def _specs(*specs: dict[str, Any]) -> dict[str, Any]:
    return {spec["name"]: spec for spec in specs}


def _manifest(path: Path) -> ParamManifest:
    return param_manifest(path, project_root=path.parent)


def _declared(manifest: ParamManifest, name: str) -> list[ManifestParam]:
    return [p for p in manifest.declared if p.name == name]


class TestShape:
    """The manifest names the executable and is frozen."""

    def test_thread_manifest_names_the_file(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body())
        manifest = _manifest(path)
        assert manifest.executable == "t"
        assert manifest.kind == "thread"
        assert manifest.file == str(path)
        assert manifest.declared == ()
        assert manifest.invoker_required == ()
        assert manifest.undeclared_used == ()

    def test_manifest_is_frozen(self, tmp_path: Path) -> None:
        manifest = _manifest(write_named(tmp_path, "t.thread", thread_body()))
        with pytest.raises(dataclasses.FrozenInstanceError):
            manifest.kind = "weave"  # type: ignore[misc]

    def test_declared_name_beats_stem(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}], name="w"))
        write_named(tmp_path, "t.thread", thread_body())
        assert _manifest(path).executable == "w"

    def test_accepts_a_string_path(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body())
        assert param_manifest(str(path)).executable == "t"


class TestDeclared:
    """Each declaration is reported with its spec fields and where it sits."""

    def test_spec_fields(self, tmp_path: Path) -> None:
        params = _specs(
            string_spec("region", description="Sales region"),
            string_spec("limit", type="int", required=False, default=10),
        )
        path = write_named(tmp_path, "t.thread", thread_body(params=params))
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
                tier="thread",
            ),
            ManifestParam(
                name="limit",
                type="int",
                required=False,
                default=10,
                description=None,
                file=str(path),
                pointer="/params/limit",
                tier="thread",
            ),
        )

    def test_children_declarations_carry_their_file(self, tmp_path: Path) -> None:
        thread = write_named(tmp_path, "t.thread", thread_body(params=_specs(string_spec("x"))))
        path = write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        (declared,) = _declared(_manifest(path), "x")
        assert declared.file == str(thread)
        assert declared.pointer == "/params/x"

    def test_inline_declaration_pointer(self, tmp_path: Path) -> None:
        inline = {**thread_body(), "name": "i", "params": _specs(string_spec("x"))}
        path = write_named(tmp_path, "w.weave", weave_body([inline]))
        (declared,) = _declared(_manifest(path), "x")
        assert declared.file == str(path)
        assert declared.pointer == "/threads/0/params/x"

    def test_shadowing_declarations_are_each_reported(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", thread_body(params=_specs(string_spec("x"))))
        params = _specs(string_spec("x", required=False, default="w"))
        path = write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}], params=params))
        assert len(_declared(_manifest(path), "x")) == 2

    def test_file_referenced_twice_declares_once(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", thread_body(params=_specs(string_spec("x"))))
        entries = [{"ref": "t.thread", "as": "a"}, {"ref": "t.thread", "as": "b"}]
        path = write_named(tmp_path, "w.weave", weave_body(entries))
        assert len(_declared(_manifest(path), "x")) == 1


class TestInvokerRequired:
    """Required specs no pin or default satisfies must come from the invoker."""

    def test_required_without_default(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body(params=_specs(string_spec("x"))))
        assert _manifest(path).invoker_required == ("x",)

    def test_default_satisfies(self, tmp_path: Path) -> None:
        params = _specs(string_spec("x", default="d"))
        path = write_named(tmp_path, "t.thread", thread_body(params=params))
        assert _manifest(path).invoker_required == ()

    def test_optional_without_default_is_not_required(self, tmp_path: Path) -> None:
        params = _specs(string_spec("x", required=False))
        path = write_named(tmp_path, "t.thread", thread_body(params=params))
        assert _manifest(path).invoker_required == ()

    def test_weave_pin_satisfies_thread_required(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", thread_body(params=_specs(string_spec("x"))))
        path = write_named(
            tmp_path, "w.weave", weave_body([{"ref": "t.thread", "params": {"x": "v"}}])
        )
        assert _manifest(path).invoker_required == ()

    def test_pin_satisfies_only_its_subtree(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", thread_body(params=_specs(string_spec("x"))))
        entries = [
            {"ref": "t.thread", "as": "a", "params": {"x": "v"}},
            {"ref": "t.thread", "as": "b"},
        ]
        path = write_named(tmp_path, "w.weave", weave_body(entries))
        assert _manifest(path).invoker_required == ("x",)

    def test_loom_pin_reaches_grandchild(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", thread_body(params=_specs(string_spec("x"))))
        write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        path = write_named(
            tmp_path, "l.loom", loom_body([{"ref": "w.weave", "params": {"x": "v"}}])
        )
        assert _manifest(path).invoker_required == ()

    def test_pin_from_parent_param_moves_the_need_up(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", thread_body(params=_specs(string_spec("x"))))
        entries = [{"ref": "t.thread", "params": {"x": "${param.y}"}}]
        params = _specs(string_spec("y"))
        path = write_named(tmp_path, "w.weave", weave_body(entries, params=params))
        manifest = _manifest(path)
        assert manifest.invoker_required == ("y",)
        assert manifest.undeclared_used == ()

    def test_ancestor_default_does_not_satisfy_redeclared_child(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", thread_body(params=_specs(string_spec("x"))))
        params = _specs(string_spec("x", default="w"))
        path = write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}], params=params))
        assert _manifest(path).invoker_required == ("x",)

    def test_child_default_satisfies_inherited_required(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", thread_body(params=_specs(string_spec("x", default="t"))))
        path = write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        assert _manifest(path).invoker_required == ()

    def test_inherited_required_spec_counts_in_child(self, tmp_path: Path) -> None:
        # The weave declares x required; its thread inherits the spec but the
        # weave scope already lists it, so it appears once.
        write_named(tmp_path, "t.thread", thread_body())
        params = _specs(string_spec("x"))
        path = write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}], params=params))
        assert _manifest(path).invoker_required == ("x",)

    def test_inline_thread_declaration(self, tmp_path: Path) -> None:
        inline = {**thread_body(), "name": "i", "params": _specs(string_spec("x"))}
        path = write_named(tmp_path, "w.weave", weave_body([inline]))
        assert _manifest(path).invoker_required == ("x",)

    def test_inline_weave_with_ref_thread_pinned(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", thread_body(params=_specs(string_spec("x"))))
        inline_weave = {"name": "iw", "threads": [{"ref": "t.thread", "params": {"x": "v"}}]}
        path = write_named(tmp_path, "l.loom", loom_body([inline_weave]))
        assert _manifest(path).invoker_required == ()


class TestUndeclaredUsed:
    """``${param.x}`` references no declaration in their scope chain covers."""

    def test_undeclared_reference(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body(alias="out.${param.region}"))
        assert _manifest(path).undeclared_used == ("region",)

    def test_declared_reference_is_not_listed(self, tmp_path: Path) -> None:
        body = thread_body(alias="out.${param.region}", params=_specs(string_spec("region")))
        path = write_named(tmp_path, "t.thread", body)
        assert _manifest(path).undeclared_used == ()

    def test_reference_with_inline_default_is_listed(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body(alias="out.${param.region:-eu}"))
        assert _manifest(path).undeclared_used == ("region",)

    def test_dotted_reference_reads_its_first_key(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body(alias="out.${param.ws.name}"))
        assert _manifest(path).undeclared_used == ("ws",)

    def test_other_namespaces_ignored(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body(alias="out.${fabric.workspace_id}"))
        assert _manifest(path).undeclared_used == ()

    def test_ancestor_declaration_covers_child_reference(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", thread_body(alias="out.${param.x}"))
        params = _specs(string_spec("x"))
        path = write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}], params=params))
        assert _manifest(path).undeclared_used == ()

    def test_sibling_declaration_does_not_cover(self, tmp_path: Path) -> None:
        write_named(tmp_path, "a.thread", thread_body(params=_specs(string_spec("x", default="d"))))
        write_named(tmp_path, "b.thread", thread_body(alias="out.${param.x}"))
        path = write_named(
            tmp_path, "w.weave", weave_body([{"ref": "a.thread"}, {"ref": "b.thread"}])
        )
        assert _manifest(path).undeclared_used == ("x",)

    def test_pinned_undeclared_name_is_not_listed(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", thread_body(alias="out.${param.x}"))
        path = write_named(
            tmp_path, "w.weave", weave_body([{"ref": "t.thread", "params": {"x": "v"}}])
        )
        assert _manifest(path).undeclared_used == ()

    def test_inline_entry_keys_resolve_in_parent_scope(self, tmp_path: Path) -> None:
        inline = {
            **thread_body(alias="out.${param.x}"),
            "name": "i",
            "condition": {"when": "${param.c} == 'y'"},
            "params": _specs(string_spec("x", default="d")),
        }
        path = write_named(tmp_path, "w.weave", weave_body([inline]))
        assert _manifest(path).undeclared_used == ("c",)

    def test_names_listed_once(self, tmp_path: Path) -> None:
        body = thread_body(alias="${param.x}.${param.x}", defaults={"n": "${param.x}"})
        path = write_named(tmp_path, "t.thread", body)
        assert _manifest(path).undeclared_used == ("x",)


class TestNoValues:
    """Nothing is bound or coerced: missing or ill-typed values never fail."""

    def test_required_unsupplied_does_not_raise(self, tmp_path: Path) -> None:
        params = _specs(string_spec("x", type="int"))
        path = write_named(tmp_path, "t.thread", thread_body(alias="out.${param.x}", params=params))
        assert _manifest(path).invoker_required == ("x",)

    def test_ill_typed_default_is_reported_as_written(self, tmp_path: Path) -> None:
        params = _specs(string_spec("x", type="int", default="not-an-int"))
        path = write_named(tmp_path, "t.thread", thread_body(params=params))
        (declared,) = _manifest(path).declared
        assert declared.default == "not-an-int"


class TestStructuralErrors:
    """A document that cannot be read raises its load error."""

    def test_missing_ref_raises(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "w.weave", weave_body([{"ref": "missing.thread"}]))
        with pytest.raises(ConfigError) as info:
            _manifest(path)
        assert [d.code for d in info.value.diagnostics] == ["C301"]

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError):
            _manifest(tmp_path / "absent.thread")

    def test_cycle_is_not_followed(self, tmp_path: Path) -> None:
        """A reference cycle is walked once round and then left; the load reports it."""
        params = _specs(string_spec("x"))
        write_named(tmp_path, "a.weave", weave_body([{"ref": "b.weave"}], params=params))
        path = write_named(tmp_path, "b.weave", weave_body([{"ref": "a.weave"}]))
        # Each weave lists the other as a thread entry: a ref cycle the walk
        # must stop following.
        manifest = _manifest(path)
        assert [(p.name, Path(p.file).name) for p in manifest.declared] == [("x", "a.weave")]
        assert manifest.invoker_required == ("x",)

    def test_wrong_kind_ref_is_walked_as_loaded(self, tmp_path: Path) -> None:
        """A thread entry naming a weave file is walked as the weave it is.

        The manifest does not check the child's kind; the load reports it.
        """
        write_named(tmp_path, "t.thread", thread_body(params=_specs(string_spec("x"))))
        write_named(tmp_path, "inner.weave", weave_body([{"ref": "t.thread"}]))
        path = write_named(tmp_path, "w.weave", weave_body([{"ref": "inner.weave"}]))
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
            return _specs(string_spec(f"{tier}_open")) if tier == open_tier else {}

        write_named(
            tmp_path,
            "t.thread",
            thread_body(
                params={
                    **_specs(
                        string_spec("pinned"), string_spec("defaulted", required=False, default="d")
                    ),
                    **open_spec("thread"),
                }
            ),
        )
        write_named(
            tmp_path,
            "w.weave",
            weave_body([{"ref": "t.thread", "params": {"pinned": "x"}}], params=open_spec("weave")),
        )
        path = write_named(
            tmp_path, "l.loom", loom_body([{"ref": "w.weave"}], params=open_spec("loom"))
        )
        expected = {f"{open_tier}_open"}
        assert set(_manifest(path).invoker_required) == expected
        assert _p101_names(path) == expected

    def test_weave_entry_pin_satisfies_the_child(self, tmp_path: Path) -> None:
        write_named(
            tmp_path, "t.thread", thread_body(params=_specs(string_spec("a"), string_spec("b")))
        )
        path = write_named(
            tmp_path,
            "w.weave",
            weave_body([{"ref": "t.thread", "params": {"a": "1"}}]),
        )
        assert set(_manifest(path).invoker_required) == {"b"}
        assert _p101_names(path) == {"b"}

    def test_defaults_satisfy_everywhere(self, tmp_path: Path) -> None:
        write_named(
            tmp_path,
            "t.thread",
            thread_body(params=_specs(string_spec("a", required=False, default="1"))),
        )
        path = write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        assert _manifest(path).invoker_required == ()
        assert _p101_names(path) == set()
