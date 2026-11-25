"""Each node of a loaded tree records where its param bindings came from.

``ResolvedDocument.provenance.params`` maps each param name a node binds (or
reads through a ``:-`` fallback) to its value source, the declaration whose
spec won, and the pinning entry when a pin supplied the value. Values are
never recorded.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import pytest

from documents import string_spec, write_named
from weevr.config import load_resolved
from weevr.config.document import Provenance, ResolvedDocument
from weevr.config.param_provenance import (
    ParamDeclaration,
    ParamPin,
    ParamProvenance,
    ParamTier,
)
from weevr.config.params import ScopePositions
from weevr.config.resolver import resolve_references
from weevr.config.scoping import WalkState, resolve_config_variables

SECRET = "s3cr3t-value"
THREAD: dict[str, Any] = {
    "config_version": "2.0",
    "sources": {"s": {"type": "delta", "alias": "raw.t"}},
}


def _write(root: Path, name: str, body: dict[str, Any]) -> str:
    return str(write_named(root, name, body))


@pytest.fixture
def tree(tmp_path: Path) -> dict[str, str]:
    """A loom -> ref weave (pinned) -> ref thread tree; returns each file path.

    Runtime binds ``r`` (declared by the loom, re-declared by the thread) and
    ``u`` (declared nowhere). The loom declares default ``d``, which the weave
    re-declares with its own default, and optional ``o``, which only a
    fallback reads. The weave entry pins ``p``; the thread reads ``f`` only
    through a fallback.
    """
    thread = _write(
        tmp_path,
        "t.thread",
        {
            **THREAD,
            "params": {"r": string_spec("r")},
            "target": {
                "alias": "out.${param.r}.${param.p}.${param.d}.${param.u}.${param.f:-fb}",
            },
        },
    )
    weave = _write(
        tmp_path,
        "w.weave",
        {
            "config_version": "2.0",
            "params": {"d": string_spec("d", required=False, default=f"{SECRET}-wd")},
            "threads": [{"ref": "t.thread"}],
        },
    )
    loom = _write(
        tmp_path,
        "l.loom",
        {
            "config_version": "2.0",
            "params": {
                "r": string_spec("r"),
                "d": string_spec("d", required=False, default=f"{SECRET}-ld"),
                "o": string_spec("o", required=False),
            },
            "weaves": [
                {
                    "ref": "w.weave",
                    "params": {"p": f"{SECRET}-pin"},
                    "condition": {"when": "${param.o:-true}"},
                }
            ],
        },
    )
    return {"loom": loom, "weave": weave, "thread": thread}


RUNTIME = {"r": f"{SECRET}-r", "u": f"{SECRET}-u"}


def _load(tree: dict[str, str]) -> ResolvedDocument:
    loom = Path(tree["loom"])
    return load_resolved(loom, runtime_params=RUNTIME, project_root=loom.parent).document


def _nodes(doc: ResolvedDocument) -> tuple[ResolvedDocument, ResolvedDocument, ResolvedDocument]:
    weave = doc.children["w"]
    return doc, weave, weave.children["t"]


def _spec_at(tier: ParamTier, file: str, name: str) -> ParamDeclaration:
    return ParamDeclaration(tier=tier, file=file, pointer=f"/params/{name}")


class TestSources:
    def test_root_node(self, tree: dict[str, str]) -> None:
        loom, _, _ = _nodes(_load(tree))
        assert dict(loom.provenance.params) == {
            "r": ParamProvenance("runtime", _spec_at("loom", tree["loom"], "r")),
            "u": ParamProvenance("runtime", None),
            "d": ParamProvenance("default", _spec_at("loom", tree["loom"], "d")),
            "o": ParamProvenance("fallback", _spec_at("loom", tree["loom"], "o")),
        }

    def test_pinned_weave(self, tree: dict[str, str]) -> None:
        _, weave, _ = _nodes(_load(tree))
        pin = ParamPin(entry="w", file=tree["loom"], pointer="/weaves/0/params/p")
        assert dict(weave.provenance.params) == {
            "r": ParamProvenance("inherited", _spec_at("loom", tree["loom"], "r")),
            "u": ParamProvenance("inherited", None),
            "p": ParamProvenance("pin", None, pin),
            "d": ParamProvenance("default", _spec_at("weave", tree["weave"], "d")),
        }

    def test_thread_beneath_the_pin(self, tree: dict[str, str]) -> None:
        _, _, thread = _nodes(_load(tree))
        pin = ParamPin(entry="w", file=tree["loom"], pointer="/weaves/0/params/p")
        assert dict(thread.provenance.params) == {
            "r": ParamProvenance("inherited", _spec_at("thread", tree["thread"], "r")),
            "u": ParamProvenance("inherited", None),
            "p": ParamProvenance("inherited", None, pin),
            "d": ParamProvenance("default", _spec_at("weave", tree["weave"], "d")),
            "f": ParamProvenance("fallback", None),
        }

    def test_a_bound_name_read_with_a_fallback_keeps_its_source(self, tmp_path: Path) -> None:
        path = _write(
            tmp_path,
            "t.thread",
            {**THREAD, "params": {"x": string_spec("x")}, "target": {"alias": "${param.x:-d}"}},
        )
        doc = load_resolved(path, runtime_params={"x": "v"}, project_root=tmp_path).document
        assert doc.provenance.params["x"].source == "runtime"

    def test_unbound_optional_name_nobody_reads_is_absent(self, tmp_path: Path) -> None:
        path = _write(
            tmp_path,
            "t.thread",
            {
                **THREAD,
                "params": {"x": string_spec("x", required=False)},
                "target": {"alias": "out.t"},
            },
        )
        doc = load_resolved(path, project_root=tmp_path).document
        assert dict(doc.provenance.params) == {}


class TestInlineChildren:
    def test_inline_weave_and_thread_have_their_own_provenance(self, tmp_path: Path) -> None:
        inline_thread = {
            **THREAD,
            "name": "it",
            "params": {"y": string_spec("y", required=False, default="dy")},
            "target": {"alias": "out.${param.y}.${param.g:-fb}"},
        }
        path = _write(
            tmp_path,
            "l.loom",
            {
                "config_version": "2.0",
                "params": {"r": string_spec("r")},
                "weaves": [{"name": "iw", "config_version": "2.0", "threads": [inline_thread]}],
            },
        )
        doc = load_resolved(path, runtime_params={"r": "v"}, project_root=tmp_path).document
        weave = doc.children["iw"]
        thread = weave.children["it"]
        assert dict(weave.provenance.params) == {
            "r": ParamProvenance("inherited", _spec_at("loom", path, "r")),
        }
        y_spec = ParamDeclaration(tier="thread", file=path, pointer="/weaves/0/threads/0/params/y")
        assert dict(thread.provenance.params) == {
            "r": ParamProvenance("inherited", _spec_at("loom", path, "r")),
            "y": ParamProvenance("default", y_spec),
            "g": ParamProvenance("fallback", None),
        }

    def test_inline_thread_in_a_ref_weave(self, tmp_path: Path) -> None:
        inline_thread = {**THREAD, "name": "it", "target": {"alias": "out.${param.g:-fb}"}}
        _write(tmp_path, "w.weave", {"config_version": "2.0", "threads": [inline_thread]})
        path = _write(tmp_path, "l.loom", {"config_version": "2.0", "weaves": [{"ref": "w.weave"}]})
        doc = load_resolved(path, project_root=tmp_path).document
        thread = doc.children["w"].children["it"]
        assert dict(thread.provenance.params) == {"g": ParamProvenance("fallback", None)}


class TestPinEntries:
    def test_weave_entry_named_by_name(self, tmp_path: Path) -> None:
        _write(
            tmp_path,
            "t.thread",
            {**THREAD, "params": {"x": string_spec("x")}, "target": {"alias": "o.${param.x}"}},
        )
        _write(tmp_path, "w.weave", {"config_version": "2.0", "threads": [{"ref": "t.thread"}]})
        path = _write(
            tmp_path,
            "l.loom",
            {
                "config_version": "2.0",
                "weaves": [{"ref": "w.weave", "name": "sales", "params": {"x": "v"}}],
            },
        )
        doc = load_resolved(path, project_root=tmp_path).document
        pin = ParamPin(entry="sales", file=path, pointer="/weaves/0/params/x")
        assert doc.children["sales"].provenance.params["x"] == ParamProvenance("pin", None, pin)

    def test_thread_entry_with_as(self, tmp_path: Path) -> None:
        thread = _write(
            tmp_path,
            "t.thread",
            {**THREAD, "params": {"x": string_spec("x")}, "target": {"alias": "o.${param.x}"}},
        )
        path = _write(
            tmp_path,
            "w.weave",
            {
                "config_version": "2.0",
                "threads": [
                    {"ref": "t.thread", "as": "first", "params": {"x": "a"}},
                    {"ref": "t.thread", "as": "second", "params": {"x": "b"}},
                ],
            },
        )
        doc = load_resolved(path, project_root=tmp_path).document
        spec = _spec_at("thread", thread, "x")
        assert doc.children["second"].provenance.params["x"] == ParamProvenance(
            "pin", spec, ParamPin(entry="second", file=path, pointer="/threads/1/params/x")
        )

    def test_thread_entry_pin_pointer(self, tmp_path: Path) -> None:
        _write(tmp_path, "t.thread", {**THREAD, "target": {"alias": "o.${param.x}"}})
        path = _write(
            tmp_path,
            "w.weave",
            {
                "config_version": "2.0",
                "threads": [{"ref": "t.thread", "params": {"x": "v"}}],
            },
        )
        doc = load_resolved(path, project_root=tmp_path).document
        pin = ParamPin(entry="t", file=path, pointer="/threads/0/params/x")
        assert doc.children["t"].provenance.params["x"] == ParamProvenance("pin", None, pin)


class TestResolvingWithoutInlineScopes:
    """A caller that skips handing over the inline scopes gets the same provenance."""

    def test_inline_thread_fallback_is_kept(self, tmp_path: Path) -> None:
        inline_thread = {**THREAD, "name": "it", "target": {"alias": "o.${param.x:-d}"}}
        config: dict[str, Any] = {"config_version": "2.0", "threads": [inline_thread]}
        walk = WalkState.start(config, file="w.weave", kind="weave")
        scopes: ScopePositions = {}
        resolved = resolve_config_variables(config, "weave", walk, inline_scopes=scopes)
        (child,) = resolve_references(resolved, "weave", tmp_path, walk, authored=config)
        assert dict(child.provenance) == {"x": ParamProvenance("fallback", None)}


class TestShape:
    def test_no_value_appears(self, tree: dict[str, str]) -> None:
        doc = _load(tree)
        for node in _nodes(doc):
            assert SECRET not in repr(node.provenance)

    def test_read_only(self, tree: dict[str, str]) -> None:
        doc = _load(tree)
        record = doc.provenance.params["r"]
        with pytest.raises(TypeError):
            doc.provenance.params["r"] = record  # type: ignore[index]
        with pytest.raises(dataclasses.FrozenInstanceError):
            record.source = "pin"  # type: ignore[misc]
        with pytest.raises(dataclasses.FrozenInstanceError):
            doc.provenance = Provenance()  # type: ignore[misc]

    def test_trees_built_from_models_have_none(self, tree: dict[str, str]) -> None:
        doc = _load(tree)
        rebuilt = ResolvedDocument.from_models(doc.model, "loom", "l", weaves=doc.weaves())
        assert dict(rebuilt.provenance.params) == {}
