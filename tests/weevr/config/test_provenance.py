"""Each node of a loaded tree records where its params and registry entries came from.

``ResolvedDocument.provenance.params`` maps each param name a node binds (or
reads through a ``:-`` fallback) to its value source, the declaration whose
spec won, and the pinning entry when a pin supplied the value. The registry
fields (``connections``, ``audit_templates``, ``lookups``, ``column_sets``,
``variables``) and ``defaults`` map each name or ``defaults:`` key the node
sees to the tier and file that declared it, the nearest tier winning. Values
are never recorded.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import pytest
import yaml

from documents import (
    TEST_PROJECT_ID,
    loom_body,
    string_spec,
    thread_body,
    weave_body,
    write_named,
)
from resolving import load_at
from weevr.config import RegistryDeclaration, load_resolved
from weevr.config.document import Provenance, ResolvedDocument
from weevr.config.inheritance import layer_declarations
from weevr.config.param_provenance import (
    ParamDeclaration,
    ParamPin,
    ParamProvenance,
    ParamTier,
)
from weevr.config.params import ScopePositions
from weevr.config.resolver import resolve_references
from weevr.config.scoping import WalkState, resolve_config_variables
from weevr.model import Thread

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


# Registry entries and ``defaults`` keys


def _write_project(root: Path, **extra: Any) -> str:
    body = {
        "kind": "project",
        "config_version": "2.0",
        "id": TEST_PROJECT_ID,
        "name": "test",
        **extra,
    }
    path = root / "weevr.yaml"
    path.write_text(yaml.safe_dump(body, sort_keys=False))
    return str(path)


def _lake(workspace: str) -> dict[str, Any]:
    return {"type": "onelake", "workspace": workspace, "lakehouse": "lh"}


def _lookup(alias: str) -> dict[str, Any]:
    return {"source": {"type": "delta", "alias": alias}}


def _column_set(alias: str) -> dict[str, Any]:
    return {"source": {"type": "delta", "alias": alias}}


def _blocks(tier: str, *, variables: bool = True) -> dict[str, Any]:
    """One entry of each registry named after ``tier``, plus a shared ``x`` in each."""
    blocks: dict[str, Any] = {
        "connections": {f"c_{tier}": _lake(tier), "x": _lake(tier)},
        "audit_templates": {
            f"a_{tier}": {"columns": {"_a": "'1'"}},
            "x": {"columns": {"_x": f"'{tier}'"}},
        },
        "lookups": {f"l_{tier}": _lookup(f"raw.{tier}"), "x": _lookup(f"raw.x_{tier}")},
        "column_sets": {f"s_{tier}": _column_set(f"raw.{tier}"), "x": _column_set("raw.x")},
    }
    if variables:
        blocks["variables"] = {f"v_{tier}": {"type": "string"}, "x": {"type": "string"}}
    return blocks


REGISTRIES = ("connections", "audit_templates", "lookups", "column_sets", "variables")
PREFIX = {
    "connections": "c",
    "audit_templates": "a",
    "lookups": "l",
    "column_sets": "s",
    "variables": "v",
}


@pytest.fixture
def declared(tmp_path: Path) -> dict[str, str]:
    """Project -> loom -> ref weave -> ref thread, every tier declaring every registry.

    Each tier declares its own name per registry and a shared ``x``; the
    ``defaults:`` blocks declare ``tags`` everywhere, ``cache`` on the project
    and ``exports`` on the loom.
    """
    project = _write_project(
        tmp_path, **_blocks("project", variables=False), defaults={"tags": ["p"], "cache": True}
    )
    thread = write_named(tmp_path, "t.thread", thread_body(**_blocks("thread")))
    weave = write_named(
        tmp_path,
        "w.weave",
        weave_body([{"ref": "t.thread"}], **_blocks("weave"), defaults={"tags": ["w"]}),
    )
    loom = write_named(
        tmp_path,
        "l.loom",
        loom_body(
            [{"ref": "w.weave"}],
            **_blocks("loom"),
            defaults={"tags": ["l"], "exports": [{"name": "e", "type": "delta", "alias": "x.e"}]},
        ),
    )
    return {"project": project, "loom": str(loom), "weave": str(weave), "thread": str(thread)}


def _declaration(files: dict[str, str], tier: Any, *at: str) -> RegistryDeclaration:
    return RegistryDeclaration(tier=tier, file=files[tier], pointer="/" + "/".join(at))


def _expected(
    files: dict[str, str], registry: str, tiers: tuple[str, ...]
) -> dict[str, RegistryDeclaration]:
    """Each tier's own name, and ``x`` from the nearest tier declaring it."""
    declaring = [t for t in tiers if not (registry == "variables" and t == "project")]
    expected = {
        f"{PREFIX[registry]}_{tier}": _declaration(
            files, tier, registry, f"{PREFIX[registry]}_{tier}"
        )
        for tier in declaring
    }
    expected["x"] = _declaration(files, declaring[-1], registry, "x")
    return expected


class TestRegistryDeclarations:
    @pytest.mark.parametrize("registry", REGISTRIES)
    def test_each_node_sees_every_enclosing_tier(
        self, declared: dict[str, str], registry: str
    ) -> None:
        loom, weave, thread = _nodes(load_at(Path(declared["loom"])).document)
        chains = [
            (loom, ("project", "loom")),
            (weave, ("project", "loom", "weave")),
            (thread, ("project", "loom", "weave", "thread")),
        ]
        for node, tiers in chains:
            assert dict(getattr(node.provenance, registry)) == _expected(
                declared, registry, tiers
            ), node.kind

    @pytest.mark.parametrize("registry", REGISTRIES)
    def test_a_weave_root_skips_the_loom(self, declared: dict[str, str], registry: str) -> None:
        weave = load_at(Path(declared["weave"])).document
        thread = weave.children["t"]
        assert dict(getattr(weave.provenance, registry)) == _expected(
            declared, registry, ("project", "weave")
        )
        assert dict(getattr(thread.provenance, registry)) == _expected(
            declared, registry, ("project", "weave", "thread")
        )

    def test_project_entries_name_weevr_yaml(self, declared: dict[str, str]) -> None:
        thread = load_at(Path(declared["thread"])).document
        assert thread.provenance.connections["c_project"] == RegistryDeclaration(
            "project", declared["project"], "/connections/c_project"
        )
        assert thread.provenance.lookups["x"] == RegistryDeclaration(
            "thread", declared["thread"], "/lookups/x"
        )

    def test_names_match_the_effective_registries(self, declared: dict[str, str]) -> None:
        for node in _nodes(load_at(Path(declared["loom"])).document):
            assert set(node.provenance.lookups) == set(node.effective.lookups or {})
            assert set(node.provenance.column_sets) == set(node.effective.column_sets or {})
            assert set(node.provenance.variables) == set(node.effective.variables or {})

    def test_thread_connections_match_its_cascaded_connections(
        self, declared: dict[str, str]
    ) -> None:
        _, _, thread = _nodes(load_at(Path(declared["loom"])).document)
        assert isinstance(thread.model, Thread)
        assert set(thread.provenance.connections) == set(thread.model.connections or {})


class TestDefaultsDeclarations:
    def test_each_key_names_its_nearest_tier(self, declared: dict[str, str]) -> None:
        loom, weave, thread = _nodes(load_at(Path(declared["loom"])).document)
        assert dict(loom.provenance.defaults) == {
            "tags": _declaration(declared, "loom", "defaults", "tags"),
            "cache": _declaration(declared, "project", "defaults", "cache"),
            "exports": _declaration(declared, "loom", "defaults", "exports"),
        }
        expected = {
            "tags": _declaration(declared, "weave", "defaults", "tags"),
            "cache": _declaration(declared, "project", "defaults", "cache"),
            "exports": _declaration(declared, "loom", "defaults", "exports"),
        }
        assert dict(weave.provenance.defaults) == expected
        assert dict(thread.provenance.defaults) == expected

    def test_a_thread_own_defaults_block_is_not_a_tier(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body(defaults={"tags": ["t"]}))
        assert dict(load_at(path).document.provenance.defaults) == {}

    def test_no_project_and_no_defaults_is_empty(self, tmp_path: Path) -> None:
        path = write_named(tmp_path, "t.thread", thread_body())
        provenance = load_at(path).document.provenance
        for registry in (*REGISTRIES, "defaults"):
            assert dict(getattr(provenance, registry)) == {}


class TestInlineRegistryDeclarations:
    def test_inline_weave_and_thread_point_into_the_enclosing_file(self, tmp_path: Path) -> None:
        inline_thread = {
            **thread_body(),
            "name": "it",
            "connections": {"c": _lake("t")},
            "lookups": {"l": _lookup("raw.t")},
        }
        inline_weave = {
            "name": "iw",
            **weave_body([inline_thread]),
            "connections": {"c": _lake("w"), "cw": _lake("w")},
            "defaults": {"tags": ["w"]},
        }
        path = str(write_named(tmp_path, "l.loom", loom_body([inline_weave])))
        doc = load_at(Path(path)).document
        weave = doc.children["iw"]
        thread = weave.children["it"]
        assert dict(weave.provenance.connections) == {
            "c": RegistryDeclaration("weave", path, "/weaves/0/connections/c"),
            "cw": RegistryDeclaration("weave", path, "/weaves/0/connections/cw"),
        }
        assert dict(weave.provenance.defaults) == {
            "tags": RegistryDeclaration("weave", path, "/weaves/0/defaults/tags"),
        }
        assert dict(thread.provenance.connections) == {
            "c": RegistryDeclaration("thread", path, "/weaves/0/threads/0/connections/c"),
            "cw": RegistryDeclaration("weave", path, "/weaves/0/connections/cw"),
        }
        assert dict(thread.provenance.lookups) == {
            "l": RegistryDeclaration("thread", path, "/weaves/0/threads/0/lookups/l"),
        }

    def test_inline_thread_in_a_ref_weave(self, tmp_path: Path) -> None:
        inline_thread = {**thread_body(), "name": "it", "column_sets": {"s": _column_set("raw.s")}}
        weave = str(write_named(tmp_path, "w.weave", weave_body([inline_thread])))
        path = write_named(tmp_path, "l.loom", loom_body([{"ref": "w.weave"}]))
        thread = load_at(path).document.children["w"].children["it"]
        assert dict(thread.provenance.column_sets) == {
            "s": RegistryDeclaration("thread", weave, "/threads/0/column_sets/s"),
        }


class TestRegistryShape:
    def test_read_only(self, declared: dict[str, str]) -> None:
        doc = load_at(Path(declared["loom"])).document
        record = doc.provenance.connections["x"]
        for registry in (*REGISTRIES, "defaults"):
            with pytest.raises(TypeError):
                getattr(doc.provenance, registry)["x"] = record
        with pytest.raises(dataclasses.FrozenInstanceError):
            record.tier = "thread"  # type: ignore[misc]

    def test_no_value_appears(self, tmp_path: Path) -> None:
        _write_project(tmp_path, connections={"c": _lake(SECRET)}, defaults={"tags": [SECRET]})
        path = write_named(tmp_path, "t.thread", thread_body())
        assert SECRET not in repr(load_at(path).document.provenance)

    def test_trees_built_from_models_have_none(self, declared: dict[str, str]) -> None:
        doc = load_at(Path(declared["loom"])).document
        rebuilt = ResolvedDocument.from_models(
            doc.model, "loom", "l", weaves=doc.weaves(), threads=doc.weave_threads()
        )
        for node in _nodes(rebuilt):
            for registry in (*REGISTRIES, "defaults"):
                assert dict(getattr(node.provenance, registry)) == {}


TIERS = ("project", "loom", "weave", "thread")
CASES = [
    (block, node, winner)
    for block in (*REGISTRIES, "defaults")
    for node in ("loom", "weave", "thread")
    for winner in TIERS[: TIERS.index(node) + 1]
    if not (block == "variables" and winner == "project")
    and not (block == "defaults" and winner == "thread")
]


def _shared(block: str, tier: str) -> dict[str, Any]:
    """``block`` declaring the shared name (``x``, or the ``tags`` key) with ``tier`` as value."""
    entry: dict[str, dict[str, Any]] = {
        "connections": {"x": _lake(tier)},
        "audit_templates": {"x": {"columns": {"_x": f"'{tier}'"}}},
        "lookups": {"x": _lookup(f"raw.{tier}")},
        "column_sets": {"x": _column_set(f"raw.{tier}")},
        "variables": {"x": {"type": "string", "default": tier}},
        "defaults": {"tags": [tier]},
    }
    return {block: entry[block]}


def _surviving(block: str, node: ResolvedDocument, thread: ResolvedDocument) -> str:
    """The tier whose value survived for the shared name.

    Read from ``node``'s effective registries, or for what only the cascade
    merges (connections, audit templates, defaults) from the thread beneath,
    which sees the same winner when no lower tier declares the name.
    """
    model = thread.model
    assert isinstance(model, Thread)
    if block == "connections":
        assert model.connections is not None
        return model.connections["x"].workspace
    if block == "audit_templates":
        assert model.target.audit_columns is not None
        return model.target.audit_columns["_x"].strip("'")
    if block == "defaults":
        assert model.tags is not None
        return model.tags[0]
    registry = getattr(node.effective, block)
    if block == "variables":
        return registry["x"].default
    return registry["x"].source.alias.removeprefix("raw.")


class TestProvenanceNamesTheCascadeWinner:
    """Each tier down to the winner declares the same name; nothing below it does."""

    @pytest.mark.parametrize(("block", "node", "winner"), CASES)
    def test_reported_tier_is_the_surviving_entry(
        self, tmp_path: Path, block: str, node: str, winner: str
    ) -> None:
        declaring = [
            tier
            for tier in TIERS[: TIERS.index(winner) + 1]
            if not (block == "variables" and tier == "project")  # the project declares none
        ]
        tiers = {tier: _shared(block, tier) if tier in declaring else {} for tier in TIERS}
        thread_extra = dict(tiers["thread"])
        if block == "audit_templates":
            thread_extra["target"] = {"alias": "out.t", "audit_template": "x"}
        project = _write_project(tmp_path, **tiers["project"])
        files = {
            "project": project,
            "thread": str(write_named(tmp_path, "t.thread", thread_body(**thread_extra))),
            "weave": str(
                write_named(
                    tmp_path, "w.weave", weave_body([{"ref": "t.thread"}], **tiers["weave"])
                )
            ),
            "loom": str(
                write_named(tmp_path, "l.loom", loom_body([{"ref": "w.weave"}], **tiers["loom"]))
            ),
        }
        loaded = _nodes(load_at(Path(files["loom"])).document)
        nodes: dict[str, ResolvedDocument] = dict(zip(TIERS[1:], loaded, strict=True))
        name = "tags" if block == "defaults" else "x"
        reported = getattr(nodes[node].provenance, block)[name]
        assert reported == RegistryDeclaration(winner, files[winner], f"/{block}/{name}")
        assert _surviving(block, nodes[node], nodes["thread"]) == winner


def test_provenance_fields_match_the_recorded_blocks() -> None:
    blocks = set(layer_declarations(None, "loom", None, {}))
    fields = {each.name for each in dataclasses.fields(Provenance)}
    assert fields == {"params", *blocks}
