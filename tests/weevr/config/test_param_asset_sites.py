"""Registry assets resolve where they are declared.

A connection, lookup, audit template or column set whose fields hold
``${param.x}`` resolves once, in its declaring config's scope. A pin on an
entry below never retargets it: the pinned subtree sees the pinned value in
its own fields and the asset as the declaring scope resolved it. Re-declaring
the asset under the same name lower down (shadowing) specializes it for that
subtree. A pin an upstream asset reads warns ``P122``, even when the pinned
subtree reads it too.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from documents import loom_body, weave_body, write_named
from resolving import load_at
from weevr.config import load_resolved
from weevr.config.assets import declared_assets
from weevr.diagnostics import Severity

THREAD: dict[str, Any] = {
    "config_version": "2.0",
    "sources": {"s": {"type": "delta", "alias": "raw.t"}},
    "target": {"alias": "out.t"},
}

# One asset of each registry kind, every one reading ${param.ws}.
ASSETS: dict[str, Any] = {
    "connections": {
        "lake": {"type": "onelake", "workspace": "${param.ws}", "lakehouse": "lh"},
    },
    "lookups": {
        "ref_lookup": {"source": {"type": "delta", "alias": "dim.${param.ws}"}, "key": ["id"]},
    },
    "column_sets": {
        "cols": {"source": {"type": "delta", "alias": "map.${param.ws}"}},
    },
    "audit_templates": {
        "tpl": {"columns": {"ws_col": "'${param.ws}'"}},
    },
}

WS = {"ws": {"name": "ws", "type": "string", "required": False, "default": "loom_ws"}}


def _thread(alias: str = "out.t", **extra: Any) -> dict[str, Any]:
    target = {"alias": alias, "audit_template": "tpl"}
    return {**THREAD, "target": target, **extra}


def _loom(root: Path, weaves: list[Any], **extra: Any) -> Path:
    return write_named(root, "l.loom", loom_body(weaves, **{"params": WS, **ASSETS, **extra}))


def _p122(loaded: Any) -> list[Any]:
    return [d for d in loaded.diagnostics if d.code == "P122"]


def _codes(loaded: Any) -> list[str]:
    return [d.code for d in loaded.diagnostics]


def _asset_values(doc: Any) -> dict[str, str]:
    """What each asset kind resolved to, as the thread at ``doc`` sees it."""
    thread = doc.model
    return {
        "connection": thread.connections["lake"].workspace,
        "lookup": doc.effective.lookups["ref_lookup"].source.alias,
        "column_set": doc.effective.column_sets["cols"].source.alias,
        "audit_template": thread.target.audit_columns["ws_col"],
    }


def _all(value: str) -> dict[str, str]:
    return {
        "connection": value,
        "lookup": f"dim.{value}",
        "column_set": f"map.{value}",
        "audit_template": f"'{value}'",
    }


class TestPinDivergence:
    """A pin below the declaring tier never retargets the asset."""

    @pytest.fixture
    def loaded(self, tmp_path: Path) -> Any:
        write_named(tmp_path, "t.thread", _thread(alias="out.${param.ws}"))
        write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"ws": "pinned"}}])
        return load_at(path)

    def test_thread_fields_see_the_pin(self, loaded: Any) -> None:
        thread = loaded.document.children["w"].children["t"]
        assert thread.model.target.alias == "out.pinned"

    def test_assets_keep_the_loom_binding(self, loaded: Any) -> None:
        thread = loaded.document.children["w"].children["t"]
        assert _asset_values(thread) == _all("loom_ws")

    def test_pin_warns_p122_naming_the_connection(self, loaded: Any, tmp_path: Path) -> None:
        loom = tmp_path / "l.loom"
        (found,) = _p122(loaded)
        assert found.file == str(loom)
        assert found.pointer == "/weaves/0/params/ws"
        assert f"connections.lake ({loom})" in (found.expected or ())
        assert "P123" not in _codes(loaded)

    def test_assets_take_the_runtime_binding_at_the_loom(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", _thread())
        write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"ws": "pinned"}}])
        thread = load_at(path, {"ws": "rt"}).document.children["w"].children["t"]
        assert _asset_values(thread) == _all("rt")

    def test_thread_pin_does_not_retarget_a_weave_asset(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", _thread(alias="out.${param.ws}"))
        weave = weave_body([{"ref": "t.thread", "params": {"ws": "pinned"}}], params=WS, **ASSETS)
        path = write_named(tmp_path, "w.weave", weave)
        loaded = load_at(path)
        thread = loaded.document.children["t"]
        assert thread.model.target.alias == "out.pinned"
        assert _asset_values(thread) == _all("loom_ws")
        (found,) = _p122(loaded)
        assert found.file == str(path)
        assert found.pointer == "/threads/0/params/ws"
        assert f"connections.lake ({path})" in (found.expected or ())
        assert "P123" not in _codes(loaded)


class TestShadowing:
    """Re-declaring the asset at a lower tier specializes it for that subtree."""

    @pytest.fixture
    def loaded(self, tmp_path: Path) -> Any:
        write_named(tmp_path, "t.thread", _thread())
        shadowing = weave_body([{"ref": "t.thread"}], params=WS, **ASSETS)
        write_named(tmp_path, "w1.weave", shadowing)
        write_named(tmp_path, "w2.weave", weave_body([{"ref": "t.thread"}]))
        path = _loom(
            tmp_path,
            [{"ref": "w1.weave", "params": {"ws": "pinned"}}, {"ref": "w2.weave"}],
        )
        return load_at(path)

    def test_shadowing_weave_resolves_with_the_pin(self, loaded: Any) -> None:
        thread = loaded.document.children["w1"].children["t"]
        assert _asset_values(thread) == _all("pinned")

    def test_sibling_keeps_the_loom_asset(self, loaded: Any) -> None:
        thread = loaded.document.children["w2"].children["t"]
        assert _asset_values(thread) == _all("loom_ws")

    def test_shadowed_pin_warns_nothing(self, loaded: Any) -> None:
        assert _p122(loaded) == []
        assert "P123" not in _codes(loaded)


class TestPinIgnoredByUpstreamAsset:
    """``P122``: a pin only an asset declared above the entry could have read."""

    @pytest.fixture
    def tree(self, tmp_path: Path) -> Path:
        write_named(tmp_path, "t.thread", _thread())
        write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        return _loom(tmp_path, [{"ref": "w.weave", "params": {"ws": "pinned"}}])

    def test_warns_at_the_pin(self, tree: Path) -> None:
        found = _p122(load_at(tree))
        assert len(found) == 1
        assert found[0].severity is Severity.WARNING
        assert found[0].pointer == "/weaves/0/params/ws"
        assert found[0].file == str(tree)

    def test_names_each_asset_and_its_declaring_file(self, tree: Path) -> None:
        (found,) = _p122(load_at(tree))
        assert list(found.expected or []) == [
            f"connections.lake ({tree})",
            f"lookups.ref_lookup ({tree})",
            f"column_sets.cols ({tree})",
            f"audit_templates.tpl ({tree})",
        ]

    def test_replaces_p123_for_the_pin(self, tree: Path) -> None:
        assert "P123" not in _codes(load_at(tree))

    def test_stays_a_warning_under_strict_params(self, tree: Path) -> None:
        loaded = load_resolved(tree, project_root=tree.parent, strict_params=True)
        (found,) = _p122(loaded)
        assert found.severity is Severity.WARNING

    def test_thread_entry_pin_under_a_weave_asset(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", THREAD)
        connections = {"connections": ASSETS["connections"]}
        weave = weave_body([{"ref": "t.thread", "params": {"ws": "p"}}], params=WS, **connections)
        path = write_named(tmp_path, "w.weave", weave)
        (found,) = _p122(load_at(path))
        assert found.pointer == "/threads/0/params/ws"
        assert list(found.expected or []) == [f"connections.lake ({path})"]

    def test_names_the_declaring_file_of_an_outer_asset(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", _thread())
        write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread", "params": {"ws": "p"}}]))
        path = _loom(tmp_path, [{"ref": "w.weave"}])
        (found,) = _p122(load_at(path))
        assert found.file == str(tmp_path / "w.weave")
        assert found.pointer == "/threads/0/params/ws"
        assert f"connections.lake ({path})" in (found.expected or ())

    def test_inline_weave_asset_is_upstream_of_its_thread_pins(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", _thread())
        inline = {
            "name": "iw",
            "config_version": "2.0",
            "threads": [{"ref": "t.thread", "params": {"ws": "p"}}],
            "connections": {
                "lake": {"type": "onelake", "workspace": "${param.ws}", "lakehouse": "x"}
            },
        }
        path = _loom(tmp_path, [inline])
        (found,) = _p122(load_at(path))
        assert found.pointer == "/weaves/0/threads/0/params/ws"
        assert f"connections.lake ({path})" in (found.expected or ())

    def test_asset_not_reading_the_pinned_name_is_not_named(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", _thread())
        write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        other = {"other": {"type": "onelake", "workspace": "fixed", "lakehouse": "lh"}}
        connections = {**ASSETS["connections"], **other}
        path = _loom(
            tmp_path,
            [{"ref": "w.weave", "params": {"ws": "pinned"}}],
            connections=connections,
        )
        (found,) = _p122(load_at(path))
        assert "connections.other" not in " ".join(found.expected or [])

    def test_no_warning_when_no_upstream_asset_reads_the_name(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", _thread())
        write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"other": "x"}}])
        loaded = load_at(path)
        assert _p122(loaded) == []
        assert "P123" in _codes(loaded)

    def test_asset_declared_below_the_pin_is_not_upstream(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", _thread())
        weave = weave_body([{"ref": "t.thread"}], params=WS, **ASSETS)
        write_named(tmp_path, "w.weave", weave)
        path = write_named(
            tmp_path,
            "l.loom",
            {"config_version": "2.0", "weaves": [{"ref": "w.weave", "params": {"ws": "p"}}]},
        )
        assert _p122(load_at(path)) == []

    def test_subtree_reading_the_pin_still_warns(self, tmp_path: Path) -> None:
        # The subtree's own fields see the pin while the asset does not.
        write_named(tmp_path, "t.thread", _thread(alias="out.${param.ws}"))
        write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"ws": "pinned"}}])
        loaded = load_at(path)
        (found,) = _p122(loaded)
        assert found.pointer == "/weaves/0/params/ws"
        assert "P123" not in _codes(loaded)

    def test_subtree_declaring_the_pin_still_warns(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", _thread())
        write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}], params=WS))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"ws": "pinned"}}])
        assert len(_p122(load_at(path))) == 1

    def test_consumed_pin_without_upstream_readers_warns_nothing(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", _thread(alias="out.${param.other}"))
        write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"other": "x"}}])
        loaded = load_at(path)
        assert _p122(loaded) == []
        assert "P123" not in _codes(loaded)

    def test_child_shadowing_every_reader_warns_p123(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", _thread())
        fixed = {
            "connections": {"lake": {"type": "onelake", "workspace": "w", "lakehouse": "lh"}},
            "lookups": {"ref_lookup": {"source": {"type": "delta", "alias": "d"}, "key": ["id"]}},
            "column_sets": {"cols": {"source": {"type": "delta", "alias": "m"}}},
            "audit_templates": {"tpl": {"columns": {"ws_col": "'w'"}}},
        }
        write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}], **fixed))
        path = _loom(tmp_path, [{"ref": "w.weave", "params": {"ws": "pinned"}}])
        loaded = load_at(path)
        assert _p122(loaded) == []
        assert "P123" in _codes(loaded)

    def test_reference_with_a_default_counts(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", THREAD)
        write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        lake = {"type": "onelake", "workspace": "${param.ws:-fallback}", "lakehouse": "lh"}
        path = write_named(
            tmp_path,
            "l.loom",
            {
                "config_version": "2.0",
                "params": WS,
                "connections": {"lake": lake},
                "weaves": [{"ref": "w.weave", "params": {"ws": "pinned"}}],
            },
        )
        (found,) = _p122(load_at(path))
        assert list(found.expected or []) == [f"connections.lake ({path})"]

    def test_identical_redeclaration_still_shadows(self, tmp_path: Path) -> None:
        # The weave's copy reads the pin itself, so the pin is consumed: no P123 either.
        write_named(tmp_path, "t.thread", THREAD)
        connections = {"connections": ASSETS["connections"]}
        write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}], **connections))
        path = write_named(
            tmp_path,
            "l.loom",
            {
                "config_version": "2.0",
                "params": WS,
                **connections,
                "weaves": [{"ref": "w.weave", "params": {"ws": "pinned"}}],
            },
        )
        loaded = load_at(path)
        assert _p122(loaded) == []
        assert "P123" not in _codes(loaded)

    def test_variable_default_is_an_asset(self, tmp_path: Path) -> None:
        write_named(tmp_path, "t.thread", THREAD)
        write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        variables = {"v": {"type": "string", "default": "${param.ws}"}}
        path = write_named(
            tmp_path,
            "l.loom",
            {
                "config_version": "2.0",
                "params": WS,
                "variables": variables,
                "weaves": [{"ref": "w.weave", "params": {"ws": "pinned"}}],
            },
        )
        loaded = load_at(path)
        thread = loaded.document.children["w"].children["t"]
        assert thread.effective.variables["v"].default == "loom_ws"
        (found,) = _p122(loaded)
        assert list(found.expected or []) == [f"variables.v ({path})"]


@pytest.mark.parametrize(
    ("workspace", "runtime", "warns"),
    [
        ("${param.ws.x}", {"ws": {"x": "a"}}, True),
        ("${fabric.workspace_id}", {"ws": "rt"}, False),
        ("${var.ws}", {"ws": "rt"}, False),
        ("a-${param.ws}-b", {"ws": "rt"}, True),
        ("${param.ws}", {"ws": "rt"}, True),
    ],
)
def test_reference_forms(
    tmp_path: Path, workspace: str, runtime: dict[str, Any], warns: bool
) -> None:
    write_named(tmp_path, "t.thread", THREAD)
    write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
    lake = {"type": "onelake", "workspace": workspace, "lakehouse": "lh"}
    path = write_named(
        tmp_path,
        "l.loom",
        {
            "config_version": "2.0",
            "connections": {"lake": lake},
            "weaves": [{"ref": "w.weave", "params": {"ws": "pinned"}}],
        },
    )
    found = _p122(load_at(path, runtime))
    assert [d.pointer for d in found] == (["/weaves/0/params/ws"] if warns else [])


class TestDeclaredAssets:
    """The scan reads every ``${param.x}`` in an asset, however deep."""

    def test_nested_lists_and_mappings(self) -> None:
        body = {
            "source": {"alias": "${param.a}", "filter": ["x = '${param.b.c}'", {"y": 1}]},
            "values": ["${fabric.workspace_id}", "${var.d}", "${param.e:-z}"],
        }
        (asset,) = declared_assets({"lookups": {"lk": body}}, "f.loom").values()
        assert asset.params == {"a", "b", "e"}

    def test_records_where_it_is_declared(self) -> None:
        assets = declared_assets({"connections": {"c": {}}}, "f.loom", ("weaves", 1))
        assert assets[("connections", "c")].at == ("weaves", 1)
        assert assets[("connections", "c")].file == "f.loom"
