"""Project cascades: ``weevr.yaml`` declares connections, audit templates and ``defaults:``.

The project is the outermost tier of every load-time cascade. Connections and
audit-template definitions merge by name down project -> loom -> weave ->
thread, the nearest declaration winning. ``defaults:`` blocks replace
whole-value, with the same per-key exceptions the loom and weave tiers have:
naming, audit-column application, exports and target policies. A project
asset's ``${param.x}`` resolves with the bindings at the project's own scope
(runtime over the project default), and a pin below never retargets it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from documents import (
    lake_connection,
    loom_body,
    thread_body,
    weave_body,
    write_named,
    write_project,
)
from resolving import leaf_node, load_at, load_failure, with_code
from weevr.config.locations import LocalConfigLocation
from weevr.config.manifest import param_manifest
from weevr.config.onelake import resolve_connection_path
from weevr.config.project import RESERVED_PROJECT_BLOCKS, resolve_project_connections
from weevr.config.root import read_project
from weevr.errors import ConfigError, ConfigLocationError
from weevr.model import Thread


def _tree(
    root: Path,
    *,
    loom: dict[str, Any] | None = None,
    weave: dict[str, Any] | None = None,
    thread: dict[str, Any] | None = None,
    weave_entry: dict[str, Any] | None = None,
) -> Path:
    """A loom -> ref weave -> ref thread tree, each tier with ``extra`` keys; returns the loom."""
    write_named(root, "t.thread", thread_body(**(thread or {})))
    write_named(root, "w.weave", weave_body([{"ref": "t.thread"}], **(weave or {})))
    entry = {"ref": "w.weave", **(weave_entry or {})}
    return write_named(root, "l.loom", loom_body([entry], **(loom or {})))


def _thread(loaded: Any) -> Thread:
    """The thread model of a load rooted at the loom, weave or thread."""
    model = leaf_node(loaded).model
    assert isinstance(model, Thread)
    return model


def _roots(root: Path) -> dict[str, Path]:
    """The same thread loaded as a loom root, a weave root and a standalone thread."""
    loom = _tree(root)
    return {"loom": loom, "weave": root / "w.weave", "thread": root / "t.thread"}


ROOT_KINDS = ["loom", "weave", "thread"]
LOWER_TIERS = ["loom", "weave", "thread"]


def _lower(tier: str, block: dict[str, Any], *, target: bool = False) -> dict[str, Any]:
    """``_tree`` keywords setting ``block`` at ``tier``.

    A loom or weave sets it as its ``defaults:``. A thread sets the keys on
    itself, or on its target when ``target`` is set.
    """
    if tier != "thread":
        return {tier: {"defaults": block}}
    if target:
        return {"thread": {"target": {"alias": "out.t", **block}}}
    return {"thread": block}


class TestProjectDocument:
    """The three blocks are project fields; only ``variables`` stays reserved."""

    def test_blocks_load(self, tmp_path: Path) -> None:
        write_project(
            tmp_path,
            connections={"lake": lake_connection("ws")},
            audit_templates={"tpl": {"columns": {"c": "1"}}},
            defaults={"tags": ["a"]},
        )
        project = read_project(LocalConfigLocation(tmp_path))
        assert project is not None
        assert project.connections is not None
        assert project.connections["lake"].workspace == "ws"
        assert project.audit_templates is not None
        assert project.audit_templates["tpl"].columns == {"c": "1"}
        assert project.defaults == {"tags": ["a"]}

    def test_only_variables_is_reserved(self) -> None:
        assert RESERVED_PROJECT_BLOCKS == ("variables",)

    def test_variables_block_is_c210(self, tmp_path: Path) -> None:
        write_project(tmp_path, variables={"v": {"type": "string"}})
        with pytest.raises(ConfigError) as info:
            read_project(LocalConfigLocation(tmp_path))
        assert [(d.code, d.pointer) for d in info.value.diagnostics] == [("C210", "/variables")]

    def test_malformed_defaults_names_weevr_yaml(self, tmp_path: Path) -> None:
        project_file = str(write_project(tmp_path, defaults={"exports": {"x": 1}}) / "weevr.yaml")
        error = load_failure(_tree(tmp_path))
        (shape,) = with_code(error.diagnostics, "C102")
        assert shape.file == project_file
        assert shape.pointer == "/defaults/exports"


class TestProjectReachesEveryRoot:
    """A project block applies to a loom, a weave and a standalone thread alike."""

    @pytest.mark.parametrize("kind", ROOT_KINDS)
    def test_connection(self, tmp_path: Path, kind: str) -> None:
        write_project(tmp_path, connections={"lake": lake_connection("proj_ws")})
        thread = _thread(load_at(_roots(tmp_path)[kind]))
        assert thread.connections is not None
        assert thread.connections["lake"].workspace == "proj_ws"

    @pytest.mark.parametrize("kind", ROOT_KINDS)
    def test_audit_template_and_application(self, tmp_path: Path, kind: str) -> None:
        write_project(
            tmp_path,
            audit_templates={"tpl": {"columns": {"_by": "'proj'"}}},
            defaults={"target": {"audit_template": "tpl"}},
        )
        thread = _thread(load_at(_roots(tmp_path)[kind]))
        assert thread.target.audit_columns == {"_by": "'proj'"}

    @pytest.mark.parametrize("kind", ROOT_KINDS)
    def test_value_default(self, tmp_path: Path, kind: str) -> None:
        write_project(tmp_path, defaults={"tags": ["proj"]})
        assert _thread(load_at(_roots(tmp_path)[kind])).tags == ["proj"]

    @pytest.mark.parametrize("kind", ROOT_KINDS)
    def test_export(self, tmp_path: Path, kind: str) -> None:
        export = {"name": "archive", "type": "parquet", "path": "/archive"}
        write_project(tmp_path, defaults={"exports": [export]})
        thread = _thread(load_at(_roots(tmp_path)[kind]))
        assert [e.name for e in thread.exports or []] == ["archive"]


class TestRegistriesMergeByName:
    """Connections and audit-template definitions: the nearest declaration of a name wins."""

    @pytest.mark.parametrize("tier", ["loom", "weave", "thread"])
    def test_lower_connection_shadows_by_name(self, tmp_path: Path, tier: str) -> None:
        write_project(
            tmp_path,
            connections={
                "lake": lake_connection("proj_ws"),
                "other": lake_connection("proj_other"),
            },
        )
        loaded = load_at(
            _tree(tmp_path, **{tier: {"connections": {"lake": lake_connection(tier)}}})
        )
        connections = _thread(loaded).connections
        assert connections is not None
        assert connections["lake"].workspace == tier
        assert connections["other"].workspace == "proj_other"

    @pytest.mark.parametrize("tier", ["loom", "weave", "thread"])
    def test_lower_template_shadows_by_name(self, tmp_path: Path, tier: str) -> None:
        write_project(
            tmp_path,
            audit_templates={"tpl": {"columns": {"_by": "'proj'"}}},
            defaults={"target": {"audit_template": "tpl"}},
        )
        shadow = {"audit_templates": {"tpl": {"columns": {"_by": f"'{tier}'"}}}}
        thread = _thread(load_at(_tree(tmp_path, **{tier: shadow})))
        assert thread.target.audit_columns == {"_by": f"'{tier}'"}

    def test_a_thread_names_a_project_template(self, tmp_path: Path) -> None:
        write_project(tmp_path, audit_templates={"tpl": {"columns": {"_by": "'proj'"}}})
        target = {"alias": "out.t", "audit_template": "tpl"}
        thread = _thread(load_at(_tree(tmp_path, thread={"target": target})))
        assert thread.target.audit_columns == {"_by": "'proj'"}


class TestDefaultsCascade:
    """``defaults:`` replaces whole-value; the per-key exceptions span the project tier."""

    @pytest.mark.parametrize("tier", LOWER_TIERS)
    def test_lower_block_replaces_the_project_block(self, tmp_path: Path, tier: str) -> None:
        write_project(tmp_path, defaults={"tags": ["proj", "wide"]})
        loaded = load_at(_tree(tmp_path, **_lower(tier, {"tags": [tier]})))
        assert _thread(loaded).tags == [tier]

    def test_unrelated_keys_inherit_side_by_side(self, tmp_path: Path) -> None:
        write_project(tmp_path, defaults={"tags": ["proj"]})
        loaded = load_at(_tree(tmp_path, weave={"defaults": {"cache": False}}))
        thread = _thread(loaded)
        assert thread.tags == ["proj"]
        assert thread.cache is False

    def test_naming_from_the_project(self, tmp_path: Path) -> None:
        write_project(tmp_path, defaults={"naming": {"columns": "snake_case"}})
        naming = _thread(load_at(_tree(tmp_path))).target.naming
        assert naming is not None
        assert naming.columns == "snake_case"

    @pytest.mark.parametrize("tier", LOWER_TIERS)
    def test_lower_naming_wins_whole(self, tmp_path: Path, tier: str) -> None:
        write_project(tmp_path, defaults={"naming": {"columns": "snake_case"}})
        lower = _lower(tier, {"naming": {"tables": "lowercase"}}, target=True)
        naming = _thread(load_at(_tree(tmp_path, **lower))).target.naming
        assert naming is not None
        assert naming.tables == "lowercase"
        assert naming.columns is None

    def test_audit_columns_are_additive(self, tmp_path: Path) -> None:
        write_project(tmp_path, defaults={"target": {"audit_columns": {"_proj": "1"}}})
        weave = {"defaults": {"target": {"audit_columns": {"_weave": "2"}}}}
        thread = _thread(load_at(_tree(tmp_path, weave=weave)))
        assert thread.target.audit_columns == {"_proj": "1", "_weave": "2"}

    def test_audit_inherit_off_drops_project_columns(self, tmp_path: Path) -> None:
        write_project(tmp_path, defaults={"target": {"audit_columns": {"_proj": "1"}}})
        target = {"alias": "out.t", "audit_template_inherit": False}
        thread = _thread(load_at(_tree(tmp_path, thread={"target": target})))
        assert not thread.target.audit_columns

    def test_audit_exclude_removes_a_project_column(self, tmp_path: Path) -> None:
        columns = {"_proj": "1", "_keep": "2"}
        write_project(tmp_path, defaults={"target": {"audit_columns": columns}})
        target = {"alias": "out.t", "audit_columns_exclude": ["_proj"]}
        thread = _thread(load_at(_tree(tmp_path, thread={"target": target})))
        assert thread.target.audit_columns == {"_keep": "2"}

    def test_exports_merge_by_name(self, tmp_path: Path) -> None:
        archive = {"name": "archive", "type": "parquet", "path": "/proj"}
        write_project(tmp_path, defaults={"exports": [archive]})
        loom_export = {"name": "copy", "type": "csv", "path": "/loom"}
        thread_export = {"name": "archive", "type": "parquet", "path": "/thread"}
        loaded = load_at(
            _tree(
                tmp_path,
                loom={"defaults": {"exports": [loom_export]}},
                thread={"exports": [thread_export]},
            )
        )
        exports = {e.name: e.path for e in _thread(loaded).exports or []}
        assert exports == {"archive": "/thread", "copy": "/loom"}

    def test_a_lower_tier_disables_a_project_export(self, tmp_path: Path) -> None:
        archive = {"name": "archive", "type": "parquet", "path": "/proj"}
        write_project(tmp_path, defaults={"exports": [archive]})
        weave = {"defaults": {"exports": [{"name": "archive", "enabled": False}]}}
        assert not _thread(load_at(_tree(tmp_path, weave=weave))).exports

    @pytest.mark.parametrize("tier", LOWER_TIERS)
    def test_target_policies_cascade_per_key(self, tmp_path: Path, tier: str) -> None:
        write_project(tmp_path, defaults={"target": {"schema_drift": "strict", "on_drift": "warn"}})
        if tier == "thread":
            lower = {"thread": {"target": {"alias": "out.t", "on_drift": "error"}}}
        else:
            lower = {tier: {"defaults": {"target": {"on_drift": "error"}}}}
        target = _thread(load_at(_tree(tmp_path, **lower))).target
        assert target.schema_drift == "strict"
        assert target.on_drift == "error"

    def test_project_execution_default_warns_c490(self, tmp_path: Path) -> None:
        write_project(tmp_path, defaults={"execution": {"log_level": "minimal"}})
        assert len(with_code(load_at(_tree(tmp_path)).diagnostics, "C490")) == 1


class TestProjectAssetParams:
    """A project asset's ``${param.x}`` resolves at the project's scope."""

    WS = {"ws": {"name": "ws", "type": "string", "required": False, "default": "proj_default"}}

    def _project(self, root: Path) -> None:
        write_project(root, params=self.WS, connections={"lake": lake_connection("${param.ws}")})

    def _workspace(self, loaded: Any) -> str:
        connections = _thread(loaded).connections
        assert connections is not None
        return connections["lake"].workspace

    def test_project_default(self, tmp_path: Path) -> None:
        self._project(tmp_path)
        assert self._workspace(load_at(_tree(tmp_path))) == "proj_default"

    def test_runtime_beats_the_project_default(self, tmp_path: Path) -> None:
        self._project(tmp_path)
        assert self._workspace(load_at(_tree(tmp_path), {"ws": "prod"})) == "prod"

    def test_a_lower_declaration_does_not_retarget_it(self, tmp_path: Path) -> None:
        self._project(tmp_path)
        shadow = {"ws": {**self.WS["ws"], "default": "loom_default"}}
        loaded = load_at(_tree(tmp_path, loom={"params": shadow}))
        assert self._workspace(loaded) == "proj_default"

    def test_a_pin_below_does_not_retarget_it(self, tmp_path: Path) -> None:
        self._project(tmp_path)
        loaded = load_at(_tree(tmp_path, weave_entry={"params": {"ws": "pinned"}}))
        assert self._workspace(loaded) == "proj_default"
        (warning,) = with_code(loaded.diagnostics, "P122")
        assert not warning.is_error
        assert "P123" not in {d.code for d in loaded.diagnostics}

    def test_reading_it_counts_as_use_of_a_runtime_param(self, tmp_path: Path) -> None:
        write_project(tmp_path, connections={"lake": lake_connection("${param.ws}")})
        loaded = load_at(_tree(tmp_path), {"ws": "prod"})
        assert self._workspace(loaded) == "prod"
        assert with_code(loaded.diagnostics, "P121") == []


class TestNameReferences:
    """``R140`` and ``R201`` see project definitions."""

    def test_unknown_template_still_fails_at_load(self, tmp_path: Path) -> None:
        write_project(tmp_path, audit_templates={"tpl": {"columns": {"c": "1"}}})
        target = {"alias": "out.t", "audit_template": "nope"}
        error = load_failure(_tree(tmp_path, thread={"target": target}))
        (unknown,) = with_code(error.diagnostics, "R140")
        assert unknown.expected is not None
        assert "tpl" in unknown.expected

    def test_project_connection_resolves(self, tmp_path: Path) -> None:
        write_project(tmp_path, connections={"lake": lake_connection("ws")})
        thread = _thread(load_at(_tree(tmp_path)))
        path = resolve_connection_path("lake", "t", None, dict(thread.connections or {}))
        assert path.startswith("abfss://ws@")

    def test_unknown_connection_is_r201(self, tmp_path: Path) -> None:
        write_project(tmp_path, connections={"lake": lake_connection("ws")})
        thread = _thread(load_at(_tree(tmp_path)))
        with pytest.raises(ConfigLocationError) as info:
            resolve_connection_path("nope", "t", None, dict(thread.connections or {}))
        (undefined,) = info.value.diagnostics
        assert undefined.code == "R201"
        assert undefined.expected == ("lake",)


class TestProjectParamsOnlyWhenRead:
    """The project tier needs a project param's value only when a project block reads it."""

    REGION = {"region": {"name": "region", "type": "string"}}

    def test_unread_required_param_shadowed_below_still_loads(self, tmp_path: Path) -> None:
        write_project(tmp_path, params=self.REGION, connections={"lake": lake_connection("ws")})
        shadow = {"region": {**self.REGION["region"], "required": False, "default": "na"}}
        loaded = load_at(_tree(tmp_path, loom={"params": shadow}))
        assert _thread(loaded).connections is not None

    def test_read_required_param_without_value_is_p101(self, tmp_path: Path) -> None:
        project_file = str(
            write_project(
                tmp_path,
                params=self.REGION,
                connections={"lake": lake_connection("${param.region}")},
            )
            / "weevr.yaml"
        )
        shadow = {"region": {**self.REGION["region"], "required": False, "default": "na"}}
        error = load_failure(_tree(tmp_path, loom={"params": shadow}))
        (missing,) = with_code(error.diagnostics, "P101")
        assert missing.file == project_file
        assert missing.pointer == "/params/region"


class TestResolveProjectConnections:
    """``resolve_project_connections``: the project's connections, resolved on their own."""

    WS = {"ws": {"name": "ws", "type": "string", "required": False, "default": "proj_default"}}

    def _project(self, root: Path, **extra: Any) -> Any:
        write_project(root, **extra)
        return read_project(LocalConfigLocation(root))

    def test_no_connections_is_none(self, tmp_path: Path) -> None:
        project = self._project(tmp_path)
        assert resolve_project_connections(project, str(tmp_path / "weevr.yaml")) is None

    def test_project_default(self, tmp_path: Path) -> None:
        project = self._project(
            tmp_path, params=self.WS, connections={"lake": lake_connection("${param.ws}")}
        )
        connections = resolve_project_connections(project, str(tmp_path / "weevr.yaml"))
        assert connections is not None
        assert connections["lake"].workspace == "proj_default"

    def test_runtime_beats_the_project_default(self, tmp_path: Path) -> None:
        project = self._project(
            tmp_path, params=self.WS, connections={"lake": lake_connection("${param.ws}")}
        )
        connections = resolve_project_connections(
            project, str(tmp_path / "weevr.yaml"), runtime_params={"ws": "prod"}
        )
        assert connections is not None
        assert connections["lake"].workspace == "prod"

    def test_fabric_reference(self, tmp_path: Path) -> None:
        project = self._project(
            tmp_path, connections={"lake": lake_connection("${fabric.workspace_id}")}
        )
        connections = resolve_project_connections(
            project, str(tmp_path / "weevr.yaml"), fabric_context={"workspace_id": "fab-ws"}
        )
        assert connections is not None
        assert connections["lake"].workspace == "fab-ws"

    def test_unresolvable_reference_fails(self, tmp_path: Path) -> None:
        project = self._project(tmp_path, connections={"lake": lake_connection("${param.ws}")})
        with pytest.raises(ConfigError) as info:
            resolve_project_connections(project, str(tmp_path / "weevr.yaml"))
        assert "P110" in {d.code for d in info.value.diagnostics}

    def test_only_named_connections_are_resolved(self, tmp_path: Path) -> None:
        project = self._project(
            tmp_path,
            connections={
                "lake": lake_connection("ws"),
                "broken": lake_connection("${param.missing}"),
            },
        )
        connections = resolve_project_connections(
            project, str(tmp_path / "weevr.yaml"), names={"lake"}
        )
        assert connections is not None
        assert set(connections) == {"lake"}

    def test_a_named_connection_the_project_lacks_is_none(self, tmp_path: Path) -> None:
        project = self._project(tmp_path, connections={"lake": lake_connection("ws")})
        assert (
            resolve_project_connections(project, str(tmp_path / "weevr.yaml"), names={"nope"})
            is None
        )

    def test_other_blocks_are_not_resolved(self, tmp_path: Path) -> None:
        project = self._project(
            tmp_path,
            connections={"lake": lake_connection("ws")},
            defaults={"tags": ["${param.unbound}"]},
        )
        connections = resolve_project_connections(project, str(tmp_path / "weevr.yaml"))
        assert connections is not None
        assert connections["lake"].workspace == "ws"


class TestManifestReadsProjectBlocks:
    """The param manifest counts references in the project's cascading blocks."""

    def test_undeclared_reference_is_listed(self, tmp_path: Path) -> None:
        write_project(
            tmp_path,
            connections={"lake": lake_connection("${param.ws}")},
            audit_templates={"tpl": {"columns": {"c": "'${param.tag}'"}}},
            defaults={"tags": ["${param.team}"]},
        )
        manifest = param_manifest(_tree(tmp_path), project_root=tmp_path)
        assert set(manifest.undeclared_used) == {"ws", "tag", "team"}

    def test_declared_reference_is_not(self, tmp_path: Path) -> None:
        ws = {"ws": {"name": "ws", "type": "string", "required": False, "default": "d"}}
        write_project(tmp_path, params=ws, connections={"lake": lake_connection("${param.ws}")})
        manifest = param_manifest(_tree(tmp_path), project_root=tmp_path)
        assert manifest.undeclared_used == ()


class TestStandaloneThreadOwnAudit:
    """A standalone thread's own audit settings apply whatever its project declares."""

    PROJECTS: dict[str, dict[str, Any] | None] = {
        "no_project": None,
        "blockless_project": {},
        "project_with_blocks": {"defaults": {"tags": ["proj"]}},
    }

    def _load(self, root: Path, project: str, target: dict[str, Any]) -> Thread:
        extra = self.PROJECTS[project]
        if extra is not None:
            write_project(root, **extra)
        templates = {"tpl": {"columns": {"_by": "'me'", "_at": "'now'"}}}
        body = thread_body(target={"alias": "out.t", **target}, audit_templates=templates)
        loaded = load_at(write_named(root, "t.thread", body))
        model = loaded.document.model
        assert isinstance(model, Thread)
        return model

    @pytest.mark.parametrize("project", list(PROJECTS))
    def test_own_template_applies(self, tmp_path: Path, project: str) -> None:
        thread = self._load(tmp_path, project, {"audit_template": "tpl"})
        assert thread.target.audit_columns == {"_by": "'me'", "_at": "'now'"}

    @pytest.mark.parametrize("project", list(PROJECTS))
    def test_own_exclude_applies(self, tmp_path: Path, project: str) -> None:
        target = {"audit_template": "tpl", "audit_columns_exclude": ["_at"]}
        thread = self._load(tmp_path, project, target)
        assert thread.target.audit_columns == {"_by": "'me'"}

    @pytest.mark.parametrize("project", list(PROJECTS))
    def test_own_unknown_template_is_r140(self, tmp_path: Path, project: str) -> None:
        extra = self.PROJECTS[project]
        if extra is not None:
            write_project(tmp_path, **extra)
        target = {"alias": "out.t", "audit_template": "nope"}
        error = load_failure(write_named(tmp_path, "t.thread", thread_body(target=target)))
        assert len(with_code(error.diagnostics, "R140")) == 1
