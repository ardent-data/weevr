"""Project registries: ``weevr.yaml`` declares lookups and column sets.

The project's ``lookups`` and ``column_sets`` are the outermost layer of every
config's effective registries: a loom, weave or thread declaring the same
name shadows them. The load-time reference checks (``R120``, ``R122``,
``R124``) see them, so a config that names a project lookup or column set
validates with no Spark session. A project lookup's ``${param.x}`` resolves
with the bindings at the project's own scope, as a project connection does.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from documents import TEST_PROJECT_ID, loom_body, thread_body, weave_body, write_named
from resolving import collected_at, load_at, load_failure, with_code
from weevr.config.document import ResolvedDocument
from weevr.config.locations import LocalConfigLocation
from weevr.config.root import read_project


def _write_project(root: Path, **extra: Any) -> None:
    root.mkdir(parents=True, exist_ok=True)
    body = {
        "kind": "project",
        "config_version": "2.0",
        "id": TEST_PROJECT_ID,
        "name": "test",
        **extra,
    }
    (root / "weevr.yaml").write_text(yaml.safe_dump(body, sort_keys=False))


def _lookup(alias: str, **extra: Any) -> dict[str, Any]:
    return {"source": {"type": "delta", "alias": alias}, **extra}


def _column_set(alias: str) -> dict[str, Any]:
    return {"source": {"type": "delta", "alias": alias}}


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


def _root(root: Path, kind: str, **tiers: Any) -> Path:
    """The tree's loom, weave or thread file."""
    root.mkdir(parents=True, exist_ok=True)
    loom = _tree(root, **tiers)
    return {"loom": loom, "weave": root / "w.weave", "thread": root / "t.thread"}[kind]


def _thread_node(loaded: Any) -> ResolvedDocument:
    document = loaded.document
    while document.children:
        document = next(iter(document.children.values()))
    return document


def _lookups(loaded: Any) -> dict[str, Any]:
    return dict(_thread_node(loaded).effective.lookups or {})


def _column_sets(loaded: Any) -> dict[str, Any]:
    return dict(_thread_node(loaded).effective.column_sets or {})


ROOT_KINDS = ["loom", "weave", "thread"]
LOWER_TIERS = ["loom", "weave", "thread"]

USES_LOOKUP = {"sources": {"s": {"lookup": "dim"}}}
RESOLVES_LOOKUP = {
    "steps": [{"resolve": {"name": "dim_id", "lookup": "dim", "match": "k", "pk": "id"}}]
}
RENAMES = {"steps": [{"rename": {"column_set": "cs"}}]}


class TestProjectDocument:
    """``lookups`` and ``column_sets`` are project fields."""

    def test_blocks_load(self, tmp_path: Path) -> None:
        _write_project(
            tmp_path,
            lookups={"dim": _lookup("ref.dim", materialize=True)},
            column_sets={"cs": _column_set("ref.map")},
        )
        project = read_project(LocalConfigLocation(tmp_path))
        assert project is not None
        assert project.lookups is not None
        assert project.lookups["dim"].materialize is True
        assert project.column_sets is not None
        assert project.column_sets["cs"].source is not None


class TestProjectLayerReachesEveryRoot:
    """The project layer is outermost for a loom, weave or standalone thread root."""

    @pytest.mark.parametrize("kind", ROOT_KINDS)
    def test_lookup(self, tmp_path: Path, kind: str) -> None:
        _write_project(tmp_path, lookups={"dim": _lookup("ref.dim")})
        lookups = _lookups(load_at(_root(tmp_path, kind)))
        assert lookups["dim"].source.alias == "ref.dim"

    @pytest.mark.parametrize("kind", ROOT_KINDS)
    def test_column_set(self, tmp_path: Path, kind: str) -> None:
        _write_project(tmp_path, column_sets={"cs": _column_set("ref.map")})
        column_sets = _column_sets(load_at(_root(tmp_path, kind)))
        assert column_sets["cs"].source.alias == "ref.map"

    @pytest.mark.parametrize("kind", ROOT_KINDS)
    def test_root_node_sees_it(self, tmp_path: Path, kind: str) -> None:
        _write_project(tmp_path, lookups={"dim": _lookup("ref.dim")})
        loaded = load_at(_root(tmp_path, kind))
        assert set(loaded.document.effective.lookups or {}) == {"dim"}

    def test_merges_by_name_with_lower_tiers(self, tmp_path: Path) -> None:
        _write_project(tmp_path, lookups={"dim": _lookup("ref.dim")})
        loaded = load_at(_tree(tmp_path, weave={"lookups": {"other": _lookup("ref.other")}}))
        assert set(_lookups(loaded)) == {"dim", "other"}


class TestLowerTiersShadowByName:
    """A loom, weave or thread declaration of the same name wins."""

    @pytest.mark.parametrize("tier", LOWER_TIERS)
    def test_lookup(self, tmp_path: Path, tier: str) -> None:
        _write_project(tmp_path, lookups={"dim": _lookup("ref.project")})
        loaded = load_at(_tree(tmp_path, **{tier: {"lookups": {"dim": _lookup(f"ref.{tier}")}}}))
        assert _lookups(loaded)["dim"].source.alias == f"ref.{tier}"

    @pytest.mark.parametrize("tier", LOWER_TIERS)
    def test_column_set(self, tmp_path: Path, tier: str) -> None:
        _write_project(tmp_path, column_sets={"cs": _column_set("ref.project")})
        own = {"column_sets": {"cs": _column_set(f"ref.{tier}")}}
        loaded = load_at(_tree(tmp_path, **{tier: own}))
        assert _column_sets(loaded)["cs"].source.alias == f"ref.{tier}"


class TestWithoutProjectRegistries:
    """No project lookups or column sets leaves ``effective`` as it was."""

    @pytest.mark.parametrize("kind", ROOT_KINDS)
    def test_no_weevr_yaml(self, tmp_path: Path, kind: str) -> None:
        loaded = load_at(_root(tmp_path, kind))
        assert loaded.document.effective.lookups is None
        assert loaded.document.effective.column_sets is None

    @pytest.mark.parametrize("kind", ROOT_KINDS)
    def test_project_without_the_blocks(self, tmp_path: Path, kind: str) -> None:
        own = {"loom": {"lookups": {"x": _lookup("ref.x")}}}
        bare = _effective_by_node(load_at(_root(tmp_path / "bare", kind, **own)).document)
        lake = {"type": "onelake", "workspace": "w", "lakehouse": "l"}
        _write_project(tmp_path / "proj", connections={"lake": lake})
        with_project = load_at(_root(tmp_path / "proj", kind, **own)).document
        assert _effective_by_node(with_project) == bare


def _effective_by_node(document: ResolvedDocument) -> list[Any]:
    """Each node's effective registries, depth first."""
    found = [document.effective]
    for child in document.children.values():
        found.extend(_effective_by_node(child))
    return found


class TestReferenceChecksSeeTheProject:
    """``R120``, ``R122`` and ``R124`` accept a project definition at load."""

    def test_resolve_step_names_a_project_lookup(self, tmp_path: Path) -> None:
        _write_project(tmp_path, lookups={"dim": _lookup("ref.dim")})
        loaded = load_at(_tree(tmp_path, thread=RESOLVES_LOOKUP))
        assert with_code(loaded.diagnostics, "R120") == []

    def test_resolve_step_naming_nothing_is_still_r120(self, tmp_path: Path) -> None:
        _write_project(tmp_path, lookups={"other": _lookup("ref.other")})
        error = load_failure(_tree(tmp_path, thread=RESOLVES_LOOKUP))
        assert with_code(error.diagnostics, "R120")

    def test_source_names_a_project_lookup(self, tmp_path: Path) -> None:
        _write_project(tmp_path, lookups={"dim": _lookup("ref.dim")})
        loaded = load_at(_tree(tmp_path, thread=USES_LOOKUP))
        assert with_code(loaded.diagnostics, "R122") == []

    def test_source_naming_nothing_is_still_r122(self, tmp_path: Path) -> None:
        _write_project(tmp_path, lookups={"other": _lookup("ref.other")})
        error = load_failure(_tree(tmp_path, thread=USES_LOOKUP))
        assert with_code(error.diagnostics, "R122")

    def test_rename_names_a_project_column_set(self, tmp_path: Path) -> None:
        _write_project(tmp_path, column_sets={"cs": _column_set("ref.map")})
        loaded = load_at(_tree(tmp_path, thread=RENAMES))
        assert with_code(loaded.diagnostics, "R124") == []

    def test_rename_naming_nothing_is_still_r124(self, tmp_path: Path) -> None:
        _write_project(tmp_path, column_sets={"other": _column_set("ref.map")})
        error = load_failure(_tree(tmp_path, thread=RENAMES))
        (undefined,) = with_code(error.diagnostics, "R124")
        assert undefined.expected is not None
        assert "other" in undefined.expected

    @pytest.mark.parametrize("kind", ["weave", "thread"])
    def test_other_roots_see_the_project(self, tmp_path: Path, kind: str) -> None:
        _write_project(
            tmp_path,
            lookups={"dim": _lookup("ref.dim")},
            column_sets={"cs": _column_set("ref.map")},
        )
        thread = {**USES_LOOKUP, **RENAMES}
        loaded = load_at(_root(tmp_path, kind, thread=thread))
        codes = {d.code for d in loaded.diagnostics}
        assert not codes & {"R122", "R123", "R124", "R125"}

    def test_standalone_thread_without_the_project_still_warns(self, tmp_path: Path) -> None:
        _write_project(tmp_path)
        loaded = load_at(_root(tmp_path, "thread", thread={**USES_LOOKUP, **RENAMES}))
        codes = {d.code for d in loaded.diagnostics}
        assert {"R123", "R125"} <= codes

    def test_collecting_load_sees_the_project(self, tmp_path: Path) -> None:
        _write_project(tmp_path, lookups={"dim": _lookup("ref.dim")})
        document, errors, _ = collected_at(_tree(tmp_path, thread=USES_LOOKUP))
        assert document is not None
        assert errors == []


class TestProjectRegistryParams:
    """A project lookup's or column set's ``${param.x}`` resolves at the project's scope."""

    WS = {"ws": {"name": "ws", "type": "string", "required": False, "default": "proj_default"}}

    def _project(self, root: Path) -> None:
        _write_project(
            root,
            params=self.WS,
            lookups={"dim": _lookup("${param.ws}.dim")},
            column_sets={"cs": _column_set("${param.ws}.map")},
        )

    def _aliases(self, loaded: Any) -> tuple[str, str]:
        return _lookups(loaded)["dim"].source.alias, _column_sets(loaded)["cs"].source.alias

    def test_project_default(self, tmp_path: Path) -> None:
        self._project(tmp_path)
        loaded = load_at(_tree(tmp_path))
        assert self._aliases(loaded) == ("proj_default.dim", "proj_default.map")

    def test_runtime_beats_the_project_default(self, tmp_path: Path) -> None:
        self._project(tmp_path)
        loaded = load_at(_tree(tmp_path), {"ws": "prod"})
        assert self._aliases(loaded) == ("prod.dim", "prod.map")

    def test_a_lower_declaration_does_not_retarget_it(self, tmp_path: Path) -> None:
        self._project(tmp_path)
        shadow = {"ws": {**self.WS["ws"], "default": "loom_default"}}
        loaded = load_at(_tree(tmp_path, loom={"params": shadow}))
        assert self._aliases(loaded) == ("proj_default.dim", "proj_default.map")

    def test_a_pin_below_does_not_retarget_it(self, tmp_path: Path) -> None:
        self._project(tmp_path)
        loaded = load_at(_tree(tmp_path, weave_entry={"params": {"ws": "pinned"}}))
        assert self._aliases(loaded) == ("proj_default.dim", "proj_default.map")
        assert with_code(loaded.diagnostics, "P122")

    def test_reading_it_counts_as_use_of_a_runtime_param(self, tmp_path: Path) -> None:
        _write_project(tmp_path, lookups={"dim": _lookup("${param.ws}.dim")})
        loaded = load_at(_tree(tmp_path), {"ws": "prod"})
        assert _lookups(loaded)["dim"].source.alias == "prod.dim"
        assert with_code(loaded.diagnostics, "P121") == []

    def test_one_definition_object_across_scopes(self, tmp_path: Path) -> None:
        self._project(tmp_path)
        loaded = load_at(_tree(tmp_path))
        loom = loaded.document
        weave = next(iter(loom.children.values()))
        thread = next(iter(weave.children.values()))
        assert loom.effective.lookups is not None
        assert weave.effective.lookups is not None
        assert thread.effective.lookups is not None
        assert loom.effective.lookups["dim"] is thread.effective.lookups["dim"]
        assert weave.effective.lookups["dim"] is thread.effective.lookups["dim"]


class TestProjectRegistryPaths:
    """A project lookup's or column set's relative file path joins the project root."""

    def test_relative_paths_join_the_root(self, tmp_path: Path) -> None:
        _write_project(
            tmp_path,
            lookups={"dim": {"source": {"type": "csv", "path": "ref/dim.csv"}}},
            column_sets={"cs": {"source": {"type": "yaml", "path": "ref/map.yaml"}}},
        )
        loaded = load_at(_tree(tmp_path))
        assert _lookups(loaded)["dim"].source.path == str(tmp_path / "ref/dim.csv")
        assert _column_sets(loaded)["cs"].source.path == str(tmp_path / "ref/map.yaml")


class TestProjectConnectionsOnTheDocument:
    """The root node carries the project's resolved connections for materializing scopes."""

    def test_resolved_with_the_projects_bindings(self, tmp_path: Path) -> None:
        ws = {"ws": {"name": "ws", "type": "string", "required": False, "default": "dflt"}}
        lake = {"type": "onelake", "workspace": "${param.ws}", "lakehouse": "lh"}
        _write_project(tmp_path, params=ws, connections={"lake": lake})
        document = load_at(_tree(tmp_path), {"ws": "prod"}).document
        assert document.project_connections is not None
        assert document.project_connections["lake"].workspace == "prod"

    def test_none_without_project_connections(self, tmp_path: Path) -> None:
        _write_project(tmp_path, lookups={"dim": _lookup("ref.dim")})
        assert load_at(_tree(tmp_path)).document.project_connections is None
