"""Project params: ``weevr.yaml`` declares params and ``strict_params`` for the whole project.

The project is the outermost tier of the spec cascade: a param it declares
resolves in every config of every loom without re-declaration, runtime params
bind over its defaults, and a lower-tier declaration of the same name shadows
it. Its ``strict_params`` applies project-wide unless a document, or the
load's own ``strict_params``, says otherwise.
"""

from __future__ import annotations

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
from resolving import load_at, load_failure, with_code
from weevr.config import load_resolved, param_manifest
from weevr.config.locations import LocalConfigLocation
from weevr.config.param_provenance import ParamDeclaration, ParamProvenance
from weevr.config.project import RESERVED_PROJECT_BLOCKS
from weevr.config.root import read_project


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


def _region(**fields: Any) -> dict[str, Any]:
    return {"region": string_spec("region", **fields)}


def _declare_once_tree(root: Path, thread_params: dict[str, Any] | None = None) -> Path:
    """A loom -> ref weave -> ref thread tree whose thread reads ``${param.region}``."""
    extra = {} if thread_params is None else {"params": thread_params}
    write_named(root, "t.thread", thread_body(alias="out.${param.region}", **extra))
    write_named(root, "w.weave", weave_body([{"ref": "t.thread"}]))
    return write_named(root, "l.loom", loom_body([{"ref": "w.weave"}]))


def _thread_node(loaded: Any) -> Any:
    return loaded.document.children["w"].children["t"]


def _thread_alias(loaded: Any) -> str:
    return _thread_node(loaded).model.target.alias


class TestProjectDocument:
    """``params`` and ``strict_params`` are project fields, not reserved blocks."""

    def test_params_and_strict_params_load(self, tmp_path: Path) -> None:
        _write_project(tmp_path, params=_region(default="emea"), strict_params=True)
        project = read_project(LocalConfigLocation(tmp_path))
        assert project is not None
        assert project.strict_params is True
        assert project.params is not None
        assert project.params["region"].default == "emea"

    def test_params_is_not_reserved(self) -> None:
        assert "params" not in RESERVED_PROJECT_BLOCKS


class TestDeclareOnce:
    """A param declared in ``weevr.yaml`` resolves three tiers down with no re-declaration."""

    def test_project_default_reaches_the_thread(self, tmp_path: Path) -> None:
        project_file = _write_project(tmp_path, params=_region(required=False, default="emea"))
        loaded = load_at(_declare_once_tree(tmp_path))
        assert _thread_alias(loaded) == "out.emea"
        assert with_code(loaded.diagnostics, "P120") == []
        provenance = _thread_node(loaded).provenance.params["region"]
        assert provenance == ParamProvenance(
            "default", ParamDeclaration("project", project_file, "/params/region")
        )

    def test_runtime_beats_the_project_default(self, tmp_path: Path) -> None:
        project_file = _write_project(tmp_path, params=_region(required=False, default="emea"))
        loaded = load_at(_declare_once_tree(tmp_path), {"region": "apac"})
        assert _thread_alias(loaded) == "out.apac"
        declaration = ParamDeclaration("project", project_file, "/params/region")
        assert loaded.document.provenance.params["region"] == ParamProvenance(
            "runtime", declaration
        )
        assert _thread_node(loaded).provenance.params["region"] == ParamProvenance(
            "inherited", declaration
        )

    def test_project_found_by_discovery(self, tmp_path: Path) -> None:
        _write_project(tmp_path, params=_region(required=False, default="emea"))
        (tmp_path / "threads").mkdir()
        thread = write_named(
            tmp_path / "threads", "t.thread", thread_body(alias="out.${param.region}")
        )
        assert load_resolved(thread).model.target.alias == "out.emea"

    def test_project_declared_runtime_param_is_used(self, tmp_path: Path) -> None:
        _write_project(tmp_path, params={**_region(), **{"unused": string_spec("unused")}})
        loaded = load_at(_declare_once_tree(tmp_path), {"region": "apac", "unused": "x"})
        assert with_code(loaded.diagnostics, "P121") == []


class TestShadowing:
    """A lower-tier declaration of a project param shadows the project spec."""

    def test_thread_declaration_shadows_the_project_spec(self, tmp_path: Path) -> None:
        _write_project(tmp_path, params=_region(required=False, default="emea"))
        loom = _declare_once_tree(tmp_path, thread_params=_region(required=False, default="na"))
        loaded = load_at(loom)
        assert _thread_alias(loaded) == "out.na"
        assert loaded.document.provenance.params["region"].spec == ParamDeclaration(
            "project", str(tmp_path / "weevr.yaml"), "/params/region"
        )
        assert _thread_node(loaded).provenance.params["region"] == ParamProvenance(
            "default",
            ParamDeclaration("thread", str(tmp_path / "t.thread"), "/params/region"),
        )

    def test_runtime_beats_both_declarations(self, tmp_path: Path) -> None:
        _write_project(tmp_path, params=_region(required=False, default="emea"))
        loom = _declare_once_tree(tmp_path, thread_params=_region(required=False, default="na"))
        assert _thread_alias(load_at(loom, {"region": "apac"})) == "out.apac"


class TestProjectErrors:
    """Errors at a project declaration name ``weevr.yaml``."""

    def test_required_project_param_missing(self, tmp_path: Path) -> None:
        project_file = _write_project(tmp_path, params=_region())
        error = load_failure(_declare_once_tree(tmp_path))
        (missing,) = with_code(error.diagnostics, "P101")
        assert missing.file == project_file
        assert missing.pointer == "/params/region"

    def test_runtime_value_its_project_type_rejects(self, tmp_path: Path) -> None:
        project_file = _write_project(tmp_path, params={"n": {"name": "n", "type": "int"}})
        error = load_failure(_declare_once_tree(tmp_path), {"n": "five", "region": "x"})
        (mismatch,) = with_code(error.diagnostics, "P102")
        assert mismatch.file == project_file
        assert mismatch.pointer == "/params/n"


class TestNoProject:
    """With no ``weevr.yaml`` the project scope is empty."""

    def test_undeclared_reference_still_warns(self, tmp_path: Path) -> None:
        loaded = load_at(_declare_once_tree(tmp_path), {"region": "apac"})
        assert _thread_alias(loaded) == "out.apac"
        assert loaded.project is None
        assert len(with_code(loaded.diagnostics, "P120")) == 1


class TestStrictParams:
    """Precedence: the load's override > the document's own > the project's > lenient."""

    def _undeclared(self, tmp_path: Path, **thread_extra: Any) -> Path:
        body = thread_body(alias="out.${param.x}", **thread_extra)
        return write_named(tmp_path, "t.thread", body)

    def test_project_strict_makes_warnings_errors(self, tmp_path: Path) -> None:
        _write_project(tmp_path, strict_params=True)
        error = load_failure(self._undeclared(tmp_path), {"x": "a"})
        assert [d.is_error for d in with_code(error.diagnostics, "P120")] == [True]

    def test_project_strict_reaches_children(self, tmp_path: Path) -> None:
        _write_project(tmp_path, strict_params=True)
        self._undeclared(tmp_path)
        weave = write_named(tmp_path, "w.weave", weave_body([{"ref": "t.thread"}]))
        error = load_failure(weave, {"x": "a"})
        assert [d.is_error for d in with_code(error.diagnostics, "P120")] == [True]

    def test_project_strict_makes_unused_runtime_an_error(self, tmp_path: Path) -> None:
        _write_project(tmp_path, strict_params=True)
        path = write_named(tmp_path, "t.thread", thread_body())
        error = load_failure(path, {"stray": "a"})
        assert [d.is_error for d in with_code(error.diagnostics, "P121")] == [True]

    def test_document_beats_project(self, tmp_path: Path) -> None:
        _write_project(tmp_path, strict_params=True)
        loaded = load_at(self._undeclared(tmp_path, strict_params=False), {"x": "a"})
        assert [d.is_error for d in with_code(loaded.diagnostics, "P120")] == [False]

    def test_load_override_beats_project(self, tmp_path: Path) -> None:
        _write_project(tmp_path, strict_params=True)
        loaded = load_at(self._undeclared(tmp_path), {"x": "a"}, strict_params=False)
        assert [d.is_error for d in with_code(loaded.diagnostics, "P120")] == [False]

    def test_project_lenient_lets_a_document_be_strict(self, tmp_path: Path) -> None:
        _write_project(tmp_path, strict_params=False)
        error = load_failure(self._undeclared(tmp_path, strict_params=True), {"x": "a"})
        assert [d.is_error for d in with_code(error.diagnostics, "P120")] == [True]

    @pytest.mark.parametrize("strict", [None, False])
    def test_unset_or_false_project_is_lenient(self, tmp_path: Path, strict: bool | None) -> None:
        extra = {} if strict is None else {"strict_params": strict}
        _write_project(tmp_path, **extra)
        loaded = load_at(self._undeclared(tmp_path), {"x": "a"})
        assert [d.is_error for d in with_code(loaded.diagnostics, "P120")] == [False]


class TestManifest:
    """The param manifest lists project declarations with their tier."""

    def test_project_declarations_are_listed(self, tmp_path: Path) -> None:
        project_file = _write_project(
            tmp_path,
            params={**_region(), "env": string_spec("env", required=False, default="dev")},
        )
        manifest = param_manifest(_declare_once_tree(tmp_path), project_root=tmp_path)
        project_params = [p for p in manifest.declared if p.tier == "project"]
        assert [(p.name, p.file, p.pointer) for p in project_params] == [
            ("region", project_file, "/params/region"),
            ("env", project_file, "/params/env"),
        ]
        assert manifest.invoker_required == ("region",)
        assert manifest.undeclared_used == ()

    def test_document_declarations_carry_their_tier(self, tmp_path: Path) -> None:
        loom = _declare_once_tree(tmp_path, thread_params=_region())
        manifest = param_manifest(loom, project_root=tmp_path)
        assert [(p.name, p.tier) for p in manifest.declared] == [("region", "thread")]

    def test_thread_default_satisfies_a_required_project_param_below_it(
        self, tmp_path: Path
    ) -> None:
        _write_project(tmp_path, params=_region())
        loom = _declare_once_tree(tmp_path, thread_params=_region(required=False, default="na"))
        manifest = param_manifest(loom, project_root=tmp_path)
        # The project spec is still in force at the loom and weave, so the invoker must supply it.
        assert manifest.invoker_required == ("region",)
