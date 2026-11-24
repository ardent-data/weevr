"""Loading ``weevr.yaml``: schema, reserved V-012 blocks and the engine floor."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import weevr.config.project as project_module
from documents import write_document
from weevr.config.document_load import load_document
from weevr.config.locations import make_location
from weevr.config.project import check_engine_floor, installed_weevr_version
from weevr.errors import ConfigError
from weevr.model import Project

PROJECT_ID = "8f9d2c4e-1b3a-4c5d-9e6f-7a8b9c0d1e2f"


def _body(**extra: Any) -> dict[str, Any]:
    return {"config_version": "2.0", "id": PROJECT_ID, "name": "sales", **extra}


def _load(tmp_path: Path, body: dict[str, Any]) -> Project:
    path = write_document(tmp_path / "weevr.yaml", body, kind="project")
    kind, model = load_document(make_location(path))
    assert kind == "project"
    assert isinstance(model, Project)
    return model


def _diagnostics(tmp_path: Path, body: dict[str, Any]) -> list[tuple[str, str | None]]:
    with pytest.raises(ConfigError) as exc_info:
        _load(tmp_path, body)
    return [(d.code, d.pointer) for d in exc_info.value.diagnostics]


@pytest.fixture
def installed(monkeypatch: pytest.MonkeyPatch) -> Any:
    def _set(version: str) -> None:
        monkeypatch.setattr(project_module, "installed_weevr_version", lambda: version)

    return _set


class TestLoad:
    """A ``weevr.yaml`` loads into a ``Project``."""

    def test_full_document(self, tmp_path: Path, installed: Any) -> None:
        installed("2.1.0")
        project = _load(
            tmp_path,
            _body(
                description="Sales marts",
                requires_weevr=">=2,<3",
                run_store={"connection": "central_lake", "layout": "shared", "schema": "ops"},
            ),
        )
        assert project.name == "sales"
        assert project.run_store is not None
        assert project.run_store.layout == "shared"

    def test_unsafe_name_is_a_shape_error(self, tmp_path: Path) -> None:
        assert _diagnostics(tmp_path, _body(name="sales-mart")) == [("C105", "/name")]

    def test_invalid_specifier_is_a_shape_error(self, tmp_path: Path) -> None:
        assert _diagnostics(tmp_path, _body(requires_weevr="two")) == [("C103", "/requires_weevr")]

    def test_unknown_key(self, tmp_path: Path) -> None:
        assert _diagnostics(tmp_path, _body(owner="team")) == [("C110", "/owner")]

    def test_unknown_run_store_key(self, tmp_path: Path) -> None:
        body = _body(run_store={"layout": "shared", "path": "x"})
        assert _diagnostics(tmp_path, body) == [("C110", "/run_store/path")]


class TestReservedBlocks:
    """V-012's project-scope blocks are reserved until V-012 lands."""

    @pytest.mark.parametrize("key", ["connections", "params", "defaults", "audit_templates"])
    def test_reserved_block(self, tmp_path: Path, key: str) -> None:
        assert _diagnostics(tmp_path, _body(**{key: {}})) == [("C210", f"/{key}")]

    def test_every_reserved_block_is_reported(self, tmp_path: Path) -> None:
        codes = _diagnostics(tmp_path, _body(params={}, connections={}))
        assert sorted(codes) == [("C210", "/connections"), ("C210", "/params")]

    def test_reserved_and_shape_errors_together(self, tmp_path: Path) -> None:
        codes = _diagnostics(tmp_path, _body(defaults={}, owner="team"))
        assert sorted(codes) == [("C110", "/owner"), ("C210", "/defaults")]


class TestEngineFloor:
    """``requires_weevr`` is checked against the installed weevr at load."""

    def test_satisfied(self, tmp_path: Path, installed: Any) -> None:
        installed("2.3.1")
        assert _load(tmp_path, _body(requires_weevr=">=2,<3")).requires_weevr == ">=2,<3"

    @pytest.mark.parametrize("version", ["3.0.0", "1.21.0"])
    def test_unsatisfied(self, tmp_path: Path, installed: Any, version: str) -> None:
        installed(version)
        path = write_document(
            tmp_path / "weevr.yaml", _body(requires_weevr=">=2,<3"), kind="project"
        )
        with pytest.raises(ConfigError) as exc_info:
            load_document(make_location(path))
        [diag] = exc_info.value.diagnostics
        assert (diag.code, diag.pointer, diag.file) == ("C208", "/requires_weevr", str(path))
        assert version in diag.message
        assert ">=2,<3" in diag.message

    def test_pre_release_install_compares_by_order(self, installed: Any) -> None:
        installed("2.0.0a2")
        project = Project.model_validate({"kind": "project", **_body(requires_weevr=">=2.0.0a1")})
        check_engine_floor(project)

    def test_pre_release_below_floor(self, installed: Any) -> None:
        installed("2.0.0a2")
        project = Project.model_validate({"kind": "project", **_body(requires_weevr=">=2")})
        with pytest.raises(ConfigError) as exc_info:
            check_engine_floor(project)
        assert [d.code for d in exc_info.value.diagnostics] == ["C208"]

    def test_no_floor_is_never_checked(self, installed: Any) -> None:
        installed("0.0.1")
        check_engine_floor(Project.model_validate({"kind": "project", **_body()}))

    def test_installed_version_is_the_core_distribution(self) -> None:
        from importlib.metadata import version

        assert installed_weevr_version() == version("weevr-core")
