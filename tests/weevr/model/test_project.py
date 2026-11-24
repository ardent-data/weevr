"""The project document model: identity, engine floor and run-store settings."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from weevr.model import Project, RunStore

PROJECT_ID = "8f9d2c4e-1b3a-4c5d-9e6f-7a8b9c0d1e2f"


def _project(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "kind": "project",
        "config_version": "2.0",
        "id": PROJECT_ID,
        "name": "sales",
    }
    body.update(overrides)
    return body


def _error_types(exc: ValidationError) -> list[str]:
    return [error["type"] for error in exc.errors()]


class TestIdentity:
    """``id`` and ``name`` identify the project."""

    def test_minimal_project(self) -> None:
        project = Project.model_validate(_project())
        assert project.kind == "project"
        assert project.id == UUID(PROJECT_ID)
        assert project.name == "sales"
        assert project.description is None
        assert project.requires_weevr is None
        assert project.run_store is None

    def test_description(self) -> None:
        project = Project.model_validate(_project(description="Sales marts"))
        assert project.description == "Sales marts"

    def test_any_uuid_version_is_accepted(self) -> None:
        project = Project.model_validate(_project(id="00000000-0000-1000-8000-000000000000"))
        assert project.id.version == 1

    @pytest.mark.parametrize("value", ["not-a-uuid", "", "8f9d2c4e"])
    def test_id_must_be_a_uuid(self, value: str) -> None:
        with pytest.raises(ValidationError):
            Project.model_validate(_project(id=value))

    def test_id_is_required(self) -> None:
        body = _project()
        del body["id"]
        with pytest.raises(ValidationError):
            Project.model_validate(body)

    @pytest.mark.parametrize("value", ["sales", "Sales_2", "s", "a" * 100])
    def test_schema_safe_names(self, value: str) -> None:
        assert Project.model_validate(_project(name=value)).name == value

    @pytest.mark.parametrize(
        "value", ["2sales", "_sales", "sales-mart", "sales mart", "", "a" * 101]
    )
    def test_name_must_be_a_schema_safe_identifier(self, value: str) -> None:
        with pytest.raises(ValidationError):
            Project.model_validate(_project(name=value))

    def test_kind_must_be_project(self) -> None:
        with pytest.raises(ValidationError):
            Project.model_validate(_project(kind="loom"))

    def test_unknown_key_is_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            Project.model_validate(_project(owner="data-team"))
        assert _error_types(exc_info.value) == ["extra_forbidden"]

    def test_frozen(self) -> None:
        project = Project.model_validate(_project())
        with pytest.raises(ValidationError):
            project.name = "other"  # type: ignore[misc]


class TestRequiresWeevr:
    """``requires_weevr`` is a PEP 440 version specifier."""

    @pytest.mark.parametrize("value", [">=2,<3", "~=2.0", "==2.1.*", ">=2.0.0a1"])
    def test_valid_specifiers(self, value: str) -> None:
        assert Project.model_validate(_project(requires_weevr=value)).requires_weevr == value

    @pytest.mark.parametrize("value", ["two", ">=", "2.0", "=>2"])
    def test_invalid_specifier_is_a_c103(self, value: str) -> None:
        with pytest.raises(ValidationError) as exc_info:
            Project.model_validate(_project(requires_weevr=value))
        assert _error_types(exc_info.value) == ["C103"]


class TestRunStore:
    """``run_store`` names where run output goes and in which layout."""

    def test_defaults(self) -> None:
        store = RunStore.model_validate({})
        assert store.connection is None
        assert store.layout == "per_project"
        assert store.schema_ is None

    def test_shared_layout(self) -> None:
        project = Project.model_validate(
            _project(
                run_store={"connection": "central_lake", "layout": "shared", "schema": "weevr_ops"}
            )
        )
        assert project.run_store == RunStore(
            connection="central_lake", layout="shared", schema="weevr_ops"
        )

    def test_per_project_accepts_every_field(self) -> None:
        store = RunStore.model_validate(
            {"connection": "lake", "layout": "per_project", "schema": "weevr_sales_ops"}
        )
        assert store.schema_ == "weevr_sales_ops"

    def test_unknown_layout(self) -> None:
        with pytest.raises(ValidationError):
            RunStore.model_validate({"layout": "central"})

    @pytest.mark.parametrize("value", ["weevr-ops", "1ops", ""])
    def test_schema_must_be_a_schema_safe_identifier(self, value: str) -> None:
        with pytest.raises(ValidationError):
            RunStore.model_validate({"schema": value})

    def test_empty_connection_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RunStore.model_validate({"connection": ""})

    def test_unknown_key_is_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            RunStore.model_validate({"path": "Files/runs"})
        assert _error_types(exc_info.value) == ["extra_forbidden"]

    def test_dumps_schema_under_its_authored_key(self) -> None:
        store = RunStore(layout="shared", schema="weevr")
        assert store.model_dump(by_alias=True)["schema"] == "weevr"
