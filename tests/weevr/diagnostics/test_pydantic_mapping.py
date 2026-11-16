"""Pydantic validation errors become coded diagnostics."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import BaseModel, ValidationError, field_validator
from pydantic_core import PydanticCustomError

from weevr.config import load_config
from weevr.diagnostics import Diagnostic, track_usage
from weevr.diagnostics.positions import json_pointer
from weevr.diagnostics.pydantic import from_validation_error
from weevr.errors import ModelValidationError
from weevr.model import Thread, Weave

BASE: dict[str, Any] = {
    "name": "t",
    "config_version": "1.0",
    "sources": {"a": {"type": "delta", "alias": "x.y"}},
    "target": {"alias": "x.z"},
}


def _thread(**changes: Any) -> dict[str, Any]:
    data = {**BASE, **changes}
    return {key: value for key, value in data.items() if value is not None}


def _diagnostics(model: type[Thread | Weave], data: dict[str, Any]) -> list[Diagnostic]:
    with pytest.raises(ValidationError) as exc_info:
        model.model_validate(data)
    return from_validation_error(exc_info.value, data, file="t.thread")


def _one(model: type[Thread | Weave], data: dict[str, Any]) -> Diagnostic:
    diagnostics = _diagnostics(model, data)
    assert len(diagnostics) == 1, diagnostics
    return diagnostics[0]


class TestCodes:
    """Each pydantic error type weevr's models produce maps to its code."""

    def test_missing_field(self) -> None:
        diag = _one(Thread, _thread(sources=None))
        assert (diag.code, diag.resolved_path, diag.file) == ("C101", "/sources", "t.thread")

    @pytest.mark.parametrize(
        ("changes", "path"),
        [
            ({"sources": 5}, "/sources"),
            ({"cache": "maybe"}, "/cache"),
            ({"steps": [{"select": {"columns": "a"}}]}, "/steps/0/select/columns"),
        ],
    )
    def test_wrong_type(self, changes: dict[str, Any], path: str) -> None:
        diag = _one(Thread, _thread(**changes))
        assert (diag.code, diag.resolved_path) == ("C102", path)

    def test_model_type_names_the_expected_shape(self) -> None:
        diag = _one(Weave, {"name": "w", "config_version": "1.0", "threads": [5]})
        assert (diag.code, diag.resolved_path) == ("C102", "/threads/0")

    def test_literal_lists_allowed_values(self) -> None:
        diag = _one(Thread, _thread(write={"mode": "bogus"}))
        assert diag.code == "C103"
        assert diag.resolved_path == "/write/mode"
        assert diag.expected == ("overwrite", "append", "merge")

    def test_unknown_step_type_lists_step_types(self) -> None:
        diag = _one(Thread, _thread(steps=[{"filter": {"expr": "a"}}, {"sample": {}}]))
        assert diag.code == "C104"
        assert diag.resolved_path == "/steps/1"
        assert diag.step_index == 1
        assert diag.expected is not None
        assert "filter" in diag.expected
        assert "resolve" in diag.expected

    def test_unknown_hook_step_type(self) -> None:
        data = {
            "name": "w",
            "config_version": "1.0",
            "threads": ["a"],
            "pre_steps": [{"type": "x"}],
        }
        diag = _one(Weave, data)
        assert diag.code == "C104"
        assert diag.expected == ("quality_gate", "sql_statement", "log_message")

    def test_constraint(self) -> None:
        diag = _one(Thread, _thread(execution={"max_parallel_threads": 0}))
        assert (diag.code, diag.resolved_path) == ("C105", "/execution/max_parallel_threads")

    def test_uncoded_validator_error_is_generic(self) -> None:
        class Plain(BaseModel):
            value: int

            @field_validator("value")
            @classmethod
            def _positive(cls, v: int) -> int:
                if v <= 0:
                    raise ValueError("value must be positive")
                return v

        data = {"value": 0}
        with pytest.raises(ValidationError) as exc_info:
            Plain.model_validate(data)
        (diag,) = from_validation_error(exc_info.value, data)
        assert (diag.code, diag.resolved_path) == ("C400", "/value")
        assert not diag.message.startswith("Value error")

    def test_coded_validator_error_keeps_its_code(self) -> None:
        error = ValidationError.from_exception_data(
            "Thread",
            [
                {
                    "type": PydanticCustomError("C201", "custom {x}", {"x": 1}),
                    "loc": ("config_version",),
                    "input": None,
                }
            ],
        )
        [diag] = from_validation_error(error, {"config_version": None})
        assert (diag.code, diag.message) == ("C201", "custom 1")

    def test_unregistered_error_type_is_schema_violation(self) -> None:
        error = ValidationError.from_exception_data(
            "Thread",
            [{"type": PydanticCustomError("zz_new", "odd"), "loc": ("a",), "input": 1}],
        )
        [diag] = from_validation_error(error, {"a": 1})
        assert diag.code == "C100"


class TestUnionNoise:
    """One diagnostic per problem, not one per union member."""

    def test_step_errors_come_from_the_selected_variant_only(self) -> None:
        diag = _one(Thread, _thread(steps=[{"filter": {"expr": 5}}]))
        assert (diag.code, diag.resolved_path) == ("C102", "/steps/0/filter/expr")
        assert (diag.step_index, diag.step_type) == (0, "filter")

    def test_step_missing_field(self) -> None:
        diag = _one(Thread, _thread(steps=[{"derive": {}}]))
        assert (diag.code, diag.resolved_path) == ("C101", "/steps/0/derive/columns")

    def test_hook_step_path_drops_the_tag(self) -> None:
        data = {
            "name": "w",
            "config_version": "1.0",
            "threads": ["a"],
            "pre_steps": [{"type": "log_message"}],
        }
        diag = _one(Weave, data)
        assert (diag.code, diag.resolved_path) == ("C101", "/pre_steps/0/message")

    def test_plain_union_collapses_to_one_type_error(self) -> None:
        diag = _one(Thread, _thread(target={"alias": "x.z", "warp": 5}))
        assert (diag.code, diag.resolved_path) == ("C102", "/target/warp")
        assert diag.expected is not None
        assert "False" in diag.expected


def test_every_error_in_a_document_is_reported() -> None:
    diagnostics = _diagnostics(Thread, _thread(sources=5, write={"mode": "x"}, cache="maybe"))
    assert sorted(d.resolved_path or "" for d in diagnostics) == [
        "/cache",
        "/sources",
        "/write/mode",
    ]


def test_json_pointer_escapes() -> None:
    assert json_pointer(("a/b", "c~d", 0)) == "/a~1b/c~0d/0"
    assert json_pointer(()) == ""


def test_converter_is_credited_in_usage() -> None:
    with track_usage() as usage:
        _one(Thread, _thread(sources=None))
    assert ("C101", "weevr.diagnostics.pydantic") in usage


class TestHydration:
    """A config that fails model validation raises with coded diagnostics."""

    def _write(self, tmp_path: Path, name: str, data: dict[str, Any]) -> Path:
        path = tmp_path / name
        path.write_text(yaml.safe_dump(data))
        return path

    def test_thread(self, tmp_path: Path) -> None:
        path = self._write(tmp_path, "t.thread", _thread(write={"mode": "x"}, cache="maybe"))
        with pytest.raises(ModelValidationError) as exc_info:
            load_config(path)
        exc = exc_info.value
        assert {(d.code, d.resolved_path) for d in exc.diagnostics} == {
            ("C103", "/write/mode"),
            ("C102", "/cache"),
        }
        assert all(d.file == str(path) for d in exc.diagnostics)
        assert isinstance(exc.cause, ValidationError)

    def test_weave_child_thread_names_its_own_file(self, tmp_path: Path) -> None:
        thread = self._write(tmp_path, "child.thread", _thread(name=None, cache="maybe"))
        weave = self._write(
            tmp_path,
            "w.weave",
            {"config_version": "1.0", "name": "w", "threads": [{"ref": "child.thread"}]},
        )
        with pytest.raises(ModelValidationError) as exc_info:
            load_config(weave)
        [diag] = exc_info.value.diagnostics
        assert (diag.code, diag.resolved_path, diag.file) == ("C102", "/cache", str(thread))


class TestPreResolutionSchema:
    """The pre-resolution schema check reports coded diagnostics too."""

    def test_validate_schema(self) -> None:
        from weevr.config.validation import validate_schema
        from weevr.errors import ConfigSchemaError

        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_schema({"config_version": "1.0", "sources": 5}, "thread")
        assert {(d.code, d.resolved_path) for d in exc_info.value.diagnostics} == {
            ("C102", "/sources"),
            ("C101", "/target"),
        }

    def test_unknown_config_type(self) -> None:
        from weevr.config.validation import validate_schema
        from weevr.errors import ConfigSchemaError

        with pytest.raises(ConfigSchemaError) as exc_info:
            validate_schema({}, "pipeline")
        assert [d.code for d in exc_info.value.diagnostics] == ["C204"]

    def test_inline_thread_points_into_the_weave(self, tmp_path: Path) -> None:
        from weevr.errors import ConfigSchemaError

        weave = tmp_path / "w.weave"
        weave.write_text(
            yaml.safe_dump(
                {
                    "config_version": "1.0",
                    "name": "w",
                    "threads": [{"name": "ok"}, {"name": "bad", "sources": ["a"]}],
                }
            )
        )
        with pytest.raises(ConfigSchemaError) as exc_info:
            load_config(weave)
        [diag] = exc_info.value.diagnostics
        assert (diag.code, diag.resolved_path, diag.file) == ("C102", "/sources", str(weave))
        assert diag.pointer == "/threads/1/sources"
        assert diag.line is not None
