"""Declared params enforce their type when their scope binds.

Every bound value of a declared param (runtime, pin or default) is checked and
coerced against the nearest declaration's ``type``. The coercion table is
narrow: ``bool`` from ``"true"``/``"false"`` (any case), ``int`` from a base-10
string, ``float`` from an int or a numeric string. ``date``/``timestamp``
validate an ISO string and bind it as written; a date or datetime object binds
as its ISO string. Anything else fails ``P102`` (``P103`` for a malformed
date/timestamp string) at the winning declaration.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest

from documents import write_document
from weevr.config import load_config, load_resolved
from weevr.config.params import ParamScope
from weevr.diagnostics import Diagnostic
from weevr.errors import ConfigError, ConfigSchemaError

THREAD: dict[str, Any] = {
    "config_version": "2.0",
    "sources": {"s": {"type": "delta", "alias": "raw.t"}},
    "target": {"alias": "out.t"},
}

ACCEPT: list[tuple[str, Any, Any]] = [
    ("string", "abc", "abc"),
    ("string", "", ""),
    ("int", 7, 7),
    ("int", "42", 42),
    ("int", "-3", -3),
    ("int", "+5", 5),
    ("float", 1.5, 1.5),
    ("float", 2, 2.0),
    ("float", "2.5", 2.5),
    ("float", "-1", -1.0),
    ("float", "1e3", 1000.0),
    ("bool", True, True),
    ("bool", False, False),
    ("bool", "true", True),
    ("bool", "FALSE", False),
    ("bool", "True", True),
    ("date", "2024-12-31", "2024-12-31"),
    ("date", "2024-1-5", "2024-1-5"),
    ("date", date(2024, 1, 2), "2024-01-02"),
    ("date", datetime(2024, 1, 2, 3, 4), "2024-01-02"),
    ("timestamp", "2024-12-31T10:20:30", "2024-12-31T10:20:30"),
    ("timestamp", "2024-01-01T10:00:00Z", "2024-01-01T10:00:00Z"),
    ("timestamp", "2024-12-31", "2024-12-31"),
    ("timestamp", "20240105T1030", "20240105T1030"),
    ("timestamp", "2024-W01-1", "2024-W01-1"),
    ("timestamp", "2024-01-05 10:30", "2024-01-05 10:30"),
    ("timestamp", datetime(2024, 1, 2, 3, 4), "2024-01-02T03:04:00"),
    ("list[string]", ["a", "b"], ["a", "b"]),
    ("list[string]", [], []),
]

REJECT: list[Any] = [
    ("string", 1, "P102"),
    ("string", True, "P102"),
    ("string", ["a"], "P102"),
    ("int", True, "P102"),
    ("int", 1.0, "P102"),
    ("int", "1.0", "P102"),
    ("int", "abc", "P102"),
    ("int", "1_000", "P102"),
    ("int", "", "P102"),
    pytest.param("int", "9" * 5000, "P102", id="int-5000-digits-P102"),
    ("float", True, "P102"),
    pytest.param("float", 10**400, "P102", id="float-huge-int-P102"),
    ("float", "x", "P102"),
    ("float", "nan", "P102"),
    ("float", "inf", "P102"),
    ("float", "1e999", "P102"),
    ("float", float("nan"), "P102"),
    ("float", [1.0], "P102"),
    ("bool", "yes", "P102"),
    ("bool", "1", "P102"),
    ("bool", 1, "P102"),
    ("bool", 0, "P102"),
    ("date", 5, "P102"),
    ("date", "31/12/2024", "P103"),
    ("date", "2024-13-01", "P103"),
    ("timestamp", 5, "P102"),
    ("timestamp", date(2024, 1, 2), "P102"),
    ("timestamp", "nope", "P103"),
    ("list[string]", "a", "P102"),
    ("list[string]", [1], "P102"),
    ("list[string]", ("a",), "P102"),
]


def _spec(ptype: str, **fields: Any) -> dict[str, Any]:
    return {"p": {"name": "p", "type": ptype, **fields}}


def _bound(scope: ParamScope) -> Any:
    return scope.values()["p"]


def _rejected(exc: pytest.ExceptionInfo[ConfigSchemaError]) -> Diagnostic:
    assert len(exc.value.diagnostics) == 1
    return exc.value.diagnostics[0]


class TestCoercionTable:
    """Each type accepts its table rows and rejects everything else."""

    @pytest.mark.parametrize(("ptype", "value", "expected"), ACCEPT)
    def test_runtime_value_accepted(self, ptype: str, value: Any, expected: Any) -> None:
        bound = _bound(ParamScope.root({"p": value}).child(_spec(ptype)))
        assert bound == expected
        assert type(bound) is type(expected)

    @pytest.mark.parametrize(("ptype", "value", "code"), REJECT)
    def test_runtime_value_rejected(self, ptype: str, value: Any, code: str) -> None:
        with pytest.raises(ConfigSchemaError) as exc:
            ParamScope.root({"p": value}).child(_spec(ptype), file="t.thread")
        diag = _rejected(exc)
        assert (diag.code, diag.file, diag.pointer) == (code, "t.thread", "/params/p")

    @pytest.mark.parametrize(("ptype", "value", "expected"), ACCEPT)
    def test_default_accepted(self, ptype: str, value: Any, expected: Any) -> None:
        bound = _bound(ParamScope.root().child(_spec(ptype, required=False, default=value)))
        assert bound == expected
        assert type(bound) is type(expected)

    @pytest.mark.parametrize(("ptype", "value", "code"), REJECT)
    def test_default_rejected(self, ptype: str, value: Any, code: str) -> None:
        with pytest.raises(ConfigSchemaError) as exc:
            ParamScope.root().child(_spec(ptype, required=False, default=value), file="t.thread")
        assert _rejected(exc).code == code

    @pytest.mark.parametrize(("ptype", "value", "expected"), ACCEPT)
    def test_pin_accepted(self, ptype: str, value: Any, expected: Any) -> None:
        bound = _bound(ParamScope.root().child(_spec(ptype), pins={"p": value}))
        assert bound == expected
        assert type(bound) is type(expected)

    @pytest.mark.parametrize(("ptype", "value", "code"), REJECT)
    def test_pin_rejected(self, ptype: str, value: Any, code: str) -> None:
        with pytest.raises(ConfigSchemaError) as exc:
            ParamScope.root().child(_spec(ptype), pins={"p": value}, file="t.thread")
        assert _rejected(exc).code == code


class TestSpecShapes:
    """A spec reads the same whether it is a mapping or a ParamSpec."""

    def test_param_spec_instance(self) -> None:
        from weevr.model.params import ParamSpec

        spec = ParamSpec(name="p", type="int")
        assert ParamScope.root({"p": "3"}).child({"p": spec}).values() == {"p": 3}

    def test_mapping_with_a_non_string_type_coerces_nothing(self) -> None:
        scope = ParamScope.root({"p": "3"}).child({"p": {"type": ["int"]}})
        assert scope.values() == {"p": "3"}

    @pytest.mark.parametrize("required", ["false", "", 0])
    def test_mapping_with_a_non_bool_required_is_required(self, required: Any) -> None:
        with pytest.raises(ConfigSchemaError) as exc:
            ParamScope.root().child({"p": {"type": "int", "required": required}})
        assert _rejected(exc).code == "P101"

    def test_mapping_without_required_is_required(self) -> None:
        with pytest.raises(ConfigSchemaError) as exc:
            ParamScope.root().child({"p": {"type": "int"}})
        assert _rejected(exc).code == "P101"


class TestWinningSpec:
    """The nearest declaration's type applies, and its file and pointer are named."""

    def test_error_points_at_inline_declaration(self) -> None:
        with pytest.raises(ConfigSchemaError) as exc:
            ParamScope.root({"p": "x"}).child(_spec("int"), file="l.loom", at=("weaves", 0))
        diag = _rejected(exc)
        assert (diag.file, diag.pointer) == ("l.loom", "/weaves/0/params/p")

    def test_inherited_spec_error_names_ancestor(self) -> None:
        parent = ParamScope.root().child(_spec("int", required=False), file="l.loom")
        with pytest.raises(ConfigSchemaError) as exc:
            parent.child(None, pins={"p": "x"}, file="t.thread")
        assert _rejected(exc).file == "l.loom"

    def test_nearer_spec_coerces_the_supplied_value(self) -> None:
        parent = ParamScope.root({"p": "5"}).child(_spec("int"), file="l.loom")
        assert parent.values() == {"p": 5}
        assert parent.child(_spec("string"), file="t.thread").values() == {"p": "5"}

    def test_nearer_spec_rejects_what_the_outer_accepts(self) -> None:
        parent = ParamScope.root({"p": "5.5"}).child(_spec("float"), file="l.loom")
        with pytest.raises(ConfigSchemaError) as exc:
            parent.child(_spec("int"), file="t.thread")
        diag = _rejected(exc)
        assert (diag.code, diag.file) == ("P102", "t.thread")

    def test_coerced_binding_keeps_its_supplied_origin(self) -> None:
        scope = ParamScope.root({"p": "5"})
        child = scope.child(_spec("int"))
        assert child.bindings["p"].value == 5
        assert child.bindings["p"].origin is scope.bindings["p"]
        assert child.bindings["p"].supplied_binding is scope.bindings["p"]

    def test_undeclared_supplied_value_is_not_coerced(self) -> None:
        assert ParamScope.root({"p": "5"}).child(None).values() == {"p": "5"}


def _tree(root: Path, *, loom: dict[str, Any], thread: dict[str, Any], alias: str) -> Path:
    write_document(root / "t.thread", {**THREAD, "target": {"alias": alias}, "params": thread})
    write_document(root / "w.weave", {"config_version": "2.0", "threads": [{"ref": "t.thread"}]})
    return write_document(
        root / "l.loom",
        {"config_version": "2.0", "weaves": [{"ref": "w.weave"}], "params": loom},
    )


class TestThroughLoad:
    """Enforcement runs in every scope a load opens, not just the top-level file."""

    def test_child_file_spec_enforced(self, tmp_path: Path) -> None:
        path = _tree(
            tmp_path,
            loom={},
            thread=_spec("int"),
            alias="out.${param.p}",
        )
        with pytest.raises(ConfigError) as exc:
            load_resolved(path, runtime_params={"p": "x"}, project_root=tmp_path)
        diag = exc.value.diagnostics[0]
        assert (diag.code, diag.file, diag.pointer) == (
            "P102",
            str(tmp_path / "t.thread"),
            "/params/p",
        )

    def test_top_level_spec_enforced_by_load_config(self, tmp_path: Path) -> None:
        path = write_document(tmp_path / "t.thread", {**THREAD, "params": _spec("bool")})
        with pytest.raises(ConfigError) as exc:
            load_config(path, runtime_params={"p": "yes"})
        diag = exc.value.diagnostics[0]
        assert (diag.code, diag.file, diag.pointer) == ("P102", str(path), "/params/p")

    def test_coerced_value_reaches_whole_value_reference(self, tmp_path: Path) -> None:
        body = {
            **THREAD,
            "params": _spec("int"),
            "sources": {"s": {"type": "delta", "alias": "raw.t", "options": {"n": "${param.p}"}}},
        }
        path = write_document(tmp_path / "t.thread", body)
        model: Any = load_resolved(path, runtime_params={"p": "42"}).model
        assert model.sources["s"].options["n"] == 42
        assert type(model.sources["s"].options["n"]) is int

    def test_coerced_value_stringifies_when_embedded(self, tmp_path: Path) -> None:
        path = _tree(tmp_path, loom=_spec("bool"), thread={}, alias="out.t_${param.p}")
        doc = load_resolved(path, runtime_params={"p": "TRUE"}, project_root=tmp_path).document
        thread: Any = doc.children["w"].children["t"].model
        assert thread.target.alias == "out.t_True"

    def test_whole_value_date_is_the_string(self, tmp_path: Path) -> None:
        body = {
            **THREAD,
            "params": _spec("date"),
            "sources": {"s": {"type": "delta", "alias": "raw.t", "options": {"d": "${param.p}"}}},
        }
        path = write_document(tmp_path / "t.thread", body)
        model: Any = load_resolved(path, runtime_params={"p": date(2024, 1, 2)}).model
        assert model.sources["s"].options["d"] == "2024-01-02"

    def test_embedded_timestamp_keeps_its_text(self, tmp_path: Path) -> None:
        body = {
            **THREAD,
            "params": _spec("timestamp"),
            "sources": {
                "s": {
                    "type": "delta",
                    "alias": "raw.t",
                    "options": {"f": "ts >= '${param.p}'"},
                }
            },
        }
        path = write_document(tmp_path / "t.thread", body)
        model: Any = load_resolved(path, runtime_params={"p": "2024-01-01T10:00:00Z"}).model
        assert model.sources["s"].options["f"] == "ts >= '2024-01-01T10:00:00Z'"

    def test_pin_coerced_and_consumed(self, tmp_path: Path) -> None:
        write_document(
            tmp_path / "t.thread",
            {**THREAD, "target": {"alias": "out.${param.p}"}, "params": _spec("int")},
        )
        write_document(
            tmp_path / "w.weave",
            {"config_version": "2.0", "threads": [{"ref": "t.thread"}]},
        )
        path = write_document(
            tmp_path / "l.loom",
            {
                "config_version": "2.0",
                "weaves": [{"ref": "w.weave", "params": {"p": "007"}}],
            },
        )
        resolved = load_resolved(path, project_root=tmp_path)
        thread: Any = resolved.document.children["w"].children["t"].model
        assert thread.target.alias == "out.7"
        assert [d.code for d in resolved.warnings if d.code == "P123"] == []
