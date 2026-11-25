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
from pydantic import BaseModel

from documents import write_document
from weevr.config import load_config, load_resolved
from weevr.config.interpolation import _Rendered, resolve_variables
from weevr.config.params import ParamScope
from weevr.config.scoping import scope_context
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

    def test_embedded_reference_keeps_the_supplied_text(self, tmp_path: Path) -> None:
        path = _tree(tmp_path, loom=_spec("bool"), thread={}, alias="out.t_${param.p}")
        doc = load_resolved(path, runtime_params={"p": "TRUE"}, project_root=tmp_path).document
        thread: Any = doc.children["w"].children["t"].model
        assert thread.target.alias == "out.t_TRUE"

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
        assert thread.target.alias == "out.007"
        assert [d.code for d in resolved.warnings if d.code == "P123"] == []


def _embedded(scope: ParamScope) -> str:
    """How ``${param.p}`` renders embedded in a larger string in ``scope``."""
    return resolve_variables("[${param.p}]", scope_context(scope))[1:-1]


def _whole(scope: ParamScope) -> Any:
    """What a whole-value ``${param.p}`` reference reads in ``scope``."""
    return resolve_variables("${param.p}", scope_context(scope))


class TestEmbeddedText:
    """An embedded reference renders the value as supplied; a whole value gets the coerced one."""

    @pytest.mark.parametrize(
        ("ptype", "supplied", "whole"),
        [
            ("bool", "true", True),
            ("bool", "TRUE", True),
            ("bool", "False", False),
            ("int", "007", 7),
            ("int", "+5", 5),
            ("float", "1.50", 1.5),
            ("string", "abc", "abc"),
            ("date", "2024-1-5", "2024-1-5"),
        ],
    )
    def test_supplied_text_is_kept(self, ptype: str, supplied: str, whole: Any) -> None:
        scope = ParamScope.root({"p": supplied}).child(_spec(ptype))
        assert _embedded(scope) == supplied
        assert _whole(scope) == whole
        assert type(_whole(scope)) is type(whole)

    @pytest.mark.parametrize(
        ("ptype", "supplied", "text"),
        [
            ("bool", True, "true"),
            ("bool", False, "false"),
            ("int", 5, "5"),
            ("float", 5, "5"),
            ("float", 1.5, "1.5"),
            ("date", date(2024, 1, 2), "2024-01-02"),
            ("timestamp", datetime(2024, 1, 2, 3, 4, 5), "2024-01-02T03:04:05"),
            ("list[string]", ["a", "b"], "['a', 'b']"),
        ],
    )
    def test_native_runtime_value_renders_as_yaml_writes_it(
        self, ptype: str, supplied: Any, text: str
    ) -> None:
        scope = ParamScope.root({"p": supplied}).child(_spec(ptype))
        assert _embedded(scope) == text

    def test_yaml_default_renders_as_written(self) -> None:
        scope = ParamScope.root().child(_spec("bool", required=False, default=True))
        assert _embedded(scope) == "true"
        assert _whole(scope) is True

    def test_string_default_keeps_its_text(self) -> None:
        scope = ParamScope.root().child(_spec("int", required=False, default="007"))
        assert _embedded(scope) == "007"
        assert _whole(scope) == 7

    def test_pin_keeps_its_text(self) -> None:
        scope = ParamScope.root().child(_spec("bool", required=False))
        scope = scope.child(None, pins={"p": "True"})
        assert _embedded(scope) == "True"
        assert _whole(scope) is True

    def test_inherited_binding_keeps_its_text(self) -> None:
        parent = ParamScope.root({"p": "007"}).child(_spec("int"))
        child = parent.child(None)
        assert _embedded(child) == "007"
        assert _whole(child) == 7

    def test_nearer_declaration_recoerces_the_supplied_text(self) -> None:
        parent = ParamScope.root({"p": "1.50"}).child(_spec("float"))
        assert _whole(parent) == 1.5
        child = parent.child(_spec("string"))
        assert _whole(child) == "1.50"
        assert _embedded(child) == "1.50"

    def test_undeclared_native_bool_renders_as_yaml_writes_it(self) -> None:
        scope = ParamScope.root({"p": True}).child(None)
        assert _embedded(scope) == "true"
        assert _whole(scope) is True

    def test_undeclared_supplied_string_keeps_its_text(self) -> None:
        scope = ParamScope.root({"p": "True"}).child(None)
        assert _embedded(scope) == "True"
        assert _whole(scope) == "True"

    def test_dotted_reference_traverses_a_declared_value(self) -> None:
        spec = {"p": {"name": "p", "type": "object"}}
        scope = ParamScope.root({"p": {"env": "dev"}}).child(spec)
        context = scope_context(scope)
        assert resolve_variables("x_${param.p.env}", context) == "x_dev"
        assert resolve_variables("${param.p.env}", context) == "dev"

    def test_loom_condition_compares_the_supplied_text(self, tmp_path: Path) -> None:
        write_document(tmp_path / "t.thread", THREAD)
        write_document(
            tmp_path / "w.weave", {"config_version": "2.0", "threads": [{"ref": "t.thread"}]}
        )
        path = write_document(
            tmp_path / "l.loom",
            {
                "config_version": "2.0",
                "params": {"flag": {"name": "flag", "type": "bool"}},
                "weaves": [{"ref": "w.weave", "condition": {"when": "${param.flag} == 'true'"}}],
            },
        )
        resolved = load_resolved(path, runtime_params={"flag": "true"}, project_root=tmp_path)
        loom: Any = resolved.document.model
        assert loom.weaves[0].condition.when == "true == 'true'"


def _wrapped(value: Any) -> list[Any]:
    """Every rendered-text wrapper reachable from a hydrated model value."""
    if isinstance(value, _Rendered):
        return [value]
    if isinstance(value, BaseModel):
        return [w for name in type(value).model_fields for w in _wrapped(getattr(value, name))]
    if isinstance(value, dict):
        return [w for item in value.values() for w in _wrapped(item)]
    if isinstance(value, (list, tuple)):
        return [w for item in value for w in _wrapped(item)]
    return []


class TestPinsReadTheSuppliedText:
    """A pin built from a reference reads the text embedded and the native value whole."""

    def test_embedded_pin_gets_text_and_whole_pin_gets_value(self, tmp_path: Path) -> None:
        thread_params = {
            "a": {"name": "a", "type": "string"},
            "b": {"name": "b", "type": "bool"},
        }
        body = {
            **THREAD,
            "params": thread_params,
            "sources": {
                "s": {"type": "delta", "alias": "raw.t", "options": {"flag": "${param.b}"}}
            },
            "target": {"alias": "out.${param.a}"},
        }
        write_document(tmp_path / "t.thread", body)
        write_document(
            tmp_path / "w.weave", {"config_version": "2.0", "threads": [{"ref": "t.thread"}]}
        )
        path = write_document(
            tmp_path / "l.loom",
            {
                "config_version": "2.0",
                "params": _spec("bool"),
                "weaves": [{"ref": "w.weave", "params": {"a": "x_${param.p}", "b": "${param.p}"}}],
            },
        )
        doc = load_resolved(path, runtime_params={"p": "TRUE"}, project_root=tmp_path).document
        thread: Any = doc.children["w"].children["t"].model
        assert thread.target.alias == "out.x_TRUE"
        assert thread.sources["s"].options["flag"] is True
        assert _wrapped(thread) == []
        assert _wrapped(doc.model) == []
