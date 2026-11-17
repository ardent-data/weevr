"""Variable, parameter, reference and entry-form failures carry coded diagnostics."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from weevr.config import load_config
from weevr.config.attribution import attribute_errors
from weevr.config.audit_templates import resolve_template_columns
from weevr.config.inheritance import check_defaults_shape
from weevr.config.loader import resolve_config
from weevr.config.locations import LocalConfigLocation
from weevr.config.macros import expand_foreach, reject_foreach_in_with
from weevr.config.resolver import (
    resolve_declared_params,
    resolve_ref_path,
    resolve_references,
    resolve_variables,
    validate_params,
)
from weevr.diagnostics import Diagnostic, Severity
from weevr.errors import (
    ConfigError,
    ConfigSchemaError,
    InheritanceError,
    ReferenceResolutionError,
    VariableResolutionError,
)

THREAD_BODY: dict[str, Any] = {
    "config_version": "1.0",
    "sources": {"s": {"type": "delta", "alias": "raw.t"}},
    "target": {"alias": "out.t"},
}


def _only(exc: ConfigError) -> Diagnostic:
    assert len(exc.diagnostics) == 1
    return exc.diagnostics[0]


def _write(path: Path, data: dict[str, Any] | str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data if isinstance(data, str) else yaml.safe_dump(data, sort_keys=False))
    return path


def _weave(threads: list[Any]) -> dict[str, Any]:
    return {"config_version": "1.0", "threads": threads}


class TestParameters:
    def test_missing_required_declared_param(self):
        specs = {"region": {"type": "string", "required": True}}
        with pytest.raises(ConfigSchemaError) as exc:
            resolve_declared_params(specs, None, file_path="a.loom")
        diag = _only(exc.value)
        assert diag.code == "P101"
        assert diag.severity is Severity.ERROR
        assert diag.file == "a.loom"
        assert diag.pointer == "/params/region"

    def test_missing_required_param_in_validation_has_same_code(self):
        specs = {"region": {"type": "string", "required": True}}
        with pytest.raises(ConfigSchemaError) as exc:
            validate_params(specs, {})
        diag = _only(exc.value)
        assert diag.code == "P101"
        assert diag.pointer == "/params/region"

    def test_param_spec_not_a_mapping(self):
        with pytest.raises(ConfigSchemaError) as exc:
            resolve_declared_params({"region": "string"}, None, file_path="a.loom")
        diag = _only(exc.value)
        assert diag.code == "C102"
        assert diag.file == "a.loom"
        assert diag.pointer == "/params/region"

    @pytest.mark.parametrize(
        ("ptype", "value"),
        [
            ("string", 1),
            ("int", "1"),
            ("int", True),
            ("float", "x"),
            ("bool", "yes"),
            ("date", 5),
            ("timestamp", 5),
            ("list[string]", "a"),
            ("list[string]", [1]),
        ],
    )
    def test_type_mismatch(self, ptype, value):
        specs = {"p": {"type": ptype, "required": True}}
        with pytest.raises(ConfigSchemaError) as exc:
            validate_params(specs, {"p": value})
        diag = _only(exc.value)
        assert diag.code == "P102"
        assert diag.pointer == "/params/p"

    @pytest.mark.parametrize(("ptype", "value"), [("date", "31/12/2024"), ("timestamp", "nope")])
    def test_format_error(self, ptype, value):
        specs = {"p": {"type": ptype, "required": True}}
        with pytest.raises(ConfigSchemaError) as exc:
            validate_params(specs, {"p": value})
        diag = _only(exc.value)
        assert diag.code == "P103"
        assert diag.pointer == "/params/p"

    def test_missing_param_through_load_names_file(self, tmp_path):
        path = _write(
            tmp_path / "t.thread",
            {**THREAD_BODY, "params": {"region": {"type": "string", "required": True}}},
        )
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        diag = _only(exc.value)
        assert diag.code == "P101"
        assert diag.file == str(path)
        assert diag.pointer == "/params/region"
        assert diag.line is not None


class TestUnresolvedVariable:
    def test_whole_value_reference_points_at_value(self):
        config = {"sources": {"s": {"alias": "${param.missing}"}}}
        with pytest.raises(VariableResolutionError) as exc:
            resolve_variables(config, {})
        diag = _only(exc.value)
        assert diag.code == "P110"
        assert diag.pointer == "/sources/s/alias"

    def test_embedded_reference_in_list_points_at_item(self):
        config = {"steps": ["ok", "x_${param.missing}"]}
        with pytest.raises(VariableResolutionError) as exc:
            resolve_variables(config, {})
        diag = _only(exc.value)
        assert diag.code == "P110"
        assert diag.pointer == "/steps/1"

    def test_through_load_names_file_and_line(self, tmp_path):
        body = {**THREAD_BODY, "target": {"alias": "out.${param.missing}"}}
        path = _write(tmp_path / "t.thread", body)
        with pytest.raises(VariableResolutionError) as exc:
            load_config(path)
        diag = _only(exc.value)
        assert diag.code == "P110"
        assert diag.file == str(path)
        assert diag.pointer == "/target/alias"
        assert diag.line is not None

    def test_thread_template_hint_keeps_code(self, tmp_path):
        body = {**THREAD_BODY, "target": {"alias": "out.${param.x}"}}
        path = _write(tmp_path / "t.thread", body)
        with pytest.raises(VariableResolutionError) as exc:
            load_config(path)
        assert _only(exc.value).code == "P110"


class TestReferences:
    def test_ref_not_found(self, tmp_path):
        with pytest.raises(ReferenceResolutionError) as exc:
            resolve_ref_path("missing.thread", tmp_path, "/threads/0/ref")
        diag = _only(exc.value)
        assert diag.code == "C301"
        assert diag.pointer == "/threads/0/ref"

    def test_ref_not_found_in_weave_names_file_and_pointer(self, tmp_path):
        weave = _write(tmp_path / "w.weave", _weave([{"name": "a"}, {"ref": "gone.thread"}]))
        config = yaml.safe_load(weave.read_text())
        with (
            pytest.raises(ReferenceResolutionError) as exc,
            attribute_errors(LocalConfigLocation(weave)),
        ):
            resolve_references(config, "weave", tmp_path)
        diag = _only(exc.value)
        assert diag.code == "C301"
        assert diag.file == str(weave)
        assert diag.pointer == "/threads/1/ref"
        assert diag.line == 4

    def test_weave_without_definition(self, tmp_path):
        loom = {"config_version": "1.0", "weaves": [{"name": "ghost"}]}
        with pytest.raises(ReferenceResolutionError) as exc:
            resolve_references(loom, "loom", tmp_path)
        diag = _only(exc.value)
        assert diag.code == "C301"
        assert diag.pointer == "/weaves/0"

    def test_unsupported_ref_extension_reuses_c007(self, tmp_path):
        with pytest.raises(ConfigError) as exc:
            resolve_ref_path("bad.yaml", tmp_path, "/threads/2/ref")
        diag = _only(exc.value)
        assert diag.code == "C007"
        assert diag.pointer == "/threads/2/ref"

    @pytest.mark.parametrize("ref", [5, None, ["a.thread"]])
    def test_non_string_ref(self, tmp_path, ref):
        weave = _weave([{"name": "a"}, {"ref": ref}])
        with pytest.raises(ReferenceResolutionError) as exc:
            resolve_references(weave, "weave", tmp_path)
        diag = _only(exc.value)
        assert diag.code == "C304"
        assert diag.pointer == "/threads/1/ref"

    def test_non_string_ref_passed_to_resolve_ref_path(self, tmp_path):
        with pytest.raises(ReferenceResolutionError) as exc:
            resolve_ref_path(5, tmp_path, "/weaves/0/ref")  # type: ignore[arg-type]
        diag = _only(exc.value)
        assert diag.code == "C304"
        assert diag.pointer == "/weaves/0/ref"

    def test_ref_outside_project(self, tmp_path):
        project = tmp_path / "proj"
        project.mkdir()
        with pytest.raises(ReferenceResolutionError) as exc:
            resolve_ref_path("../out.thread", project, "/threads/0/ref")
        diag = _only(exc.value)
        assert diag.code == "C303"
        assert diag.pointer == "/threads/0/ref"

    def test_weave_name_outside_project(self, tmp_path):
        project = tmp_path / "proj"
        project.mkdir()
        loom = {"config_version": "1.0", "weaves": [{"name": "../outside"}]}
        with pytest.raises(ReferenceResolutionError) as exc:
            resolve_references(loom, "loom", project)
        diag = _only(exc.value)
        assert diag.code == "C303"
        assert diag.pointer == "/weaves/0/name"

    def test_circular_reference_through_load_names_file(self, tmp_path):
        _write(tmp_path / "a.weave", _weave([{"ref": "a.weave"}]))
        loom = _write(
            tmp_path / "l.loom",
            {"config_version": "1.0", "weaves": [{"ref": "a.weave"}]},
        )
        with pytest.raises(ReferenceResolutionError) as exc:
            load_config(loom, project_root=tmp_path)
        diag = _only(exc.value)
        assert diag.code == "C302"
        assert diag.expected == ("a.weave", "a.weave")
        assert diag.pointer == "/threads/0/ref"
        assert diag.file == str(tmp_path / "a.weave")

    def test_cycle_lists_every_file_in_order(self, tmp_path):
        weave = _weave([{"ref": "b.weave"}])
        visited = dict.fromkeys(["a.weave", "b.weave", "c.weave"])
        with pytest.raises(ReferenceResolutionError) as exc:
            resolve_references(weave, "weave", tmp_path, visited=visited)
        diag = _only(exc.value)
        assert diag.code == "C302"
        assert diag.expected == ("b.weave", "c.weave", "b.weave")

    def test_circular_convention_reference(self, tmp_path):
        _write(tmp_path / "a.weave", {"config_version": "1.0", "threads": []})
        loom = {"config_version": "1.0", "weaves": [{"name": "a"}]}
        with pytest.raises(ReferenceResolutionError) as exc:
            resolve_references(loom, "loom", tmp_path, visited={"a.weave": None})
        diag = _only(exc.value)
        assert diag.code == "C302"
        assert diag.expected == ("a.weave", "a.weave")
        assert diag.pointer == "/weaves/0/name"


class TestEntryForms:
    def test_inline_thread_without_name(self, tmp_path):
        weave = _weave([{"name": "a", **THREAD_BODY}, {**THREAD_BODY}])
        with pytest.raises(ConfigError) as exc:
            resolve_references(weave, "weave", tmp_path)
        diag = _only(exc.value)
        assert diag.code == "C305"
        assert diag.pointer == "/threads/1"

    def test_inline_weave_without_name(self, tmp_path):
        loom = {"config_version": "1.0", "weaves": [{"threads": []}]}
        with pytest.raises(ConfigError) as exc:
            resolve_references(loom, "loom", tmp_path)
        diag = _only(exc.value)
        assert diag.code == "C305"
        assert diag.pointer == "/weaves/0"

    def test_duplicate_inline_names(self, tmp_path):
        weave = _weave([{"name": "a", **THREAD_BODY}, {"name": "a", **THREAD_BODY}])
        with pytest.raises(ConfigError) as exc:
            resolve_references(weave, "weave", tmp_path)
        diag = _only(exc.value)
        assert diag.code == "C306"
        assert diag.pointer == "/threads/1"

    def test_duplicate_effective_names_between_refs(self, tmp_path):
        _write(tmp_path / "a.thread", THREAD_BODY)
        _write(tmp_path / "b.thread", THREAD_BODY)
        weave = _weave(
            [{"ref": "a.thread", "as": "x"}, {"ref": "b.thread", "as": "x"}],
        )
        with pytest.raises(ConfigError) as exc:
            resolve_references(weave, "weave", tmp_path)
        diag = _only(exc.value)
        assert diag.code == "C306"
        assert diag.pointer == "/threads/1"

    def test_repeated_ref_without_alias(self, tmp_path):
        _write(tmp_path / "a.thread", THREAD_BODY)
        weave = _weave([{"ref": "a.thread", "as": "x"}, {"ref": "a.thread"}])
        with pytest.raises(ConfigError) as exc:
            resolve_references(weave, "weave", tmp_path)
        diag = _only(exc.value)
        assert diag.code == "C307"
        assert diag.pointer == "/threads/1"

    def test_unresolved_variable_in_entry_params(self, tmp_path):
        _write(tmp_path / "a.thread", THREAD_BODY)
        weave = _weave([{"ref": "a.thread", "params": {"x": "${param.nope}"}}])
        with pytest.raises(VariableResolutionError) as exc:
            resolve_references(weave, "weave", tmp_path)
        diag = _only(exc.value)
        assert diag.code == "P110"
        assert diag.pointer == "/threads/0/params/x"

    def test_inline_weave_entry_pointer_is_prefixed(self, tmp_path):
        loom = {
            "config_version": "1.0",
            "weaves": [
                {
                    "name": "w",
                    "config_version": "1.0",
                    "threads": [{**THREAD_BODY, "name": "a"}, {**THREAD_BODY, "name": "a"}],
                },
            ],
        }
        with pytest.raises(ConfigError) as exc:
            resolve_references(loom, "loom", tmp_path)
        diag = _only(exc.value)
        assert diag.code == "C306"
        assert diag.pointer == "/weaves/0/threads/1"


class TestForeach:
    @pytest.mark.parametrize(
        ("foreach", "pointer"),
        [
            ("nope", "/steps/0/foreach"),
            ({"as": "x", "steps": [{}]}, "/steps/0/foreach/values"),
            ({"values": [1], "steps": [{}]}, "/steps/0/foreach/as"),
            ({"values": [1], "as": "x"}, "/steps/0/foreach/steps"),
        ],
    )
    def test_invalid_foreach(self, foreach, pointer):
        with pytest.raises(ConfigError) as exc:
            expand_foreach([{"foreach": foreach}])
        diag = _only(exc.value)
        assert diag.code == "C308"
        assert diag.pointer == pointer

    def test_foreach_in_with(self):
        thread = {"with": {"prep": {"steps": [{"foreach": {}}]}}}
        with pytest.raises(ConfigError) as exc:
            reject_foreach_in_with(thread)
        diag = _only(exc.value)
        assert diag.code == "C309"
        assert diag.pointer == "/with/prep/steps"


class TestEnvelopeAndRegistries:
    def test_unsupported_extension_at_load(self, tmp_path):
        path = _write(tmp_path / "t.yaml", THREAD_BODY)
        with pytest.raises(ConfigError) as exc:
            resolve_config(LocalConfigLocation(path), require_typed_extension=True)
        diag = _only(exc.value)
        assert diag.code == "C007"
        assert diag.file == str(path)

    def test_name_not_matching_file_stem(self, tmp_path):
        path = _write(tmp_path / "t.thread", {**THREAD_BODY, "name": "other"})
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        diag = _only(exc.value)
        assert diag.code == "C205"
        assert diag.file == str(path)
        assert diag.pointer == "/name"
        assert diag.expected == ("t",)
        assert diag.line is not None

    def test_unknown_audit_template(self):
        with pytest.raises(ConfigError) as exc:
            resolve_template_columns(["nope"], {"mine": {"c": "1"}})
        diag = _only(exc.value)
        assert diag.code == "R140"
        assert diag.expected is not None and "mine" in diag.expected

    @pytest.mark.parametrize(
        ("defaults", "pointer"),
        [
            ({"target": "x"}, "/defaults/target"),
            ({"target": {"audit_columns": []}}, "/defaults/target/audit_columns"),
            ({"audit_columns": 3}, "/defaults/audit_columns"),
            ({"exports": {}}, "/defaults/exports"),
            ({"exports": [{}, "x"]}, "/defaults/exports/1"),
        ],
    )
    def test_defaults_wrong_type(self, defaults, pointer):
        with pytest.raises(InheritanceError) as exc:
            check_defaults_shape(defaults, "loom")
        diag = _only(exc.value)
        assert diag.code == "C102"
        assert diag.pointer == pointer

    def test_missing_warp(self, tmp_path):
        thread = {
            **THREAD_BODY,
            "target": {"alias": "gold.dim", "warp": "nope"},
        }
        path = _write(tmp_path / "dim.thread", thread)
        with pytest.raises(ConfigError) as exc:
            load_config(path, project_root=tmp_path)
        diag = _only(exc.value)
        assert diag.code == "C310"
        assert diag.file == str(path)


def test_unresolved_variable_under_with_points_at_the_authored_key(tmp_path: Path) -> None:
    import yaml

    from weevr.config import load_config
    from weevr.errors import VariableResolutionError

    thread = {
        "config_version": "1.0",
        "sources": {"main": {"type": "delta", "alias": "raw.a"}},
        "target": {"alias": "out.a"},
        "with": {"orders": {"from": "main", "steps": [{"filter": {"expr": "x = '${nope}'"}}]}},
    }
    path = tmp_path / "t.thread"
    path.write_text(yaml.safe_dump(thread, sort_keys=False))
    with pytest.raises(VariableResolutionError) as exc_info:
        load_config(path)
    (diag,) = exc_info.value.diagnostics
    assert diag.code == "P110"
    assert diag.pointer == "/with/orders/steps/0/filter/expr"
    assert diag.line is not None


def test_a_user_key_named_with_underscore_keeps_its_name(tmp_path: Path) -> None:
    import yaml

    from weevr.config import load_config
    from weevr.errors import VariableResolutionError

    thread = {
        "config_version": "1.0",
        "sources": {"main": {"type": "delta", "alias": "raw.a"}},
        "target": {"alias": "out.a"},
        "steps": [{"derive": {"columns": {"with_": "'${nope}'"}}}],
    }
    path = tmp_path / "t.thread"
    path.write_text(yaml.safe_dump(thread, sort_keys=False))
    with pytest.raises(VariableResolutionError) as exc_info:
        load_config(path)
    (diag,) = exc_info.value.diagnostics
    assert diag.pointer == "/steps/0/derive/columns/with_"
    assert diag.line is not None


def test_authored_path_translates_only_schema_fields() -> None:
    from weevr.config.validation import authored_path

    assert authored_path(("with_", "orders")) == ("with", "orders")
    assert authored_path(("threads", 2, "with_", "x")) == ("threads", 2, "with", "x")
    assert authored_path(("steps", 0, "derive", "columns", "with_")) == (
        "steps",
        0,
        "derive",
        "columns",
        "with_",
    )
    assert authored_path(("threads", 2, "params", "with_")) == ("threads", 2, "params", "with_")
