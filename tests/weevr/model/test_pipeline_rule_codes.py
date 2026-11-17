"""Pipeline step rule violations report their own codes with step context."""

from __future__ import annotations

import ast
import inspect
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

import weevr.model.pipeline as pipeline_module
from weevr.diagnostics.pydantic import from_validation_error
from weevr.diagnostics.registry import REGISTRY
from weevr.model import Thread
from weevr.model.sub_pipeline import SubPipeline

BASE: dict[str, Any] = {
    "name": "t",
    "config_version": "1.0",
    "sources": {"a": {"type": "delta", "alias": "x.y"}},
    "target": {"alias": "x.z"},
}

RESOLVE_OK: dict[str, Any] = {"name": "k", "lookup": "l", "match": {"a": "b"}, "pk": "id"}

BATCH_ITEM: dict[str, Any] = {"name": "k", "lookup": "l", "match": {"a": "b"}}
EMPTY_LITERAL: dict[str, Any] = {"null_mode": "literal", "null_literal": ""}

CASES: list[tuple[str, str, dict[str, Any]]] = [
    ("C401", "derive", {"columns": {}}),
    ("C401", "select", {"columns": []}),
    ("C401", "drop", {"columns": []}),
    ("C401", "cast", {"columns": {}}),
    ("C401", "sort", {"columns": []}),
    ("C401", "union", {"sources": []}),
    ("C401", "aggregate", {"measures": {}}),
    ("C401", "window", {"functions": {}, "partition_by": ["a"]}),
    ("C401", "window", {"functions": {"f": "sum(x)"}, "partition_by": []}),
    ("C401", "pivot", {"group_by": ["a"], "pivot_column": "b", "values": [], "aggregate": "x"}),
    ("C401", "unpivot", {"columns": [], "name_column": "n", "value_column": "v"}),
    ("C401", "case_when", {"column": "c", "cases": []}),
    ("C401", "fill_null", {"columns": {}}),
    ("C401", "coalesce", {"columns": {}}),
    ("C401", "coalesce", {"columns": {"c": []}}),
    ("C401", "concat", {"target": "t", "columns": []}),
    ("C401", "map", {"column": "c", "values": {}}),
    ("C401", "format", {"c": {"pattern": ""}}),
    ("C401", "resolve", {"batch": []}),
    ("C402", "map", {"column": "c", "values": {"a": "b"}, "default": "x", "unmapped": "null"}),
    ("C402", "format", {"c": {"pattern": "x", "number": "0"}}),
    (
        "C402",
        "resolve",
        {**RESOLVE_OK, "batch": [{"name": "k", "lookup": "l", "match": {"a": "b"}}]},
    ),
    (
        "C402",
        "resolve",
        {
            **RESOLVE_OK,
            "effective": {"date_column": "d", "from": "a", "to": "b", "current": "c"},
        },
    ),
    ("C403", "fill_null", {}),
    ("C403", "fill_null", {"mode": "type_defaults"}),
    (
        "C403",
        "concat",
        {"target": "t", "columns": ["a"], "null_mode": "literal", "null_literal": ""},
    ),
    ("C403", "format", {"c": {}}),
    ("C403", "resolve", {k: v for k, v in RESOLVE_OK.items() if k != "pk"}),
    ("C403", "resolve", {**RESOLVE_OK, "include_prefix": "p_"}),
    ("C403", "resolve", {**RESOLVE_OK, "effective": {"date_column": "d"}}),
    ("C403", "resolve", {**RESOLVE_OK, "effective": {}}),
    ("C404", "fill_null", {"columns": {"a": 0}, "code": "unknown"}),
    ("C405", "unpivot", {"columns": ["a"], "name_column": "n", "value_column": "n"}),
    ("C405", "resolve", {**RESOLVE_OK, "on_invalid": -1, "on_unknown": -1}),
    (
        "C406",
        "window",
        {
            "functions": {"f": "sum(x)"},
            "partition_by": ["a"],
            "frame": {"type": "rows", "start": 5, "end": 1},
        },
    ),
    ("C105", "format", {"c": {"pattern": "{0:3}"}}),
    ("C105", "format", {"c": {"pattern": "{1:0}"}}),
    ("C103", "format", {"c": {"number": "abc"}}),
    ("C103", "string_ops", {"columns": ["a"], "expr": "upper(x)"}),
    ("C103", "date_ops", {"columns": ["a"], "expr": "to_date(x)"}),
    ("C102", "join", {"source": "a", "type": "inner", "on": "id"}),
]


def _diagnostics(step: dict[str, Any]) -> list[Any]:
    data = {**BASE, "steps": [{"filter": {"expr": "1=1"}}, step]}
    with pytest.raises(ValidationError) as exc_info:
        Thread.model_validate(data)
    return from_validation_error(exc_info.value, data)


@pytest.mark.parametrize(("code", "step_type", "params"), CASES)
def test_step_rule_has_its_code(code: str, step_type: str, params: dict[str, Any]) -> None:
    diagnostics = _diagnostics({step_type: params})
    assert [d.code for d in diagnostics] == [code]
    assert diagnostics[0].step_index == 1
    assert diagnostics[0].step_type == step_type


def test_empty_sub_pipeline_steps() -> None:
    with pytest.raises(ValidationError) as exc_info:
        SubPipeline.model_validate({"from": "a", "steps": []})
    diagnostics = from_validation_error(exc_info.value, {"from": "a", "steps": []})
    assert [d.code for d in diagnostics] == ["C401"]


SHARED_SCHEMA_CODES = {"C102", "C103", "C105"}


def test_codes_are_registered_in_the_step_range() -> None:
    """Step rules use their own block, or a shared schema code."""
    used = {code for code, _, _ in CASES}
    assert used <= set(REGISTRY)
    assert all("C401" <= code <= "C439" for code in used - SHARED_SCHEMA_CODES)


@pytest.mark.parametrize("module", ["pipeline.py", "sub_pipeline.py"])
def test_validators_do_not_raise_plain_errors(module: str) -> None:
    tree = ast.parse((Path(inspect.getfile(pipeline_module)).parent / module).read_text())
    offenders: list[int] = []
    for func in ast.walk(tree):
        if not isinstance(func, ast.FunctionDef):
            continue
        decorated = any("validator" in ast.unparse(dec) for dec in func.decorator_list)
        if not decorated:
            continue
        for node in ast.walk(func):
            if (
                isinstance(node, ast.Raise)
                and isinstance(node.exc, ast.Call)
                and ast.unparse(node.exc.func) in {"ValueError", "TypeError"}
            ):
                offenders.append(node.lineno)
    assert offenders == []
