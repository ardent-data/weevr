"""Model rules outside the pipeline steps report their own diagnostic codes."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

import weevr.model as model_package
from weevr.diagnostics import REGISTRY, Severity
from weevr.diagnostics.pydantic import from_validation_error
from weevr.model.column_set import ColumnSet, ColumnSetSource, ReservedWordConfig
from weevr.model.dimension import DimensionConfig, ScdDateConfig, SystemMemberConfig
from weevr.model.export import Export
from weevr.model.fact import FactConfig
from weevr.model.hooks import QualityGateStep
from weevr.model.load import CdcConfig, LoadConfig, WatermarkStoreConfig
from weevr.model.lookup import Lookup
from weevr.model.seed import SeedConfig
from weevr.model.source import SOURCE_TYPES, Source
from weevr.model.target import ColumnMapping, Target
from weevr.model.thread import Thread
from weevr.model.validation import Assertion
from weevr.model.warp import WarpConfig
from weevr.model.write import WriteConfig

_DELTA = {"type": "delta", "alias": "db.src"}
_THREAD: dict[str, Any] = {
    "name": "t",
    "config_version": "1.0",
    "sources": {"a": _DELTA},
    "target": {"alias": "db.out"},
}
_DIM: dict[str, Any] = {
    "business_key": ["id"],
    "surrogate_key": {"name": "sk", "algorithm": "sha256", "columns": ["id"]},
}
_EXPORT: dict[str, Any] = {"name": "e", "type": "parquet", "path": "/x"}


def _t(**changes: Any) -> dict[str, Any]:
    return {**_THREAD, **changes}


def _dim(**changes: Any) -> dict[str, Any]:
    return {**_DIM, **changes}


def _exp(**changes: Any) -> dict[str, Any]:
    return {**_EXPORT, **changes}


def _with_alias(source_name: str) -> dict[str, Any]:
    return _t(
        sources={"a": _DELTA, "b": _DELTA},
        steps=[{"join": {"source": "b", "type": "left", "on": ["id"], "alias": source_name}}],
    )


# (model, data, code, resolved_path)
SOURCE_CASES = [
    (Source, {"lookup": "l", "type": "delta"}, "C404", ""),
    (Source, {"connection": "c", "alias": "a", "table": "t"}, "C402", ""),
    (
        Source,
        {
            "type": "date_sequence",
            "column": "d",
            "start": "2024-01-01",
            "end": "2024-02-01",
            "alias": "x",
        },
        "C404",
        "",
    ),
    (
        Source,
        {
            "type": "date_sequence",
            "column": "d",
            "start": "2024-01-01",
            "end": "2024-02-01",
            "path": "x",
        },
        "C404",
        "",
    ),
    (
        Source,
        {
            "type": "date_sequence",
            "column": "d",
            "start": "2024-01-01",
            "end": "2024-02-01",
            "options": {"a": 1},
        },
        "C404",
        "",
    ),
    (
        Source,
        {
            "type": "date_sequence",
            "column": "d",
            "start": "2024-01-01",
            "end": "2024-02-01",
            "dedup": {"keys": ["a"]},
        },
        "C404",
        "",
    ),
    (Source, {"connection": "c"}, "C403", ""),
    (Source, {"table": "t"}, "C403", ""),
    (Source, {"type": "date_sequence", "start": "2024-01-01", "end": "2024-02-01"}, "C403", ""),
    (Source, {"type": "date_sequence", "column": "d", "end": "2024-02-01"}, "C403", ""),
    (Source, {"type": "date_sequence", "column": "d", "start": "2024-01-01"}, "C403", ""),
    (Source, {}, "C403", ""),
    (Source, {"type": "delta"}, "C403", ""),
    (Source, {"type": "parquet"}, "C403", ""),
    (Source, {"connection": "c", "table": "t", "type": "parquet"}, "C404", ""),
    (Source, {"type": "excel", "path": "p"}, "C103", "/type"),
    (Source, {"type": "foo", "lookup": "l"}, "C103", "/type"),
    (
        Source,
        {
            "type": "date_sequence",
            "column": "d",
            "start": "2024-01-01",
            "end": "2024-02-01",
            "step": "hour",
        },
        "C103",
        "",
    ),
    (Source, {"type": "int_sequence", "column": "n", "start": 1, "end": 5, "step": 0}, "C105", ""),
]

COLUMN_SET_CASES = [
    (ColumnSetSource, {"type": "yaml"}, "C403", ""),
    (ColumnSetSource, {"type": "yaml", "path": "p", "connection": "c"}, "C404", ""),
    (ColumnSetSource, {"type": "yaml", "path": "p", "table": "t"}, "C404", ""),
    (ColumnSetSource, {"type": "yaml", "path": "p", "schema": "s"}, "C404", ""),
    (ColumnSetSource, {"type": "delta", "connection": "c", "table": "t", "alias": "a"}, "C402", ""),
    (ColumnSetSource, {"type": "delta", "connection": "c", "table": "t", "path": "p"}, "C404", ""),
    (ColumnSetSource, {"type": "delta", "connection": "c"}, "C403", ""),
    (ColumnSetSource, {"type": "delta", "alias": "a", "table": "t"}, "C403", ""),
    (ColumnSetSource, {"type": "delta", "alias": "a", "schema": "s"}, "C403", ""),
    (ColumnSetSource, {"type": "delta"}, "C403", ""),
    (
        ColumnSet,
        {"source": {"type": "yaml", "path": "p"}, "param": "x"},
        "C402",
        "",
    ),
    (ColumnSet, {}, "C403", ""),
    (ReservedWordConfig, {"strategy": "rename"}, "C403", ""),
    (Lookup, {"source": {"type": "delta", "alias": "a"}, "values": ["v"]}, "C403", ""),
    (
        Lookup,
        {"source": {"type": "delta", "alias": "a"}, "key": ["k"], "values": ["k"]},
        "R101",
        "",
    ),
    (
        Lookup,
        {"source": {"type": "delta", "alias": "a"}, "on_failure": "warn"},
        "C404",
        "",
    ),
]

DIMENSION_CASES = [
    (SystemMemberConfig, {"sk": 1, "code": "c", "label": "l"}, "C105", "/sk"),
    (ScdDateConfig, {"min": "nope"}, "C103", ""),
    (ScdDateConfig, {"min": "2030-01-01", "max": "2020-01-01"}, "C406", ""),
    (DimensionConfig, _dim(business_key=[]), "C401", ""),
    (
        DimensionConfig,
        _dim(surrogate_key={"name": "sk", "algorithm": "sha256", "columns": []}),
        "C401",
        "",
    ),
    (
        DimensionConfig,
        _dim(change_detection={"g": {"columns": ["a"], "on_change": "version"}}),
        "C403",
        "",
    ),
    (
        DimensionConfig,
        _dim(
            track_history=True,
            change_detection={
                "g": {"columns": "auto", "on_change": "overwrite"},
                "h": {"columns": "auto", "on_change": "overwrite"},
            },
        ),
        "C402",
        "",
    ),
    (
        DimensionConfig,
        _dim(
            change_detection={
                "g": {"columns": ["a"], "on_change": "overwrite"},
                "h": {"columns": ["a"], "on_change": "overwrite"},
            }
        ),
        "C440",
        "",
    ),
    (FactConfig, {"foreign_keys": []}, "C401", "/foreign_keys"),
    (SeedConfig, {"rows": []}, "C401", "/rows"),
]

EXPORT_CASES = [
    (Export, _exp(name="not valid"), "C103", ""),
    (Export, _exp(alias="a"), "C404", ""),
    (Export, _exp(connection="c", table="t"), "C404", ""),
    (Export, _exp(type="delta", path=None, connection="c", table="t", alias="a"), "C402", ""),
    (Export, _exp(type="delta", connection="c", table="t"), "C402", ""),
    (Export, _exp(type="delta", path=None, connection="c"), "C403", ""),
    (Export, _exp(type="delta", path=None, table="t"), "C403", ""),
    (Export, _exp(path=None), "C403", ""),
    (Export, _exp(alias="a", type="delta"), "C402", ""),
]

_GATE = {"type": "quality_gate"}
HOOK_CASES = [
    (QualityGateStep, {**_GATE, "check": "source_freshness", "max_age": "1h"}, "C403", ""),
    (QualityGateStep, {**_GATE, "check": "source_freshness", "source": "s"}, "C403", ""),
    (QualityGateStep, {**_GATE, "check": "row_count_delta"}, "C403", ""),
    (QualityGateStep, {**_GATE, "check": "row_count", "min_count": 1}, "C403", ""),
    (QualityGateStep, {**_GATE, "check": "row_count", "target": "t"}, "C403", ""),
    (QualityGateStep, {**_GATE, "check": "table_exists"}, "C403", ""),
    (QualityGateStep, {**_GATE, "check": "expression"}, "C403", ""),
    (
        Thread,
        _t(pre_steps=[{**_GATE, "check": "row_count_delta", "target": "t"}]),
        "C404",
        "/pre_steps",
    ),
]

LOAD_CASES = [
    (CdcConfig, {"preset": "delta_cdf", "operation_column": "op"}, "C402", ""),
    (CdcConfig, {}, "C403", ""),
    (CdcConfig, {"operation_column": "op"}, "C403", ""),
    (WatermarkStoreConfig, {"type": "metadata_table"}, "C403", ""),
    (LoadConfig, {"mode": "incremental_watermark", "watermark_type": "date"}, "C403", ""),
    (LoadConfig, {"mode": "incremental_watermark", "watermark_column": "w"}, "C403", ""),
    (LoadConfig, {"mode": "cdc"}, "C403", ""),
    (
        LoadConfig,
        {"mode": "cdc", "cdc": {"preset": "delta_cdf"}, "watermark_column": "w"},
        "C404",
        "",
    ),
    (
        LoadConfig,
        {
            "mode": "cdc",
            "cdc": {"operation_column": "o", "insert_value": "I"},
            "watermark_column": "w",
        },
        "C403",
        "",
    ),
    (LoadConfig, {"mode": "full", "cdc": {"preset": "delta_cdf"}}, "C404", ""),
    (LoadConfig, {"mode": "full", "watermark_format": "x"}, "C403", ""),
    (
        LoadConfig,
        {
            "mode": "incremental_watermark",
            "watermark_column": "w",
            "watermark_type": "int",
            "watermark_format": "x",
        },
        "C404",
        "",
    ),
]

TARGET_CASES = [
    (ColumnMapping, {"expr": "a", "drop": True}, "C402", ""),
    (Target, {"alias": "a", "audit_columns_exclude": [""]}, "C401", ""),
    (Target, {}, "C403", ""),
    (Target, {"alias": "a", "connection": "c", "table": "t"}, "C402", ""),
    (Target, {"connection": "c", "path": "p"}, "C403", ""),
    (Target, {"table": "t", "alias": "a"}, "C403", ""),
    (Target, {"alias": "a", "dimension": _dim(), "fact": {"foreign_keys": ["a"]}}, "C402", ""),
]

THREAD_CASES = [
    (Thread, _t(sources={}), "C401", "/sources"),
    (
        Thread,
        _t(
            sources={"a": {**_DELTA, "dedup": {"keys": ["k"], "order_by": "w ASC"}}},
            load={
                "mode": "incremental_watermark",
                "watermark_column": "w",
                "watermark_type": "date",
            },
        ),
        "C441",
        "",
    ),
    (
        Thread,
        _t(
            sources={"a": {**_DELTA, "dedup": {"keys": ["k"]}}},
            load={"mode": "cdc", "cdc": {"preset": "delta_cdf"}},
        ),
        "C404",
        "",
    ),
    (
        Thread,
        _t(
            target={"alias": "x", "dimension": _dim()},
            load={"mode": "cdc", "cdc": {"preset": "delta_cdf"}},
        ),
        "C404",
        "",
    ),
    (
        Thread,
        _t(**{"with": {"a": {"from": "a", "steps": [{"filter": {"expr": "1=1"}}]}}}),
        "C442",
        "",
    ),
    (
        Thread,
        _t(**{"with": {"c": {"from": "zzz", "steps": [{"filter": {"expr": "1=1"}}]}}}),
        "C443",
        "",
    ),
    (Thread, _with_alias("a"), "C442", ""),
    (
        Thread,
        _t(
            sources={"a": _DELTA, "b": _DELTA},
            steps=[
                {"join": {"source": "b", "type": "left", "on": ["id"], "alias": "j"}},
                {"join": {"source": "b", "type": "left", "on": ["id"], "alias": "j"}},
            ],
        ),
        "C442",
        "",
    ),
]

VALIDATION_CASES = [
    (Assertion, {"type": "fk_sentinel_rate", "sentinel": 1, "max_rate": 0.1}, "C403", ""),
    (Assertion, {"type": "fk_sentinel_rate", "column": "c", "max_rate": 0.1}, "C403", ""),
    (
        Assertion,
        {"type": "fk_sentinel_rate", "column": "c", "columns": ["d"], "sentinel": 1},
        "C402",
        "",
    ),
    (
        Assertion,
        {"type": "fk_sentinel_rate", "column": "c", "sentinel": 1, "sentinels": {"x": 1}},
        "C402",
        "",
    ),
]

_COLS = [{"name": "a", "type": "int"}, {"name": "b", "type": "int"}]
WARP_CASES = [
    (WarpConfig, {"config_version": "1.0", "columns": _COLS + _COLS[:1]}, "C444", ""),
    (
        WarpConfig,
        {"config_version": "1.0", "columns": _COLS, "keys": {"surrogate": "zz"}},
        "C445",
        "",
    ),
    (
        WarpConfig,
        {"config_version": "1.0", "columns": _COLS, "keys": {"business": ["a", "zz"]}},
        "C445",
        "",
    ),
]

WRITE_CASES = [
    (WriteConfig, {"mode": "merge"}, "C403", ""),
    (WriteConfig, {"on_no_match_source": "soft_delete"}, "C403", ""),
]

ALL_CASES = (
    SOURCE_CASES
    + COLUMN_SET_CASES
    + DIMENSION_CASES
    + EXPORT_CASES
    + HOOK_CASES
    + LOAD_CASES
    + TARGET_CASES
    + THREAD_CASES
    + VALIDATION_CASES
    + WARP_CASES
    + WRITE_CASES
)


def _case_id(case: tuple[Any, ...]) -> str:
    return f"{case[0].__name__}-{case[2]}-{list(case[1])[:3]}"


@pytest.mark.parametrize("case", ALL_CASES, ids=_case_id)
def test_rule_reports_its_code(case: tuple[type[BaseModel], dict[str, Any], str, str]) -> None:
    model, data, code, path = case
    with pytest.raises(ValidationError) as exc_info:
        model.model_validate(data)
    diagnostics = from_validation_error(exc_info.value, data)
    assert len(diagnostics) == 1, diagnostics
    diag = diagnostics[0]
    assert (diag.code, diag.resolved_path) == (code, path)
    assert diag.severity is Severity.ERROR


def test_step_expected_values_travel_with_invalid_step() -> None:
    data = {
        "type": "date_sequence",
        "column": "d",
        "start": "2024-01-01",
        "end": "2024-02-01",
        "step": "hour",
    }
    with pytest.raises(ValidationError) as exc_info:
        Source.model_validate(data)
    (diag,) = from_validation_error(exc_info.value, data)
    assert diag.expected == ("day", "month", "week", "year")


def test_unsupported_source_type_lists_allowed_types() -> None:
    data = {"type": "parqet", "path": "p"}
    with pytest.raises(ValidationError) as exc_info:
        Source.model_validate(data)
    (diag,) = from_validation_error(exc_info.value, data)
    assert diag.expected == SOURCE_TYPES
    assert diag.hint is None


def test_excel_source_type_carries_a_hint() -> None:
    data = {"type": "excel", "path": "p"}
    with pytest.raises(ValidationError) as exc_info:
        Source.model_validate(data)
    (diag,) = from_validation_error(exc_info.value, data)
    assert diag.code == "C103"
    assert diag.expected == SOURCE_TYPES
    assert diag.hint is not None


def test_every_code_this_area_uses_is_exercised() -> None:
    covered = {case[2] for case in ALL_CASES}
    own = {f"C{n}" for n in range(440, 446)} | {"R101"}
    shared = {"C103", "C105", "C401", "C402", "C403", "C404", "C406"}
    assert covered == own | shared
    assert covered <= set(REGISTRY)


_EXCLUDED = {"pipeline.py", "sub_pipeline.py"}


def _plain_raises(path: Path) -> list[int]:
    tree = ast.parse(path.read_text())
    lines: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Raise) or node.exc is None:
            continue
        exc = node.exc
        if (
            isinstance(exc, ast.Call)
            and isinstance(exc.func, ast.Name)
            and exc.func.id in {"ValueError", "TypeError"}
        ):
            lines.append(node.lineno)
    return lines


def test_no_model_validator_raises_a_plain_error() -> None:
    model_dir = Path(model_package.__path__[0])
    offenders = {
        path.name: lines
        for path in sorted(model_dir.glob("*.py"))
        if path.name not in _EXCLUDED and (lines := _plain_raises(path))
    }
    assert offenders == {}
