"""A thread that reads another thread's target and re-adds its audit columns warns at load."""

import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from documents import write_document
from weevr.config import load_resolved
from weevr.context import Context
from weevr.diagnostics import Diagnostic

from .conftest import MakeProject

_AUDIT = {"_loaded_at": "current_timestamp()", "_run": "'x'"}


def _upstream() -> dict[str, Any]:
    return {
        "config_version": "2.0",
        "sources": {"main": {"type": "delta", "alias": "raw.sales"}},
        "target": {"alias": "silver.sales"},
    }


def _downstream(**target: Any) -> dict[str, Any]:
    return {
        "config_version": "2.0",
        "sources": {"main": {"type": "delta", "alias": "silver.sales"}},
        "target": {"alias": "gold.sales", **target},
    }


def _project(make_project: MakeProject, downstream: dict[str, Any], defaults: Any = None) -> Path:
    project = make_project(
        _upstream(),
        thread_file="up.thread",
        weave={
            "threads": [{"ref": "up.thread"}, {"ref": "down.thread"}],
            "defaults": defaults if defaults is not None else {"target": {"audit_columns": _AUDIT}},
        },
    )
    write_document(project / "down.thread", json.dumps(downstream))
    return project


def _r150(
    make_project: MakeProject, downstream: dict[str, Any], defaults: Any = None
) -> list[Diagnostic]:
    project = _project(make_project, downstream, defaults)
    loaded = load_resolved(project / "w.weave", project_root=project)
    return [d for d in loaded.diagnostics if d.code == "R150"]


def test_chained_threads_inheriting_audit_columns_warn(make_project: MakeProject) -> None:
    (diag,) = _r150(make_project, _downstream())
    assert diag.node == "down"
    assert diag.is_error is False
    assert sorted(diag.expected or []) == sorted(_AUDIT)
    assert "audit_columns_exclude" in (diag.hint or "")
    assert diag.file is not None and diag.file.endswith("down.thread")
    assert diag.resolved_path == "/target/audit_columns"


def test_excluding_all_overlapping_columns_is_quiet(make_project: MakeProject) -> None:
    assert _r150(make_project, _downstream(audit_columns_exclude=["_*"])) == []


def test_partial_overlap_lists_only_overlapping_names(make_project: MakeProject) -> None:
    (diag,) = _r150(make_project, _downstream(audit_columns_exclude=["_run"]))
    assert list(diag.expected or []) == ["_loaded_at"]


def test_downstream_without_audit_columns_is_quiet(make_project: MakeProject) -> None:
    project = make_project(
        _upstream(),
        thread_file="up.thread",
        weave={"threads": [{"ref": "up.thread"}, {"ref": "down.thread"}]},
    )
    up = _upstream()
    up["target"]["audit_columns"] = _AUDIT
    write_document(project / "up.thread", json.dumps(up))
    write_document(project / "down.thread", json.dumps(_downstream()))
    loaded = load_resolved(project / "w.weave", project_root=project)
    assert [d for d in loaded.diagnostics if d.code == "R150"] == []


def test_unchained_threads_are_quiet(make_project: MakeProject) -> None:
    other = _downstream()
    other["sources"]["main"]["alias"] = "raw.other"
    assert _r150(make_project, other) == []


def test_loom_load_reports_it_too(make_project: MakeProject) -> None:
    project = _project(make_project, _downstream())
    loaded = load_resolved(project / "l.loom", project_root=project)
    assert [d.node for d in loaded.diagnostics if d.code == "R150"] == ["down"]


def test_validate_mode_reports_it_before_any_read(make_project: MakeProject) -> None:
    project = _project(make_project, _downstream())
    spark = MagicMock(spec=SparkSession)
    result = Context(spark=spark, project=str(project)).run("w.weave", mode="validate")
    assert [d.node for d in result.diagnostics if d.code == "R150"] == ["down"]


def test_cross_weave_edge_in_a_loom_warns(make_project: MakeProject) -> None:
    """A thread in the second weave reading the first weave's target warns."""
    project = make_project(
        _upstream(),
        thread_file="up.thread",
        weave={"threads": [{"ref": "up.thread"}]},
        loom={
            "weaves": [{"ref": "w.weave"}, {"ref": "w2.weave"}],
            "defaults": {"target": {"audit_columns": _AUDIT}},
        },
    )
    write_document(project / "down.thread", json.dumps(_downstream()))
    write_document(
        project / "w2.weave",
        json.dumps({"config_version": "2.0", "threads": [{"ref": "down.thread"}]}),
    )
    loaded = load_resolved(project / "l.loom", project_root=project)
    (diag,) = [d for d in loaded.diagnostics if d.code == "R150"]
    assert diag.node == "down"
    assert "thread 'up'" in diag.message
    assert sorted(diag.expected or []) == sorted(_AUDIT)


def test_warning_is_logged_like_any_load_warning(
    make_project: MakeProject, caplog: pytest.LogCaptureFixture
) -> None:
    project = _project(make_project, _downstream())
    with caplog.at_level(logging.WARNING, logger="weevr.config"):
        loaded = load_resolved(project / "w.weave", project_root=project)
    logged = [r.getMessage() for r in caplog.records if "audit columns" in r.getMessage()]
    assert len(logged) == 1
    assert sum(d.code == "R150" for d in loaded.diagnostics) == 1


def test_context_load_logs_it_once(
    make_project: MakeProject, caplog: pytest.LogCaptureFixture
) -> None:
    project = _project(make_project, _downstream())
    context = Context(spark=MagicMock(spec=SparkSession), project=str(project))
    with caplog.at_level(logging.WARNING):
        loaded = context.load("w.weave")
    logged = [r.getMessage() for r in caplog.records if "audit columns" in r.getMessage()]
    assert len(logged) == 1
    assert sum(d.code == "R150" for d in loaded.diagnostics) == 1
