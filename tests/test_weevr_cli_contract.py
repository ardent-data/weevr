"""Lock the weevr-core surface that weevr-cli depends on.

weevr-cli (https://github.com/ardent-data/weevr-cli) installs weevr-core and
imports the names below, reads the listed attributes, and matches one error
message. A failure here, including an import error at collection, means a
weevr change would break the CLI: keep the surface, or ship the change together
with a weevr-cli release that adapts.
"""

from __future__ import annotations

import dataclasses
import inspect
import logging
import shutil
import types
import typing
from importlib.metadata import distribution, version
from pathlib import Path

import pytest

from weevr.config import load_config, load_resolved
from weevr.config.parser import TYPED_EXTENSIONS
from weevr.config.validation import validate_incremental_config
from weevr.engine.planner import ExecutionPlan
from weevr.errors import (
    ConfigError,
    ConfigSchemaError,
    ReferenceResolutionError,
    VariableResolutionError,
    WeevError,
)
from weevr.model import Thread
from weevr.result import LoadedConfig, PlanIssue

FIXTURE_PROJECT = Path(__file__).parent / "weevr" / "fixtures" / "load_parity" / "parity.weevr"


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """A copy of the parity fixture project."""
    target = tmp_path / "parity.weevr"
    shutil.copytree(FIXTURE_PROJECT, target)
    return target


def _field_model(model: type, field: str) -> type:
    """The model class behind an optional sub-model field (``X | None``)."""
    annotation = model.model_fields[field].annotation  # type: ignore[attr-defined]
    if isinstance(annotation, types.UnionType) or typing.get_origin(annotation) is typing.Union:
        return next(a for a in typing.get_args(annotation) if a is not type(None))
    return annotation


def test_distribution_name() -> None:
    assert version("weevr-core")


def test_typed_extensions() -> None:
    assert {".thread", ".weave", ".loom"} <= set(TYPED_EXTENSIONS)


def test_load_resolved_keywords() -> None:
    assert {"runtime_params", "project_root"} <= set(inspect.signature(load_resolved).parameters)


def test_error_classes_caught_by_type() -> None:
    for error in (ConfigSchemaError, ReferenceResolutionError, VariableResolutionError):
        assert issubclass(error, ConfigError)
    assert issubclass(ConfigError, WeevError)


def test_loaded_config_surface(project: Path) -> None:
    loaded = load_resolved(project / "nightly.loom", project_root=project)
    assert isinstance(loaded, LoadedConfig)
    assert loaded.resolved_weaves
    assert loaded.resolved_threads
    assert isinstance(loaded.warnings, tuple)
    assert isinstance(loaded.validate_plans(), tuple)
    plans = loaded.execution_plan
    assert plans
    assert isinstance(plans[0], ExecutionPlan)


def test_execution_plan_fields() -> None:
    assert {
        "weave_name",
        "dependencies",
        "explicit_dependencies",
        "execution_order",
        "cache_targets",
        "lookup_schedule",
    } <= set(ExecutionPlan.model_fields)


def test_plan_issue_fields() -> None:
    assert {"weave_name", "message"} <= {f.name for f in dataclasses.fields(PlanIssue)}


def test_thread_fields_read() -> None:
    assert {"qualified_key", "load", "write"} <= set(Thread.model_fields)
    load_fields = set(_field_model(Thread, "load").model_fields)  # type: ignore[attr-defined]
    assert {"mode", "watermark_inclusive", "watermark_column", "cdc"} <= load_fields
    cdc_model = _field_model(_field_model(Thread, "load"), "cdc")
    assert callable(getattr(cdc_model, "model_dump", None))
    assert "mode" in _field_model(Thread, "write").model_fields  # type: ignore[attr-defined]


def test_engine_registers_no_weevr_command() -> None:
    """weevr-cli owns the ``weevr`` console script."""
    for dist in ("weevr", "weevr-core"):
        entry_points = distribution(dist).entry_points
        scripts = {ep.name for ep in entry_points if ep.group == "console_scripts"}
        assert "weevr" not in scripts


def test_validate_incremental_config_shape() -> None:
    assert validate_incremental_config({"load": {"mode": "full"}}) == []


def test_missing_parameter_error(tmp_path: Path) -> None:
    """weevr-cli spots a missing runtime parameter by this message and key."""
    cfg = tmp_path / "t.thread"
    cfg.write_text(
        'config_version: "1.0"\nparams:\n  region:\n    type: string\n    required: true\n'
        "sources:\n  d:\n    type: delta\n    alias: raw.${param.region}\n"
        "target:\n  alias: out.x\n"
    )
    with pytest.raises(ConfigError) as exc:
        load_config(cfg)
    assert exc.value.config_key == "region"
    assert "Required parameter" in exc.value.message
    assert exc.value.file_path == str(cfg)


def test_error_attributes_read() -> None:
    cause = ValueError("x")
    err = WeevError("boom", cause=cause)
    assert (err.message, err.cause) == ("boom", cause)


def test_load_warnings_logged_under_weevr(project: Path, caplog: pytest.LogCaptureFixture) -> None:
    """weevr-cli silences the ``weevr`` logger and reads warnings from the result."""
    with caplog.at_level(logging.WARNING, logger="weevr"):
        loaded = load_resolved(project / "nightly.loom", project_root=project)
    logged = [r.getMessage() for r in caplog.records if r.name.startswith("weevr")]
    assert loaded.warnings
    assert logged == list(loaded.warnings)
