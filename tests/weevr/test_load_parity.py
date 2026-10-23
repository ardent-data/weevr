"""Pinned output of the config load across weave and loom configs.

Loads the ``load_parity`` fixture project through ``Context.load`` and compares
the hydrated models, their children, and the load warnings against committed
expectations. Set ``WEEVR_UPDATE_GOLDEN=1`` to rewrite the expectation files
after an intended change.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from weevr.context import Context

FIXTURE_PROJECT = Path(__file__).parent / "fixtures" / "load_parity" / "parity.weevr"
GOLDEN_DIR = Path(__file__).parent / "fixtures" / "load_parity" / "expected"

PARITY_CONFIGS = ["nightly.loom", "weaves/sales.weave", "threads/stg_orders.thread"]


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """Copy the fixture project so resolved paths are stable per test."""
    target = tmp_path / "parity.weevr"
    shutil.copytree(FIXTURE_PROJECT, target)
    return target


def _snapshot(loaded: Any, root: Path) -> dict[str, Any]:
    """Serialize a loaded config and its children with the project root masked."""
    data = {
        "config_type": loaded.config_type,
        "config_name": loaded.config_name,
        "model": loaded.model.model_dump(mode="json"),
        "weaves": {
            name: weave.model_dump(mode="json") for name, weave in (loaded._weaves or {}).items()
        },
        "threads": {
            weave: {name: thread.model_dump(mode="json") for name, thread in threads.items()}
            for weave, threads in (loaded._threads or {}).items()
        },
        "execution_order": [plan.execution_order for plan in loaded.execution_plan or []],
    }
    return json.loads(json.dumps(data).replace(str(root), "<root>"))


def _golden_path(config: str) -> Path:
    return GOLDEN_DIR / (config.replace("/", "__") + ".json")


def _check_golden(config: str, actual: dict[str, Any]) -> None:
    path = _golden_path(config)
    if os.environ.get("WEEVR_UPDATE_GOLDEN"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(actual, indent=2, sort_keys=True) + "\n")
    assert actual == json.loads(path.read_text())


class TestContextLoadParity:
    """``Context.load`` output for the parity corpus matches the pinned expectations."""

    @pytest.mark.parametrize("config", PARITY_CONFIGS)
    def test_load_matches_expectation(self, project: Path, config: str) -> None:
        ctx = Context(spark=MagicMock(spec=SparkSession), project=str(project))
        _check_golden(config, _snapshot(ctx.load(config), project))

    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            (
                "nightly.loom",
                [
                    "WARN: loom '(unnamed)' declares defaults.execution (log_level)",
                    "WARN: thread 'stg_orders' declares an execution block (log_level)",
                ],
            ),
            (
                "weaves/sales.weave",
                ["WARN: thread 'stg_orders' declares an execution block (log_level)"],
            ),
            (
                "threads/stg_orders.thread",
                ["WARN: thread '#0' declares an execution block (log_level)"],
            ),
        ],
    )
    def test_load_warnings(
        self,
        project: Path,
        config: str,
        expected: list[str],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        ctx = Context(spark=MagicMock(spec=SparkSession), project=str(project))
        with caplog.at_level(logging.WARNING, logger="weevr.context"):
            ctx.load(config)
        warnings = [r.message for r in caplog.records if r.message.startswith("WARN:")]
        assert [w.split(" — ")[0] for w in warnings] == expected


_CYCLE_THREAD = """\
config_version: "1.0"
sources:
  data:
    type: delta
    alias: raw.{name}
target:
  alias: out.{name}
"""

_CYCLE_WEAVE = """\
config_version: "1.0"
threads:
  - ref: a.thread
    dependencies: [b]
  - ref: b.thread
    dependencies: [a]
"""


class TestValidateWording:
    """``Context.run(mode="validate")`` DAG messages keep their current wording."""

    @staticmethod
    def _project(tmp_path: Path) -> Path:
        project = tmp_path / "cycle.weevr"
        project.mkdir()
        for name in ("a", "b"):
            (project / f"{name}.thread").write_text(_CYCLE_THREAD.format(name=name))
        (project / "cyc.weave").write_text(_CYCLE_WEAVE)
        (project / "cyc.loom").write_text('config_version: "1.0"\nweaves:\n  - ref: cyc.weave\n')
        return project

    @staticmethod
    def _dag_errors(project: Path, config: str) -> list[str]:
        ctx = Context(spark=MagicMock(spec=SparkSession), project=str(project))
        result = ctx.run(config, mode="validate")
        return [e for e in result.validation_errors or [] if e.startswith("DAG validation")]

    def test_weave_cycle_wording(self, tmp_path: Path) -> None:
        errors = self._dag_errors(self._project(tmp_path), "cyc.weave")
        assert errors == [
            "DAG validation failed: Circular dependency detected in weave 'cyc': a → b → a"
        ]

    def test_loom_cycle_wording(self, tmp_path: Path) -> None:
        errors = self._dag_errors(self._project(tmp_path), "cyc.loom")
        assert errors == [
            "DAG validation failed for weave 'cyc': "
            "Circular dependency detected in weave 'cyc': a → b → a"
        ]
