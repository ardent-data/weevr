"""Standalone runs: each executable kind runs on its own, with no borrowed cascade.

A thread, a weave and a loom each load and plan from the document, the project's
``weevr.yaml`` and params alone. A thread run on its own does not pick up the
defaults of a loom or weave that happens to reference it, and a bare thread with no
``weevr.yaml`` above it loads with no project.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from documents import TEST_PROJECT_ID, write_document, write_project
from weevr import Context
from weevr.config import load_config, load_resolved
from weevr.model.thread import Thread

_THREAD = (
    'config_version: "2.0"\n'
    "sources:\n  data:\n    type: delta\n    alias: raw\n"
    "target:\n  alias: out\n"
)


def _ctx(project: Path, **kwargs) -> Context:
    return Context(spark=MagicMock(spec=SparkSession), project=str(project), **kwargs)


def _project(tmp_path: Path, loom_defaults: str = "") -> Path:
    """A project whose loom declares defaults and references a weave of thread ``t``."""
    project = write_project(tmp_path / "p.weevr")
    (project / "threads").mkdir()
    write_document(project / "threads" / "t.thread", _THREAD)
    write_document(
        project / "w.weave", 'config_version: "2.0"\nthreads:\n  - ref: threads/t.thread\n'
    )
    write_document(
        project / "l.loom",
        'config_version: "2.0"\nweaves:\n  - ref: w.weave\n' + loom_defaults,
    )
    return project


class TestStandalonePlan:
    """Each executable kind plans from its document, ``weevr.yaml`` and params."""

    @pytest.mark.parametrize("path", ["threads/t.thread", "w.weave", "l.loom"])
    def test_plan_succeeds(self, tmp_path: Path, path: str) -> None:
        project = _project(tmp_path)
        result = _ctx(project).run(path, mode="plan")
        assert result.status == "success"
        assert result.project is not None
        assert str(result.project.id) == TEST_PROJECT_ID
        assert result.project_root == str(project)


class TestNoLoomDefaultsLeak:
    """A standalone thread run sees none of the loom's defaults."""

    _LOOM_DEFAULTS = "defaults:\n  naming:\n    columns: snake_case\n"

    def _naming(self, thread: Thread):
        return thread.target.naming

    def test_standalone_thread_has_no_loom_defaults(self, tmp_path: Path) -> None:
        project = _project(tmp_path, self._LOOM_DEFAULTS)
        thread = _ctx(project).load("threads/t.thread").model
        assert isinstance(thread, Thread)
        assert self._naming(thread) is None

    def test_thread_via_loom_has_the_defaults(self, tmp_path: Path) -> None:
        project = _project(tmp_path, self._LOOM_DEFAULTS)
        loaded = _ctx(project).load("l.loom")
        threads = [
            t for by_name in loaded.document.weave_threads().values() for t in by_name.values()
        ]
        assert len(threads) == 1
        assert self._naming(threads[0]) is not None

    def test_standalone_plan_does_not_error_with_loom_defaults_present(
        self, tmp_path: Path
    ) -> None:
        project = _project(tmp_path, self._LOOM_DEFAULTS)
        assert _ctx(project).run("threads/t.thread", mode="plan").status == "success"


class TestStandaloneParams:
    """Params given to the Context bind into a standalone document."""

    def test_param_binds_into_standalone_thread(self, tmp_path: Path) -> None:
        project = write_project(tmp_path / "p.weevr")
        write_document(
            project / "t.thread",
            'config_version: "2.0"\n'
            "params:\n  tbl:\n    name: tbl\n    type: string\n    required: true\n"
            "sources:\n  data:\n    type: delta\n    alias: raw\n"
            'target:\n  alias: "${param.tbl}"\n',
        )
        ctx = _ctx(project, params={"tbl": "bound_table"})
        assert ctx.run("t.thread", mode="plan").status == "success"
        thread = ctx.load("t.thread").model
        assert thread.target.alias == "bound_table"


class TestBareThreadWithoutProject:
    """A lone thread with no ``weevr.yaml`` above it loads with no project."""

    def test_core_load_has_no_project(self, tmp_path: Path) -> None:
        write_document(tmp_path / "t.thread", _THREAD)
        loaded = load_resolved(tmp_path / "t.thread")
        assert loaded.project is None
        assert isinstance(loaded.model, Thread)

    def test_load_config_without_project_root(self, tmp_path: Path) -> None:
        write_document(tmp_path / "t.thread", _THREAD)
        assert isinstance(load_config(tmp_path / "t.thread"), Thread)
