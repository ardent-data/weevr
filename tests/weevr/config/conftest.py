"""Shared fixtures for config loading tests."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

MakeProject = Callable[..., Path]


@pytest.fixture
def make_project(tmp_path: Path) -> MakeProject:
    """Write a ``p.weevr`` project: one thread, a weave over it, a loom over the weave.

    ``weave`` and ``loom`` add or override top-level keys of ``w.weave`` and
    ``l.loom`` (for example ``lookups``, ``defaults``, or replacement
    ``threads`` / ``weaves`` lists). Files are written as JSON, which YAML reads.
    """

    def make(
        thread: dict[str, Any] | None = None,
        *,
        thread_file: str = "fact.thread",
        weave: dict[str, Any] | None = None,
        loom: dict[str, Any] | None = None,
    ) -> Path:
        project = tmp_path / "p.weevr"
        project.mkdir()
        if thread is not None:
            (project / thread_file).write_text(json.dumps(thread))
        weave_body = {"config_version": "1.0", "threads": [{"ref": thread_file}], **(weave or {})}
        (project / "w.weave").write_text(json.dumps(weave_body))
        loom_body = {"config_version": "1.0", "weaves": [{"ref": "w.weave"}], **(loom or {})}
        (project / "l.loom").write_text(json.dumps(loom_body))
        return project

    return make
