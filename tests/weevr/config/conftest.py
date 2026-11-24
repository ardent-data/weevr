"""Shared fixtures for config loading tests."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from documents import write_document

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
            write_document(project / thread_file, json.dumps(thread))
        weave_body = {"config_version": "2.0", "threads": [{"ref": thread_file}], **(weave or {})}
        write_document(project / "w.weave", json.dumps(weave_body))
        loom_body = {"config_version": "2.0", "weaves": [{"ref": "w.weave"}], **(loom or {})}
        write_document(project / "l.loom", json.dumps(loom_body))
        return project

    return make
