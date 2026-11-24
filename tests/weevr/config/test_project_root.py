"""Project roots: found by ``weevr.yaml``, walked up to from a loaded file."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from documents import write_document
from weevr.config import find_project_root, load_config, load_resolved
from weevr.config.locations import LocalConfigLocation, RemoteConfigLocation, make_location
from weevr.config.root import read_project
from weevr.errors import ConfigError
from weevr.model import Thread

PROJECT_ID = "8f9d2c4e-1b3a-4c5d-9e6f-7a8b9c0d1e2f"

THREAD = """\
config_version: "2.0"
sources:
  main:
    type: csv
    path: data/in.csv
target:
  path: out
"""


def _project(root: Path, name: str = "sales", project_id: str = PROJECT_ID) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    write_document(
        root / "weevr.yaml",
        {"config_version": "2.0", "id": project_id, "name": name},
        kind="project",
    )
    return root


def _thread(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return write_document(path, THREAD)


class TestFindProjectRoot:
    """``find_project_root`` returns the nearest ancestor holding ``weevr.yaml``."""

    def test_root_itself(self, tmp_path: Path) -> None:
        root = _project(tmp_path / "proj")
        found = find_project_root(make_location(root))
        assert found is not None
        assert Path(str(found)) == root

    def test_from_a_nested_file(self, tmp_path: Path) -> None:
        root = _project(tmp_path / "proj")
        thread = _thread(root / "threads" / "silver" / "t.thread")
        found = find_project_root(make_location(thread))
        assert found is not None
        assert Path(str(found)) == root

    def test_nearest_wins(self, tmp_path: Path) -> None:
        _project(tmp_path / "outer")
        inner = _project(tmp_path / "outer" / "inner")
        thread = _thread(inner / "threads" / "t.thread")
        found = find_project_root(make_location(thread))
        assert found is not None
        assert Path(str(found)) == inner

    def test_none_when_absent(self, tmp_path: Path) -> None:
        thread = _thread(tmp_path / "loose" / "t.thread")
        assert find_project_root(make_location(thread)) is None

    def test_relative_location_walks_real_ancestors(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        root = _project(tmp_path / "proj")
        _thread(root / "threads" / "t.thread")
        monkeypatch.chdir(root / "threads")
        found = find_project_root(LocalConfigLocation(Path("t.thread")))
        assert found is not None
        assert Path(str(found)) == root

    def test_remote_walk_stops_at_files(self) -> None:
        """A remote walk probes up to the lakehouse ``Files`` folder, never above it."""
        probed: list[str] = []

        def _exists(self: RemoteConfigLocation) -> bool:
            probed.append(str(self))
            return False

        base = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh"
        start = RemoteConfigLocation(f"{base}/Files/proj/threads/t.thread", MagicMock())
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(RemoteConfigLocation, "exists", _exists)
            assert find_project_root(start) is None
        assert probed[-1] == f"{base}/Files/weevr.yaml"
        assert all(p.startswith(f"{base}/Files/") for p in probed)

    def test_remote_root_found(self) -> None:
        base = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Files/proj"

        def _exists(self: RemoteConfigLocation) -> bool:
            return str(self) == f"{base}/weevr.yaml"

        start = RemoteConfigLocation(f"{base}/threads/t.thread", MagicMock())
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(RemoteConfigLocation, "exists", _exists)
            found = find_project_root(start)
        assert str(found) == base


class TestReadProject:
    """``read_project`` loads a root's ``weevr.yaml``, or ``None`` when it has none."""

    def test_reads(self, tmp_path: Path) -> None:
        project = read_project(make_location(_project(tmp_path / "p")))
        assert project is not None
        assert project.id == UUID(PROJECT_ID)

    def test_absent(self, tmp_path: Path) -> None:
        assert read_project(make_location(tmp_path)) is None

    def test_wrong_kind(self, tmp_path: Path) -> None:
        write_document(tmp_path / "weevr.yaml", THREAD, kind="thread")
        with pytest.raises(ConfigError) as exc_info:
            read_project(make_location(tmp_path))
        assert [d.code for d in exc_info.value.diagnostics] == ["C207"]


class TestCoreLoadWalksUp:
    """A core-API load with no root uses the nearest ``weevr.yaml`` above the file."""

    def test_load_resolved_uses_walk_up_root(self, tmp_path: Path) -> None:
        root = _project(tmp_path / "proj")
        thread = _thread(root / "threads" / "t.thread")
        loaded = load_resolved(thread)
        assert loaded.project is not None
        assert loaded.project.name == "sales"
        assert Path(loaded.project_root) == root
        assert loaded.document.project == loaded.project
        assert loaded.model.qualified_key == "threads/t.thread"
        assert loaded.model.sources["main"].path == str(root / "data" / "in.csv")

    def test_load_config_uses_walk_up_root(self, tmp_path: Path) -> None:
        root = _project(tmp_path / "proj")
        thread = _thread(root / "threads" / "t.thread")
        model = load_config(thread)
        assert isinstance(model, Thread)
        assert model.sources["main"].path == str(root / "data" / "in.csv")

    def test_no_project_falls_back_to_file_folder(self, tmp_path: Path) -> None:
        thread = _thread(tmp_path / "loose" / "t.thread")
        loaded = load_resolved(thread)
        assert loaded.project is None
        assert Path(loaded.project_root) == thread.parent
        assert loaded.model.sources["main"].path == str(thread.parent / "data" / "in.csv")

    def test_explicit_root_reads_its_project(self, tmp_path: Path) -> None:
        root = _project(tmp_path / "proj")
        thread = _thread(root / "t.thread")
        loaded = load_resolved(thread, project_root=root)
        assert loaded.project is not None
        assert Path(loaded.project_root) == root

    def test_explicit_root_without_project(self, tmp_path: Path) -> None:
        thread = _thread(tmp_path / "t.thread")
        loaded = load_resolved(thread, project_root=tmp_path)
        assert loaded.project is None
        assert Path(loaded.project_root) == tmp_path

    def test_invalid_walked_up_project_fails(self, tmp_path: Path) -> None:
        root = tmp_path / "proj"
        root.mkdir()
        write_document(root / "weevr.yaml", {"config_version": "2.0", "name": "x"}, kind="project")
        thread = _thread(root / "t.thread")
        with pytest.raises(ConfigError) as exc_info:
            load_resolved(thread)
        assert [d.file for d in exc_info.value.diagnostics] == [str(root / "weevr.yaml")]
