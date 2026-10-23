"""Tests for the shared config load (weevr.config.loader)."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import pytest

from weevr.config import load_config, load_resolved
from weevr.config.loader import resolve_source_paths
from weevr.errors import ConfigError, VariableResolutionError
from weevr.model import Thread, Weave


class TestResolveSourcePaths:
    """resolve_source_paths joins relative file-Source paths onto the project root."""

    def test_relative_path_resolved_with_local_root(self, tmp_path: Path) -> None:
        thread_dict: dict = {
            "sources": {"customers": {"type": "csv", "path": "data/customers.csv"}}
        }
        resolve_source_paths(thread_dict, tmp_path)
        assert thread_dict["sources"]["customers"]["path"] == str(tmp_path / "data/customers.csv")

    def test_relative_path_resolved_with_abfs_root(self) -> None:
        root = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Files/project.weevr"
        thread_dict: dict = {
            "sources": {"customers": {"type": "csv", "path": "data/customers.csv"}}
        }
        resolve_source_paths(thread_dict, root)
        assert thread_dict["sources"]["customers"]["path"] == f"{root}/data/customers.csv"

    def test_absolute_path_unchanged(self) -> None:
        thread_dict: dict = {
            "sources": {"main": {"type": "csv", "path": "/absolute/path/data.csv"}}
        }
        resolve_source_paths(thread_dict, Path("/project.weevr"))
        assert thread_dict["sources"]["main"]["path"] == "/absolute/path/data.csv"

    def test_uri_path_unchanged(self) -> None:
        uri = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Files/data.csv"
        thread_dict: dict = {"sources": {"main": {"type": "csv", "path": uri}}}
        resolve_source_paths(thread_dict, Path("/project.weevr"))
        assert thread_dict["sources"]["main"]["path"] == uri

    def test_delta_source_skipped(self) -> None:
        thread_dict: dict = {"sources": {"main": {"type": "delta", "alias": "raw.customers"}}}
        resolve_source_paths(thread_dict, Path("/project.weevr"))
        assert "path" not in thread_dict["sources"]["main"]

    def test_no_sources_is_noop(self) -> None:
        thread_dict: dict = {"name": "test"}
        resolve_source_paths(thread_dict, Path("/project.weevr"))
        assert thread_dict == {"name": "test"}

    def test_fabric_mount_prefix_stripped(self) -> None:
        root = Path("/lakehouse/default/Files/fabcon.weevr")
        thread_dict: dict = {
            "sources": {"customers": {"type": "csv", "path": "data/customers.csv"}}
        }
        resolve_source_paths(thread_dict, root)
        expected = "Files/fabcon.weevr/data/customers.csv"
        assert thread_dict["sources"]["customers"]["path"] == expected

    def test_abfs_root_trailing_slash_normalized(self) -> None:
        root = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Files/project.weevr/"
        thread_dict: dict = {"sources": {"main": {"type": "json", "path": "data/events.json"}}}
        resolve_source_paths(thread_dict, root)
        expected = (
            "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Files/project.weevr/data/events.json"
        )
        assert thread_dict["sources"]["main"]["path"] == expected


FIXTURE_PROJECT = Path(__file__).parent.parent / "fixtures" / "load_parity" / "parity.weevr"


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """A copy of the parity fixture project."""
    target = tmp_path / "parity.weevr"
    shutil.copytree(FIXTURE_PROJECT, target)
    return target


class TestLoadResolvedShapes:
    """load_resolved returns typed children for each config kind."""

    def test_weave_threads_keyed_by_effective_name(self, project: Path) -> None:
        loaded = load_resolved(project / "weaves/sales.weave", project_root=project)
        threads = loaded.resolved_threads["sales"]
        assert list(threads) == ["stg_orders", "orders_east", "orders_west"]
        assert all(isinstance(t, Thread) for t in threads.values())
        assert threads["orders_east"].target.alias == "gold.orders_east"
        assert dict(loaded.resolved_weaves) == {}
        assert [p.weave_name for p in loaded.execution_plan] == ["sales"]

    def test_loom_returns_every_weave_and_plan(self, project: Path) -> None:
        loaded = load_resolved(project / "nightly.loom", project_root=project)
        assert list(loaded.resolved_weaves) == ["sales", "inline_ops"]
        assert all(isinstance(w, Weave) for w in loaded.resolved_weaves.values())
        assert list(loaded.resolved_threads) == ["sales", "inline_ops"]
        assert list(loaded.resolved_threads["inline_ops"]) == ["ops_audit", "ops_summary"]
        assert [p.weave_name for p in loaded.execution_plan] == ["sales", "inline_ops"]

    def test_thread_has_no_children(self, project: Path) -> None:
        loaded = load_resolved(project / "threads/stg_orders.thread", project_root=project)
        assert isinstance(loaded.model, Thread)
        assert dict(loaded.resolved_threads) == {}
        assert dict(loaded.resolved_weaves) == {}
        assert loaded.execution_plan is None
        assert loaded.model.sources["orders"].path == str(project / "data" / "orders.csv")

    def test_project_root_defaults_to_file_directory(self, project: Path) -> None:
        loaded = load_resolved(project / "nightly.loom")
        assert loaded.model.qualified_key == "nightly.loom"
        assert list(loaded.resolved_weaves) == ["sales", "inline_ops"]


class TestLoadResolvedWarnings:
    """Load warnings come back as data and are logged."""

    def test_warnings_returned_and_logged(
        self, project: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="weevr.config"):
            loaded = load_resolved(project / "weaves/sales.weave", project_root=project)
        assert isinstance(loaded.warnings, tuple)
        assert len(loaded.warnings) == 1
        assert "thread 'stg_orders' declares an execution block" in loaded.warnings[0]
        assert [r.message for r in caplog.records if r.name == "weevr.config"] == list(
            loaded.warnings
        )


class TestLoadResolvedReadOnly:
    """The children mappings can't be mutated."""

    def test_threads_outer_mapping_is_read_only(self, project: Path) -> None:
        loaded = load_resolved(project / "nightly.loom")
        with pytest.raises(TypeError):
            loaded.resolved_threads["x"] = {}  # type: ignore[index]

    def test_threads_inner_mapping_is_read_only(self, project: Path) -> None:
        loaded = load_resolved(project / "nightly.loom")
        with pytest.raises(TypeError):
            loaded.resolved_threads["sales"]["x"] = None  # type: ignore[index]

    def test_weaves_mapping_is_read_only(self, project: Path) -> None:
        loaded = load_resolved(project / "nightly.loom")
        with pytest.raises(TypeError):
            loaded.resolved_weaves["x"] = None  # type: ignore[index]


class TestLoadResolvedRejects:
    """load_resolved only takes typed thread, weave, and loom files."""

    def test_yaml_extension_rejected(self, tmp_path: Path) -> None:
        cfg = tmp_path / "x.yaml"
        cfg.write_text('config_version: "1.0"\nthreads:\n  - ref: t.thread\n')
        with pytest.raises(ConfigError, match=r"Expected \.thread, \.weave, or \.loom") as exc:
            load_resolved(cfg)
        assert exc.value.file_path == str(cfg)

    def test_name_stem_mismatch_rejected(self, tmp_path: Path) -> None:
        cfg = tmp_path / "t.thread"
        cfg.write_text(
            'config_version: "1.0"\nname: other\nsources:\n  d:\n    type: delta\n'
            "    alias: raw.x\ntarget:\n  alias: out\n"
        )
        with pytest.raises(ConfigError, match="does not match filename stem"):
            load_resolved(cfg)

    def test_fabric_reference_without_default_raises(self, tmp_path: Path) -> None:
        cfg = tmp_path / "t.thread"
        cfg.write_text(
            'config_version: "1.0"\nsources:\n  d:\n    type: delta\n'
            '    alias: "${fabric.workspace_id}"\ntarget:\n  alias: out\n'
        )
        with pytest.raises(VariableResolutionError, match="fabric.workspace_id"):
            load_resolved(cfg)


_DUP_THREAD = """\
config_version: "1.0"
sources:
  data:
    type: delta
    alias: raw.{name}
target:
  alias: out.{name}
"""


class TestDuplicateWeaveNames:
    """Two weave entries in a loom with the same effective name fail the load."""

    @staticmethod
    def _project(tmp_path: Path, weaves: str) -> Path:
        project = tmp_path / "dup.weevr"
        for folder, thread in (("a", "x"), ("b", "y")):
            (project / folder).mkdir(parents=True)
            (project / folder / f"{thread}.thread").write_text(_DUP_THREAD.format(name=thread))
            (project / folder / "main.weave").write_text(
                f'config_version: "1.0"\nthreads:\n  - ref: {folder}/{thread}.thread\n'
            )
        (project / "all.loom").write_text(f'config_version: "1.0"\nweaves:\n{weaves}')
        return project

    def test_same_stem_in_two_folders_rejected(self, tmp_path: Path) -> None:
        project = self._project(tmp_path, "  - ref: a/main.weave\n  - ref: b/main.weave\n")
        with pytest.raises(ConfigError, match="Duplicate weave name 'main'") as exc:
            load_resolved(project / "all.loom", project_root=project)
        assert "a/main.weave" in str(exc.value)
        assert "b/main.weave" in str(exc.value)
        assert "name:" in str(exc.value)

    def test_load_config_rejects_too(self, tmp_path: Path) -> None:
        project = self._project(tmp_path, "  - ref: a/main.weave\n  - ref: b/main.weave\n")
        with pytest.raises(ConfigError, match="Duplicate weave name 'main'"):
            load_config(project / "all.loom", project_root=project)

    def test_same_file_twice_rejected(self, tmp_path: Path) -> None:
        project = self._project(tmp_path, "  - ref: a/main.weave\n  - ref: a/main.weave\n")
        with pytest.raises(ConfigError, match="Duplicate weave name 'main'"):
            load_resolved(project / "all.loom", project_root=project)

    def test_inline_weave_with_same_name_rejected(self, tmp_path: Path) -> None:
        project = self._project(
            tmp_path,
            "  - ref: a/main.weave\n"
            "  - name: main\n"
            '    config_version: "1.0"\n'
            "    threads:\n"
            "      - ref: b/y.thread\n",
        )
        with pytest.raises(ConfigError, match="'a/main.weave' and an inline weave"):
            load_resolved(project / "all.loom", project_root=project)

    def test_explicit_name_keeps_both(self, tmp_path: Path) -> None:
        project = self._project(
            tmp_path, "  - ref: a/main.weave\n    name: staging\n  - ref: b/main.weave\n"
        )
        loaded = load_resolved(project / "all.loom", project_root=project)
        assert {name: list(t) for name, t in loaded.resolved_threads.items()} == {
            "staging": ["x"],
            "main": ["y"],
        }
        assert [(p.weave_name, p.threads) for p in loaded.execution_plan or []] == [
            ("staging", ["x"]),
            ("main", ["y"]),
        ]
