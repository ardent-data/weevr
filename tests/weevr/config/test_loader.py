"""Tests for the shared config load (weevr.config.loader)."""

from __future__ import annotations

from pathlib import Path

from weevr.config.loader import resolve_source_paths


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
