"""Local Spark integration tests for RemoteConfigLocation.

These tests exercise the real Hadoop FileSystem JVM bridge against a
``file://`` URI under ``tmp_path``. They are gated behind the ``spark``
pytest marker (deselected by default — see pyproject.toml addopts).
"""

from pathlib import Path

import pytest

from weevr.config.locations import RemoteConfigLocation, make_location

pytestmark = pytest.mark.spark


def _file_uri(path: Path) -> str:
    """Convert a local path to a ``file://`` URI string."""
    return "file://" + str(path)


class TestRemoteConfigLocationFileUri:
    def test_exists_true_against_real_file(self, spark, tmp_path: Path) -> None:
        f = tmp_path / "silver.loom"
        f.write_text("config_version: '1.0'\nname: silver\n", encoding="utf-8")
        loc = RemoteConfigLocation(_file_uri(f), spark)
        assert loc.exists() is True

    def test_exists_false_against_missing_file(self, spark, tmp_path: Path) -> None:
        loc = RemoteConfigLocation(_file_uri(tmp_path / "missing.loom"), spark)
        assert loc.exists() is False

    def test_read_text_roundtrip(self, spark, tmp_path: Path) -> None:
        content = "config_version: '1.0'\nname: silver\nweaves: []\n"
        f = tmp_path / "silver.loom"
        f.write_text(content, encoding="utf-8")
        loc = RemoteConfigLocation(_file_uri(f), spark)
        assert loc.read_text() == content

    def test_read_text_missing_raises_filenotfound(self, spark, tmp_path: Path) -> None:
        loc = RemoteConfigLocation(_file_uri(tmp_path / "absent.loom"), spark)
        with pytest.raises(FileNotFoundError):
            loc.read_text()

    def test_join_then_read(self, spark, tmp_path: Path) -> None:
        sub = tmp_path / "dimensions"
        sub.mkdir()
        f = sub / "dim_customer.thread"
        f.write_text("config_version: '1.0'\n", encoding="utf-8")

        root = RemoteConfigLocation(_file_uri(tmp_path), spark)
        leaf = root.join("dimensions/dim_customer.thread")
        assert isinstance(leaf, RemoteConfigLocation)
        assert leaf.exists() is True
        assert leaf.read_text() == "config_version: '1.0'\n"

    def test_dot_dot_normalization_against_real_file(self, spark, tmp_path: Path) -> None:
        f = tmp_path / "silver.loom"
        f.write_text("name: silver\n", encoding="utf-8")
        sub = RemoteConfigLocation(_file_uri(tmp_path / "sub"), spark)
        # Normalized join walks back up to silver.loom.
        leaf = sub.join("../silver.loom")
        assert leaf.exists() is True
        assert leaf.read_text() == "name: silver\n"

    def test_make_location_with_file_uri(self, spark, tmp_path: Path) -> None:
        f = tmp_path / "x.thread"
        f.write_text("hello: world\n", encoding="utf-8")
        loc = make_location(_file_uri(f), spark)
        assert isinstance(loc, RemoteConfigLocation)
        assert loc.read_text() == "hello: world\n"
