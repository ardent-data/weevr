"""Unit tests for the ConfigLocation abstraction."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from weevr.config.locations import (
    ConfigLocation,
    LocalConfigLocation,
    RemoteConfigLocation,
    _normalize_uri_path,
    _split_scheme,
    make_location,
)

# ---------------------------------------------------------------------------
# LocalConfigLocation
# ---------------------------------------------------------------------------


class TestLocalConfigLocation:
    def test_join_relative(self, tmp_path: Path) -> None:
        loc = LocalConfigLocation(tmp_path)
        joined = loc.join("foo/bar.thread")
        assert isinstance(joined, LocalConfigLocation)
        assert joined.path == tmp_path / "foo" / "bar.thread"

    def test_join_rejects_absolute(self, tmp_path: Path) -> None:
        loc = LocalConfigLocation(tmp_path)
        with pytest.raises(ValueError, match="absolute"):
            loc.join("/etc/passwd")

    def test_exists_true(self, tmp_path: Path) -> None:
        f = tmp_path / "x.yaml"
        f.write_text("a: 1")
        assert LocalConfigLocation(f).exists() is True

    def test_exists_false(self, tmp_path: Path) -> None:
        assert LocalConfigLocation(tmp_path / "missing").exists() is False

    def test_read_text_roundtrip(self, tmp_path: Path) -> None:
        f = tmp_path / "x.yaml"
        f.write_text("hello: world\n", encoding="utf-8")
        assert LocalConfigLocation(f).read_text() == "hello: world\n"

    def test_read_text_missing_raises_filenotfound(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            LocalConfigLocation(tmp_path / "missing.yaml").read_text()

    def test_is_relative_to_positive(self, tmp_path: Path) -> None:
        child = tmp_path / "sub" / "leaf.thread"
        child.parent.mkdir(parents=True)
        child.write_text("x: 1")
        assert LocalConfigLocation(child).is_relative_to(LocalConfigLocation(tmp_path)) is True

    def test_is_relative_to_negative(self, tmp_path: Path) -> None:
        other = tmp_path / "other"
        other.mkdir()
        leaf = tmp_path / "leaf.thread"
        leaf.write_text("x: 1")
        assert LocalConfigLocation(leaf).is_relative_to(LocalConfigLocation(other)) is False

    def test_is_relative_to_traversal_escape(self, tmp_path: Path) -> None:
        # ../ that walks outside tmp_path resolves to a path that is not
        # relative to tmp_path.
        outside = tmp_path / "sub" / ".." / ".." / "outside.thread"
        # Note: don't need to actually create the file; resolve() works
        # symbolically.
        loc = LocalConfigLocation(outside)
        assert loc.is_relative_to(LocalConfigLocation(tmp_path)) is False

    def test_is_relative_to_cross_type(self, tmp_path: Path) -> None:
        # Mixing local and remote always returns False.
        local = LocalConfigLocation(tmp_path)
        remote = RemoteConfigLocation("abfss://w@h/lh/Files/proj", spark=MagicMock())
        assert local.is_relative_to(remote) is False

    def test_name_stem_suffix_parent(self, tmp_path: Path) -> None:
        loc = LocalConfigLocation(tmp_path / "sub" / "silver.loom")
        assert loc.name == "silver.loom"
        assert loc.stem == "silver"
        assert loc.suffix == ".loom"
        parent = loc.parent
        assert isinstance(parent, LocalConfigLocation)
        assert parent.path == tmp_path / "sub"

    def test_str_and_fspath(self, tmp_path: Path) -> None:
        loc = LocalConfigLocation(tmp_path / "x.thread")
        assert str(loc) == str(tmp_path / "x.thread")
        # __fspath__ works with os.fspath
        import os

        assert os.fspath(loc) == str(loc)

    def test_equality_and_hash(self, tmp_path: Path) -> None:
        a = LocalConfigLocation(tmp_path / "x")
        b = LocalConfigLocation(tmp_path / "x")
        c = LocalConfigLocation(tmp_path / "y")
        assert a == b
        assert a != c
        assert hash(a) == hash(b)


# ---------------------------------------------------------------------------
# URI normalization helpers
# ---------------------------------------------------------------------------


class TestSplitScheme:
    def test_simple(self) -> None:
        assert _split_scheme("abfss://workspace@host/lh/Files") == (
            "abfss://",
            "workspace@host/lh/Files",
        )

    def test_file_uri(self) -> None:
        assert _split_scheme("file:///tmp/x.thread") == ("file://", "/tmp/x.thread")

    def test_no_scheme_raises(self) -> None:
        with pytest.raises(ValueError, match="Not a URI"):
            _split_scheme("/tmp/x.thread")


class TestNormalizeUriPath:
    def test_simple_path(self) -> None:
        assert _normalize_uri_path("workspace@host/lh/Files/proj") == (
            "workspace@host/lh/Files/proj"
        )

    def test_dot_segments_removed(self) -> None:
        assert _normalize_uri_path("workspace@host/lh/./Files/./proj") == (
            "workspace@host/lh/Files/proj"
        )

    def test_double_dot_segments_resolved(self) -> None:
        assert _normalize_uri_path("workspace@host/lh/Files/sub/../proj") == (
            "workspace@host/lh/Files/proj"
        )

    def test_double_dot_above_authority_raises(self) -> None:
        with pytest.raises(ValueError, match="escapes"):
            _normalize_uri_path("workspace@host/../escape")

    def test_authority_preserved(self) -> None:
        # First segment must never be normalized — it's container@host.
        out = _normalize_uri_path("ws@host/x/../y")
        assert out.startswith("ws@host/")

    def test_trailing_slash_preserved(self) -> None:
        assert _normalize_uri_path("ws@host/lh/").endswith("/")

    def test_trailing_slash_dropped_at_authority_root(self) -> None:
        # Normalization that collapses the path back to the bare authority
        # discards the trailing slash — at the authority root the slash
        # carries no information.
        assert _normalize_uri_path("ws@host/lh/../") == "ws@host"

    def test_empty_segments_collapsed(self) -> None:
        assert _normalize_uri_path("ws@host/lh//Files") == "ws@host/lh/Files"


# ---------------------------------------------------------------------------
# RemoteConfigLocation — name/stem/suffix/parent (no JVM needed)
# ---------------------------------------------------------------------------


class TestRemoteConfigLocationPureString:
    URI = "abfss://workspace@onelake.dfs.fabric.microsoft.com/lh/Files/proj.weevr/silver.loom"

    def test_str_preserves_double_slash(self) -> None:
        # Regression for the original bug: str() must contain abfss:// not abfss:/.
        loc = RemoteConfigLocation(self.URI, spark=MagicMock())
        assert "abfss://" in str(loc)
        assert "abfss:/w" not in str(loc)

    def test_name(self) -> None:
        loc = RemoteConfigLocation(self.URI, spark=MagicMock())
        assert loc.name == "silver.loom"

    def test_stem(self) -> None:
        loc = RemoteConfigLocation(self.URI, spark=MagicMock())
        assert loc.stem == "silver"

    def test_suffix(self) -> None:
        loc = RemoteConfigLocation(self.URI, spark=MagicMock())
        assert loc.suffix == ".loom"

    def test_parent(self) -> None:
        loc = RemoteConfigLocation(self.URI, spark=MagicMock())
        parent = loc.parent
        assert isinstance(parent, RemoteConfigLocation)
        assert str(parent).endswith("/proj.weevr")
        assert "abfss://" in str(parent)

    def test_parent_at_authority_root(self) -> None:
        loc = RemoteConfigLocation("abfss://ws@host", spark=MagicMock())
        parent = loc.parent
        assert str(parent) == "abfss://ws@host"

    def test_constructor_rejects_non_uri(self) -> None:
        with pytest.raises(ValueError, match="requires a URI"):
            RemoteConfigLocation("/tmp/x.thread", spark=MagicMock())


class TestRemoteConfigLocationJoin:
    URI = "abfss://workspace@onelake.dfs.fabric.microsoft.com/lh/Files/proj.weevr"

    def test_join_simple(self) -> None:
        loc = RemoteConfigLocation(self.URI, spark=MagicMock())
        joined = loc.join("silver.loom")
        assert str(joined) == self.URI + "/silver.loom"
        # Critical: double slash preserved.
        assert "abfss://" in str(joined)

    def test_join_with_subdir(self) -> None:
        loc = RemoteConfigLocation(self.URI, spark=MagicMock())
        joined = loc.join("dimensions/dim_customer.thread")
        assert str(joined).endswith("/dimensions/dim_customer.thread")

    def test_join_normalizes_dot_dot(self) -> None:
        loc = RemoteConfigLocation(self.URI + "/sub", spark=MagicMock())
        joined = loc.join("../silver.loom")
        assert str(joined) == self.URI + "/silver.loom"

    def test_join_rejects_absolute_path(self) -> None:
        loc = RemoteConfigLocation(self.URI, spark=MagicMock())
        with pytest.raises(ValueError, match="absolute"):
            loc.join("/etc/passwd")

    def test_join_rejects_uri(self) -> None:
        loc = RemoteConfigLocation(self.URI, spark=MagicMock())
        with pytest.raises(ValueError, match="absolute URI"):
            loc.join("abfss://other@host/x")

    def test_join_traversal_above_authority_raises(self) -> None:
        loc = RemoteConfigLocation(self.URI, spark=MagicMock())
        with pytest.raises(ValueError, match="escapes"):
            loc.join("../../../../../../../etc/passwd")


class TestRemoteConfigLocationIsRelativeTo:
    BASE = "abfss://ws@host/lh/Files/proj.weevr"

    def test_positive(self) -> None:
        spark = MagicMock()
        base = RemoteConfigLocation(self.BASE, spark)
        child = RemoteConfigLocation(self.BASE + "/silver.loom", spark)
        assert child.is_relative_to(base) is True

    def test_self(self) -> None:
        spark = MagicMock()
        base = RemoteConfigLocation(self.BASE, spark)
        assert base.is_relative_to(base) is True

    def test_different_authority(self) -> None:
        spark = MagicMock()
        base = RemoteConfigLocation(self.BASE, spark)
        other = RemoteConfigLocation("abfss://other@host/lh/Files/proj.weevr", spark)
        assert other.is_relative_to(base) is False

    def test_different_scheme(self) -> None:
        spark = MagicMock()
        base = RemoteConfigLocation(self.BASE, spark)
        other = RemoteConfigLocation("file://" + self.BASE.split("://", 1)[1], spark)
        assert other.is_relative_to(base) is False

    def test_sibling_prefix_not_a_match(self) -> None:
        # A path that shares a prefix string but is a sibling, not a child.
        spark = MagicMock()
        base = RemoteConfigLocation(self.BASE, spark)
        sibling = RemoteConfigLocation(self.BASE + "-evil/silver.loom", spark)
        assert sibling.is_relative_to(base) is False

    def test_cross_type_returns_false(self, tmp_path: Path) -> None:
        spark = MagicMock()
        remote = RemoteConfigLocation(self.BASE, spark)
        assert remote.is_relative_to(LocalConfigLocation(tmp_path)) is False


# ---------------------------------------------------------------------------
# RemoteConfigLocation — JVM bridge with mocks
# ---------------------------------------------------------------------------


def _make_mock_spark_with_jvm() -> tuple[MagicMock, MagicMock, MagicMock]:
    """Construct a mock SparkSession with the minimum JVM surface area.

    Returns:
        Tuple of (spark mock, fs mock, jvm mock) so individual tests can
        override behavior.
    """
    spark = MagicMock(name="spark")
    jvm = MagicMock(name="jvm")
    spark._jvm = jvm
    spark._jsc = MagicMock(name="jsc")
    spark._jsc.hadoopConfiguration.return_value = MagicMock(name="hadoopConf")

    # The Path constructor and FileSystem.get chain.
    hpath = MagicMock(name="hadoopPath")
    jvm.org.apache.hadoop.fs.Path.return_value = hpath

    fs = MagicMock(name="filesystem")
    hpath.getFileSystem.return_value = fs

    return spark, fs, jvm


class TestRemoteConfigLocationExists:
    URI = "abfss://ws@host/lh/Files/proj.weevr/silver.loom"

    def test_exists_true(self) -> None:
        spark, fs, _ = _make_mock_spark_with_jvm()
        fs.exists.return_value = True
        loc = RemoteConfigLocation(self.URI, spark)
        assert loc.exists() is True
        fs.exists.assert_called_once()

    def test_exists_false(self) -> None:
        spark, fs, _ = _make_mock_spark_with_jvm()
        fs.exists.return_value = False
        loc = RemoteConfigLocation(self.URI, spark)
        assert loc.exists() is False

    def test_exists_jvm_error_wrapped(self) -> None:
        spark, fs, _ = _make_mock_spark_with_jvm()
        fs.exists.side_effect = RuntimeError("py4j boom")
        loc = RemoteConfigLocation(self.URI, spark)
        with pytest.raises(OSError, match="Failed to check existence"):
            loc.exists()


class TestRemoteConfigLocationReadText:
    URI = "abfss://ws@host/lh/Files/proj.weevr/silver.loom"

    def test_read_text_happy_path(self) -> None:
        spark, fs, jvm = _make_mock_spark_with_jvm()
        fs.exists.return_value = True
        stream = MagicMock(name="inputStream")
        fs.open.return_value = stream

        # IOUtils.toByteArray returns a Java byte[]; in real Py4J the proxy
        # exposes the buffer protocol so bytes() can coerce it. Use a
        # bytearray here (not a bytes literal) so the production code path
        # is forced through the bytes() coercion in read_text — if the
        # coercion is ever removed, this assertion still passes only if
        # the result is correctly decoded.
        jvm.org.apache.commons.io.IOUtils.toByteArray.return_value = bytearray(b"name: x\n")

        loc = RemoteConfigLocation(self.URI, spark)
        assert loc.read_text() == "name: x\n"
        stream.close.assert_called_once()

    def test_read_text_not_exists_raises_filenotfound(self) -> None:
        spark, fs, _ = _make_mock_spark_with_jvm()
        fs.exists.return_value = False
        loc = RemoteConfigLocation(self.URI, spark)
        with pytest.raises(FileNotFoundError):
            loc.read_text()

    def test_read_text_jvm_filenotfound_wrapped(self) -> None:
        spark, fs, _ = _make_mock_spark_with_jvm()
        fs.exists.side_effect = RuntimeError("java.io.FileNotFoundException: file not found")
        loc = RemoteConfigLocation(self.URI, spark)
        with pytest.raises(FileNotFoundError):
            loc.read_text()

    def test_read_text_jvm_other_error_wrapped_oserror(self) -> None:
        spark, fs, _ = _make_mock_spark_with_jvm()
        fs.exists.return_value = True
        fs.open.side_effect = RuntimeError("AccessControlException")
        loc = RemoteConfigLocation(self.URI, spark)
        with pytest.raises(OSError, match="Failed to open"):
            loc.read_text()

    def test_read_text_close_error_is_swallowed(self) -> None:
        spark, fs, jvm = _make_mock_spark_with_jvm()
        fs.exists.return_value = True
        stream = MagicMock(name="inputStream")
        stream.close.side_effect = RuntimeError("close failed")
        fs.open.return_value = stream
        jvm.org.apache.commons.io.IOUtils.toByteArray.return_value = bytearray(b"x: 1\n")
        loc = RemoteConfigLocation(self.URI, spark)
        # Should still return the content; close error is non-fatal.
        assert loc.read_text() == "x: 1\n"


# ---------------------------------------------------------------------------
# make_location factory
# ---------------------------------------------------------------------------


class TestMakeLocation:
    def test_local_path_string(self, tmp_path: Path) -> None:
        loc = make_location(str(tmp_path / "x.thread"))
        assert isinstance(loc, LocalConfigLocation)

    def test_local_pathlib(self, tmp_path: Path) -> None:
        loc = make_location(tmp_path / "x.thread")
        assert isinstance(loc, LocalConfigLocation)

    def test_remote_uri_requires_spark(self) -> None:
        with pytest.raises(ValueError, match="requires an active SparkSession"):
            make_location("abfss://ws@host/lh/Files/proj/x.loom")

    def test_remote_uri_with_spark(self) -> None:
        spark = MagicMock()
        loc = make_location("abfss://ws@host/lh/Files/proj/x.loom", spark)
        assert isinstance(loc, RemoteConfigLocation)
        assert "abfss://" in str(loc)

    def test_passthrough_existing_location(self, tmp_path: Path) -> None:
        original = LocalConfigLocation(tmp_path)
        assert make_location(original) is original

    def test_iso_concrete_classes_implement_abc(self) -> None:
        assert issubclass(LocalConfigLocation, ConfigLocation)
        assert issubclass(RemoteConfigLocation, ConfigLocation)
