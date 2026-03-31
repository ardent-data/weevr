"""Tests for OneLake path builder utilities."""

from unittest.mock import MagicMock

from weevr.config.paths import build_onelake_path, resolve_fuse_path
from weevr.model.connection import OneLakeConnection


def _make_connection(
    workspace: str = "ws-guid",
    lakehouse: str = "lh-guid",
    default_schema: str | None = None,
) -> OneLakeConnection:
    return OneLakeConnection(
        type="onelake",
        workspace=workspace,
        lakehouse=lakehouse,
        default_schema=default_schema,
    )


def _make_spark(
    workspace_id: str | None = "ws-guid", lakehouse_id: str | None = "lh-guid"
) -> MagicMock:
    mapping: dict[str, str] = {}
    if workspace_id is not None:
        mapping["trident.workspace.id"] = workspace_id
    if lakehouse_id is not None:
        mapping["trident.lakehouse.id"] = lakehouse_id

    spark = MagicMock()
    spark.conf.get.side_effect = lambda key, default=None: mapping.get(key, default)
    return spark


class TestBuildOneLakePath:
    """Tests for build_onelake_path."""

    def test_workspace_lakehouse_table(self):
        """Workspace + lakehouse + table produces correct abfss:// URI."""
        conn = _make_connection(workspace="ws-guid", lakehouse="lh-guid")
        result = build_onelake_path(conn, schema=None, table="my_table")
        assert result == (
            "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh-guid/Tables/my_table"
        )

    def test_default_schema_from_connection(self):
        """default_schema on connection is included in the URI."""
        conn = _make_connection(workspace="ws-guid", lakehouse="lh-guid", default_schema="gold")
        result = build_onelake_path(conn, schema=None, table="my_table")
        assert result == (
            "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh-guid/Tables/gold/my_table"
        )

    def test_source_schema_overrides_default_schema(self):
        """Schema argument takes precedence over connection.default_schema."""
        conn = _make_connection(workspace="ws-guid", lakehouse="lh-guid", default_schema="gold")
        result = build_onelake_path(conn, schema="silver", table="my_table")
        assert result == (
            "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh-guid/Tables/silver/my_table"
        )

    def test_no_default_schema_no_source_schema(self):
        """No schema anywhere produces URI without schema segment."""
        conn = _make_connection(workspace="ws-guid", lakehouse="lh-guid", default_schema=None)
        result = build_onelake_path(conn, schema=None, table="my_table")
        assert result == (
            "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh-guid/Tables/my_table"
        )


class TestResolveFusePath:
    """Tests for resolve_fuse_path."""

    def test_fuse_path_translated_to_abfss(self):
        """FUSE mount path is translated to abfss:// URI."""
        spark = _make_spark(workspace_id="ws-guid", lakehouse_id="lh-guid")
        result = resolve_fuse_path("/lakehouse/default/Tables/foo", spark)
        assert result == ("abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh-guid/Tables/foo")

    def test_abfss_path_passes_through_unchanged(self):
        """abfss:// path is returned as-is without any modification."""
        spark = _make_spark()
        path = "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh-guid/Tables/foo"
        result = resolve_fuse_path(path, spark)
        assert result == path

    def test_fuse_path_missing_spark_conf_keys_returns_original(self):
        """FUSE path with missing trident.* keys returns the original path unchanged."""
        spark = _make_spark(workspace_id=None, lakehouse_id=None)
        path = "/lakehouse/default/Tables/foo"
        result = resolve_fuse_path(path, spark)
        assert result == path

    def test_regular_relative_path_passes_through(self):
        """A path that doesn't start with /lakehouse/ passes through unchanged."""
        spark = _make_spark()
        path = "some/relative/path/Tables/foo"
        result = resolve_fuse_path(path, spark)
        assert result == path
