"""Tests for Fabric context provider."""

from unittest.mock import MagicMock

from weevr.config.fabric import build_fabric_context


class TestBuildFabricContext:
    """Test build_fabric_context extracts trident.* spark.conf properties."""

    def test_all_trident_keys_present(self):
        """Returns full context when all trident.* keys are in spark.conf."""
        spark = MagicMock()
        conf_data = {
            "trident.workspace.id": "ws-guid-1234",
            "trident.lakehouse.id": "lh-guid-5678",
            "trident.workspace.name": "my-workspace",
        }
        spark.conf.get.side_effect = lambda key, default=None: conf_data.get(key, default)

        result = build_fabric_context(spark)

        assert result == {
            "fabric.workspace_id": "ws-guid-1234",
            "fabric.lakehouse_id": "lh-guid-5678",
            "fabric.workspace_name": "my-workspace",
        }

    def test_some_trident_keys_missing(self):
        """Returns partial context with None for missing keys."""
        spark = MagicMock()
        conf_data = {
            "trident.workspace.id": "ws-guid-1234",
        }
        spark.conf.get.side_effect = lambda key, default=None: conf_data.get(key, default)

        result = build_fabric_context(spark)

        assert result["fabric.workspace_id"] == "ws-guid-1234"
        assert result["fabric.lakehouse_id"] is None
        assert result["fabric.workspace_name"] is None

    def test_no_trident_keys(self):
        """Returns all None values when no trident.* keys are present."""
        spark = MagicMock()
        spark.conf.get.side_effect = lambda key, default=None: None

        result = build_fabric_context(spark)

        assert result == {
            "fabric.workspace_id": None,
            "fabric.lakehouse_id": None,
            "fabric.workspace_name": None,
        }

    def test_spark_conf_get_raises_exception(self):
        """Returns None for all values when spark.conf.get raises an exception."""
        spark = MagicMock()
        spark.conf.get.side_effect = Exception("not set")

        result = build_fabric_context(spark)

        assert result == {
            "fabric.workspace_id": None,
            "fabric.lakehouse_id": None,
            "fabric.workspace_name": None,
        }
