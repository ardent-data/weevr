"""Tests for CDC preset expansion and routing logic (pure Python, no Spark)."""

from weevr.model.load import CdcConfig
from weevr.operations.writers import resolve_cdc_columns


class TestResolveCdcColumns:
    """Test CDC column mapping resolution."""

    def test_delta_cdf_preset(self):
        """delta_cdf preset expands to correct column/value mappings."""
        cfg = CdcConfig(preset="delta_cdf")
        cols = resolve_cdc_columns(cfg)
        assert cols["operation_column"] == "_change_type"
        assert cols["insert_value"] == "insert"
        assert cols["update_value"] == "update_postimage"
        assert cols["delete_value"] == "delete"

    def test_explicit_mapping(self):
        """Explicit mapping passes through unchanged."""
        cfg = CdcConfig(
            operation_column="op_type",
            insert_value="I",
            update_value="U",
            delete_value="D",
        )
        cols = resolve_cdc_columns(cfg)
        assert cols["operation_column"] == "op_type"
        assert cols["insert_value"] == "I"
        assert cols["update_value"] == "U"
        assert cols["delete_value"] == "D"

    def test_explicit_without_delete(self):
        """Explicit mapping with no delete_value returns None for delete."""
        cfg = CdcConfig(
            operation_column="op",
            insert_value="INS",
            update_value="UPD",
        )
        cols = resolve_cdc_columns(cfg)
        assert cols["delete_value"] is None

    def test_explicit_insert_only(self):
        """Explicit mapping with only insert_value."""
        cfg = CdcConfig(
            operation_column="change",
            insert_value="new",
        )
        cols = resolve_cdc_columns(cfg)
        assert cols["insert_value"] == "new"
        assert cols["update_value"] is None
        assert cols["delete_value"] is None


class TestCdcConfigOnDelete:
    """Test CdcConfig on_delete behavior (model-level tests)."""

    def test_default_is_hard_delete(self):
        """Default on_delete is hard_delete."""
        cfg = CdcConfig(preset="delta_cdf")
        assert cfg.on_delete == "hard_delete"

    def test_soft_delete(self):
        """on_delete=soft_delete is valid."""
        cfg = CdcConfig(preset="delta_cdf", on_delete="soft_delete")
        assert cfg.on_delete == "soft_delete"
