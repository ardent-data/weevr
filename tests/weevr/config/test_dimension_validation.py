"""Tests for dimension write/keys override validation."""

from weevr.config.validation import validate_dimension_overrides


class TestValidateDimensionOverrides:
    """Test validate_dimension_overrides diagnostics."""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _base_config(self) -> dict:
        """Minimal thread config with a dimension block."""
        return {
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {
                "alias": "dim_out",
                "dimension": {
                    "business_key": ["customer_id"],
                    "surrogate_key": "customer_sk",
                    "change_detection": {"columns": ["name", "email"]},
                },
            },
        }

    # ------------------------------------------------------------------
    # write.match_keys
    # ------------------------------------------------------------------

    def test_dimension_with_write_match_keys_errors(self):
        """dimension + write.match_keys must raise an error."""
        config = self._base_config()
        config["write"] = {"match_keys": ["customer_id"], "mode": "merge"}
        diags = validate_dimension_overrides(config)
        assert any("ERROR" in d and "match_keys" in d for d in diags)

    # ------------------------------------------------------------------
    # write.on_match
    # ------------------------------------------------------------------

    def test_dimension_with_write_on_match_errors(self):
        """dimension + write.on_match must raise an error."""
        config = self._base_config()
        config["write"] = {"on_match": "update", "mode": "merge"}
        diags = validate_dimension_overrides(config)
        assert any("ERROR" in d and "on_match" in d for d in diags)

    # ------------------------------------------------------------------
    # write.mode
    # ------------------------------------------------------------------

    def test_dimension_with_write_mode_overwrite_errors(self):
        """dimension + write.mode=overwrite must raise an error."""
        config = self._base_config()
        config["write"] = {"mode": "overwrite"}
        diags = validate_dimension_overrides(config)
        assert any("ERROR" in d and "mode" in d for d in diags)

    def test_dimension_with_write_mode_merge_allowed(self):
        """dimension + write.mode=merge is valid (no error)."""
        config = self._base_config()
        config["write"] = {"mode": "merge"}
        diags = validate_dimension_overrides(config)
        assert not any("ERROR" in d for d in diags)

    def test_dimension_without_write_mode_allowed(self):
        """dimension without write.mode set is valid (engine defaults to merge)."""
        config = self._base_config()
        config["write"] = {"on_no_match_source": "soft_delete"}
        diags = validate_dimension_overrides(config)
        assert not any("ERROR" in d for d in diags)

    # ------------------------------------------------------------------
    # write.on_no_match_source and write.on_no_match_target
    # ------------------------------------------------------------------

    def test_dimension_with_on_no_match_source_soft_delete_allowed(self):
        """dimension + write.on_no_match_source=soft_delete is valid."""
        config = self._base_config()
        config["write"] = {"on_no_match_source": "soft_delete", "mode": "merge"}
        diags = validate_dimension_overrides(config)
        assert not any("ERROR" in d for d in diags)

    def test_dimension_with_on_no_match_target_allowed(self):
        """dimension + write.on_no_match_target is valid."""
        config = self._base_config()
        config["write"] = {"on_no_match_target": "insert", "mode": "merge"}
        diags = validate_dimension_overrides(config)
        assert not any("ERROR" in d for d in diags)

    # ------------------------------------------------------------------
    # keys.*
    # ------------------------------------------------------------------

    def test_dimension_with_keys_business_key_errors(self):
        """dimension + keys.business_key must raise an error."""
        config = self._base_config()
        config["keys"] = {"business_key": ["customer_id"]}
        diags = validate_dimension_overrides(config)
        assert any("ERROR" in d and "business_key" in d for d in diags)

    def test_dimension_with_keys_surrogate_key_errors(self):
        """dimension + keys.surrogate_key must raise an error."""
        config = self._base_config()
        config["keys"] = {"surrogate_key": "customer_sk"}
        diags = validate_dimension_overrides(config)
        assert any("ERROR" in d and "surrogate_key" in d for d in diags)

    def test_dimension_with_keys_change_detection_errors(self):
        """dimension + keys.change_detection must raise an error."""
        config = self._base_config()
        config["keys"] = {"change_detection": {"columns": ["name"]}}
        diags = validate_dimension_overrides(config)
        assert any("ERROR" in d and "change_detection" in d for d in diags)

    # ------------------------------------------------------------------
    # No regression: no dimension block
    # ------------------------------------------------------------------

    def test_no_dimension_write_fields_allowed(self):
        """Without dimension block, any write/keys fields are allowed."""
        config = {
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
            "write": {
                "mode": "merge",
                "match_keys": ["id"],
                "on_match": "update",
            },
            "keys": {
                "business_key": ["id"],
                "surrogate_key": "sk",
                "change_detection": {"columns": ["col1"]},
            },
        }
        diags = validate_dimension_overrides(config)
        assert len(diags) == 0

    # ------------------------------------------------------------------
    # dimension present but no write / no keys
    # ------------------------------------------------------------------

    def test_dimension_without_write_no_error(self):
        """dimension without any write block produces no error."""
        config = self._base_config()
        diags = validate_dimension_overrides(config)
        assert len(diags) == 0

    def test_dimension_without_keys_no_error(self):
        """dimension without any keys block produces no error."""
        config = self._base_config()
        config["write"] = {"mode": "merge"}
        diags = validate_dimension_overrides(config)
        assert len(diags) == 0
