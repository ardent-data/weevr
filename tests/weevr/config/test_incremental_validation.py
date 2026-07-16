"""Tests for incremental config cross-cutting validation."""

from weevr.config.validation import validate_incremental_config


class TestValidateIncrementalConfig:
    """Test validate_incremental_config diagnostics."""

    def test_incremental_parameter_with_param_ref_valid(self):
        """incremental_parameter with ${param.} in steps is valid (no diagnostics)."""
        config = {
            "load": {"mode": "incremental_parameter"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "steps": [{"filter": "date > '${param.start_date}'"}],
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("WARN" in d for d in diags)

    def test_incremental_parameter_no_param_ref_warns(self):
        """incremental_parameter without ${param.} references produces warning."""
        config = {
            "load": {"mode": "incremental_parameter"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "steps": [{"filter": "date > '2024-01-01'"}],
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert any("WARN" in d and "incremental_parameter" in d for d in diags)

    def test_cdc_with_merge_valid(self):
        """cdc mode with write.mode=merge produces no error."""
        config = {
            "load": {"mode": "cdc"},
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("ERROR" in d for d in diags)

    def test_cdc_with_overwrite_errors(self):
        """cdc mode with write.mode=overwrite produces error."""
        config = {
            "load": {"mode": "cdc"},
            "write": {"mode": "overwrite"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert any("ERROR" in d and "cdc" in d and "merge" in d for d in diags)

    def test_incremental_watermark_with_explicit_overwrite_errors(self):
        """incremental_watermark with write.mode=overwrite produces error."""
        config = {
            "load": {"mode": "incremental_watermark", "watermark_column": "updated_at"},
            "write": {"mode": "overwrite"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        errors = [d for d in diags if "ERROR" in d and "incremental_watermark" in d]
        assert errors
        # Explicitly configured overwrite must not be blamed on the default
        assert all("(the default)" not in d for d in errors)

    def test_incremental_watermark_with_defaulted_overwrite_errors(self):
        """incremental_watermark with no write block (defaulted overwrite) produces error."""
        config = {
            "load": {"mode": "incremental_watermark", "watermark_column": "updated_at"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert any(
            "ERROR" in d and "incremental_watermark" in d and "(the default)" in d for d in diags
        )

    def test_incremental_watermark_with_append_no_error(self):
        """incremental_watermark with write.mode=append produces no error."""
        config = {
            "load": {"mode": "incremental_watermark", "watermark_column": "updated_at"},
            "write": {"mode": "append"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("ERROR" in d for d in diags)

    def test_incremental_watermark_with_merge_no_error(self):
        """incremental_watermark with write.mode=merge produces no error."""
        config = {
            "load": {"mode": "incremental_watermark", "watermark_column": "updated_at"},
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("ERROR" in d for d in diags)

    def test_cdc_update_value_without_watermark_errors(self):
        """Generic CDC with update_value but no watermark_column produces error."""
        config = {
            "load": {
                "mode": "cdc",
                "cdc": {"operation_column": "op", "insert_value": "I", "update_value": "U"},
            },
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert any("ERROR" in d and "watermark_column" in d for d in diags)

    def test_cdc_delete_value_without_watermark_errors(self):
        """Generic CDC with delete_value but no watermark_column produces error."""
        config = {
            "load": {
                "mode": "cdc",
                "cdc": {"operation_column": "op", "insert_value": "I", "delete_value": "D"},
            },
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert any("ERROR" in d and "watermark_column" in d for d in diags)

    def test_cdc_insert_only_without_watermark_no_error(self):
        """Insert-only generic CDC needs no ordering column."""
        config = {
            "load": {
                "mode": "cdc",
                "cdc": {"operation_column": "op", "insert_value": "I"},
            },
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("ERROR" in d and "watermark_column" in d for d in diags)

    def test_cdc_update_value_with_watermark_no_error(self):
        """Generic CDC with update_value and a watermark_column passes."""
        config = {
            "load": {
                "mode": "cdc",
                "watermark_column": "updated_at",
                "watermark_type": "timestamp",
                "cdc": {"operation_column": "op", "insert_value": "I", "update_value": "U"},
            },
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("ERROR" in d and "watermark_column" in d for d in diags)

    def test_cdc_cdf_preset_without_watermark_no_error(self):
        """The delta_cdf preset orders by commit version; no watermark needed."""
        config = {
            "load": {"mode": "cdc", "cdc": {"preset": "delta_cdf"}},
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("ERROR" in d and "watermark_column" in d for d in diags)

    def test_watermark_inclusive_with_append_warns(self):
        """watermark_inclusive=True with append produces warning."""
        config = {
            "load": {"mode": "incremental_watermark", "watermark_inclusive": True},
            "write": {"mode": "append"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert any("WARN" in d and "watermark_inclusive" in d for d in diags)

    def test_watermark_inclusive_with_merge_no_warning(self):
        """watermark_inclusive=True with merge produces no warning."""
        config = {
            "load": {"mode": "incremental_watermark", "watermark_inclusive": True},
            "write": {"mode": "merge", "match_keys": ["id"]},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("WARN" in d and "watermark_inclusive" in d for d in diags)

    def test_full_mode_no_diagnostics(self):
        """Full mode produces no diagnostics."""
        config = {
            "load": {"mode": "full"},
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert len(diags) == 0

    def test_no_load_block_no_diagnostics(self):
        """No load block produces no diagnostics."""
        config = {
            "sources": {"src": {"type": "delta", "alias": "t"}},
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert len(diags) == 0

    def test_incremental_parameter_with_param_in_sources(self):
        """incremental_parameter with ${param.} in sources is valid."""
        config = {
            "load": {"mode": "incremental_parameter"},
            "sources": {"src": {"type": "delta", "alias": "${param.table_path}"}},
            "steps": [],
            "target": {"alias": "out"},
        }
        diags = validate_incremental_config(config)
        assert not any("WARN" in d and "incremental_parameter" in d for d in diags)
