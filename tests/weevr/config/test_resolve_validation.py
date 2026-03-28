"""Tests for resolve step lookup name validation."""

from weevr.config.validation import validate_resolve_lookups


class TestValidateResolveLookups:
    """Test validate_resolve_lookups diagnostics."""

    def test_valid_lookup_reference(self):
        """Resolve step referencing a defined lookup produces no errors."""
        config = {
            "steps": [
                {"resolve": {"name": "fk", "lookup": "dim_plant", "match": "bk", "pk": "id"}}
            ],
        }
        diags = validate_resolve_lookups(config, available_lookups={"dim_plant"})
        assert not diags

    def test_undefined_lookup_reference(self):
        """Resolve step referencing an undefined lookup produces an error."""
        config = {
            "steps": [
                {"resolve": {"name": "fk", "lookup": "dim_missing", "match": "bk", "pk": "id"}}
            ],
        }
        diags = validate_resolve_lookups(config, available_lookups={"dim_plant"})
        assert any("ERROR" in d and "dim_missing" in d for d in diags)

    def test_batch_items_validated(self):
        """Batch resolve items each have their lookups validated."""
        config = {
            "steps": [
                {
                    "resolve": {
                        "pk": "id",
                        "batch": [
                            {"name": "fk1", "lookup": "dim_a", "match": "bk1"},
                            {"name": "fk2", "lookup": "dim_missing", "match": "bk2"},
                        ],
                    }
                }
            ],
        }
        diags = validate_resolve_lookups(config, available_lookups={"dim_a", "dim_b"})
        errors = [d for d in diags if "ERROR" in d]
        assert len(errors) == 1
        assert "dim_missing" in errors[0]

    def test_no_resolve_steps_no_errors(self):
        """Config without resolve steps produces no diagnostics."""
        config = {
            "steps": [
                {"filter": {"expr": "x > 0"}},
                {"derive": {"columns": {"y": "x + 1"}}},
            ],
        }
        diags = validate_resolve_lookups(config, available_lookups=set())
        assert not diags

    def test_multiple_resolve_steps(self):
        """Multiple resolve steps are all validated."""
        config = {
            "steps": [
                {"resolve": {"name": "fk1", "lookup": "dim_a", "match": "b", "pk": "id"}},
                {"resolve": {"name": "fk2", "lookup": "dim_b", "match": "b", "pk": "id"}},
            ],
        }
        diags = validate_resolve_lookups(config, available_lookups={"dim_a"})
        assert len([d for d in diags if "ERROR" in d]) == 1
        assert any("dim_b" in d for d in diags)

    def test_empty_available_lookups(self):
        """All resolve lookups are errors when no lookups are available."""
        config = {
            "steps": [{"resolve": {"name": "fk", "lookup": "dim_x", "match": "b", "pk": "id"}}],
        }
        diags = validate_resolve_lookups(config, available_lookups=set())
        assert any("dim_x" in d for d in diags)
