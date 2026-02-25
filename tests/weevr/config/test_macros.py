"""Tests for foreach macro expansion."""

import pytest

from weevr.config.macros import expand_foreach
from weevr.errors.exceptions import ConfigError


class TestExpandForeach:
    """Test foreach macro expansion logic."""

    def test_expand_inline_values(self):
        """Foreach with inline values produces expanded steps."""
        steps = [
            {
                "foreach": {
                    "values": ["a", "b", "c"],
                    "as": "col",
                    "steps": [{"derive": {"columns": {"{col}_upper": "upper({col})"}}}],
                }
            }
        ]
        result = expand_foreach(steps)
        assert len(result) == 3
        assert result[0] == {"derive": {"columns": {"a_upper": "upper(a)"}}}
        assert result[1] == {"derive": {"columns": {"b_upper": "upper(b)"}}}
        assert result[2] == {"derive": {"columns": {"c_upper": "upper(c)"}}}

    def test_expand_substitution(self):
        """Placeholder {var} replaced in all string values."""
        steps = [
            {
                "foreach": {
                    "values": ["name"],
                    "as": "col",
                    "steps": [{"string_ops": {"columns": ["{col}"], "expr": "trim({col})"}}],
                }
            }
        ]
        result = expand_foreach(steps)
        assert len(result) == 1
        # Note: {col} in expr is a column_ops placeholder, not a foreach var
        # The foreach var is also named "col", so both occurrences get substituted
        assert result[0]["string_ops"]["columns"] == ["name"]

    def test_expand_multiple_template_steps(self):
        """Foreach with 2 template steps x 3 values = 6 expanded steps."""
        steps = [
            {
                "foreach": {
                    "values": ["x", "y", "z"],
                    "as": "c",
                    "steps": [
                        {"cast": {"columns": {"{c}": "string"}}},
                        {"rename": {"columns": {"{c}": "{c}_clean"}}},
                    ],
                }
            }
        ]
        result = expand_foreach(steps)
        assert len(result) == 6
        assert result[0] == {"cast": {"columns": {"x": "string"}}}
        assert result[1] == {"rename": {"columns": {"x": "x_clean"}}}
        assert result[2] == {"cast": {"columns": {"y": "string"}}}
        assert result[3] == {"rename": {"columns": {"y": "y_clean"}}}

    def test_expand_preserves_non_foreach(self):
        """Non-foreach steps pass through unchanged."""
        steps = [{"filter": {"expr": "x > 0"}}]
        result = expand_foreach(steps)
        assert result == steps

    def test_expand_mixed_steps(self):
        """Foreach interleaved with regular steps maintains order."""
        steps = [
            {"filter": {"expr": "active = true"}},
            {
                "foreach": {
                    "values": ["a", "b"],
                    "as": "col",
                    "steps": [{"cast": {"columns": {"{col}": "string"}}}],
                }
            },
            {"select": {"columns": ["id"]}},
        ]
        result = expand_foreach(steps)
        assert len(result) == 4
        assert result[0] == {"filter": {"expr": "active = true"}}
        assert result[1] == {"cast": {"columns": {"a": "string"}}}
        assert result[2] == {"cast": {"columns": {"b": "string"}}}
        assert result[3] == {"select": {"columns": ["id"]}}

    def test_expand_nested_dict_substitution(self):
        """Substitution works in nested dict values."""
        steps = [
            {
                "foreach": {
                    "values": ["amount"],
                    "as": "col",
                    "steps": [
                        {
                            "case_when": {
                                "column": "{col}_tier",
                                "cases": [{"when": "{col} > 100", "then": "'high'"}],
                            }
                        }
                    ],
                }
            }
        ]
        result = expand_foreach(steps)
        assert result[0]["case_when"]["column"] == "amount_tier"
        assert result[0]["case_when"]["cases"][0]["when"] == "amount > 100"

    def test_expand_no_foreach(self):
        """Steps list with no foreach blocks returns identical output."""
        steps = [
            {"filter": {"expr": "x > 0"}},
            {"derive": {"columns": {"y": "x + 1"}}},
        ]
        result = expand_foreach(steps)
        assert result == steps

    def test_empty_values_raises(self):
        """Foreach with empty values raises ConfigError."""
        steps = [{"foreach": {"values": [], "as": "col", "steps": [{"filter": {"expr": "1=1"}}]}}]
        with pytest.raises(ConfigError, match="non-empty list"):
            expand_foreach(steps)

    def test_missing_as_raises(self):
        """Foreach without 'as' raises ConfigError."""
        steps = [{"foreach": {"values": ["a"], "steps": [{"filter": {"expr": "1=1"}}]}}]
        with pytest.raises(ConfigError, match="'as' variable name"):
            expand_foreach(steps)

    def test_missing_steps_raises(self):
        """Foreach without 'steps' raises ConfigError."""
        steps = [{"foreach": {"values": ["a"], "as": "col"}}]
        with pytest.raises(ConfigError, match="'steps' must be a non-empty list"):
            expand_foreach(steps)

    def test_numeric_values(self):
        """Foreach with numeric values substitutes correctly."""
        steps = [
            {
                "foreach": {
                    "values": [1, 2, 3],
                    "as": "n",
                    "steps": [{"derive": {"columns": {"col_{n}": "col * {n}"}}}],
                }
            }
        ]
        result = expand_foreach(steps)
        assert len(result) == 3
        assert result[0] == {"derive": {"columns": {"col_1": "col * 1"}}}
