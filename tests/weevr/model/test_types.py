"""Tests for SparkExpr NewType alias."""

from weevr.model.types import SparkExpr


class TestSparkExpr:
    """Test SparkExpr NewType."""

    def test_sparkexpr_is_callable(self):
        """SparkExpr constructor returns a str."""
        expr = SparkExpr("col > 0")
        assert isinstance(expr, str)

    def test_sparkexpr_value(self):
        """SparkExpr preserves the string value."""
        expr = SparkExpr("amount > 100 AND status = 'active'")
        assert expr == "amount > 100 AND status = 'active'"

    def test_sparkexpr_empty_string(self):
        """SparkExpr accepts an empty string."""
        expr = SparkExpr("")
        assert expr == ""

    def test_sparkexpr_is_str_subtype(self):
        """SparkExpr values behave as plain strings."""
        expr = SparkExpr("id IS NOT NULL")
        assert expr.upper() == "ID IS NOT NULL"
        assert len(expr) == 14
