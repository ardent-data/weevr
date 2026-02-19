"""Tests for ValidationRule and Assertion models."""

import pytest
from pydantic import ValidationError

from weevr.model.types import SparkExpr
from weevr.model.validation import Assertion, ValidationRule


class TestValidationRule:
    """Test ValidationRule model."""

    def test_minimal_rule(self):
        """ValidationRule with rule and name."""
        vr = ValidationRule(rule=SparkExpr("id IS NOT NULL"), name="id_not_null")
        assert vr.rule == "id IS NOT NULL"
        assert vr.name == "id_not_null"
        assert vr.severity == "error"

    def test_all_severities(self):
        """ValidationRule accepts all four severity levels."""
        for severity in ("info", "warn", "error", "fatal"):
            vr = ValidationRule(
                rule=SparkExpr("x > 0"), name="test", severity=severity  # type: ignore[arg-type]
            )
            assert vr.severity == severity

    def test_invalid_severity_raises(self):
        """Unknown severity raises ValidationError."""
        with pytest.raises(ValidationError):
            ValidationRule(
                rule=SparkExpr("x > 0"),
                name="test",
                severity="critical",  # type: ignore[arg-type]
            )

    def test_rule_is_sparkexpr(self):
        """Rule field is typed as SparkExpr (str)."""
        vr = ValidationRule(rule=SparkExpr("amount > 0"), name="positive_amount")
        assert isinstance(vr.rule, str)

    def test_frozen(self):
        """ValidationRule is immutable."""
        vr = ValidationRule(rule=SparkExpr("x > 0"), name="r")
        with pytest.raises(ValidationError):
            vr.name = "other"  # type: ignore[misc]

    def test_round_trip(self):
        """ValidationRule round-trips."""
        vr = ValidationRule(rule=SparkExpr("col IS NOT NULL"), name="not_null", severity="warn")
        assert ValidationRule.model_validate(vr.model_dump()) == vr


class TestAssertion:
    """Test Assertion model."""

    def test_row_count_assertion(self):
        """row_count assertion with min/max."""
        a = Assertion(type="row_count", min=1, max=1000)
        assert a.type == "row_count"
        assert a.min == 1
        assert a.max == 1000

    def test_column_not_null_assertion(self):
        """column_not_null assertion with columns."""
        a = Assertion(type="column_not_null", columns=["id", "name"])
        assert a.columns == ["id", "name"]

    def test_unique_assertion(self):
        """unique assertion with columns."""
        a = Assertion(type="unique", columns=["email"])
        assert a.type == "unique"

    def test_all_types_valid(self):
        """All three assertion types are accepted."""
        for t in ("row_count", "column_not_null", "unique"):
            a = Assertion(type=t)  # type: ignore[arg-type]
            assert a.type == t

    def test_invalid_type_raises(self):
        """Unknown assertion type raises ValidationError."""
        with pytest.raises(ValidationError):
            Assertion(type="schema_match")  # type: ignore[arg-type]

    def test_optional_fields_default_none(self):
        """columns, min, max default to None."""
        a = Assertion(type="row_count")
        assert a.columns is None
        assert a.min is None
        assert a.max is None

    def test_frozen(self):
        """Assertion is immutable."""
        a = Assertion(type="unique", columns=["id"])
        with pytest.raises(ValidationError):
            a.type = "row_count"  # type: ignore[misc]

    def test_round_trip(self):
        """Assertion round-trips."""
        a = Assertion(type="row_count", min=0, max=500)
        assert Assertion.model_validate(a.model_dump()) == a
