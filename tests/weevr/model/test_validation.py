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
                rule=SparkExpr("x > 0"),
                name="test",
                severity=severity,  # type: ignore[arg-type]
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
        """All four assertion types are accepted."""
        for t in ("row_count", "column_not_null", "unique", "expression"):
            a = Assertion(type=t)  # type: ignore[arg-type]
            assert a.type == t

    def test_invalid_type_raises(self):
        """Unknown assertion type raises ValidationError."""
        with pytest.raises(ValidationError):
            Assertion(type="schema_match")  # type: ignore[arg-type]

    def test_severity_default_warn(self):
        """Assertion severity defaults to warn."""
        a = Assertion(type="row_count")
        assert a.severity == "warn"

    def test_severity_all_levels(self):
        """Assertion accepts all four severity levels."""
        for sev in ("info", "warn", "error", "fatal"):
            a = Assertion(type="row_count", severity=sev)  # type: ignore[arg-type]
            assert a.severity == sev

    def test_expression_assertion(self):
        """Expression assertion with custom Spark SQL expression."""
        a = Assertion(
            type="expression",
            expression=SparkExpr("COUNT(*) > 0"),
            severity="error",
        )
        assert a.type == "expression"
        assert a.expression == "COUNT(*) > 0"

    def test_optional_fields_default_none(self):
        """columns, min, max, expression default to None."""
        a = Assertion(type="row_count")
        assert a.columns is None
        assert a.min is None
        assert a.max is None
        assert a.expression is None

    def test_frozen(self):
        """Assertion is immutable."""
        a = Assertion(type="unique", columns=["id"])
        with pytest.raises(ValidationError):
            a.type = "row_count"  # type: ignore[misc]

    def test_round_trip(self):
        """Assertion round-trips."""
        a = Assertion(type="row_count", min=0, max=500)
        assert Assertion.model_validate(a.model_dump()) == a


# ---------------------------------------------------------------------------
# fk_sentinel_rate assertion model (M114)
# ---------------------------------------------------------------------------


class TestSentinelGroup:
    """Test SentinelGroup model."""

    def test_basic(self):
        """SentinelGroup with value and no per-group rate."""
        from weevr.model.validation import SentinelGroup

        sg = SentinelGroup(value=-4)
        assert sg.value == -4
        assert sg.max_rate is None

    def test_with_max_rate(self):
        """SentinelGroup with per-group max_rate."""
        from weevr.model.validation import SentinelGroup

        sg = SentinelGroup(value=-1, max_rate=0.10)
        assert sg.max_rate == 0.10

    def test_string_code_value(self):
        """SentinelGroup supports string system member code."""
        from weevr.model.validation import SentinelGroup

        sg = SentinelGroup(value="invalid")
        assert sg.value == "invalid"


class TestFkSentinelRateAssertion:
    """Test fk_sentinel_rate assertion fields and validation."""

    def test_single_column_single_sentinel(self):
        """Single column with single sentinel int."""
        a = Assertion(
            type="fk_sentinel_rate",
            column="plant_id",
            sentinel=-4,
            max_rate=0.05,
            message="plant FK invalid rate exceeded",
        )
        assert a.type == "fk_sentinel_rate"
        assert a.column == "plant_id"
        assert a.sentinel == -4
        assert a.max_rate == 0.05

    def test_columns_list(self):
        """Multiple columns with shared config."""
        a = Assertion(
            type="fk_sentinel_rate",
            columns=["plant_id", "company_id"],
            sentinel=-4,
            max_rate=0.05,
        )
        assert a.columns == ["plant_id", "company_id"]

    def test_named_sentinel_groups_dict_of_int(self):
        """Named sentinel groups as dict-of-int (shared max_rate)."""
        a = Assertion(
            type="fk_sentinel_rate",
            column="plant_id",
            sentinels={"invalid": -4, "unknown": -1},  # type: ignore[arg-type]
            max_rate=0.10,
        )
        assert a.sentinels is not None
        assert a.sentinels["invalid"].value == -4  # type: ignore[union-attr]
        assert a.sentinels["unknown"].value == -1  # type: ignore[union-attr]

    def test_named_sentinel_groups_dict_of_dict(self):
        """Named sentinel groups with per-group max_rate."""
        a = Assertion(
            type="fk_sentinel_rate",
            column="plant_id",
            sentinels={  # type: ignore[arg-type]
                "invalid": {"value": -4, "max_rate": 0.02},
                "unknown": {"value": -1, "max_rate": 0.10},
            },
        )
        assert a.sentinels is not None
        assert a.sentinels["invalid"].value == -4  # type: ignore[union-attr]
        assert a.sentinels["invalid"].max_rate == 0.02  # type: ignore[union-attr]
        assert a.sentinels["unknown"].max_rate == 0.10  # type: ignore[union-attr]

    def test_column_columns_mutual_exclusion(self):
        """column and columns are mutually exclusive."""
        with pytest.raises(ValidationError, match="mutually exclusive"):
            Assertion(
                type="fk_sentinel_rate",
                column="plant_id",
                columns=["plant_id"],
                sentinel=-4,
                max_rate=0.05,
            )

    def test_sentinel_sentinels_mutual_exclusion(self):
        """sentinel and sentinels are mutually exclusive."""
        with pytest.raises(ValidationError, match="mutually exclusive"):
            Assertion(
                type="fk_sentinel_rate",
                column="plant_id",
                sentinel=-4,
                sentinels={"invalid": -4},  # type: ignore[arg-type]
                max_rate=0.05,
            )

    def test_system_member_code_as_sentinel(self):
        """String system member code as sentinel value."""
        a = Assertion(
            type="fk_sentinel_rate",
            column="plant_id",
            sentinel="invalid",
            max_rate=0.05,
        )
        assert a.sentinel == "invalid"

    def test_round_trip(self):
        """fk_sentinel_rate assertion round-trips."""
        a = Assertion(
            type="fk_sentinel_rate",
            column="plant_id",
            sentinel=-4,
            max_rate=0.05,
            message="test",
        )
        restored = Assertion.model_validate(a.model_dump())
        assert restored.type == "fk_sentinel_rate"
        assert restored.sentinel == -4
