"""Tests for Target and ColumnMapping models."""

import pytest
from pydantic import ValidationError

from weevr.model.target import ColumnMapping, Target
from weevr.model.types import SparkExpr


class TestColumnMapping:
    """Test ColumnMapping model."""

    def test_minimal_empty(self):
        """ColumnMapping with no fields is valid."""
        cm = ColumnMapping()
        assert cm.expr is None
        assert cm.type is None
        assert cm.default is None
        assert cm.drop is False

    def test_with_expr(self):
        """ColumnMapping with a SparkExpr."""
        cm = ColumnMapping(expr=SparkExpr("amount * 1.1"))
        assert cm.expr == "amount * 1.1"

    def test_with_type(self):
        """ColumnMapping with a type cast."""
        cm = ColumnMapping(type="decimal(18,2)")
        assert cm.type == "decimal(18,2)"

    def test_with_default(self):
        """ColumnMapping with a default value."""
        cm = ColumnMapping(default=0)
        assert cm.default == 0

    def test_drop_flag(self):
        """ColumnMapping with drop=True."""
        cm = ColumnMapping(drop=True)
        assert cm.drop is True

    def test_expr_and_drop_raises(self):
        """expr and drop=True together raise ValidationError."""
        with pytest.raises(ValidationError, match="mutually exclusive"):
            ColumnMapping(expr=SparkExpr("col + 1"), drop=True)

    def test_frozen(self):
        """ColumnMapping is immutable."""
        cm = ColumnMapping(type="string")
        with pytest.raises(ValidationError):
            cm.type = "int"  # type: ignore[misc]

    def test_round_trip(self):
        """ColumnMapping round-trips."""
        cm = ColumnMapping(expr=SparkExpr("x * 2"), type="double")
        assert ColumnMapping.model_validate(cm.model_dump()) == cm


class TestTarget:
    """Test Target model."""

    def test_default_mapping_mode(self):
        """Default mapping mode is 'auto'."""
        t = Target()
        assert t.mapping_mode == "auto"

    def test_explicit_mapping_mode(self):
        """explicit mapping mode is valid."""
        t = Target(mapping_mode="explicit")
        assert t.mapping_mode == "explicit"

    def test_invalid_mapping_mode_raises(self):
        """Unknown mapping mode raises ValidationError."""
        with pytest.raises(ValidationError):
            Target(mapping_mode="manual")  # type: ignore[arg-type]

    def test_with_columns(self):
        """Target accepts columns dict with ColumnMapping values."""
        t = Target(
            columns={
                "amount_usd": {"expr": "amount * rate", "type": "double"},
                "is_active": {"default": True},
            }
        )
        assert isinstance(t.columns["amount_usd"], ColumnMapping)
        assert t.columns["amount_usd"].expr == "amount * rate"

    def test_with_partition_by(self):
        """Target accepts partition_by list."""
        t = Target(partition_by=["year", "month"])
        assert t.partition_by == ["year", "month"]

    def test_with_audit_template(self):
        """Target accepts audit_template string."""
        t = Target(audit_template="standard_v1")
        assert t.audit_template == "standard_v1"

    def test_all_none_defaults(self):
        """Target with no fields has all-None optional fields."""
        t = Target()
        assert t.columns is None
        assert t.partition_by is None
        assert t.audit_template is None

    def test_frozen(self):
        """Target is immutable."""
        t = Target(mapping_mode="auto")
        with pytest.raises(ValidationError):
            t.mapping_mode = "explicit"  # type: ignore[misc]

    def test_round_trip(self):
        """Target round-trips."""
        t = Target(
            mapping_mode="explicit",
            columns={"col_a": {"type": "string"}},
            partition_by=["date"],
            audit_template="standard",
        )
        assert Target.model_validate(t.model_dump()) == t
