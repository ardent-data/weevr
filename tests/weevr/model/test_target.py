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
            columns={  # type: ignore[arg-type]
                "amount_usd": {"expr": "amount * rate", "type": "double"},
                "is_active": {"default": True},
            }
        )
        assert t.columns is not None
        assert isinstance(t.columns["amount_usd"], ColumnMapping)
        assert t.columns["amount_usd"].expr == "amount * rate"

    def test_with_partition_by(self):
        """Target accepts partition_by list."""
        t = Target(partition_by=["year", "month"])
        assert t.partition_by == ["year", "month"]

    def test_with_audit_columns(self):
        """Target accepts audit_columns dict."""
        t = Target(audit_columns={"_loaded_at": "current_timestamp()", "_run_id": "'abc'"})
        assert t.audit_columns is not None
        assert t.audit_columns["_loaded_at"] == "current_timestamp()"
        assert t.audit_columns["_run_id"] == "'abc'"

    def test_all_none_defaults(self):
        """Target with no fields has all-None optional fields."""
        t = Target()
        assert t.columns is None
        assert t.partition_by is None
        assert t.audit_columns is None
        assert t.alias is None
        assert t.path is None

    def test_with_alias(self):
        """Target accepts alias for Delta table name."""
        t = Target(alias="gold.dim_customer")
        assert t.alias == "gold.dim_customer"

    def test_with_path(self):
        """Target accepts path for physical write location."""
        t = Target(path="/mnt/lakehouse/tables/dim_customer")
        assert t.path == "/mnt/lakehouse/tables/dim_customer"

    def test_frozen(self):
        """Target is immutable."""
        t = Target(mapping_mode="auto")
        with pytest.raises(ValidationError):
            t.mapping_mode = "explicit"  # type: ignore[misc]

    def test_round_trip(self):
        """Target round-trips."""
        t = Target(
            mapping_mode="explicit",
            columns={"col_a": {"type": "string"}},  # type: ignore[arg-type]
            partition_by=["date"],
            audit_columns={"_loaded_at": "current_timestamp()"},
        )
        assert Target.model_validate(t.model_dump()) == t

    def test_audit_template_string_sugar(self):
        """audit_template accepts a string and normalizes it to a list."""
        t = Target(audit_template="fabric")  # type: ignore[arg-type]
        assert t.audit_template == ["fabric"]

    def test_audit_template_list(self):
        """audit_template accepts a list of strings."""
        t = Target(audit_template=["fabric", "custom"])
        assert t.audit_template == ["fabric", "custom"]

    def test_audit_template_default_none(self):
        """audit_template defaults to None."""
        t = Target()
        assert t.audit_template is None

    def test_audit_template_inherit_default_true(self):
        """audit_template_inherit defaults to True."""
        t = Target()
        assert t.audit_template_inherit is True

    def test_audit_template_inherit_false(self):
        """audit_template_inherit accepts False."""
        t = Target(audit_template_inherit=False)
        assert t.audit_template_inherit is False

    def test_audit_columns_exclude_list(self):
        """audit_columns_exclude accepts a list of strings."""
        t = Target(audit_columns_exclude=["_loaded_at", "_run_*"])
        assert t.audit_columns_exclude == ["_loaded_at", "_run_*"]

    def test_audit_columns_exclude_default_none(self):
        """audit_columns_exclude defaults to None."""
        t = Target()
        assert t.audit_columns_exclude is None

    def test_round_trip_with_new_fields(self):
        """Target round-trips with all new audit template fields."""
        t = Target(
            audit_template=["fabric", "custom"],
            audit_template_inherit=False,
            audit_columns_exclude=["_loaded_at"],
        )
        assert Target.model_validate(t.model_dump()) == t
