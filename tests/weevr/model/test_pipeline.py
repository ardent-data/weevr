"""Tests for pipeline step models and discriminated union."""

import pytest
from pydantic import TypeAdapter, ValidationError

from weevr.model.pipeline import (
    CastStep,
    DedupStep,
    DeriveParams,
    DeriveStep,
    DropStep,
    FilterParams,
    FilterStep,
    JoinKeyPair,
    JoinParams,
    JoinStep,
    RenameStep,
    SelectStep,
    SortStep,
    Step,
    UnionStep,
)
from weevr.model.types import SparkExpr

_step_adapter: TypeAdapter[Step] = TypeAdapter(Step)  # type: ignore[type-arg]


class TestJoinKeyPair:
    """Test JoinKeyPair model."""

    def test_basic(self):
        """JoinKeyPair stores left and right keys."""
        jkp = JoinKeyPair(left="customer_id", right="cust_id")
        assert jkp.left == "customer_id"
        assert jkp.right == "cust_id"

    def test_frozen(self):
        """JoinKeyPair is immutable."""
        jkp = JoinKeyPair(left="a", right="b")
        with pytest.raises(ValidationError):
            jkp.left = "c"  # type: ignore[misc]

    def test_round_trip(self):
        """JoinKeyPair round-trips."""
        jkp = JoinKeyPair(left="x", right="y")
        assert JoinKeyPair.model_validate(jkp.model_dump()) == jkp


class TestJoinParams:
    """Test JoinParams key normalisation."""

    def test_simple_keys_normalised(self):
        """Simple string keys are normalised to JoinKeyPair with left==right."""
        jp = JoinParams(source="orders", on=["id"])  # type: ignore[list-item]
        assert isinstance(jp.on[0], JoinKeyPair)
        assert jp.on[0].left == "id"
        assert jp.on[0].right == "id"

    def test_explicit_pairs_accepted(self):
        """Explicit JoinKeyPair dicts are accepted."""
        jp = JoinParams(source="orders", on=[{"left": "customer_id", "right": "cust_id"}])  # type: ignore[list-item]
        assert jp.on[0].left == "customer_id"
        assert jp.on[0].right == "cust_id"

    def test_mixed_keys(self):
        """Mix of simple and explicit keys both normalised."""
        jp = JoinParams(
            source="orders",
            on=["id", {"left": "date", "right": "order_date"}],  # type: ignore[list-item]
        )
        assert jp.on[0] == JoinKeyPair(left="id", right="id")
        assert jp.on[1] == JoinKeyPair(left="date", right="order_date")

    def test_default_join_type(self):
        """Default join type is 'inner'."""
        jp = JoinParams(source="t", on=["id"])  # type: ignore[list-item]
        assert jp.type == "inner"

    def test_all_join_types(self):
        """All 7 join types are valid."""
        for jt in ("inner", "left", "right", "full", "cross", "semi", "anti"):
            jp = JoinParams(source="t", on=["id"], type=jt)  # type: ignore[arg-type,list-item]
            assert jp.type == jt

    def test_invalid_join_type_raises(self):
        """Unknown join type raises ValidationError."""
        with pytest.raises(ValidationError):
            JoinParams(source="t", on=["id"], type="outer")  # type: ignore[arg-type,list-item]

    def test_null_safe_default_true(self):
        """null_safe defaults to True."""
        jp = JoinParams(source="t", on=["id"])  # type: ignore[list-item]
        assert jp.null_safe is True

    def test_null_safe_can_be_disabled(self):
        """null_safe can be set to False."""
        jp = JoinParams(source="t", on=["id"], null_safe=False)  # type: ignore[list-item]
        assert jp.null_safe is False


class TestStepDiscriminator:
    """Test Step discriminated union dispatch for all 10 step types."""

    def _validate(self, d: dict) -> Step:  # type: ignore[type-arg]
        return _step_adapter.validate_python(d)

    def test_filter_step(self):
        """Dict with 'filter' key dispatches to FilterStep."""
        step = self._validate({"filter": {"expr": "amount > 0"}})
        assert isinstance(step, FilterStep)
        assert step.filter.expr == "amount > 0"

    def test_derive_step(self):
        """Dict with 'derive' key dispatches to DeriveStep."""
        step = self._validate({"derive": {"columns": {"amount_usd": "amount * rate"}}})
        assert isinstance(step, DeriveStep)
        assert "amount_usd" in step.derive.columns
        assert step.derive.columns["amount_usd"] == "amount * rate"

    def test_join_step(self):
        """Dict with 'join' key dispatches to JoinStep."""
        step = self._validate({"join": {"source": "orders", "on": ["id"]}})
        assert isinstance(step, JoinStep)
        assert step.join.source == "orders"

    def test_select_step(self):
        """Dict with 'select' key dispatches to SelectStep."""
        step = self._validate({"select": {"columns": ["id", "name"]}})
        assert isinstance(step, SelectStep)
        assert step.select.columns == ["id", "name"]

    def test_drop_step(self):
        """Dict with 'drop' key dispatches to DropStep."""
        step = self._validate({"drop": {"columns": ["_tmp"]}})
        assert isinstance(step, DropStep)

    def test_rename_step(self):
        """Dict with 'rename' key dispatches to RenameStep."""
        step = self._validate({"rename": {"columns": {"old_name": "new_name"}}})
        assert isinstance(step, RenameStep)
        assert step.rename.columns == {"old_name": "new_name"}

    def test_cast_step(self):
        """Dict with 'cast' key dispatches to CastStep."""
        step = self._validate({"cast": {"columns": {"amount": "double"}}})
        assert isinstance(step, CastStep)

    def test_dedup_step(self):
        """Dict with 'dedup' key dispatches to DedupStep."""
        step = self._validate({"dedup": {"keys": ["id"], "order_by": "ts"}})
        assert isinstance(step, DedupStep)
        assert step.dedup.order_by == "ts"

    def test_dedup_default_keep(self):
        """DedupParams defaults keep to 'last'."""
        step = self._validate({"dedup": {"keys": ["id"]}})
        assert isinstance(step, DedupStep)
        assert step.dedup.keep == "last"

    def test_dedup_keep_first(self):
        """DedupParams accepts keep='first'."""
        step = self._validate({"dedup": {"keys": ["id"], "keep": "first"}})
        assert isinstance(step, DedupStep)
        assert step.dedup.keep == "first"

    def test_sort_step(self):
        """Dict with 'sort' key dispatches to SortStep."""
        step = self._validate({"sort": {"columns": ["date", "id"]}})
        assert isinstance(step, SortStep)
        assert step.sort.ascending is True

    def test_union_step(self):
        """Dict with 'union' key dispatches to UnionStep."""
        step = self._validate({"union": {"sources": ["table_a", "table_b"]}})
        assert isinstance(step, UnionStep)
        assert step.union.mode == "by_name"

    def test_union_default_allow_missing(self):
        """UnionParams defaults allow_missing to False."""
        step = self._validate({"union": {"sources": ["s1", "s2"]}})
        assert isinstance(step, UnionStep)
        assert step.union.allow_missing is False

    def test_union_allow_missing_true(self):
        """UnionParams accepts allow_missing=True."""
        step = self._validate({"union": {"sources": ["s1", "s2"], "allow_missing": True}})
        assert isinstance(step, UnionStep)
        assert step.union.allow_missing is True

    def test_unknown_step_type_raises(self):
        """Dict with an unknown step key raises ValidationError."""
        with pytest.raises(ValidationError):
            self._validate({"pivot": {"columns": ["x"]}})

    def test_ambiguous_step_keys_raises(self):
        """Dict with two step type keys raises ValidationError."""
        with pytest.raises(ValidationError):
            self._validate({"filter": {"expr": "x > 0"}, "derive": {"columns": {"y": "1"}}})


class TestStepFreezeAndRoundTrip:
    """Test that all step types are frozen and round-trip correctly."""

    def test_filter_step_frozen(self):
        """FilterStep is immutable."""
        s = FilterStep(filter=FilterParams(expr=SparkExpr("x > 0")))
        with pytest.raises(ValidationError):
            s.filter = FilterParams(expr=SparkExpr("y > 0"))  # type: ignore[misc]

    def test_derive_step_frozen(self):
        """DeriveStep is immutable."""
        s = DeriveStep(derive=DeriveParams(columns={"col": SparkExpr("1")}))
        with pytest.raises(ValidationError):
            s.derive = DeriveParams(columns={"other": SparkExpr("2")})  # type: ignore[misc]

    def test_filter_step_round_trip(self):
        """FilterStep round-trips via Step union adapter."""
        s = FilterStep(filter=FilterParams(expr=SparkExpr("amount > 0")))
        dumped = s.model_dump()
        restored = _step_adapter.validate_python(dumped)
        assert isinstance(restored, FilterStep)
        assert restored.filter.expr == "amount > 0"

    def test_join_step_round_trip(self):
        """JoinStep with explicit pairs round-trips."""
        s = JoinStep(
            join=JoinParams(
                source="orders",
                type="left",
                on=[JoinKeyPair(left="a", right="b")],
            )
        )
        dumped = s.model_dump()
        restored = _step_adapter.validate_python(dumped)
        assert isinstance(restored, JoinStep)
        assert restored.join.on[0].left == "a"

    def test_join_on_not_list_raises(self):
        """JoinParams raises when 'on' is not a list."""
        with pytest.raises(ValidationError):
            JoinParams(source="t", on="not_a_list")  # type: ignore[arg-type]

    def test_non_step_object_in_union_raises(self):
        """Passing a non-dict, non-Step object to the Step union raises ValidationError."""
        with pytest.raises(ValidationError):
            _step_adapter.validate_python(42)

    def test_all_step_types_round_trip(self):
        """All 10 step types construct and dump without error."""
        steps_data = [
            {"filter": {"expr": "x > 0"}},
            {"derive": {"columns": {"c": "a + b"}}},
            {"join": {"source": "t", "on": ["id"]}},
            {"select": {"columns": ["a", "b"]}},
            {"drop": {"columns": ["tmp"]}},
            {"rename": {"columns": {"old": "new"}}},
            {"cast": {"columns": {"amount": "double"}}},
            {"dedup": {"keys": ["id"]}},
            {"sort": {"columns": ["date"]}},
            {"union": {"sources": ["s1", "s2"]}},
        ]
        for data in steps_data:
            step = _step_adapter.validate_python(data)
            dumped = step.model_dump()  # type: ignore[union-attr]
            assert isinstance(dumped, dict)
