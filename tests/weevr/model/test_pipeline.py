"""Tests for pipeline step models and discriminated union."""

import pytest
from pydantic import TypeAdapter, ValidationError

from weevr.model.pipeline import (
    AggregateParams,
    AggregateStep,
    CaseWhenBranch,
    CaseWhenParams,
    CaseWhenStep,
    CastStep,
    CoalesceParams,
    CoalesceStep,
    DateOpsParams,
    DateOpsStep,
    DedupStep,
    DeriveParams,
    DeriveStep,
    DropStep,
    FillNullParams,
    FillNullStep,
    FilterParams,
    FilterStep,
    JoinKeyPair,
    JoinParams,
    JoinStep,
    PivotParams,
    PivotStep,
    RenameParams,
    RenameStep,
    SelectStep,
    SortStep,
    Step,
    StringOpsParams,
    StringOpsStep,
    UnionStep,
    UnpivotParams,
    UnpivotStep,
    WindowFrame,
    WindowParams,
    WindowStep,
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

    def test_yaml_boolean_on_key_coercion(self):
        """YAML 1.1 parses bare 'on' as boolean True; model remaps it."""
        data = {True: ["id"], "source": "orders", "type": "inner"}
        jp = JoinParams.model_validate(data)
        assert jp.on[0].left == "id"
        assert jp.on[0].right == "id"


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
            self._validate({"explode": {"column": "tags"}})

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
        """All 19 step types construct and dump without error."""
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
            {"aggregate": {"measures": {"total": "sum(amount)"}}},
            {"window": {"functions": {"rn": "row_number()"}, "partition_by": ["region"]}},
            {
                "pivot": {
                    "group_by": ["region"],
                    "pivot_column": "status",
                    "values": ["active", "inactive"],
                    "aggregate": "sum(amount)",
                },
            },
            {
                "unpivot": {
                    "columns": ["q1", "q2"],
                    "name_column": "quarter",
                    "value_column": "revenue",
                },
            },
            {
                "case_when": {
                    "column": "tier",
                    "cases": [{"when": "amount > 100", "then": "'high'"}],
                    "otherwise": "'low'",
                },
            },
            {"fill_null": {"columns": {"amount": 0, "name": "unknown"}}},
            {"coalesce": {"columns": {"email": ["work_email", "personal_email"]}}},
            {"string_ops": {"columns": ["name"], "expr": "trim({col})"}},
            {"date_ops": {"columns": ["created_at"], "expr": "date_format({col}, 'yyyy-MM-dd')"}},
        ]
        for data in steps_data:
            step = _step_adapter.validate_python(data)
            dumped = step.model_dump()  # type: ignore[union-attr]
            assert isinstance(dumped, dict)


# ---------------------------------------------------------------------------
# New M08a supporting models
# ---------------------------------------------------------------------------


class TestWindowFrame:
    """Test WindowFrame model."""

    def test_valid_rows(self):
        """Rows frame with valid bounds."""
        wf = WindowFrame(type="rows", start=-2, end=0)
        assert wf.type == "rows"
        assert wf.start == -2
        assert wf.end == 0

    def test_valid_range(self):
        """Range frame with valid bounds."""
        wf = WindowFrame(type="range", start=-100, end=100)
        assert wf.type == "range"

    def test_start_greater_than_end_raises(self):
        """start > end raises ValidationError."""
        with pytest.raises(ValidationError, match="start.*<=.*end"):
            WindowFrame(type="rows", start=5, end=2)

    def test_frozen(self):
        """WindowFrame is immutable."""
        wf = WindowFrame(type="rows", start=-1, end=0)
        with pytest.raises(ValidationError):
            wf.type = "range"  # type: ignore[misc]


class TestCaseWhenBranch:
    """Test CaseWhenBranch model."""

    def test_valid(self):
        """Valid construction."""
        b = CaseWhenBranch(when=SparkExpr("amount > 100"), then=SparkExpr("'high'"))
        assert b.when == "amount > 100"
        assert b.then == "'high'"

    def test_missing_when_raises(self):
        """Missing when raises ValidationError."""
        with pytest.raises(ValidationError):
            CaseWhenBranch(then=SparkExpr("'high'"))  # type: ignore[call-arg]

    def test_missing_then_raises(self):
        """Missing then raises ValidationError."""
        with pytest.raises(ValidationError):
            CaseWhenBranch(when=SparkExpr("amount > 100"))  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# New M08a param models
# ---------------------------------------------------------------------------


class TestAggregateParams:
    """Test AggregateParams model."""

    def test_with_group_by(self):
        """Valid aggregate with group_by."""
        p = AggregateParams(group_by=["region"], measures={"total": SparkExpr("sum(amount)")})
        assert p.group_by == ["region"]
        assert len(p.measures) == 1

    def test_without_group_by(self):
        """Whole-DataFrame aggregation (no group_by)."""
        p = AggregateParams(measures={"cnt": SparkExpr("count(*)")})
        assert p.group_by is None

    def test_empty_measures_raises(self):
        """Empty measures raises ValidationError."""
        with pytest.raises(ValidationError, match="measures must not be empty"):
            AggregateParams(measures={})

    def test_multiple_measures(self):
        """Multiple measures in one step."""
        p = AggregateParams(
            measures={
                "total": SparkExpr("sum(amount)"),
                "cnt": SparkExpr("count(*)"),
                "avg_amt": SparkExpr("avg(amount)"),
            }
        )
        assert len(p.measures) == 3


class TestWindowParams:
    """Test WindowParams model."""

    def test_valid_full(self):
        """Valid window with all fields."""
        frame = WindowFrame(type="rows", start=-2, end=0)
        p = WindowParams(
            functions={"rn": SparkExpr("row_number()")},
            partition_by=["region"],
            order_by=["date desc"],
            frame=frame,
        )
        assert len(p.functions) == 1
        assert p.partition_by == ["region"]
        assert p.order_by == ["date desc"]
        assert p.frame == frame

    def test_optional_order_by_and_frame(self):
        """order_by and frame are optional."""
        p = WindowParams(functions={"cnt": SparkExpr("count(*)")}, partition_by=["region"])
        assert p.order_by is None
        assert p.frame is None

    def test_empty_functions_raises(self):
        """Empty functions raises ValidationError."""
        with pytest.raises(ValidationError, match="functions must not be empty"):
            WindowParams(functions={}, partition_by=["region"])

    def test_empty_partition_by_raises(self):
        """Empty partition_by raises ValidationError."""
        with pytest.raises(ValidationError, match="partition_by must not be empty"):
            WindowParams(functions={"rn": SparkExpr("row_number()")}, partition_by=[])


class TestPivotParams:
    """Test PivotParams model."""

    def test_valid(self):
        """Valid pivot construction."""
        p = PivotParams(
            group_by=["region"],
            pivot_column="status",
            values=["active", "inactive"],
            aggregate=SparkExpr("sum(amount)"),
        )
        assert p.pivot_column == "status"
        assert len(p.values) == 2

    def test_empty_values_raises(self):
        """Empty values raises ValidationError."""
        with pytest.raises(ValidationError, match="values must not be empty"):
            PivotParams(
                group_by=["r"], pivot_column="s", values=[], aggregate=SparkExpr("count(*)")
            )

    def test_mixed_value_types(self):
        """Values can be mixed types."""
        p = PivotParams(
            group_by=["r"],
            pivot_column="x",
            values=["a", 1, 2.5, True],
            aggregate=SparkExpr("count(*)"),
        )
        assert len(p.values) == 4


class TestUnpivotParams:
    """Test UnpivotParams model."""

    def test_valid(self):
        """Valid unpivot construction."""
        p = UnpivotParams(columns=["q1", "q2", "q3"], name_column="quarter", value_column="rev")
        assert len(p.columns) == 3
        assert p.name_column == "quarter"

    def test_name_equals_value_raises(self):
        """name_column == value_column raises ValidationError."""
        with pytest.raises(ValidationError, match="must be different"):
            UnpivotParams(columns=["a"], name_column="col", value_column="col")

    def test_empty_columns_raises(self):
        """Empty columns raises ValidationError."""
        with pytest.raises(ValidationError, match="columns must not be empty"):
            UnpivotParams(columns=[], name_column="n", value_column="v")


class TestCaseWhenParams:
    """Test CaseWhenParams model."""

    def test_valid_with_otherwise(self):
        """Valid case_when with otherwise."""
        p = CaseWhenParams(
            column="tier",
            cases=[CaseWhenBranch(when=SparkExpr("amount > 100"), then=SparkExpr("'high'"))],
            otherwise=SparkExpr("'low'"),
        )
        assert p.column == "tier"
        assert len(p.cases) == 1
        assert p.otherwise == "'low'"

    def test_valid_without_otherwise(self):
        """Valid case_when without otherwise (null for unmatched)."""
        p = CaseWhenParams(
            column="tier",
            cases=[CaseWhenBranch(when=SparkExpr("amount > 100"), then=SparkExpr("'high'"))],
        )
        assert p.otherwise is None

    def test_empty_cases_raises(self):
        """Empty cases raises ValidationError."""
        with pytest.raises(ValidationError, match="cases must not be empty"):
            CaseWhenParams(column="tier", cases=[])


class TestFillNullParams:
    """Test FillNullParams model."""

    def test_valid(self):
        """Valid fill_null construction."""
        p = FillNullParams(columns={"amount": 0, "name": "unknown"})
        assert p.columns is not None and len(p.columns) == 2

    def test_empty_columns_raises(self):
        """Empty columns raises ValidationError."""
        with pytest.raises(ValidationError, match="columns must not be empty"):
            FillNullParams(columns={})


class TestCoalesceParams:
    """Test CoalesceParams model."""

    def test_valid(self):
        """Valid coalesce construction."""
        p = CoalesceParams(columns={"email": ["work_email", "personal_email"]})
        assert p.columns["email"] == ["work_email", "personal_email"]

    def test_empty_columns_raises(self):
        """Empty columns dict raises ValidationError."""
        with pytest.raises(ValidationError, match="columns must not be empty"):
            CoalesceParams(columns={})

    def test_empty_source_list_raises(self):
        """Empty source list for a column raises ValidationError."""
        with pytest.raises(ValidationError, match="source list.*must not be empty"):
            CoalesceParams(columns={"email": []})


class TestStringOpsParams:
    """Test StringOpsParams model."""

    def test_valid(self):
        """Valid string_ops construction."""
        p = StringOpsParams(columns=["name"], expr="trim({col})")
        assert p.on_empty == "warn"

    def test_on_empty_error(self):
        """on_empty accepts 'error'."""
        p = StringOpsParams(columns=["name"], expr="upper({col})", on_empty="error")
        assert p.on_empty == "error"

    def test_missing_col_placeholder_raises(self):
        """Expression without {col} raises ValidationError."""
        with pytest.raises(ValidationError, match="\\{col\\}"):
            StringOpsParams(columns=["name"], expr="trim(name)")


class TestDateOpsParams:
    """Test DateOpsParams model."""

    def test_valid(self):
        """Valid date_ops construction."""
        p = DateOpsParams(columns=["created_at"], expr="date_format({col}, 'yyyy-MM-dd')")
        assert p.on_empty == "warn"

    def test_on_empty_error(self):
        """on_empty accepts 'error'."""
        p = DateOpsParams(columns=["ts"], expr="year({col})", on_empty="error")
        assert p.on_empty == "error"

    def test_missing_col_placeholder_raises(self):
        """Expression without {col} raises ValidationError."""
        with pytest.raises(ValidationError, match="\\{col\\}"):
            DateOpsParams(columns=["ts"], expr="year(ts)")


# ---------------------------------------------------------------------------
# Discriminated union dispatch for new step types
# ---------------------------------------------------------------------------


class TestNewStepDiscriminator:
    """Test Step discriminated union dispatch for new M08a step types."""

    def _validate(self, d: dict) -> Step:  # type: ignore[type-arg]
        return _step_adapter.validate_python(d)

    def test_aggregate_step(self):
        """Dict with 'aggregate' key dispatches to AggregateStep."""
        step = self._validate({"aggregate": {"measures": {"total": "sum(amount)"}}})
        assert isinstance(step, AggregateStep)

    def test_window_step(self):
        """Dict with 'window' key dispatches to WindowStep."""
        step = self._validate(
            {"window": {"functions": {"rn": "row_number()"}, "partition_by": ["region"]}}
        )
        assert isinstance(step, WindowStep)

    def test_pivot_step(self):
        """Dict with 'pivot' key dispatches to PivotStep."""
        step = self._validate(
            {
                "pivot": {
                    "group_by": ["region"],
                    "pivot_column": "status",
                    "values": ["active"],
                    "aggregate": "sum(amount)",
                }
            }
        )
        assert isinstance(step, PivotStep)

    def test_unpivot_step(self):
        """Dict with 'unpivot' key dispatches to UnpivotStep."""
        step = self._validate(
            {"unpivot": {"columns": ["q1", "q2"], "name_column": "qtr", "value_column": "rev"}}
        )
        assert isinstance(step, UnpivotStep)

    def test_case_when_step(self):
        """Dict with 'case_when' key dispatches to CaseWhenStep."""
        step = self._validate(
            {
                "case_when": {
                    "column": "tier",
                    "cases": [{"when": "amount > 100", "then": "'high'"}],
                }
            }
        )
        assert isinstance(step, CaseWhenStep)

    def test_fill_null_step(self):
        """Dict with 'fill_null' key dispatches to FillNullStep."""
        step = self._validate({"fill_null": {"columns": {"amount": 0}}})
        assert isinstance(step, FillNullStep)

    def test_coalesce_step(self):
        """Dict with 'coalesce' key dispatches to CoalesceStep."""
        step = self._validate({"coalesce": {"columns": {"email": ["e1", "e2"]}}})
        assert isinstance(step, CoalesceStep)

    def test_string_ops_step(self):
        """Dict with 'string_ops' key dispatches to StringOpsStep."""
        step = self._validate({"string_ops": {"columns": ["name"], "expr": "trim({col})"}})
        assert isinstance(step, StringOpsStep)

    def test_date_ops_step(self):
        """Dict with 'date_ops' key dispatches to DateOpsStep."""
        step = self._validate(
            {"date_ops": {"columns": ["ts"], "expr": "date_format({col}, 'yyyy-MM-dd')"}}
        )
        assert isinstance(step, DateOpsStep)


# ---------------------------------------------------------------------------
# RenameParams model
# ---------------------------------------------------------------------------


class TestRenameParams:
    """Test RenameParams model validation, including column_set field."""

    def test_static_columns_only(self):
        """RenameParams with only columns dict parses correctly."""
        p = RenameParams(columns={"old_name": "new_name"})
        assert p.columns == {"old_name": "new_name"}
        assert p.column_set is None

    def test_column_set_populated(self):
        """RenameParams with column_set reference parses correctly."""
        p = RenameParams(columns={}, column_set="my_dict")
        assert p.column_set == "my_dict"
        assert p.columns == {}

    def test_backward_compat_no_column_set(self):
        """RenameParams without column_set defaults to None."""
        p = RenameParams(columns={"a": "b"})
        assert p.column_set is None

    def test_column_set_only_empty_columns(self):
        """RenameParams with only column_set and empty columns is valid."""
        p = RenameParams(column_set="x")
        assert p.column_set == "x"
        assert p.columns == {}

    def test_column_set_and_static_columns_together(self):
        """RenameParams with both columns and column_set is valid."""
        p = RenameParams(columns={"a": "b"}, column_set="extra_renames")
        assert p.columns == {"a": "b"}
        assert p.column_set == "extra_renames"

    def test_column_set_none_explicit(self):
        """Explicitly passing column_set=None is equivalent to the default."""
        p = RenameParams(columns={"x": "y"}, column_set=None)
        assert p.column_set is None
