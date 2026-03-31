"""Tests for pipeline step models and discriminated union."""

import pytest
from pydantic import TypeAdapter, ValidationError

from weevr.model.pipeline import (
    AggregateParams,
    AggregateStep,
    CaseWhenBranch,
    CaseWhenParams,
    CaseWhenStep,
    CastParams,
    CastStep,
    CoalesceParams,
    CoalesceStep,
    DateOpsParams,
    DateOpsStep,
    DedupStep,
    DeriveParams,
    DeriveStep,
    DropParams,
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
    SelectParams,
    SelectStep,
    SortParams,
    SortStep,
    Step,
    StringOpsParams,
    StringOpsStep,
    UnionParams,
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
# Instance-based round-trip for multi-word step discriminator
# ---------------------------------------------------------------------------


class TestMultiWordStepInstanceRoundTrip:
    """Verify that already-constructed multi-word step instances survive a
    second pass through the Step TypeAdapter (instance-branch of discriminator).
    """

    def test_case_when_step_instance_round_trip(self):
        """CaseWhenStep instance dispatches correctly via the instance branch."""
        step = CaseWhenStep(
            case_when=CaseWhenParams(
                column="tier",
                cases=[CaseWhenBranch(when=SparkExpr("amount > 100"), then=SparkExpr("'high'"))],
                otherwise=SparkExpr("'low'"),
            )
        )
        result = _step_adapter.validate_python(step)
        assert isinstance(result, CaseWhenStep)
        assert result.case_when.column == "tier"

    def test_fill_null_step_instance_round_trip(self):
        """FillNullStep instance dispatches correctly via the instance branch."""
        step = FillNullStep(fill_null=FillNullParams(columns={"amount": 0}))
        result = _step_adapter.validate_python(step)
        assert isinstance(result, FillNullStep)
        assert result.fill_null.columns == {"amount": 0}

    def test_string_ops_step_instance_round_trip(self):
        """StringOpsStep instance dispatches correctly via the instance branch."""
        step = StringOpsStep(string_ops=StringOpsParams(columns=["name"], expr="trim({col})"))
        result = _step_adapter.validate_python(step)
        assert isinstance(result, StringOpsStep)
        assert result.string_ops.columns == ["name"]

    def test_date_ops_step_instance_round_trip(self):
        """DateOpsStep instance dispatches correctly via the instance branch."""
        step = DateOpsStep(
            date_ops=DateOpsParams(
                columns=["created_at"],
                expr="date_format({col}, 'yyyy-MM-dd')",
            )
        )
        result = _step_adapter.validate_python(step)
        assert isinstance(result, DateOpsStep)
        assert result.date_ops.columns == ["created_at"]


# ---------------------------------------------------------------------------
# Additional supporting models
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
# Additional param models
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
    """Test Step discriminated union dispatch for additional step types."""

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


class TestFormatSpecValidation:
    """Test FormatSpec edge case validators."""

    def test_empty_pattern_raises(self):
        """Empty pattern string raises ValidationError."""
        from weevr.model.pipeline import FormatSpec

        with pytest.raises(ValidationError, match="must not be empty"):
            FormatSpec(pattern="")


class TestConcatParamsValidation:
    """Test ConcatParams null_literal validator."""

    def test_empty_null_literal_with_literal_mode_raises(self):
        """Empty null_literal when null_mode='literal' raises."""
        from weevr.model.pipeline import ConcatParams

        with pytest.raises(ValidationError, match="null_literal"):
            ConcatParams(target="out", columns=["a", "b"], null_mode="literal", null_literal="")

    def test_null_literal_with_skip_mode_ok(self):
        """Empty null_literal with skip mode is fine (not used)."""
        from weevr.model.pipeline import ConcatParams

        p = ConcatParams(target="out", columns=["a", "b"], null_mode="skip", null_literal="")
        assert p.null_literal == ""


# ---------------------------------------------------------------------------
# Resolve step models
# ---------------------------------------------------------------------------


class TestCurrentConfig:
    """Test CurrentConfig model."""

    def test_basic(self):
        """CurrentConfig with column and default value."""
        from weevr.model.pipeline import CurrentConfig

        c = CurrentConfig(column="is_current")
        assert c.column == "is_current"
        assert c.value is True

    def test_custom_value(self):
        """CurrentConfig with custom value."""
        from weevr.model.pipeline import CurrentConfig

        c = CurrentConfig(column="is_current", value="Y")
        assert c.value == "Y"


class TestEffectiveConfig:
    """Test EffectiveConfig model validation."""

    def test_date_range_mode(self):
        """Date range with all three fields."""
        from weevr.model.pipeline import EffectiveConfig

        e = EffectiveConfig.model_validate(
            {"date_column": "order_date", "from": "eff_from", "to": "eff_to"}
        )
        assert e.date_column == "order_date"
        assert e.from_ == "eff_from"
        assert e.to == "eff_to"
        assert e.current is None

    def test_current_string_sugar(self):
        """Current flag as plain string (column name, value=True)."""
        from weevr.model.pipeline import EffectiveConfig

        e = EffectiveConfig(current="is_current")  # type: ignore[arg-type]
        assert e.current is not None
        assert e.current.column == "is_current"  # type: ignore[union-attr]
        assert e.current.value is True  # type: ignore[union-attr]

    def test_current_dict_form(self):
        """Current flag with custom value via dict."""
        from weevr.model.pipeline import EffectiveConfig

        e = EffectiveConfig(current={"column": "is_current", "value": "Y"})  # type: ignore[arg-type]
        assert e.current.column == "is_current"  # type: ignore[union-attr]
        assert e.current.value == "Y"  # type: ignore[union-attr]

    def test_date_range_partial_raises(self):
        """Partial date range fields raise ValidationError."""
        from weevr.model.pipeline import EffectiveConfig

        with pytest.raises(ValidationError, match="all-or-nothing"):
            EffectiveConfig.model_validate({"date_column": "order_date"})

    def test_date_range_and_current_exclusive(self):
        """Date range and current are mutually exclusive."""
        from weevr.model.pipeline import EffectiveConfig

        with pytest.raises(ValidationError, match="mutually exclusive"):
            EffectiveConfig.model_validate(
                {
                    "date_column": "order_date",
                    "from": "eff_from",
                    "to": "eff_to",
                    "current": "is_current",
                }
            )

    def test_empty_raises(self):
        """EffectiveConfig with no fields raises ValidationError."""
        from weevr.model.pipeline import EffectiveConfig

        with pytest.raises(ValidationError):
            EffectiveConfig()


class TestResolveParams:
    """Test ResolveParams model validation and match sugar."""

    def test_required_fields(self):
        """ResolveParams requires name, lookup, match, pk."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            name="plant_id", lookup="dim_plant", match={"plant_code": "natural_id"}, pk="id"
        )
        assert p.name == "plant_id"
        assert p.lookup == "dim_plant"
        assert p.match == {"plant_code": "natural_id"}
        assert p.pk == "id"

    def test_match_sugar_string(self):
        """String match sugar: 'col' -> {'col': 'col'}."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match="natural_id",  # type: ignore[arg-type]
            pk="id",
        )
        assert p.match == {"natural_id": "natural_id"}

    def test_match_sugar_list(self):
        """List match sugar: ['a', 'b'] -> {'a': 'a', 'b': 'b'}."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match=["mandt", "plant"],  # type: ignore[arg-type]
            pk="id",
        )
        assert p.match == {"mandt": "mandt", "plant": "plant"}

    def test_match_dict_passthrough(self):
        """Dict match passes through unchanged."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            name="plant_id",
            lookup="dim_plant",
            match={"plant": "natural_id", "region": "region_code"},
            pk="id",
        )
        assert p.match == {"plant": "natural_id", "region": "region_code"}

    def test_defaults(self):
        """Default sentinel and behavior values."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(name="fk", lookup="dim", match="bk", pk="id")  # type: ignore[arg-type]
        assert p.on_invalid == -4
        assert p.on_unknown == -1
        assert p.on_duplicate == "warn"
        assert p.on_failure == "abort"
        assert p.normalize is None
        assert p.drop_source_columns is False
        assert p.include is None
        assert p.include_prefix is None
        assert p.effective is None
        assert p.where is None
        assert p.batch is None

    def test_include_string_sugar(self):
        """String include sugar: 'col' -> ['col']."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            name="fk",
            lookup="dim",
            match="bk",  # type: ignore[arg-type]
            pk="id",
            include="description",  # type: ignore[arg-type]
        )
        assert p.include == ["description"]

    def test_include_list(self):
        """List include passes through."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            name="fk",
            lookup="dim",
            match="bk",  # type: ignore[arg-type]
            pk="id",
            include=["description", "category"],
        )
        assert p.include == ["description", "category"]

    def test_include_dict(self):
        """Dict include for rename passes through."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            name="fk",
            lookup="dim",
            match="bk",  # type: ignore[arg-type]
            pk="id",
            include={"description": "plant_desc"},
        )
        assert p.include == {"description": "plant_desc"}

    def test_include_prefix_without_include_raises(self):
        """include_prefix without include raises ValidationError."""
        from weevr.model.pipeline import ResolveParams

        with pytest.raises(ValidationError, match="include_prefix"):
            ResolveParams(
                name="fk",
                lookup="dim",
                match="bk",  # type: ignore[arg-type]
                pk="id",
                include_prefix="dim_",
            )

    def test_effective_composable_with_where(self):
        """Effective and where can coexist."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            name="fk",
            lookup="dim",
            match="bk",  # type: ignore[arg-type]
            pk="id",
            effective={"current": "is_current"},  # type: ignore[arg-type]
            where="region = ${region_code}",
        )
        assert p.effective is not None
        assert p.where is not None

    def test_batch_and_single_exclusive(self):
        """Batch mode and single-mode required fields are mutually exclusive."""
        from weevr.model.pipeline import ResolveParams

        with pytest.raises(ValidationError, match="mutually exclusive"):
            ResolveParams(
                name="fk",
                lookup="dim",
                match="bk",  # type: ignore[arg-type]
                pk="id",
                batch=[{"name": "fk2", "lookup": "d2", "match": "b2"}],  # type: ignore[list-item]
            )

    def test_batch_mode_no_required_single_fields(self):
        """Batch mode does not require name/lookup/match/pk at outer level."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            pk="id",
            on_invalid=-4,
            batch=[  # type: ignore[list-item]
                {"name": "fk1", "lookup": "dim1", "match": "bk1"},
                {"name": "fk2", "lookup": "dim2", "match": "bk2"},
            ],
        )
        assert p.batch is not None
        assert len(p.batch) == 2

    def test_on_invalid_equals_on_unknown_raises(self):
        """Equal sentinel values for on_invalid and on_unknown raise."""
        from weevr.model.pipeline import ResolveParams

        with pytest.raises(ValidationError, match="must be different"):
            ResolveParams(
                name="fk",
                lookup="dim",
                match={"bk": "bk"},
                pk="id",
                on_invalid=-1,
                on_unknown=-1,
            )


class TestResolveBatchItem:
    """Test ResolveBatchItem model."""

    def test_required_fields(self):
        """ResolveBatchItem requires name, lookup, match."""
        from weevr.model.pipeline import ResolveBatchItem

        item = ResolveBatchItem(name="fk", lookup="dim", match="bk")  # type: ignore[arg-type]
        assert item.name == "fk"
        assert item.match == {"bk": "bk"}

    def test_match_sugar(self):
        """ResolveBatchItem supports match sugar."""
        from weevr.model.pipeline import ResolveBatchItem

        item = ResolveBatchItem(name="fk", lookup="dim", match=["a", "b"])  # type: ignore[arg-type]
        assert item.match == {"a": "a", "b": "b"}

    def test_override_fields(self):
        """ResolveBatchItem accepts optional override fields."""
        from weevr.model.pipeline import ResolveBatchItem

        item = ResolveBatchItem(
            name="fk",
            lookup="dim",
            match="bk",  # type: ignore[arg-type]
            pk="sk",
            on_invalid=-9,
            effective={"current": "is_active"},  # type: ignore[arg-type]
        )
        assert item.pk == "sk"
        assert item.on_invalid == -9
        assert item.effective is not None


class TestResolveStepDiscriminator:
    """Test resolve step in the Step discriminated union."""

    def _validate(self, d: dict) -> Step:  # type: ignore[type-arg]
        return _step_adapter.validate_python(d)

    def test_resolve_step_dispatches(self):
        """Dict with 'resolve' key dispatches to ResolveStep."""
        from weevr.model.pipeline import ResolveStep

        step = self._validate(
            {"resolve": {"name": "plant_id", "lookup": "dim_plant", "match": "bk", "pk": "id"}}
        )
        assert isinstance(step, ResolveStep)
        assert step.resolve.name == "plant_id"

    def test_resolve_step_round_trip(self):
        """ResolveStep round-trips via Step union adapter."""
        from weevr.model.pipeline import ResolveStep

        step = self._validate(
            {"resolve": {"name": "plant_id", "lookup": "dim_plant", "match": "bk", "pk": "id"}}
        )
        assert isinstance(step, ResolveStep)
        dumped = step.model_dump()
        restored = _step_adapter.validate_python(dumped)
        assert isinstance(restored, ResolveStep)
        assert restored.resolve.match == {"bk": "bk"}


class TestBatchDefaultMerging:
    """Test batch default merging on ResolveParams."""

    def test_shared_pk_applied_to_items(self):
        """Shared pk is applied to items that lack it."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            pk="id",
            on_invalid=-4,
            batch=[  # type: ignore[list-item]
                {"name": "fk1", "lookup": "dim1", "match": "bk1"},
                {"name": "fk2", "lookup": "dim2", "match": "bk2"},
            ],
        )
        items = p.resolve_batch_items()
        assert len(items) == 2
        assert items[0].pk == "id"
        assert items[1].pk == "id"

    def test_item_pk_overrides_shared(self):
        """Item-level pk overrides the shared default."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            pk="id",
            batch=[  # type: ignore[list-item]
                {"name": "fk1", "lookup": "dim1", "match": "bk1", "pk": "sk"},
                {"name": "fk2", "lookup": "dim2", "match": "bk2"},
            ],
        )
        items = p.resolve_batch_items()
        assert items[0].pk == "sk"
        assert items[1].pk == "id"

    def test_shared_on_invalid_applied(self):
        """Shared on_invalid applied to items without it."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            pk="id",
            on_invalid=-9,
            batch=[  # type: ignore[list-item]
                {"name": "fk1", "lookup": "dim1", "match": "bk1"},
            ],
        )
        items = p.resolve_batch_items()
        assert items[0].on_invalid == -9

    def test_item_effective_overrides_shared(self):
        """Item-level effective overrides shared effective."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            pk="id",
            effective={"current": "is_current"},  # type: ignore[arg-type]
            batch=[  # type: ignore[list-item]
                {"name": "fk1", "lookup": "dim1", "match": "bk1"},
                {
                    "name": "fk2",
                    "lookup": "dim2",
                    "match": "bk2",
                    "effective": {"current": {"column": "active", "value": "Y"}},
                },
            ],
        )
        items = p.resolve_batch_items()
        assert items[0].effective is not None
        assert items[0].effective.current.column == "is_current"  # type: ignore[union-attr]
        assert items[1].effective is not None
        assert items[1].effective.current.column == "active"  # type: ignore[union-attr]

    def test_item_with_all_fields_ignores_shared(self):
        """Item with all fields set ignores shared defaults."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            pk="shared_pk",
            on_invalid=-99,
            on_unknown=-88,
            batch=[  # type: ignore[list-item]
                {
                    "name": "fk1",
                    "lookup": "dim1",
                    "match": "bk1",
                    "pk": "item_pk",
                    "on_invalid": -1,
                    "on_unknown": -2,
                },
            ],
        )
        items = p.resolve_batch_items()
        assert items[0].pk == "item_pk"
        assert items[0].on_invalid == -1
        assert items[0].on_unknown == -2

    def test_items_inherit_on_duplicate(self):
        """Items inherit shared on_duplicate when not set."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            pk="id",
            on_duplicate="error",
            batch=[  # type: ignore[list-item]
                {"name": "fk1", "lookup": "dim1", "match": "bk1"},
            ],
        )
        items = p.resolve_batch_items()
        assert items[0].on_duplicate == "error"

    def test_missing_pk_on_item_and_shared_raises(self):
        """Item without pk and no shared pk raises ValueError."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            batch=[  # type: ignore[list-item]
                {"name": "fk1", "lookup": "dim1", "match": "bk1"},
            ],
        )
        with pytest.raises(ValueError, match="pk"):
            p.resolve_batch_items()


# ---------------------------------------------------------------------------
# Empty collection validators
# ---------------------------------------------------------------------------


class TestSelectParamsNonEmpty:
    """Test SelectParams.columns non-empty validation."""

    def test_valid_columns(self):
        """SelectParams with at least one column is accepted."""
        p = SelectParams(columns=["id", "name"])
        assert p.columns == ["id", "name"]

    def test_empty_columns_raises(self):
        """Empty columns list raises ValidationError."""
        with pytest.raises(ValidationError, match="columns must not be empty"):
            SelectParams(columns=[])


class TestDropParamsNonEmpty:
    """Test DropParams.columns non-empty validation."""

    def test_valid_columns(self):
        """DropParams with at least one column is accepted."""
        p = DropParams(columns=["_tmp"])
        assert p.columns == ["_tmp"]

    def test_empty_columns_raises(self):
        """Empty columns list raises ValidationError."""
        with pytest.raises(ValidationError, match="columns must not be empty"):
            DropParams(columns=[])


class TestCastParamsNonEmpty:
    """Test CastParams.columns non-empty validation."""

    def test_valid_columns(self):
        """CastParams with at least one mapping is accepted."""
        p = CastParams(columns={"amount": "double"})
        assert p.columns == {"amount": "double"}

    def test_empty_columns_raises(self):
        """Empty columns dict raises ValidationError."""
        with pytest.raises(ValidationError, match="columns must not be empty"):
            CastParams(columns={})


class TestDeriveParamsNonEmpty:
    """Test DeriveParams.columns non-empty validation."""

    def test_valid_columns(self):
        """DeriveParams with at least one expression is accepted."""
        p = DeriveParams(columns={"full_name": SparkExpr("first_name || ' ' || last_name")})
        assert len(p.columns) == 1

    def test_empty_columns_raises(self):
        """Empty columns dict raises ValidationError."""
        with pytest.raises(ValidationError, match="columns must not be empty"):
            DeriveParams(columns={})


class TestSortParamsNonEmpty:
    """Test SortParams.columns non-empty validation."""

    def test_valid_columns(self):
        """SortParams with at least one column is accepted."""
        p = SortParams(columns=["date"])
        assert p.columns == ["date"]

    def test_empty_columns_raises(self):
        """Empty columns list raises ValidationError."""
        with pytest.raises(ValidationError, match="columns must not be empty"):
            SortParams(columns=[])


class TestUnionParamsNonEmpty:
    """Test UnionParams.sources non-empty validation."""

    def test_valid_sources(self):
        """UnionParams with at least one source is accepted."""
        p = UnionParams(sources=["table_a", "table_b"])
        assert p.sources == ["table_a", "table_b"]

    def test_empty_sources_raises(self):
        """Empty sources list raises ValidationError."""
        with pytest.raises(ValidationError, match="sources must not be empty"):
            UnionParams(sources=[])


class TestResolveParamsBatchNonEmpty:
    """Test ResolveParams.batch non-empty validation."""

    def test_empty_batch_raises(self):
        """Empty batch list raises ValidationError."""
        from weevr.model.pipeline import ResolveParams

        with pytest.raises(ValidationError, match="batch must contain at least one item"):
            ResolveParams(batch=[])


class TestResolveIncludeColumnAsSyntax:
    """Test {column, as} object form for resolve include."""

    def test_resolve_params_include_column_as_list(self):
        """List of {column, as} objects normalizes to dict[str, str]."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            name="fk",
            lookup="dim",
            match="bk",  # type: ignore[arg-type]
            pk="id",
            include=[  # type: ignore[arg-type]
                {"column": "description", "as": "plant_desc"},
                {"column": "category", "as": "plant_cat"},
            ],
        )
        assert p.include == {"description": "plant_desc", "category": "plant_cat"}

    def test_resolve_params_include_column_without_as(self):
        """Object with column but no as normalizes to {col: col}."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            name="fk",
            lookup="dim",
            match="bk",  # type: ignore[arg-type]
            pk="id",
            include=[{"column": "description"}],  # type: ignore[arg-type]
        )
        assert p.include == {"description": "description"}

    def test_resolve_params_include_mixed_list(self):
        """Mixed list of strings and {column, as} objects normalizes to dict[str, str]."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            name="fk",
            lookup="dim",
            match="bk",  # type: ignore[arg-type]
            pk="id",
            include=["category", {"column": "description", "as": "plant_desc"}],  # type: ignore[arg-type]
        )
        assert p.include == {"category": "category", "description": "plant_desc"}

    def test_resolve_params_include_list_str_unchanged(self):
        """Plain list[str] still passes through as list[str]."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            name="fk",
            lookup="dim",
            match="bk",  # type: ignore[arg-type]
            pk="id",
            include=["description", "category"],
        )
        assert p.include == ["description", "category"]

    def test_resolve_params_include_dict_passthrough(self):
        """dict[str, str] include still passes through unchanged."""
        from weevr.model.pipeline import ResolveParams

        p = ResolveParams(
            name="fk",
            lookup="dim",
            match="bk",  # type: ignore[arg-type]
            pk="id",
            include={"description": "plant_desc"},
        )
        assert p.include == {"description": "plant_desc"}

    def test_resolve_batch_item_include_column_as_list(self):
        """ResolveBatchItem list of {column, as} objects normalizes to dict[str, str]."""
        from weevr.model.pipeline import ResolveBatchItem

        item = ResolveBatchItem(
            name="fk",
            lookup="dim",
            match="bk",  # type: ignore[arg-type]
            include=[  # type: ignore[arg-type]
                {"column": "description", "as": "plant_desc"},
                {"column": "category", "as": "plant_cat"},
            ],
        )
        assert item.include == {"description": "plant_desc", "category": "plant_cat"}

    def test_resolve_batch_item_include_column_without_as(self):
        """ResolveBatchItem object with column but no as normalizes to {col: col}."""
        from weevr.model.pipeline import ResolveBatchItem

        item = ResolveBatchItem(
            name="fk",
            lookup="dim",
            match="bk",  # type: ignore[arg-type]
            include=[{"column": "description"}],  # type: ignore[arg-type]
        )
        assert item.include == {"description": "description"}

    def test_resolve_batch_item_include_mixed_list(self):
        """ResolveBatchItem mixed list normalizes to dict[str, str]."""
        from weevr.model.pipeline import ResolveBatchItem

        item = ResolveBatchItem(
            name="fk",
            lookup="dim",
            match="bk",  # type: ignore[arg-type]
            include=["category", {"column": "description", "as": "plant_desc"}],  # type: ignore[arg-type]
        )
        assert item.include == {"category": "category", "description": "plant_desc"}

    def test_resolve_batch_item_include_list_str_unchanged(self):
        """ResolveBatchItem plain list[str] still passes through as list[str]."""
        from weevr.model.pipeline import ResolveBatchItem

        item = ResolveBatchItem(
            name="fk",
            lookup="dim",
            match="bk",  # type: ignore[arg-type]
            include=["description", "category"],
        )
        assert item.include == ["description", "category"]
