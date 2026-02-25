"""Tests for analytical pipeline step handlers — aggregate, window, pivot, unpivot."""

import pytest
from pyspark.sql import SparkSession

from weevr.model.pipeline import (
    AggregateParams,
    PivotParams,
    UnpivotParams,
    WindowFrame,
    WindowParams,
)
from weevr.model.types import SparkExpr
from weevr.operations.pipeline.analytical import (
    _parse_order_by,
    apply_aggregate,
    apply_pivot,
    apply_unpivot,
    apply_window,
)

pytestmark = pytest.mark.spark


class TestParseOrderBy:
    """Test the SQL-style order-by string parser."""

    def test_ascending_default(self, spark: SparkSession):
        """Column name without suffix sorts ascending."""
        cols = _parse_order_by(["amount"])
        assert len(cols) == 1

    def test_explicit_asc(self, spark: SparkSession):
        """Explicit 'asc' suffix sorts ascending."""
        cols = _parse_order_by(["amount asc"])
        assert len(cols) == 1

    def test_desc(self, spark: SparkSession):
        """'desc' suffix sorts descending."""
        cols = _parse_order_by(["amount desc"])
        assert len(cols) == 1

    def test_desc_case_insensitive(self, spark: SparkSession):
        """'DESC' suffix is case-insensitive."""
        cols = _parse_order_by(["amount DESC"])
        assert len(cols) == 1


class TestApplyAggregate:
    """Test aggregate step handler."""

    def test_basic_group_by(self, spark: SparkSession):
        """Group by region, sum amount."""
        df = spark.createDataFrame(
            [("east", 100), ("east", 200), ("west", 50)],
            ["region", "amount"],
        )
        params = AggregateParams(
            group_by=["region"],
            measures={"total": SparkExpr("sum(amount)")},
        )
        result = apply_aggregate(df, params)
        rows = {r["region"]: r["total"] for r in result.collect()}
        assert rows["east"] == 300
        assert rows["west"] == 50

    def test_whole_dataframe(self, spark: SparkSession):
        """No group_by → single row aggregation."""
        df = spark.createDataFrame([(10,), (20,), (30,)], ["v"])
        params = AggregateParams(
            measures={"total": SparkExpr("sum(v)"), "cnt": SparkExpr("count(*)")},
        )
        result = apply_aggregate(df, params)
        row = result.collect()[0]
        assert row["total"] == 60
        assert row["cnt"] == 3

    def test_multiple_measures(self, spark: SparkSession):
        """Multiple aggregates in one step."""
        df = spark.createDataFrame([(1,), (2,), (3,)], ["v"])
        params = AggregateParams(
            measures={
                "s": SparkExpr("sum(v)"),
                "a": SparkExpr("avg(v)"),
                "mx": SparkExpr("max(v)"),
            },
        )
        result = apply_aggregate(df, params)
        row = result.collect()[0]
        assert row["s"] == 6
        assert row["mx"] == 3

    def test_null_in_group_by(self, spark: SparkSession):
        """Null group_by values form a null group."""
        df = spark.createDataFrame(
            [("a", 1), (None, 2), (None, 3)],
            ["grp", "v"],
        )
        params = AggregateParams(
            group_by=["grp"],
            measures={"total": SparkExpr("sum(v)")},
        )
        result = apply_aggregate(df, params)
        null_row = [r for r in result.collect() if r["grp"] is None]
        assert len(null_row) == 1
        assert null_row[0]["total"] == 5


class TestApplyWindow:
    """Test window step handler."""

    def test_row_number(self, spark: SparkSession):
        """Row number over partition."""
        df = spark.createDataFrame(
            [("a", 3), ("a", 1), ("b", 2)],
            ["grp", "val"],
        )
        params = WindowParams(
            functions={"rn": SparkExpr("row_number()")},
            partition_by=["grp"],
            order_by=["val"],
        )
        result = apply_window(df, params)
        assert "rn" in result.columns
        rows = result.orderBy("grp", "val").collect()
        assert rows[0]["rn"] == 1  # a, val=1
        assert rows[1]["rn"] == 2  # a, val=3

    def test_running_total(self, spark: SparkSession):
        """Running total with rows frame."""
        df = spark.createDataFrame(
            [("a", 1, 10), ("a", 2, 20), ("a", 3, 30)],
            ["grp", "seq", "val"],
        )
        params = WindowParams(
            functions={"running": SparkExpr("sum(val)")},
            partition_by=["grp"],
            order_by=["seq"],
            frame=WindowFrame(type="rows", start=-2147483648, end=0),
        )
        result = apply_window(df, params)
        rows = result.orderBy("seq").collect()
        assert rows[0]["running"] == 10
        assert rows[1]["running"] == 30
        assert rows[2]["running"] == 60

    def test_multiple_functions(self, spark: SparkSession):
        """Multiple window functions in one step."""
        df = spark.createDataFrame([("a", 1), ("a", 2)], ["grp", "val"])
        params = WindowParams(
            functions={
                "rn": SparkExpr("row_number()"),
                "s": SparkExpr("sum(val)"),
            },
            partition_by=["grp"],
            order_by=["val"],
        )
        result = apply_window(df, params)
        assert "rn" in result.columns
        assert "s" in result.columns

    def test_order_by_desc(self, spark: SparkSession):
        """Descending order_by applied correctly."""
        df = spark.createDataFrame(
            [("a", 1), ("a", 3), ("a", 2)],
            ["grp", "val"],
        )
        params = WindowParams(
            functions={"rn": SparkExpr("row_number()")},
            partition_by=["grp"],
            order_by=["val desc"],
        )
        result = apply_window(df, params)
        rows = result.orderBy("rn").collect()
        assert rows[0]["val"] == 3  # first by desc order


class TestApplyPivot:
    """Test pivot step handler."""

    def test_basic_pivot(self, spark: SparkSession):
        """Pivot rows to columns."""
        df = spark.createDataFrame(
            [("east", "active", 100), ("east", "inactive", 50), ("west", "active", 200)],
            ["region", "status", "amount"],
        )
        params = PivotParams(
            group_by=["region"],
            pivot_column="status",
            values=["active", "inactive"],
            aggregate=SparkExpr("sum(amount)"),
        )
        result = apply_pivot(df, params)
        assert "active" in result.columns
        assert "inactive" in result.columns
        rows = {r["region"]: r for r in result.collect()}
        assert rows["east"]["active"] == 100
        assert rows["east"]["inactive"] == 50
        assert rows["west"]["active"] == 200

    def test_missing_pivot_value(self, spark: SparkSession):
        """Missing pivot value produces null column."""
        df = spark.createDataFrame(
            [("east", "active", 100)],
            ["region", "status", "amount"],
        )
        params = PivotParams(
            group_by=["region"],
            pivot_column="status",
            values=["active", "inactive"],
            aggregate=SparkExpr("sum(amount)"),
        )
        result = apply_pivot(df, params)
        row = result.collect()[0]
        assert row["active"] == 100
        assert row["inactive"] is None

    def test_numeric_pivot_values(self, spark: SparkSession):
        """Pivot with numeric values in the values list."""
        df = spark.createDataFrame(
            [("a", 1, 10), ("a", 2, 20)],
            ["grp", "code", "val"],
        )
        params = PivotParams(
            group_by=["grp"],
            pivot_column="code",
            values=[1, 2, 3],
            aggregate=SparkExpr("sum(val)"),
        )
        result = apply_pivot(df, params)
        assert result.count() == 1


class TestApplyUnpivot:
    """Test unpivot step handler."""

    def test_basic_unpivot(self, spark: SparkSession):
        """Unpivot columns to rows."""
        df = spark.createDataFrame(
            [("a", 10, 20), ("b", 30, 40)],
            ["id", "q1", "q2"],
        )
        params = UnpivotParams(
            columns=["q1", "q2"],
            name_column="quarter",
            value_column="revenue",
        )
        result = apply_unpivot(df, params)
        assert result.count() == 4
        assert set(result.columns) == {"id", "quarter", "revenue"}
        names = {r["quarter"] for r in result.collect()}
        assert names == {"q1", "q2"}

    def test_preserves_id_columns(self, spark: SparkSession):
        """Non-unpivoted columns are preserved."""
        df = spark.createDataFrame(
            [("a", "x", 10, 20)],
            ["id", "cat", "q1", "q2"],
        )
        params = UnpivotParams(
            columns=["q1", "q2"],
            name_column="qtr",
            value_column="val",
        )
        result = apply_unpivot(df, params)
        assert "id" in result.columns
        assert "cat" in result.columns
        assert result.count() == 2

    def test_null_values(self, spark: SparkSession):
        """Nulls in unpivoted columns are preserved."""
        df = spark.createDataFrame(
            [("a", 10, None)],
            ["id", "q1", "q2"],
        )
        params = UnpivotParams(
            columns=["q1", "q2"],
            name_column="qtr",
            value_column="val",
        )
        result = apply_unpivot(df, params)
        rows = {r["qtr"]: r["val"] for r in result.collect()}
        assert rows["q1"] == 10
        assert rows["q2"] is None
