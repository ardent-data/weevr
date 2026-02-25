"""Tests for column-ops pipeline step handlers — string_ops, date_ops, type selectors."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from weevr.errors.exceptions import ConfigError
from weevr.model.pipeline import DateOpsParams, StringOpsParams
from weevr.operations.pipeline.column_ops import (
    apply_date_ops,
    apply_string_ops,
    resolve_type_selectors,
)

pytestmark = pytest.mark.spark


class TestResolveTypeSelectors:
    """Test the *:type column selector resolution logic."""

    def test_star_string(self, spark: SparkSession):
        """*:string matches all StringType columns."""
        df = spark.createDataFrame(
            [("a", 1, "b")],
            T.StructType(
                [
                    T.StructField("name", T.StringType()),
                    T.StructField("val", T.IntegerType()),
                    T.StructField("desc", T.StringType()),
                ]
            ),
        )
        assert resolve_type_selectors(df, ["*:string"]) == ["name", "desc"]

    def test_star_numeric(self, spark: SparkSession):
        """*:numeric matches int, long, float, double, decimal."""
        df = spark.createDataFrame(
            [(1, 2.0, "x")],
            T.StructType(
                [
                    T.StructField("a", T.IntegerType()),
                    T.StructField("b", T.DoubleType()),
                    T.StructField("c", T.StringType()),
                ]
            ),
        )
        assert resolve_type_selectors(df, ["*:numeric"]) == ["a", "b"]

    def test_star_date(self, spark: SparkSession):
        """*:date matches DateType."""
        from datetime import date

        df = spark.createDataFrame(
            [(date(2024, 1, 1), "x")],
            T.StructType(
                [
                    T.StructField("dt", T.DateType()),
                    T.StructField("name", T.StringType()),
                ]
            ),
        )
        assert resolve_type_selectors(df, ["*:date"]) == ["dt"]

    def test_star_boolean(self, spark: SparkSession):
        """*:boolean matches BooleanType."""
        df = spark.createDataFrame(
            [(True, 1)],
            T.StructType(
                [
                    T.StructField("flag", T.BooleanType()),
                    T.StructField("val", T.IntegerType()),
                ]
            ),
        )
        assert resolve_type_selectors(df, ["*:boolean"]) == ["flag"]

    def test_name_pattern_with_type(self, spark: SparkSession):
        """name_*:string matches columns matching both name glob and type."""
        df = spark.createDataFrame(
            [("a", "b", 1)],
            T.StructType(
                [
                    T.StructField("name_first", T.StringType()),
                    T.StructField("name_last", T.StringType()),
                    T.StructField("name_id", T.IntegerType()),
                ]
            ),
        )
        result = resolve_type_selectors(df, ["name_*:string"])
        assert result == ["name_first", "name_last"]

    def test_explicit_column_name(self, spark: SparkSession):
        """Explicit column name without ':' passes through as-is."""
        df = spark.createDataFrame([(1,)], ["amount"])
        assert resolve_type_selectors(df, ["amount"]) == ["amount"]

    def test_mixed_selectors(self, spark: SparkSession):
        """Mix of explicit names and glob selectors resolves both."""
        from datetime import date

        df = spark.createDataFrame(
            [(1, date(2024, 1, 1))],
            T.StructType(
                [
                    T.StructField("amount", T.IntegerType()),
                    T.StructField("dt", T.DateType()),
                ]
            ),
        )
        assert resolve_type_selectors(df, ["amount", "*:date"]) == ["amount", "dt"]

    def test_no_matches(self, spark: SparkSession):
        """Selector with no matches returns empty list."""
        df = spark.createDataFrame([(1,)], ["val"])
        assert resolve_type_selectors(df, ["*:date"]) == []

    def test_deduplication(self, spark: SparkSession):
        """Same column matched by multiple selectors appears once."""
        df = spark.createDataFrame(
            [("a",)],
            T.StructType([T.StructField("name", T.StringType())]),
        )
        result = resolve_type_selectors(df, ["name", "*:string"])
        assert result == ["name"]

    def test_unknown_type_selector_raises(self, spark: SparkSession):
        """Unknown type selector raises ConfigError."""
        df = spark.createDataFrame([(1,)], ["val"])
        with pytest.raises(ConfigError, match="Unknown type selector"):
            resolve_type_selectors(df, ["*:array"])


class TestApplyStringOps:
    """Test string_ops step handler."""

    def test_trim_all_strings(self, spark: SparkSession):
        """Apply trim to all string columns via *:string."""
        df = spark.createDataFrame(
            [("  hello  ", " world ")],
            T.StructType(
                [
                    T.StructField("a", T.StringType()),
                    T.StructField("b", T.StringType()),
                ]
            ),
        )
        params = StringOpsParams(columns=["*:string"], expr="trim({col})")
        result = apply_string_ops(df, params)
        row = result.collect()[0]
        assert row["a"] == "hello"
        assert row["b"] == "world"

    def test_explicit_column(self, spark: SparkSession):
        """Explicit column name transforms only that column."""
        df = spark.createDataFrame(
            [("hello", "world")],
            T.StructType(
                [
                    T.StructField("a", T.StringType()),
                    T.StructField("b", T.StringType()),
                ]
            ),
        )
        params = StringOpsParams(columns=["a"], expr="upper({col})")
        result = apply_string_ops(df, params)
        row = result.collect()[0]
        assert row["a"] == "HELLO"
        assert row["b"] == "world"

    def test_empty_match_warn(self, spark: SparkSession):
        """No matching columns with on_empty=warn → df unchanged."""
        df = spark.createDataFrame([(1,)], ["val"])
        params = StringOpsParams(columns=["*:string"], expr="trim({col})", on_empty="warn")
        result = apply_string_ops(df, params)
        assert result.collect() == df.collect()

    def test_empty_match_error(self, spark: SparkSession):
        """No matching columns with on_empty=error → ConfigError."""
        df = spark.createDataFrame([(1,)], ["val"])
        params = StringOpsParams(columns=["*:string"], expr="trim({col})", on_empty="error")
        with pytest.raises(ConfigError, match="no columns matched"):
            apply_string_ops(df, params)


class TestApplyDateOps:
    """Test date_ops step handler."""

    def test_format_dates(self, spark: SparkSession):
        """Apply date_format to date columns."""
        from datetime import date

        df = spark.createDataFrame(
            [(date(2024, 3, 15),)],
            T.StructType([T.StructField("dt", T.DateType())]),
        )
        params = DateOpsParams(columns=["dt"], expr="date_format({col}, 'yyyy-MM-dd')")
        result = apply_date_ops(df, params)
        assert result.collect()[0]["dt"] == "2024-03-15"

    def test_timestamp_selector(self, spark: SparkSession):
        """*:timestamp selects TimestampType columns."""
        from datetime import datetime

        df = spark.createDataFrame(
            [(datetime(2024, 1, 1, 12, 0),)],
            T.StructType([T.StructField("ts", T.TimestampType())]),
        )
        params = DateOpsParams(columns=["*:timestamp"], expr="date_format({col}, 'yyyy-MM-dd')")
        result = apply_date_ops(df, params)
        assert result.collect()[0]["ts"] == "2024-01-01"
