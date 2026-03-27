"""Tests for concat step handler — null-aware string concatenation."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from weevr.model.pipeline import ConcatParams
from weevr.operations.pipeline.concat import apply_concat

pytestmark = pytest.mark.spark


class TestApplyConcat:
    """Test concat step handler."""

    def test_basic_concat(self, spark: SparkSession):
        """Three string columns joined with comma separator, no nulls."""
        df = spark.createDataFrame([("Alice", "Bob", "Carol")], ["a", "b", "c"])
        params = ConcatParams(target="result", columns=["a", "b", "c"], separator=", ")
        step_result = apply_concat(df, params)
        row = step_result.df.collect()[0]
        assert row["result"] == "Alice, Bob, Carol"

    def test_null_mode_skip(self, spark: SparkSession):
        """Nulls in skip mode are omitted from the result."""
        schema = StructType(
            [
                StructField("a", StringType(), True),
                StructField("b", StringType(), True),
                StructField("c", StringType(), True),
            ]
        )
        df = spark.createDataFrame([("Alice", None, "Carol")], schema)
        params = ConcatParams(
            target="result", columns=["a", "b", "c"], separator=", ", null_mode="skip"
        )
        step_result = apply_concat(df, params)
        row = step_result.df.collect()[0]
        assert row["result"] == "Alice, Carol"

    def test_null_mode_empty(self, spark: SparkSession):
        """Nulls in empty mode become empty strings and are included in join."""
        schema = StructType(
            [
                StructField("a", StringType(), True),
                StructField("b", StringType(), True),
                StructField("c", StringType(), True),
            ]
        )
        df = spark.createDataFrame([("Alice", None, "Carol")], schema)
        params = ConcatParams(
            target="result", columns=["a", "b", "c"], separator=", ", null_mode="empty"
        )
        step_result = apply_concat(df, params)
        row = step_result.df.collect()[0]
        assert row["result"] == "Alice, , Carol"

    def test_null_mode_literal(self, spark: SparkSession):
        """Nulls in literal mode become the null_literal string."""
        schema = StructType(
            [
                StructField("a", StringType(), True),
                StructField("b", StringType(), True),
                StructField("c", StringType(), True),
            ]
        )
        df = spark.createDataFrame([("Alice", None, "Carol")], schema)
        params = ConcatParams(
            target="result",
            columns=["a", "b", "c"],
            separator=", ",
            null_mode="literal",
            null_literal="<NULL>",
        )
        step_result = apply_concat(df, params)
        row = step_result.df.collect()[0]
        assert row["result"] == "Alice, <NULL>, Carol"

    def test_trim_inputs(self, spark: SparkSession):
        """Columns with leading/trailing whitespace are trimmed before join."""
        df = spark.createDataFrame([("  Alice  ", "  Bob  ", "  Carol  ")], ["a", "b", "c"])
        params = ConcatParams(target="result", columns=["a", "b", "c"], separator=", ", trim=True)
        step_result = apply_concat(df, params)
        row = step_result.df.collect()[0]
        assert row["result"] == "Alice, Bob, Carol"

    def test_trim_result(self, spark: SparkSession):
        """Result string has trailing separator stripped when null is skipped."""
        schema = StructType(
            [
                StructField("a", StringType(), True),
                StructField("b", StringType(), True),
            ]
        )
        df = spark.createDataFrame([("Alice", None)], schema)
        params = ConcatParams(
            target="result",
            columns=["a", "b"],
            separator=", ",
            null_mode="skip",
            trim=True,
        )
        step_result = apply_concat(df, params)
        row = step_result.df.collect()[0]
        assert row["result"] == "Alice"

    def test_trim_makes_whitespace_blank(self, spark: SparkSession):
        """Whitespace-only value with trim=True is treated as blank and skipped."""
        schema = StructType(
            [
                StructField("a", StringType(), True),
                StructField("b", StringType(), True),
                StructField("c", StringType(), True),
            ]
        )
        df = spark.createDataFrame([("Alice", "   ", "Carol")], schema)
        params = ConcatParams(
            target="result",
            columns=["a", "b", "c"],
            separator=", ",
            null_mode="skip",
            trim=True,
        )
        step_result = apply_concat(df, params)
        row = step_result.df.collect()[0]
        assert row["result"] == "Alice, Carol"

    def test_collapse_separators(self, spark: SparkSession):
        """Adjacent separators from skipped nulls are collapsed to a single separator."""
        schema = StructType(
            [
                StructField("a", StringType(), True),
                StructField("b", StringType(), True),
                StructField("c", StringType(), True),
            ]
        )
        df = spark.createDataFrame([("Alice", None, "Carol")], schema)
        params = ConcatParams(
            target="result",
            columns=["a", "b", "c"],
            separator="-",
            null_mode="skip",
            collapse_separators=True,
        )
        step_result = apply_concat(df, params)
        row = step_result.df.collect()[0]
        assert row["result"] == "Alice-Carol"

    def test_all_null_returns_null(self, spark: SparkSession):
        """All columns null results in a NULL output, not an empty string."""
        schema = StructType(
            [
                StructField("a", StringType(), True),
                StructField("b", StringType(), True),
            ]
        )
        df = spark.createDataFrame([(None, None)], schema)
        params = ConcatParams(target="result", columns=["a", "b"], separator=", ", null_mode="skip")
        step_result = apply_concat(df, params)
        row = step_result.df.collect()[0]
        assert row["result"] is None

    def test_single_column(self, spark: SparkSession):
        """Single column passthrough with trim applied (DEC-025)."""
        df = spark.createDataFrame([("  hello  ",)], ["a"])
        params = ConcatParams(target="result", columns=["a"], separator="", trim=True)
        step_result = apply_concat(df, params)
        row = step_result.df.collect()[0]
        assert row["result"] == "hello"

    def test_auto_cast_non_string(self, spark: SparkSession):
        """Integer column is auto-cast to string before concat (DEC-005)."""
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame([("Alice", 30)], schema)
        params = ConcatParams(target="result", columns=["name", "age"], separator="-")
        step_result = apply_concat(df, params)
        row = step_result.df.collect()[0]
        assert row["result"] == "Alice-30"

    def test_default_separator_empty(self, spark: SparkSession):
        """No separator results in columns directly concatenated."""
        df = spark.createDataFrame([("foo", "bar", "baz")], ["a", "b", "c"])
        params = ConcatParams(target="result", columns=["a", "b", "c"], separator="")
        step_result = apply_concat(df, params)
        row = step_result.df.collect()[0]
        assert row["result"] == "foobarbaz"
