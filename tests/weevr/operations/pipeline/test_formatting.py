"""Tests for the format step handler."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from weevr.model.pipeline import FormatParams, FormatSpec
from weevr.operations.pipeline.formatting import apply_format

pytestmark = pytest.mark.spark


class TestApplyFormat:
    """Tests for apply_format()."""

    def test_pattern_basic(self, spark: SparkSession):
        """Phone number: 1234567890 → (123)456-7890."""
        df = spark.createDataFrame([("1234567890",)], ["phone"])
        params = FormatParams(columns={"phone": FormatSpec(pattern="({1:3}){4:3}-{7:4}")})
        result = apply_format(df, params)
        assert result.df.collect()[0].phone == "(123)456-7890"

    def test_pattern_on_short_null(self, spark: SparkSession):
        """Short string with on_short=null → NULL."""
        df = spark.createDataFrame([("12",)], ["phone"])
        params = FormatParams(
            columns={"phone": FormatSpec(pattern="({1:3}){4:3}-{7:4}", on_short="null")}
        )
        result = apply_format(df, params)
        assert result.df.collect()[0].phone is None

    def test_pattern_on_short_partial(self, spark: SparkSession):
        """Short string with on_short=partial → partial result (no NULL guard)."""
        df = spark.createDataFrame([("12345",)], ["phone"])
        params = FormatParams(
            columns={
                "formatted": FormatSpec(source="phone", pattern="({1:3}){4:3}", on_short="partial")
            }
        )
        result = apply_format(df, params)
        row = result.df.collect()[0]
        # Should produce partial output, not NULL
        assert row.formatted is not None

    def test_number_format(self, spark: SparkSession):
        """Number formatting with DecimalFormat pattern."""
        df = spark.createDataFrame(
            [(1234567.89,)], schema=StructType([StructField("amount", DoubleType())])
        )
        params = FormatParams(
            columns={"amount_display": FormatSpec(source="amount", number="#,##0.00")}
        )
        result = apply_format(df, params)
        assert result.df.collect()[0].amount_display == "1,234,567.89"

    def test_date_format(self, spark: SparkSession):
        """Date formatting with SimpleDateFormat pattern."""
        from datetime import date

        df = spark.createDataFrame(
            [(date(2026, 3, 27),)], schema=StructType([StructField("event_date", DateType())])
        )
        params = FormatParams(columns={"event_date": FormatSpec(date="yyyy-MM-dd")})
        result = apply_format(df, params)
        assert result.df.collect()[0].event_date == "2026-03-27"

    def test_null_source_returns_null(self, spark: SparkSession):
        """NULL source value → NULL output."""
        df = spark.createDataFrame(
            [(None,)], schema=StructType([StructField("phone", StringType())])
        )
        params = FormatParams(columns={"phone": FormatSpec(pattern="({1:3}){4:3}-{7:4}")})
        result = apply_format(df, params)
        assert result.df.collect()[0].phone is None

    def test_source_optional_in_place(self, spark: SparkSession):
        """No source → format in-place (source = target name)."""
        df = spark.createDataFrame([("1234567890",)], ["phone"])
        params = FormatParams(columns={"phone": FormatSpec(pattern="({1:3}){4:3}-{7:4}")})
        result = apply_format(df, params)
        assert result.df.collect()[0].phone == "(123)456-7890"

    def test_source_explicit(self, spark: SparkSession):
        """Explicit source column different from target."""
        df = spark.createDataFrame([("1234567890",)], ["raw_phone"])
        params = FormatParams(
            columns={
                "formatted_phone": FormatSpec(source="raw_phone", pattern="({1:3}){4:3}-{7:4}")
            }
        )
        result = apply_format(df, params)
        row = result.df.collect()[0]
        assert row.raw_phone == "1234567890"
        assert row.formatted_phone == "(123)456-7890"

    def test_multi_column_single_step(self, spark: SparkSession):
        """Multiple format specs in one step."""
        from datetime import date

        df = spark.createDataFrame(
            [("1234567890", 1234.5, date(2026, 1, 15))],
            schema=StructType(
                [
                    StructField("phone", StringType()),
                    StructField("amount", DoubleType()),
                    StructField("event_date", DateType()),
                ]
            ),
        )
        params = FormatParams(
            columns={
                "phone": FormatSpec(pattern="({1:3}){4:3}-{7:4}"),
                "amount_display": FormatSpec(source="amount", number="#,##0.00"),
                "event_date": FormatSpec(date="yyyy-MM-dd"),
            }
        )
        result = apply_format(df, params)
        row = result.df.collect()[0]
        assert row.phone == "(123)456-7890"
        assert row.amount_display == "1,234.50"
        assert row.event_date == "2026-01-15"

    def test_strict_types_false_auto_cast(self, spark: SparkSession):
        """Non-numeric source with number format + strict_types=False → auto-cast attempt."""
        df = spark.createDataFrame([("1234.5",)], ["val"])
        params = FormatParams(
            columns={"formatted": FormatSpec(source="val", number="#,##0.00", strict_types=False)}
        )
        result = apply_format(df, params)
        assert result.df.collect()[0].formatted == "1,234.50"
