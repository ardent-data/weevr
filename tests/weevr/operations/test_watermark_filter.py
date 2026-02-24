"""Tests for watermark filter expression construction."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from weevr.operations.readers import build_watermark_filter

pytestmark = pytest.mark.spark


class TestBuildWatermarkFilter:
    """Test build_watermark_filter produces correct Column filters."""

    def test_timestamp_exclusive(self, spark: SparkSession):
        """Timestamp type with > excludes the boundary row."""
        df = spark.createDataFrame(
            [("2024-01-14T00:00:00",), ("2024-01-15T10:30:00",), ("2024-01-16T00:00:00",)],
            ["ts"],
        ).withColumn("ts", F.col("ts").cast("timestamp"))

        filt = build_watermark_filter("ts", "timestamp", "2024-01-15T10:30:00")
        assert df.filter(filt).count() == 1  # only the 16th

    def test_timestamp_inclusive(self, spark: SparkSession):
        """Timestamp type with >= includes the boundary row."""
        df = spark.createDataFrame(
            [("2024-01-14T00:00:00",), ("2024-01-15T10:30:00",), ("2024-01-16T00:00:00",)],
            ["ts"],
        ).withColumn("ts", F.col("ts").cast("timestamp"))

        filt = build_watermark_filter("ts", "timestamp", "2024-01-15T10:30:00", inclusive=True)
        assert df.filter(filt).count() == 2  # the 15th and 16th

    def test_date_exclusive(self, spark: SparkSession):
        """Date type with > excludes the boundary row."""
        df = spark.createDataFrame(
            [("2024-01-14",), ("2024-01-15",), ("2024-01-16",)],
            ["event_date"],
        ).withColumn("event_date", F.col("event_date").cast("date"))

        filt = build_watermark_filter("event_date", "date", "2024-01-15")
        assert df.filter(filt).count() == 1

    def test_date_inclusive(self, spark: SparkSession):
        """Date type with >= includes the boundary row."""
        df = spark.createDataFrame(
            [("2024-01-14",), ("2024-01-15",), ("2024-01-16",)],
            ["event_date"],
        ).withColumn("event_date", F.col("event_date").cast("date"))

        filt = build_watermark_filter("event_date", "date", "2024-01-15", inclusive=True)
        assert df.filter(filt).count() == 2

    def test_int_exclusive(self, spark: SparkSession):
        """Int type with > excludes the boundary value."""
        df = spark.createDataFrame([(10,), (42,), (100,)], ["row_id"])

        filt = build_watermark_filter("row_id", "int", "42")
        assert df.filter(filt).count() == 1  # only 100

    def test_int_inclusive(self, spark: SparkSession):
        """Int type with >= includes the boundary value."""
        df = spark.createDataFrame([(10,), (42,), (100,)], ["row_id"])

        filt = build_watermark_filter("row_id", "int", "42", inclusive=True)
        assert df.filter(filt).count() == 2  # 42 and 100

    def test_long_exclusive(self, spark: SparkSession):
        """Long type with > excludes the boundary value."""
        df = spark.createDataFrame([(500000,), (1000000,), (2000000,)], ["seq"])

        filt = build_watermark_filter("seq", "long", "1000000")
        assert df.filter(filt).count() == 1

    def test_long_inclusive(self, spark: SparkSession):
        """Long type with >= includes the boundary value."""
        df = spark.createDataFrame([(500000,), (1000000,), (2000000,)], ["seq"])

        filt = build_watermark_filter("seq", "long", "1000000", inclusive=True)
        assert df.filter(filt).count() == 2

    def test_default_not_inclusive(self, spark: SparkSession):
        """Default inclusive is False (strict >)."""
        df = spark.createDataFrame([(1,), (5,), (10,)], ["val"])

        filt = build_watermark_filter("val", "int", "5")
        result = df.filter(filt).collect()
        assert len(result) == 1
        assert result[0]["val"] == 10
