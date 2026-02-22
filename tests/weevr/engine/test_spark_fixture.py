"""Smoke tests for Spark test infrastructure."""

import pytest
from pyspark.sql import SparkSession

from spark_helpers import assert_dataframes_equal, create_delta_table

pytestmark = pytest.mark.spark


class TestSparkFixture:
    """Verify the shared SparkSession fixture works correctly."""

    def test_spark_session_available(self, spark: SparkSession) -> None:
        """SparkSession is active and accessible."""
        assert spark is not None
        assert spark.sparkContext.master == "local[2]"

    def test_simple_dataframe_creation(self, spark: SparkSession) -> None:
        """Basic DataFrame can be created from Python data."""
        df = spark.createDataFrame([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        assert df.count() == 2

    def test_delta_table_create_and_read(self, spark: SparkSession, tmp_delta_path) -> None:
        """Delta table can be written and read back during tests."""
        path = tmp_delta_path("smoke_test")
        data = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
        create_delta_table(spark, path, data)

        df = spark.read.format("delta").load(path)
        assert df.count() == 2
        assert set(df.columns) == {"id", "value"}

    def test_assert_dataframes_equal(self, spark: SparkSession) -> None:
        """assert_dataframes_equal passes for identical DataFrames."""
        data = [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]
        df1 = spark.createDataFrame(data)
        df2 = spark.createDataFrame(data)
        assert_dataframes_equal(df1, df2)

    def test_tmp_delta_path_isolation(self, tmp_delta_path) -> None:
        """tmp_delta_path returns unique paths for different names."""
        path_a = tmp_delta_path("table_a")
        path_b = tmp_delta_path("table_b")
        assert path_a != path_b
