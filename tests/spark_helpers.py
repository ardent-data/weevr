"""Shared helper functions for Spark-based tests.

These are plain functions (not fixtures) so they can be imported directly by
test modules that need programmatic DataFrame setup or comparison.
"""

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def create_delta_table(
    spark: SparkSession,
    path: str,
    data: list[dict[str, Any]],
    schema: StructType | None = None,
) -> None:
    """Create a Delta table from Python data for test setup.

    Args:
        spark: Active SparkSession.
        path: Filesystem path where the Delta table will be written.
        data: List of row dicts (column name -> value).
        schema: Optional explicit schema; inferred from data when omitted.
    """
    if schema is not None:
        df = spark.createDataFrame(data, schema=schema)
    else:
        df = spark.createDataFrame(data)
    df.write.format("delta").mode("overwrite").save(path)


def assert_dataframes_equal(actual: DataFrame, expected: DataFrame) -> None:
    """Assert two DataFrames contain the same rows, regardless of order.

    Sorts both DataFrames by all columns before comparing so that row order
    does not affect the result.

    Args:
        actual: DataFrame produced by the system under test.
        expected: DataFrame with expected content.
    """
    actual_sorted = actual.orderBy(actual.columns)  # type: ignore[arg-type]
    expected_sorted = expected.orderBy(expected.columns)  # type: ignore[arg-type]
    assert actual_sorted.collect() == expected_sorted.collect(), (
        f"DataFrames differ.\nActual:\n{actual.collect()}\nExpected:\n{expected.collect()}"
    )
