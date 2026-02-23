"""Tests for quarantine table writer."""

import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from weevr.operations.quarantine import write_quarantine

pytestmark = pytest.mark.spark


@pytest.fixture()
def quarantine_df(spark: SparkSession):
    """Sample quarantine DataFrame with metadata columns."""
    return spark.createDataFrame([
        {
            "id": 1,
            "amount": -5,
            "__rule_name": "positive_amount",
            "__rule_expression": "amount > 0",
            "__severity": "error",
            "__quarantine_ts": "2026-01-01T00:00:00+00:00",
        },
        {
            "id": 2,
            "amount": -10,
            "__rule_name": "positive_amount",
            "__rule_expression": "amount > 0",
            "__severity": "error",
            "__quarantine_ts": "2026-01-01T00:00:00+00:00",
        },
    ])


class TestWriteQuarantine:
    def test_writes_to_quarantine_path(self, spark, quarantine_df, tmp_delta_path):
        target_path = tmp_delta_path("target")
        rows = write_quarantine(spark, quarantine_df, target_path)
        assert rows == 2

        # Verify the quarantine table was written
        q_path = f"{target_path}_quarantine"
        result = spark.read.format("delta").load(q_path)
        assert result.count() == 2
        assert "__rule_name" in result.columns
        assert "__rule_expression" in result.columns
        assert "__severity" in result.columns
        assert "__quarantine_ts" in result.columns

    def test_overwrite_replaces_previous(self, spark, quarantine_df, tmp_delta_path):
        target_path = tmp_delta_path("target_overwrite")
        write_quarantine(spark, quarantine_df, target_path)

        # Write again with different data
        new_df = quarantine_df.filter(F.col("id") == 1)
        rows = write_quarantine(spark, new_df, target_path)
        assert rows == 1

        q_path = f"{target_path}_quarantine"
        result = spark.read.format("delta").load(q_path)
        assert result.count() == 1

    def test_none_quarantine_df(self, spark, tmp_delta_path):
        target_path = tmp_delta_path("target_none")
        rows = write_quarantine(spark, None, target_path)
        assert rows == 0
        # No table should have been created
        assert not os.path.exists(f"{target_path}_quarantine/_delta_log")

    def test_empty_quarantine_df(self, spark, quarantine_df, tmp_delta_path):
        target_path = tmp_delta_path("target_empty")
        empty_df = quarantine_df.filter(F.lit(False))
        rows = write_quarantine(spark, empty_df, target_path)
        assert rows == 0
        # No table should have been created
        assert not os.path.exists(f"{target_path}_quarantine/_delta_log")
