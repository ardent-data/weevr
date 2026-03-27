"""Tests for the map step handler."""

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from weevr.model.pipeline import MapParams
from weevr.operations.pipeline.value_mapping import apply_map

_STRING_SCHEMA = StructType([StructField("status", StringType(), nullable=True)])


@pytest.mark.spark
class TestApplyMap:
    """Tests for apply_map()."""

    def test_basic_mapping(self, spark):
        df = spark.createDataFrame([("A",), ("B",), ("C",)], ["status"])
        params = MapParams(column="status", values={"A": "Active", "B": "Blocked", "C": "Closed"})
        result = apply_map(df, params)
        rows = result.df.select("status").collect()
        assert [r.status for r in rows] == ["Active", "Blocked", "Closed"]

    def test_target_column(self, spark):
        df = spark.createDataFrame([("A",)], ["code"])
        params = MapParams(column="code", target="label", values={"A": "Active"})
        result = apply_map(df, params)
        row = result.df.collect()[0]
        assert row.code == "A"
        assert row.label == "Active"

    def test_target_default_overwrites(self, spark):
        df = spark.createDataFrame([("A",)], ["status"])
        params = MapParams(column="status", values={"A": "Active"})
        result = apply_map(df, params)
        row = result.df.collect()[0]
        assert row.status == "Active"

    def test_on_null_specified(self, spark):
        df = spark.createDataFrame([(None,)], _STRING_SCHEMA)
        params = MapParams(column="status", values={"A": "Active"}, on_null="Missing")
        result = apply_map(df, params)
        assert result.df.collect()[0].status == "Missing"

    def test_on_null_falls_to_default(self, spark):
        df = spark.createDataFrame([(None,)], _STRING_SCHEMA)
        params = MapParams(column="status", values={"A": "Active"}, default="Fallback")
        result = apply_map(df, params)
        assert result.df.collect()[0].status == "Fallback"

    def test_on_null_neither_set(self, spark):
        df = spark.createDataFrame([(None,)], _STRING_SCHEMA)
        params = MapParams(column="status", values={"A": "Active"})
        result = apply_map(df, params)
        assert result.df.collect()[0].status is None

    def test_default_for_unmapped(self, spark):
        df = spark.createDataFrame([("X",)], ["status"])
        params = MapParams(column="status", values={"A": "Active"}, default="Other")
        result = apply_map(df, params)
        assert result.df.collect()[0].status == "Other"

    def test_unmapped_keep(self, spark):
        df = spark.createDataFrame([("X",)], ["status"])
        params = MapParams(column="status", values={"A": "Active"}, unmapped="keep")
        result = apply_map(df, params)
        assert result.df.collect()[0].status == "X"

    def test_unmapped_null(self, spark):
        df = spark.createDataFrame([("X",)], ["status"])
        params = MapParams(column="status", values={"A": "Active"}, unmapped="null")
        result = apply_map(df, params)
        assert result.df.collect()[0].status is None

    def test_unmapped_validate(self, spark):
        df = spark.createDataFrame([("A",), ("X",)], ["status"])
        params = MapParams(column="status", values={"A": "Active"}, unmapped="validate")
        result = apply_map(df, params)
        rows = result.df.collect()
        # Mapped value should work
        assert rows[0].status == "Active"
        # Unmapped value retained
        assert rows[1].status == "X"
        # Flag column added for unmapped rows (named per target column)
        assert rows[0]["__map_unmapped_status"] is False
        assert rows[1]["__map_unmapped_status"] is True

    def test_case_sensitive_default(self, spark):
        df = spark.createDataFrame([("A",), ("a",)], ["code"])
        params = MapParams(column="code", values={"A": "Alpha"}, unmapped="keep")
        result = apply_map(df, params)
        rows = result.df.select("code").collect()
        assert rows[0].code == "Alpha"
        assert rows[1].code == "a"  # Not mapped

    def test_case_insensitive(self, spark):
        df = spark.createDataFrame([("A",), ("a",)], ["code"])
        params = MapParams(
            column="code", values={"A": "Alpha"}, case_sensitive=False, unmapped="keep"
        )
        result = apply_map(df, params)
        rows = result.df.select("code").collect()
        assert rows[0].code == "Alpha"
        assert rows[1].code == "Alpha"

    def test_metadata_unmapped_count(self, spark):
        df = spark.createDataFrame([("A",), ("X",), ("Y",)], ["status"])
        params = MapParams(column="status", values={"A": "Active"}, unmapped="validate")
        result = apply_map(df, params)
        assert result.metadata.get("unmapped_count") == 2
