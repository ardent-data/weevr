"""Tests for reshaping step handlers — dedup and sort."""

from pyspark.sql import SparkSession

from weevr.model.pipeline import DedupParams, SortParams
from weevr.operations.pipeline.reshaping import apply_dedup, apply_sort


class TestApplyDedup:
    """Tests for the dedup step handler."""

    def test_dedup_without_order_removes_exact_duplicates(
        self, spark: SparkSession
    ) -> None:
        df = spark.createDataFrame([
            {"id": 1, "val": "a"},
            {"id": 1, "val": "a"},
            {"id": 2, "val": "b"},
        ])
        params = DedupParams(keys=["id", "val"])
        result = apply_dedup(df, params)
        assert result.count() == 2

    def test_dedup_without_order_on_key_subset(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([
            {"id": 1, "val": "a"},
            {"id": 1, "val": "b"},
            {"id": 2, "val": "c"},
        ])
        params = DedupParams(keys=["id"])
        result = apply_dedup(df, params)
        assert result.count() == 2

    def test_dedup_keep_first_with_order_by(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([
            {"id": 1, "ts": 100, "val": "first"},
            {"id": 1, "ts": 200, "val": "second"},
        ])
        params = DedupParams(keys=["id"], order_by="ts", keep="first")
        result = apply_dedup(df, params)
        assert result.count() == 1
        assert result.collect()[0]["val"] == "first"

    def test_dedup_keep_last_with_order_by(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([
            {"id": 1, "ts": 100, "val": "first"},
            {"id": 1, "ts": 200, "val": "second"},
        ])
        params = DedupParams(keys=["id"], order_by="ts", keep="last")
        result = apply_dedup(df, params)
        assert result.count() == 1
        assert result.collect()[0]["val"] == "second"

    def test_dedup_default_keep_is_last(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([
            {"id": 1, "ts": 100, "val": "first"},
            {"id": 1, "ts": 200, "val": "second"},
        ])
        params = DedupParams(keys=["id"], order_by="ts")
        result = apply_dedup(df, params)
        assert result.count() == 1
        assert result.collect()[0]["val"] == "second"

    def test_dedup_composite_keys(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([
            {"a": 1, "b": 1, "ts": 1, "val": "keep"},
            {"a": 1, "b": 1, "ts": 2, "val": "drop"},
            {"a": 1, "b": 2, "ts": 1, "val": "keep2"},
        ])
        params = DedupParams(keys=["a", "b"], order_by="ts", keep="first")
        result = apply_dedup(df, params)
        assert result.count() == 2

    def test_dedup_no_duplicates_returns_all_rows(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
        params = DedupParams(keys=["id"])
        result = apply_dedup(df, params)
        assert result.count() == 3


class TestApplySort:
    """Tests for the sort step handler."""

    def test_sort_single_column_ascending(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"n": 3}, {"n": 1}, {"n": 2}])
        params = SortParams(columns=["n"], ascending=True)
        result = apply_sort(df, params)
        values = [r["n"] for r in result.collect()]
        assert values == [1, 2, 3]

    def test_sort_single_column_descending(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"n": 3}, {"n": 1}, {"n": 2}])
        params = SortParams(columns=["n"], ascending=False)
        result = apply_sort(df, params)
        values = [r["n"] for r in result.collect()]
        assert values == [3, 2, 1]

    def test_sort_default_is_ascending(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"n": 3}, {"n": 1}])
        params = SortParams(columns=["n"])
        result = apply_sort(df, params)
        values = [r["n"] for r in result.collect()]
        assert values == [1, 3]

    def test_sort_multiple_columns(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([
            {"a": 1, "b": 2},
            {"a": 1, "b": 1},
            {"a": 2, "b": 3},
        ])
        params = SortParams(columns=["a", "b"], ascending=True)
        result = apply_sort(df, params)
        rows = result.collect()
        assert rows[0]["a"] == 1 and rows[0]["b"] == 1
        assert rows[1]["a"] == 1 and rows[1]["b"] == 2
        assert rows[2]["a"] == 2

    def test_sort_preserves_all_rows(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"n": i} for i in range(10)])
        params = SortParams(columns=["n"], ascending=True)
        result = apply_sort(df, params)
        assert result.count() == 10

    def test_sort_string_column(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"name": "charlie"}, {"name": "alice"}, {"name": "bob"}])
        params = SortParams(columns=["name"], ascending=True)
        result = apply_sort(df, params)
        names = [r["name"] for r in result.collect()]
        assert names == ["alice", "bob", "charlie"]
