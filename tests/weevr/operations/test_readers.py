"""Tests for source readers."""

import json

import pytest
from pyspark.sql import SparkSession

from spark_helpers import create_delta_table
from weevr.errors.exceptions import ExecutionError
from weevr.model.source import DedupConfig, Source
from weevr.operations.readers import read_source, read_sources

pytestmark = pytest.mark.spark


class TestDeltaSource:
    """Tests for Delta source reading."""

    def test_reads_delta_table(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("delta_basic")
        data = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        create_delta_table(spark, path, data)

        source = Source(type="delta", alias=path)
        df = read_source(spark, "customers", source)

        assert df.count() == 2
        assert set(df.columns) == {"id", "name"}

    def test_delta_source_preserves_all_rows(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("delta_rows")
        data = [{"x": i} for i in range(10)]
        create_delta_table(spark, path, data)

        source = Source(type="delta", alias=path)
        df = read_source(spark, "nums", source)

        assert df.count() == 10

    def test_missing_delta_path_raises_execution_error(self, spark: SparkSession) -> None:
        source = Source(type="delta", alias="/nonexistent/path/delta")
        with pytest.raises(ExecutionError) as exc_info:
            read_source(spark, "missing", source)

        err = exc_info.value
        assert err.source_name == "missing"


class TestFileSource:
    """Tests for CSV, JSON, and Parquet source reading."""

    def test_reads_csv_source(self, spark: SparkSession, tmp_path) -> None:
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("id,value\n1,foo\n2,bar\n")

        source = Source(
            type="csv",
            path=str(tmp_path),
            options={"header": "true", "inferSchema": "true"},
        )
        df = read_source(spark, "csv_src", source)

        assert df.count() == 2
        assert "id" in df.columns
        assert "value" in df.columns

    def test_reads_json_source(self, spark: SparkSession, tmp_path) -> None:
        json_path = tmp_path / "data.json"
        rows = [{"id": 1, "val": "x"}, {"id": 2, "val": "y"}]
        json_path.write_text("\n".join(json.dumps(r) for r in rows))

        source = Source(type="json", path=str(tmp_path))
        df = read_source(spark, "json_src", source)

        assert df.count() == 2
        assert set(df.columns) == {"id", "val"}

    def test_reads_parquet_source(self, spark: SparkSession, tmp_path) -> None:
        parquet_path = str(tmp_path / "parquet_data")
        data = [{"a": 10, "b": "p"}, {"a": 20, "b": "q"}]
        spark.createDataFrame(data).write.format("parquet").save(parquet_path)

        source = Source(type="parquet", path=parquet_path)
        df = read_source(spark, "parq_src", source)

        assert df.count() == 2
        assert set(df.columns) == {"a", "b"}

    def test_csv_options_passed_through(self, spark: SparkSession, tmp_path) -> None:
        csv_path = tmp_path / "pipe.csv"
        csv_path.write_text("id|name\n1|alice\n2|bob\n")

        source = Source(
            type="csv",
            path=str(tmp_path),
            options={"header": "true", "sep": "|"},
        )
        df = read_source(spark, "pipe_csv", source)

        assert df.count() == 2
        assert "name" in df.columns

    def test_unsupported_type_raises_execution_error(self, spark: SparkSession) -> None:
        source = Source.__new__(Source)
        object.__setattr__(source, "type", "avro")
        object.__setattr__(source, "alias", None)
        object.__setattr__(source, "path", "/some/path")
        object.__setattr__(source, "options", {})
        object.__setattr__(source, "dedup", None)

        with pytest.raises(ExecutionError, match="Unsupported source type: 'avro'"):
            read_source(spark, "bad_type", source)


class TestSourceDedup:
    """Tests for source-level deduplication."""

    def test_dedup_without_order_by(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dedup_no_order")
        data = [
            {"id": 1, "val": "a"},
            {"id": 1, "val": "a"},
            {"id": 2, "val": "b"},
        ]
        create_delta_table(spark, path, data)

        source = Source(
            type="delta",
            alias=path,
            dedup=DedupConfig(keys=["id"]),
        )
        df = read_source(spark, "dedup_src", source)

        assert df.count() == 2

    def test_dedup_with_order_by_keep_latest(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dedup_with_order")
        data = [
            {"id": 1, "ts": 100, "val": "old"},
            {"id": 1, "ts": 200, "val": "new"},
            {"id": 2, "ts": 50, "val": "only"},
        ]
        create_delta_table(spark, path, data)

        source = Source(
            type="delta",
            alias=path,
            dedup=DedupConfig(keys=["id"], order_by="ts DESC"),
        )
        df = read_source(spark, "ordered_dedup", source)

        assert df.count() == 2
        row_id1 = df.filter("id = 1").collect()[0]
        assert row_id1["val"] == "new"

    def test_dedup_with_order_by_keep_earliest(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dedup_asc_order")
        data = [
            {"id": 1, "ts": 100, "val": "first"},
            {"id": 1, "ts": 200, "val": "second"},
        ]
        create_delta_table(spark, path, data)

        source = Source(
            type="delta",
            alias=path,
            dedup=DedupConfig(keys=["id"], order_by="ts ASC"),
        )
        df = read_source(spark, "asc_dedup", source)

        assert df.count() == 1
        assert df.collect()[0]["val"] == "first"

    def test_dedup_composite_keys(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dedup_composite")
        data = [
            {"a": 1, "b": 1, "val": "x"},
            {"a": 1, "b": 1, "val": "y"},
            {"a": 1, "b": 2, "val": "z"},
        ]
        create_delta_table(spark, path, data)

        source = Source(
            type="delta",
            alias=path,
            dedup=DedupConfig(keys=["a", "b"]),
        )
        df = read_source(spark, "composite_dedup", source)

        assert df.count() == 2

    def test_no_dedup_when_not_configured(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("no_dedup")
        data = [{"id": 1}, {"id": 1}, {"id": 2}]
        create_delta_table(spark, path, data)

        source = Source(type="delta", alias=path)
        df = read_source(spark, "no_dedup_src", source)

        assert df.count() == 3


class TestReadSources:
    """Tests for the read_sources multi-source function."""

    def test_reads_multiple_sources(self, spark: SparkSession, tmp_delta_path) -> None:
        path_a = tmp_delta_path("src_a")
        path_b = tmp_delta_path("src_b")
        create_delta_table(spark, path_a, [{"id": 1}])
        create_delta_table(spark, path_b, [{"id": 2}, {"id": 3}])

        sources = {
            "a": Source(type="delta", alias=path_a),
            "b": Source(type="delta", alias=path_b),
        }
        result = read_sources(spark, sources)

        assert set(result.keys()) == {"a", "b"}
        assert result["a"].count() == 1
        assert result["b"].count() == 2

    def test_returns_empty_dict_for_no_sources(self, spark: SparkSession) -> None:
        result = read_sources(spark, {})
        assert result == {}

    def test_single_source_dict(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("single")
        create_delta_table(spark, path, [{"k": "v"}])

        result = read_sources(spark, {"main": Source(type="delta", alias=path)})

        assert "main" in result
        assert result["main"].count() == 1

    def test_error_in_one_source_propagates(self, spark: SparkSession) -> None:
        sources = {"bad": Source(type="delta", alias="/nonexistent/path")}
        with pytest.raises(ExecutionError) as exc_info:
            read_sources(spark, sources)

        assert exc_info.value.source_name == "bad"
