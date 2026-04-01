"""Tests for source readers (Spark integration tests)."""

import json
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from spark_helpers import create_delta_table
from weevr.errors.exceptions import ExecutionError
from weevr.model.connection import OneLakeConnection
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
        object.__setattr__(source, "lookup", None)
        object.__setattr__(source, "connection", None)
        object.__setattr__(source, "schema_override", None)
        object.__setattr__(source, "table", None)

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


class TestConnectionBasedRead:
    """Tests for connection-based source reading via _read_raw."""

    def _make_connection(self) -> OneLakeConnection:
        return OneLakeConnection(
            type="onelake",
            workspace="ws-guid-1234",
            lakehouse="lh-guid-5678",
        )

    @patch("weevr.operations.readers._read_raw")
    def test_connection_source_reads_via_delta_load(self, mock_read_raw) -> None:
        """Source with connection + table delegates to _read_raw which calls delta load."""
        mock_df = MagicMock()
        mock_read_raw.return_value = mock_df

        conn = self._make_connection()
        source = Source(connection="primary", table="customers")
        spark = MagicMock()

        connections = {"primary": conn}
        result = read_source(spark, "customers", source, connections=connections)

        assert result is mock_df
        mock_read_raw.assert_called_once_with(spark, source, connections)

    def test_connection_source_builds_abfss_path(self) -> None:
        """Source with connection + table builds correct abfss:// path."""
        conn = OneLakeConnection(
            type="onelake",
            workspace="ws-guid-1234",
            lakehouse="lh-guid-5678",
        )
        source = Source(connection="primary", table="customers")
        spark = MagicMock()
        mock_df = MagicMock()
        spark.read.format.return_value.load.return_value = mock_df

        connections = {"primary": conn}
        result = read_source(spark, "customers", source, connections=connections)

        spark.read.format.assert_called_with("delta")
        load_call_args = spark.read.format.return_value.load.call_args[0]
        assert load_call_args[0].startswith("abfss://ws-guid-1234@")
        assert "lh-guid-5678" in load_call_args[0]
        assert "customers" in load_call_args[0]
        assert result is mock_df

    def test_connection_source_with_schema_override(self) -> None:
        """Source with connection + schema + table uses schema in path."""
        conn = OneLakeConnection(
            type="onelake",
            workspace="ws-guid-1234",
            lakehouse="lh-guid-5678",
        )
        source = Source(connection="primary", table="orders", schema="gold")
        spark = MagicMock()
        mock_df = MagicMock()
        spark.read.format.return_value.load.return_value = mock_df

        connections = {"primary": conn}
        read_source(spark, "orders", source, connections=connections)

        load_call_args = spark.read.format.return_value.load.call_args[0]
        assert "gold/orders" in load_call_args[0]

    def test_connection_source_uses_default_schema_when_no_override(self) -> None:
        """Connection default_schema is used when source has no schema_override."""
        conn = OneLakeConnection(
            type="onelake",
            workspace="ws-guid-1234",
            lakehouse="lh-guid-5678",
            default_schema="silver",
        )
        source = Source(connection="primary", table="events")
        spark = MagicMock()
        mock_df = MagicMock()
        spark.read.format.return_value.load.return_value = mock_df

        connections = {"primary": conn}
        read_source(spark, "events", source, connections=connections)

        load_call_args = spark.read.format.return_value.load.call_args[0]
        assert "silver/events" in load_call_args[0]

    def test_connection_not_in_connections_dict_raises(self) -> None:
        """Source with undefined connection name raises ExecutionError."""
        source = Source(connection="missing_conn", table="products")
        spark = MagicMock()

        with pytest.raises(ExecutionError, match="missing_conn"):
            read_source(spark, "products", source, connections={})

    def test_connection_with_none_connections_raises(self) -> None:
        """Source with connection but no connections dict raises ExecutionError."""
        source = Source(connection="primary", table="products")
        spark = MagicMock()

        with pytest.raises(ExecutionError, match="primary"):
            read_source(spark, "products", source, connections=None)

    def test_non_connection_source_unaffected_by_connections_param(self) -> None:
        """Passing connections to a non-connection source has no effect."""
        conn = OneLakeConnection(
            type="onelake",
            workspace="ws-guid",
            lakehouse="lh-guid",
        )
        spark = MagicMock()
        mock_df = MagicMock()
        spark.read.format.return_value.table.return_value = mock_df

        source = Source(type="delta", alias="db.my_table")
        connections = {"primary": conn}

        result = read_source(spark, "my_table", source, connections=connections)

        spark.read.format.assert_called_with("delta")
        spark.read.format.return_value.table.assert_called_with("db.my_table")
        assert result is mock_df


class TestDateSequenceSource:
    """Tests for date_sequence generated source reading."""

    def test_daily_range_produces_correct_row_count(self, spark: SparkSession) -> None:
        source = Source(
            type="date_sequence",
            column="date",
            start="2025-01-01",
            end="2025-01-10",
            step="day",
        )
        df = read_source(spark, "dates", source)

        assert df.count() == 10

    def test_weekly_range_produces_correct_row_count(self, spark: SparkSession) -> None:
        source = Source(
            type="date_sequence",
            column="date",
            start="2025-01-01",
            end="2025-01-31",
            step="week",
        )
        df = read_source(spark, "dates", source)

        # Jan 1, 8, 15, 22, 29 → 5 rows
        assert df.count() == 5

    def test_monthly_range_produces_correct_row_count(self, spark: SparkSession) -> None:
        source = Source(
            type="date_sequence",
            column="date",
            start="2025-01-01",
            end="2025-12-01",
            step="month",
        )
        df = read_source(spark, "dates", source)

        assert df.count() == 12

    def test_yearly_range_produces_correct_row_count(self, spark: SparkSession) -> None:
        source = Source(
            type="date_sequence",
            column="date",
            start="2020-01-01",
            end="2025-01-01",
            step="year",
        )
        df = read_source(spark, "dates", source)

        assert df.count() == 6

    def test_output_column_name_matches_source_column(self, spark: SparkSession) -> None:
        source = Source(
            type="date_sequence",
            column="report_date",
            start="2025-01-01",
            end="2025-01-03",
        )
        df = read_source(spark, "dates", source)

        assert df.columns == ["report_date"]

    def test_output_type_is_date(self, spark: SparkSession) -> None:
        from pyspark.sql.types import DateType

        source = Source(
            type="date_sequence",
            column="dt",
            start="2025-01-01",
            end="2025-01-05",
        )
        df = read_source(spark, "dates", source)

        assert isinstance(df.schema["dt"].dataType, DateType)

    def test_start_and_end_dates_are_inclusive(self, spark: SparkSession) -> None:
        import datetime

        source = Source(
            type="date_sequence",
            column="d",
            start="2025-03-01",
            end="2025-03-03",
            step="day",
        )
        df = read_source(spark, "dates", source)

        dates = {row["d"] for row in df.collect()}
        assert datetime.date(2025, 3, 1) in dates
        assert datetime.date(2025, 3, 3) in dates

    def test_empty_range_produces_zero_rows(self, spark: SparkSession) -> None:
        source = Source(
            type="date_sequence",
            column="date",
            start="2025-01-10",
            end="2025-01-01",
            step="day",
        )
        df = read_source(spark, "dates", source)

        assert df.count() == 0

    def test_single_row_when_start_equals_end(self, spark: SparkSession) -> None:
        source = Source(
            type="date_sequence",
            column="date",
            start="2025-06-15",
            end="2025-06-15",
        )
        df = read_source(spark, "dates", source)

        assert df.count() == 1

    def test_step_defaults_to_day_when_omitted(self, spark: SparkSession) -> None:
        source = Source(
            type="date_sequence",
            column="date",
            start="2025-01-01",
            end="2025-01-05",
        )
        df = read_source(spark, "dates", source)

        # Default day step: Jan 1-5 = 5 rows
        assert df.count() == 5


class TestIntSequenceSource:
    """Tests for int_sequence generated source reading."""

    def test_range_1_to_100_produces_100_rows(self, spark: SparkSession) -> None:
        source = Source(type="int_sequence", column="n", start=1, end=100)
        df = read_source(spark, "seq", source)
        assert df.count() == 100

    def test_range_with_step_produces_correct_rows(self, spark: SparkSession) -> None:
        source = Source(type="int_sequence", column="n", start=1, end=10, step=3)
        df = read_source(spark, "seq", source)
        # spark.range(1, 11, 3) → [1, 4, 7, 10]
        assert df.count() == 4
        values = sorted(row["n"] for row in df.collect())
        assert values == [1, 4, 7, 10]

    def test_output_column_name_matches_source_column(self, spark: SparkSession) -> None:
        source = Source(type="int_sequence", column="my_id", start=1, end=5)
        df = read_source(spark, "seq", source)
        assert df.columns == ["my_id"]

    def test_output_type_is_long(self, spark: SparkSession) -> None:
        from pyspark.sql.types import LongType

        source = Source(type="int_sequence", column="val", start=0, end=3)
        df = read_source(spark, "seq", source)
        assert isinstance(df.schema["val"].dataType, LongType)

    def test_end_value_is_inclusive_on_step_boundary(self, spark: SparkSession) -> None:
        # end=10 with step=3 from start=1 hits 10 exactly
        source = Source(type="int_sequence", column="n", start=1, end=10, step=3)
        df = read_source(spark, "seq", source)
        values = sorted(row["n"] for row in df.collect())
        assert 10 in values

    def test_empty_range_when_start_greater_than_end(self, spark: SparkSession) -> None:
        source = Source(type="int_sequence", column="n", start=100, end=1)
        df = read_source(spark, "seq", source)
        assert df.count() == 0

    def test_single_row_when_start_equals_end(self, spark: SparkSession) -> None:
        source = Source(type="int_sequence", column="n", start=5, end=5)
        df = read_source(spark, "seq", source)
        assert df.count() == 1
        assert df.collect()[0]["n"] == 5

    def test_default_step_is_one(self, spark: SparkSession) -> None:
        source = Source(type="int_sequence", column="n", start=1, end=5)
        df = read_source(spark, "seq", source)
        values = sorted(row["n"] for row in df.collect())
        assert values == [1, 2, 3, 4, 5]

    def test_empty_range_has_correct_schema(self, spark: SparkSession) -> None:
        from pyspark.sql.types import LongType

        source = Source(type="int_sequence", column="n", start=100, end=1)
        df = read_source(spark, "seq", source)
        assert df.count() == 0
        assert df.columns == ["n"]
        assert isinstance(df.schema["n"].dataType, LongType)
