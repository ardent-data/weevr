"""Tests for source readers (Spark integration tests)."""

import json
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_helpers import create_delta_table
from weevr.errors.exceptions import ExecutionError
from weevr.model.connection import OneLakeConnection
from weevr.model.load import CdcConfig, LoadConfig
from weevr.model.source import DedupConfig, Source
from weevr.operations.readers import (
    _typed_watermark_col,
    build_watermark_filter,
    read_cdc_source,
    read_source,
    read_source_incremental,
    read_sources,
)
from weevr.state.watermark import WatermarkState

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
        object.__setattr__(source, "start", None)
        object.__setattr__(source, "end", None)
        object.__setattr__(source, "column", None)
        object.__setattr__(source, "step", None)

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


class TestGeneratedSourceIntegration:
    """Integration tests: generated sources in pipelines and as join inputs."""

    def test_date_sequence_with_derived_columns(self, spark: SparkSession) -> None:
        """date_sequence source supports downstream column derivation."""
        source = Source(
            type="date_sequence",
            column="calendar_date",
            start="2025-03-01",
            end="2025-03-05",
            step="day",
        )
        df = read_source(spark, "dates", source)
        result = (
            df.withColumn("year", F.year(F.col("calendar_date")))
            .withColumn("month", F.month(F.col("calendar_date")))
            .withColumn("day", F.dayofmonth(F.col("calendar_date")))
        )

        assert result.count() == 5
        assert set(result.columns) == {"calendar_date", "year", "month", "day"}

        row = result.filter(F.col("calendar_date") == "2025-03-03").collect()[0]
        assert row["year"] == 2025
        assert row["month"] == 3
        assert row["day"] == 3

    def test_date_sequence_derived_column_types(self, spark: SparkSession) -> None:
        """Derived year/month/day columns have integer types."""
        from pyspark.sql.types import IntegerType

        source = Source(
            type="date_sequence",
            column="calendar_date",
            start="2025-01-01",
            end="2025-01-03",
        )
        df = read_source(spark, "dates", source)
        result = (
            df.withColumn("year", F.year(F.col("calendar_date")))
            .withColumn("month", F.month(F.col("calendar_date")))
            .withColumn("day", F.dayofmonth(F.col("calendar_date")))
        )

        assert isinstance(result.schema["year"].dataType, IntegerType)
        assert isinstance(result.schema["month"].dataType, IntegerType)
        assert isinstance(result.schema["day"].dataType, IntegerType)

    def test_int_sequence_joined_with_delta_source(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """int_sequence source can be inner-joined with a delta source."""
        path = tmp_delta_path("join_data")
        data = [{"id": 1, "label": "a"}, {"id": 3, "label": "c"}, {"id": 5, "label": "e"}]
        create_delta_table(spark, path, data)

        int_source = Source(type="int_sequence", column="n", start=1, end=5)
        delta_source = Source(type="delta", alias=path)

        int_df = read_source(spark, "keys", int_source)
        delta_df = read_source(spark, "data", delta_source)
        joined = int_df.join(delta_df, int_df["n"] == delta_df["id"], "inner")

        assert joined.count() == 3
        assert set(joined.columns) == {"n", "id", "label"}

    def test_int_sequence_left_join_preserves_all_keys(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Left join with an int_sequence source keeps all generated keys."""
        path = tmp_delta_path("left_join_data")
        data = [{"id": 2, "val": "x"}, {"id": 4, "val": "y"}]
        create_delta_table(spark, path, data)

        int_source = Source(type="int_sequence", column="n", start=1, end=4)
        delta_source = Source(type="delta", alias=path)

        int_df = read_source(spark, "keys", int_source)
        delta_df = read_source(spark, "data", delta_source)
        joined = int_df.join(delta_df, int_df["n"] == delta_df["id"], "left")

        assert joined.count() == 4
        matched = joined.filter(F.col("val").isNotNull()).count()
        assert matched == 2

    def test_delta_source_reading_unchanged(self, spark: SparkSession, tmp_delta_path) -> None:
        """Existing delta source reading is unaffected by generated source changes."""
        path = tmp_delta_path("compat_check")
        data = [{"k": 10, "v": "alpha"}, {"k": 20, "v": "beta"}, {"k": 30, "v": "gamma"}]
        create_delta_table(spark, path, data)

        source = Source(type="delta", alias=path)
        df = read_source(spark, "compat", source)

        assert df.count() == 3
        assert set(df.columns) == {"k", "v"}
        values = sorted(row["k"] for row in df.collect())
        assert values == [10, 20, 30]

    def test_generated_source_as_with_block_from(self, spark: SparkSession) -> None:
        """A generated source can serve as the from: input for a with: sub-pipeline.

        Simulates the CTE resolution that _resolve_with_block performs: read the
        generated source, then apply a column derivation step the way the engine
        would via run_pipeline.
        """
        from weevr.model.pipeline import DeriveParams, DeriveStep
        from weevr.model.types import SparkExpr
        from weevr.operations.pipeline import run_pipeline

        source = Source(
            type="date_sequence",
            column="dt",
            start="2025-06-01",
            end="2025-06-07",
            step="day",
        )
        df = read_source(spark, "dates", source)

        # Replicate what the engine does inside _resolve_with_block:
        # run_pipeline applies steps to the CTE DataFrame.
        steps: list = [
            DeriveStep(derive=DeriveParams(columns={"week_num": SparkExpr("weekofyear(dt)")})),
        ]
        cte_df = run_pipeline(df, steps, sources={})

        assert cte_df.count() == 7
        assert "week_num" in cte_df.columns
        assert "dt" in cte_df.columns


def _generic_cdc_config() -> CdcConfig:
    return CdcConfig(
        operation_column="OPFLAG",
        insert_value="I",
        update_value="U",
        delete_value="D",
    )


def _watermark_state(value: str, type_: str = "timestamp") -> WatermarkState:
    return WatermarkState(
        thread_name="t",
        watermark_column="AEDATTM",
        watermark_type=type_,  # type: ignore[arg-type]
        last_value=value,
        last_updated=datetime(2026, 1, 1),
    )


def _cdc_load_config(
    *, watermark_column: str | None = "AEDATTM", inclusive: bool = False
) -> LoadConfig:
    return LoadConfig(
        mode="cdc",
        cdc=_generic_cdc_config(),
        watermark_column=watermark_column,
        watermark_type="timestamp" if watermark_column else None,
        watermark_inclusive=inclusive,
    )


class TestReadCdcSourceGenericWatermark:
    """Unit tests for ``read_cdc_source`` generic CDC + watermark composition.

    Mirrors the ``read_source_incremental`` pattern: filter by prior HWM,
    capture new HWM from the filtered DataFrame *before* operation routing.
    Empty windows return ``None`` so the executor leaves prior state alone.
    """

    def test_first_run_with_rows_captures_max(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("cdc_first_run_rows")
        data = [
            {"id": 1, "OPFLAG": "I", "AEDATTM": "2026-01-01 10:00:00"},
            {"id": 2, "OPFLAG": "U", "AEDATTM": "2026-01-02 10:00:00"},
            {"id": 3, "OPFLAG": "D", "AEDATTM": "2026-01-03 10:00:00"},
        ]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = _cdc_load_config()

        df, new_hwm = read_cdc_source(
            spark,
            source,
            load.cdc,  # type: ignore[arg-type]
            load_config=load,
            prior_state=None,
        )

        assert df.count() == 3
        assert new_hwm is not None
        assert "2026-01-03" in new_hwm

    def test_first_run_empty_source_returns_none_hwm(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """DEC-002: empty first-run source -> capture None, skip persistence."""
        path = tmp_delta_path("cdc_first_run_empty")
        empty_df = spark.createDataFrame([], "id INT, OPFLAG STRING, AEDATTM TIMESTAMP")
        empty_df.write.format("delta").save(path)
        source = Source(type="delta", alias=path)
        load = _cdc_load_config()

        df, new_hwm = read_cdc_source(
            spark,
            source,
            load.cdc,  # type: ignore[arg-type]
            load_config=load,
            prior_state=None,
        )
        assert df.count() == 0
        assert new_hwm is None

    def test_subsequent_run_filters_and_captures_new_max(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("cdc_subsequent")
        data = [
            {"id": 1, "OPFLAG": "I", "AEDATTM": "2026-01-01 10:00:00"},
            {"id": 2, "OPFLAG": "U", "AEDATTM": "2026-01-02 10:00:00"},
            {"id": 3, "OPFLAG": "U", "AEDATTM": "2026-01-05 10:00:00"},
            {"id": 4, "OPFLAG": "D", "AEDATTM": "2026-01-07 10:00:00"},
        ]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = _cdc_load_config()
        prior = _watermark_state("2026-01-02 10:00:00")

        df, new_hwm = read_cdc_source(
            spark,
            source,
            load.cdc,  # type: ignore[arg-type]
            load_config=load,
            prior_state=prior,
        )

        assert df.count() == 2  # rows 3 and 4 only (strict greater-than)
        assert new_hwm is not None
        assert "2026-01-07" in new_hwm

    def test_subsequent_run_empty_window_returns_none_hwm(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("cdc_empty_window")
        data = [
            {"id": 1, "OPFLAG": "I", "AEDATTM": "2026-01-01 10:00:00"},
            {"id": 2, "OPFLAG": "U", "AEDATTM": "2026-01-02 10:00:00"},
        ]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = _cdc_load_config()
        prior = _watermark_state("2026-12-31 23:59:59")

        df, new_hwm = read_cdc_source(
            spark,
            source,
            load.cdc,  # type: ignore[arg-type]
            load_config=load,
            prior_state=prior,
        )

        assert df.count() == 0
        assert new_hwm is None

    def test_inclusive_boundary_row_included(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("cdc_inclusive")
        data = [
            {"id": 1, "OPFLAG": "I", "AEDATTM": "2026-01-02 10:00:00"},
            {"id": 2, "OPFLAG": "U", "AEDATTM": "2026-01-03 10:00:00"},
        ]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = _cdc_load_config(inclusive=True)
        prior = _watermark_state("2026-01-02 10:00:00")

        df, _ = read_cdc_source(
            spark,
            source,
            load.cdc,  # type: ignore[arg-type]
            load_config=load,
            prior_state=prior,
        )
        # Inclusive: boundary row at 2026-01-02 10:00:00 is kept
        assert df.count() == 2

    def test_exclusive_boundary_row_excluded(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("cdc_exclusive")
        data = [
            {"id": 1, "OPFLAG": "I", "AEDATTM": "2026-01-02 10:00:00"},
            {"id": 2, "OPFLAG": "U", "AEDATTM": "2026-01-03 10:00:00"},
        ]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = _cdc_load_config(inclusive=False)
        prior = _watermark_state("2026-01-02 10:00:00")

        df, _ = read_cdc_source(
            spark,
            source,
            load.cdc,  # type: ignore[arg-type]
            load_config=load,
            prior_state=prior,
        )
        # Exclusive: boundary row excluded; only row at 2026-01-03 returned
        assert df.count() == 1

    def test_hwm_capture_includes_delete_rows(self, spark: SparkSession, tmp_delta_path) -> None:
        """DEC-003: HWM is computed pre-routing — D rows participate."""
        path = tmp_delta_path("cdc_hwm_with_deletes")
        data = [
            {"id": 1, "OPFLAG": "I", "AEDATTM": "2026-01-01 10:00:00"},
            {"id": 2, "OPFLAG": "U", "AEDATTM": "2026-01-02 10:00:00"},
            # D row carries the maximum timestamp
            {"id": 3, "OPFLAG": "D", "AEDATTM": "2026-01-10 10:00:00"},
        ]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = _cdc_load_config()

        _, new_hwm = read_cdc_source(
            spark,
            source,
            load.cdc,  # type: ignore[arg-type]
            load_config=load,
            prior_state=None,
        )

        assert new_hwm is not None
        # The D row's timestamp should be captured even though it would be
        # routed to a delete operation downstream.
        assert "2026-01-10" in new_hwm

    @pytest.mark.parametrize(
        ("watermark_type", "ddl", "row_value", "prior_value", "later_value"),
        [
            (
                "timestamp",
                "id INT, OPFLAG STRING, AEDATTM TIMESTAMP",
                datetime(2026, 1, 5, 10, 0, 0),
                "2026-01-01 10:00:00",
                "2026-01-05",
            ),
            (
                "date",
                "id INT, OPFLAG STRING, AEDATTM DATE",
                date(2026, 1, 5),
                "2026-01-01",
                "2026-01-05",
            ),
            (
                "long",
                "id INT, OPFLAG STRING, AEDATTM BIGINT",
                100,
                10,
                "100",
            ),
            (
                "int",
                "id INT, OPFLAG STRING, AEDATTM INT",
                42,
                10,
                "42",
            ),
        ],
    )
    def test_all_watermark_types_supported(
        self,
        spark: SparkSession,
        tmp_delta_path,
        watermark_type: str,
        ddl: str,
        row_value,
        prior_value,
        later_value: str,
    ) -> None:
        """DEC-004 (widened): timestamp, date, long, int all work."""
        path = tmp_delta_path(f"cdc_type_{watermark_type}")
        df_in = spark.createDataFrame([(1, "I", row_value)], ddl)
        df_in.write.format("delta").save(path)

        source = Source(type="delta", alias=path)
        load = LoadConfig(
            mode="cdc",
            cdc=_generic_cdc_config(),
            watermark_column="AEDATTM",
            watermark_type=watermark_type,  # type: ignore[arg-type]
        )
        prior = WatermarkState(
            thread_name="t",
            watermark_column="AEDATTM",
            watermark_type=watermark_type,  # type: ignore[arg-type]
            last_value=str(prior_value),
            last_updated=datetime(2026, 1, 1),
        )

        df, new_hwm = read_cdc_source(
            spark,
            source,
            load.cdc,  # type: ignore[arg-type]
            load_config=load,
            prior_state=prior,
        )

        assert df.count() == 1
        assert new_hwm is not None
        assert later_value in new_hwm

    def test_cdf_preset_returns_none_hwm(self, spark: SparkSession, tmp_delta_path) -> None:
        """CDF preset returns (df, None) — HWM capture is not applicable.

        The second tuple element is reserved for the generic-CDC watermark
        path; CDF tracks progress via commit version instead.
        """
        path = tmp_delta_path("cdf_preset_hwm")
        create_delta_table(spark, path, [{"id": 1, "name": "Alice"}])
        spark.sql(
            f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
        )
        initial_version = (
            DeltaTable.forPath(spark, path).history(1).select("version").collect()[0]["version"]
        )
        spark.createDataFrame([{"id": 2, "name": "Bob"}]).write.format("delta").mode("append").save(
            path
        )

        source = Source(type="delta", alias=path)
        cdc_config = CdcConfig(preset="delta_cdf")

        result = read_cdc_source(spark, source, cdc_config, last_version=initial_version)

        assert isinstance(result, tuple)
        assert len(result) == 2
        df, new_hwm = result
        assert new_hwm is None
        assert "_change_type" in df.columns

    def test_no_watermark_config_reads_full_source(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """EC-013: generic CDC without watermark_column reads full source."""
        path = tmp_delta_path("cdc_no_watermark")
        data = [
            {"id": 1, "OPFLAG": "I", "AEDATTM": "2026-01-01 10:00:00"},
            {"id": 2, "OPFLAG": "U", "AEDATTM": "2026-01-02 10:00:00"},
            {"id": 3, "OPFLAG": "D", "AEDATTM": "2026-01-03 10:00:00"},
        ]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = LoadConfig(mode="cdc", cdc=_generic_cdc_config())

        df, new_hwm = read_cdc_source(
            spark,
            source,
            load.cdc,  # type: ignore[arg-type]
            load_config=load,
            prior_state=None,
        )

        assert df.count() == 3
        assert new_hwm is None


class TestTypedWatermarkCol:
    """Unit tests for the ``_typed_watermark_col`` helper."""

    def test_passthrough_when_format_is_none(self, spark: SparkSession) -> None:
        col = _typed_watermark_col("AEDATTM", "timestamp", None)
        # Bare column reference — no parse wrapper.
        assert str(col) == str(F.col("AEDATTM"))

    def test_wraps_in_to_timestamp_for_timestamp_type(self, spark: SparkSession) -> None:
        col = _typed_watermark_col("AEDATTM", "timestamp", "yyyy-MM-dd HH:mm:ss.SSSSSX")
        rendered = str(col)
        assert "to_timestamp(" in rendered
        assert "AEDATTM" in rendered

    def test_wraps_in_to_date_for_date_type(self, spark: SparkSession) -> None:
        col = _typed_watermark_col("event_date", "date", "yyyyMMdd")
        rendered = str(col)
        assert "to_date(" in rendered
        assert "event_date" in rendered


class TestBuildWatermarkFilter:
    """Unit tests for ``build_watermark_filter`` parameter shape."""

    def test_without_format_matches_legacy_repr(self, spark: SparkSession) -> None:
        """Regression guard: omitting watermark_format must produce the
        same Column expression as the pre-M124 implementation.
        """
        col = build_watermark_filter(
            watermark_column="AEDATTM",
            watermark_type="timestamp",
            last_value="2026-01-01 10:00:00",
        )
        # Frozen snapshot of the legacy expression shape: bare column on
        # the left, literal-cast on the right, strict greater-than.
        expected = F.col("AEDATTM") > F.lit("2026-01-01 10:00:00").cast("timestamp")
        assert str(col) == str(expected)

    def test_with_format_composes_typed_column_and_literal_cast(self, spark: SparkSession) -> None:
        col = build_watermark_filter(
            watermark_column="AEDATTM",
            watermark_type="timestamp",
            last_value="2026-01-01 10:00:00",
            watermark_format="yyyy-MM-dd HH:mm:ss.SSSSSX",
        )
        rendered = str(col)
        assert "to_timestamp(" in rendered
        assert "CAST(2026-01-01 10:00:00 AS TIMESTAMP)" in rendered

    def test_with_format_inclusive_uses_ge(self, spark: SparkSession) -> None:
        col = build_watermark_filter(
            watermark_column="AEDATTM",
            watermark_type="timestamp",
            last_value="2026-01-01 10:00:00",
            inclusive=True,
            watermark_format="yyyy-MM-dd HH:mm:ss.SSSSSX",
        )
        rendered = str(col)
        assert " >= " in rendered
        assert "to_timestamp(" in rendered


_SAP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSX"


def _string_ts_load_config() -> LoadConfig:
    return LoadConfig(
        mode="incremental_watermark",
        watermark_column="AEDATTM",
        watermark_type="timestamp",
        watermark_format=_SAP_FORMAT,
    )


class TestReadSourceIncrementalWatermarkFormat:
    """Integration tests for ``read_source_incremental`` + ``watermark_format``."""

    def test_first_run_string_timestamps_capture_canonical_hwm(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("inc_first_string_ts")
        data = [
            {"id": 1, "AEDATTM": "2026-01-01 10:00:00.12345Z"},
            {"id": 2, "AEDATTM": "2026-01-02 10:00:00.12345Z"},
            {"id": 3, "AEDATTM": "2026-01-03 10:00:00.12345Z"},
        ]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = _string_ts_load_config()

        df, new_hwm = read_source_incremental(spark, "src", source, load, prior_state=None)

        assert df.count() == 3
        # HWM is captured as a Spark Timestamp converted to a Python str —
        # *not* the raw source string. The trailing Z and the literal source
        # text are gone because the typed aggregate produced a real Timestamp.
        assert new_hwm is not None
        assert new_hwm.startswith("2026-01-03 10:00:00")
        assert "Z" not in new_hwm

    def test_subsequent_run_filters_above_prior_hwm(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("inc_subsequent_string_ts")
        data = [
            {"id": 1, "AEDATTM": "2026-01-01 10:00:00.00000Z"},
            {"id": 2, "AEDATTM": "2026-01-05 10:00:00.00000Z"},
            {"id": 3, "AEDATTM": "2026-01-07 10:00:00.00000Z"},
        ]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = _string_ts_load_config()
        prior = WatermarkState(
            thread_name="t",
            watermark_column="AEDATTM",
            watermark_type="timestamp",
            last_value="2026-01-05 10:00:00",
            last_updated=datetime(2026, 1, 5),
        )

        df, new_hwm = read_source_incremental(spark, "src", source, load, prior_state=prior)

        assert df.count() == 1  # only row 3 (strict greater-than)
        assert new_hwm is not None
        assert new_hwm.startswith("2026-01-07 10:00:00")

    def test_unparseable_rows_silently_dropped(self, spark: SparkSession, tmp_delta_path) -> None:
        """DEC-003: parseable rows flow through, unparseable rows drop out
        of both the predicate and the HWM aggregate.
        """
        path = tmp_delta_path("inc_mixed_format")
        data = [
            {"id": 1, "AEDATTM": "2026-01-01 10:00:00.00000Z"},
            {"id": 2, "AEDATTM": "not-a-timestamp"},
            {"id": 3, "AEDATTM": "2026-01-04 10:00:00.00000Z"},
        ]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = _string_ts_load_config()
        prior = WatermarkState(
            thread_name="t",
            watermark_column="AEDATTM",
            watermark_type="timestamp",
            last_value="2026-01-01 10:00:00",
            last_updated=datetime(2026, 1, 1),
        )

        df, new_hwm = read_source_incremental(spark, "src", source, load, prior_state=prior)

        # Garbage row never satisfies the predicate; only the parseable row
        # above the prior HWM remains.
        assert df.count() == 1
        assert new_hwm is not None
        assert new_hwm.startswith("2026-01-04 10:00:00")

    def test_debug_log_emitted_when_format_set(
        self, spark: SparkSession, tmp_delta_path, caplog
    ) -> None:
        import logging

        path = tmp_delta_path("inc_debug_log")
        data = [{"id": 1, "AEDATTM": "2026-01-01 10:00:00.00000Z"}]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = _string_ts_load_config()

        with caplog.at_level(logging.DEBUG, logger="weevr.operations.readers"):
            read_source_incremental(spark, "src", source, load, prior_state=None)

        assert any("watermark_format applied" in rec.getMessage() for rec in caplog.records)

    def test_format_unset_path_unchanged(self, spark: SparkSession, tmp_delta_path) -> None:
        """Regression guard: when watermark_format is None, the existing
        Delta-typed timestamp path is byte-identical to today.
        """
        path = tmp_delta_path("inc_no_format")
        data = [
            {"id": 1, "AEDATTM": datetime(2026, 1, 1, 10, 0, 0)},
            {"id": 2, "AEDATTM": datetime(2026, 1, 5, 10, 0, 0)},
        ]
        spark.createDataFrame(data).write.format("delta").save(path)
        source = Source(type="delta", alias=path)
        load = LoadConfig(
            mode="incremental_watermark",
            watermark_column="AEDATTM",
            watermark_type="timestamp",
        )

        df, new_hwm = read_source_incremental(spark, "src", source, load, prior_state=None)

        assert df.count() == 2
        assert new_hwm is not None
        assert new_hwm.startswith("2026-01-05 10:00:00")


def _cdc_string_ts_load_config() -> LoadConfig:
    return LoadConfig(
        mode="cdc",
        cdc=_generic_cdc_config(),
        watermark_column="AEDATTM",
        watermark_type="timestamp",
        watermark_format=_SAP_FORMAT,
    )


class TestReadCdcSourceWatermarkFormat:
    """Integration tests for ``read_cdc_source`` generic CDC + ``watermark_format``."""

    def test_first_run_string_timestamps_capture_canonical_hwm(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("cdc_first_string_ts")
        data = [
            {"id": 1, "OPFLAG": "I", "AEDATTM": "2026-01-01 10:00:00.12345Z"},
            {"id": 2, "OPFLAG": "U", "AEDATTM": "2026-01-02 10:00:00.12345Z"},
            {"id": 3, "OPFLAG": "D", "AEDATTM": "2026-01-03 10:00:00.12345Z"},
        ]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = _cdc_string_ts_load_config()

        df, new_hwm = read_cdc_source(
            spark,
            source,
            load.cdc,  # type: ignore[arg-type]
            load_config=load,
            prior_state=None,
        )

        assert df.count() == 3
        assert new_hwm is not None
        assert new_hwm.startswith("2026-01-03 10:00:00")
        # Typed parse → canonical Python str of Timestamp, no trailing Z.
        assert "Z" not in new_hwm

    def test_subsequent_run_filters_above_prior_hwm(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("cdc_subsequent_string_ts")
        data = [
            {"id": 1, "OPFLAG": "I", "AEDATTM": "2026-01-01 10:00:00.00000Z"},
            {"id": 2, "OPFLAG": "U", "AEDATTM": "2026-01-05 10:00:00.00000Z"},
            {"id": 3, "OPFLAG": "U", "AEDATTM": "2026-01-07 10:00:00.00000Z"},
        ]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = _cdc_string_ts_load_config()
        prior = WatermarkState(
            thread_name="t",
            watermark_column="AEDATTM",
            watermark_type="timestamp",
            last_value="2026-01-05 10:00:00",
            last_updated=datetime(2026, 1, 5),
        )

        df, new_hwm = read_cdc_source(
            spark,
            source,
            load.cdc,  # type: ignore[arg-type]
            load_config=load,
            prior_state=prior,
        )

        assert df.count() == 1  # only row 3 (strict greater-than)
        assert new_hwm is not None
        assert new_hwm.startswith("2026-01-07 10:00:00")

    def test_delete_row_at_max_advances_hwm(self, spark: SparkSession, tmp_delta_path) -> None:
        """DEC-003 / M123: D row carries the max — HWM still advances past it."""
        path = tmp_delta_path("cdc_delete_at_max")
        data = [
            {"id": 1, "OPFLAG": "I", "AEDATTM": "2026-01-01 10:00:00.00000Z"},
            {"id": 2, "OPFLAG": "U", "AEDATTM": "2026-01-02 10:00:00.00000Z"},
            # Delete row carries the highest timestamp.
            {"id": 3, "OPFLAG": "D", "AEDATTM": "2026-01-10 10:00:00.00000Z"},
        ]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = _cdc_string_ts_load_config()

        _, new_hwm = read_cdc_source(
            spark,
            source,
            load.cdc,  # type: ignore[arg-type]
            load_config=load,
            prior_state=None,
        )

        assert new_hwm is not None
        assert new_hwm.startswith("2026-01-10 10:00:00")

    def test_debug_log_emitted_when_format_set(
        self, spark: SparkSession, tmp_delta_path, caplog
    ) -> None:
        import logging

        path = tmp_delta_path("cdc_debug_log")
        data = [{"id": 1, "OPFLAG": "I", "AEDATTM": "2026-01-01 10:00:00.00000Z"}]
        create_delta_table(spark, path, data)
        source = Source(type="delta", alias=path)
        load = _cdc_string_ts_load_config()

        with caplog.at_level(logging.DEBUG, logger="weevr.operations.readers"):
            read_cdc_source(
                spark,
                source,
                load.cdc,  # type: ignore[arg-type]
                load_config=load,
                prior_state=None,
            )

        assert any("watermark_format applied" in rec.getMessage() for rec in caplog.records)

    def test_cdf_preset_path_does_not_reference_watermark_format(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """The CDF preset branch must not consult ``load_config`` at all —
        validator forbids the pairing, but the reader path itself is also
        independent.
        """
        path = tmp_delta_path("cdc_cdf_no_format")
        create_delta_table(spark, path, [{"id": 1, "name": "Alice"}])
        spark.sql(
            f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
        )
        spark.createDataFrame([{"id": 2, "name": "Bob"}]).write.format("delta").mode("append").save(
            path
        )

        source = Source(type="delta", alias=path)
        cdf_config = CdcConfig(preset="delta_cdf")

        # No load_config passed at all — CDF path must succeed.
        df, new_hwm = read_cdc_source(spark, source, cdf_config, last_version=0)
        assert new_hwm is None
        assert "_change_type" in df.columns
