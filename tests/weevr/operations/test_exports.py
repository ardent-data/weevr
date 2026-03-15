"""Tests for export write operations."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from weevr.model.export import Export
from weevr.operations.audit import AuditContext
from weevr.operations.exports import resolve_export_path, resolve_exports, write_export


def _ctx(**overrides: str) -> AuditContext:
    """Build an AuditContext with sensible defaults."""
    defaults = {
        "thread_name": "stg_orders",
        "thread_qualified_key": "staging.stg_orders",
        "thread_source": "bronze.orders",
        "thread_sources_json": "[]",
        "weave_name": "staging",
        "loom_name": "my-project",
        "run_timestamp": "2026-03-15T10:30:00+00:00",
        "run_id": "abc-123-def-456",
    }
    defaults.update(overrides)
    return AuditContext(**defaults)  # type: ignore[arg-type]


class TestResolveExportPath:
    """Test context variable substitution in export paths."""

    def test_run_timestamp(self):
        """${run.timestamp} is resolved."""
        result = resolve_export_path("/archive/${run.timestamp}/data", _ctx())
        assert result == "/archive/2026-03-15T10:30:00+00:00/data"

    def test_run_id(self):
        """${run.id} is resolved."""
        result = resolve_export_path("/export/${run.id}", _ctx())
        assert result == "/export/abc-123-def-456"

    def test_thread_name(self):
        """${thread.name} is resolved."""
        result = resolve_export_path("/data/${thread.name}", _ctx())
        assert result == "/data/stg_orders"

    def test_multiple_variables(self):
        """Multiple variables are resolved in one path."""
        result = resolve_export_path(
            "/archive/${loom.name}/${thread.name}/${run.timestamp}",
            _ctx(),
        )
        assert result == "/archive/my-project/stg_orders/2026-03-15T10:30:00+00:00"

    def test_no_variables_passthrough(self):
        """Path without variables is returned unchanged."""
        result = resolve_export_path("/static/path/data", _ctx())
        assert result == "/static/path/data"

    def test_unknown_property_left_unresolved(self):
        """Unknown properties within known namespaces are left as-is."""
        result = resolve_export_path("/data/${thread.unknown}", _ctx())
        assert result == "/data/${thread.unknown}"


class TestResolveExports:
    """Test resolve_exports() with variable substitution."""

    def test_resolves_path_variables(self):
        """Exports with path variables get resolved."""
        exports = [
            Export(name="archive", type="parquet", path="/data/${run.timestamp}"),
        ]
        resolved = resolve_exports(exports, _ctx())
        assert resolved[0].path == "/data/2026-03-15T10:30:00+00:00"

    def test_preserves_static_paths(self):
        """Exports without variables are returned unchanged."""
        exports = [
            Export(name="static", type="csv", path="/static/path"),
        ]
        resolved = resolve_exports(exports, _ctx())
        assert resolved[0].path == "/static/path"

    def test_alias_exports_unchanged(self):
        """Delta exports using alias (no path) are returned as-is."""
        exports = [
            Export(name="copy", type="delta", alias="db.copy"),
        ]
        resolved = resolve_exports(exports, _ctx())
        assert resolved[0].alias == "db.copy"


class TestWriteExport:
    """Test write_export() for each format."""

    def test_write_delta(self, spark: SparkSession, tmp_path: Path):
        """Write export in Delta format."""
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        export = Export(name="delta_out", type="delta", path=str(tmp_path / "delta_export"))
        result = write_export(spark, df, export)
        assert result.status == "success"
        assert result.rows_written == 2
        assert result.name == "delta_out"
        assert result.type == "delta"
        # Verify data was written
        read_back = spark.read.format("delta").load(str(tmp_path / "delta_export"))
        assert read_back.count() == 2

    def test_write_parquet(self, spark: SparkSession, tmp_path: Path):
        """Write export in Parquet format."""
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        export = Export(name="pq_out", type="parquet", path=str(tmp_path / "parquet_export"))
        result = write_export(spark, df, export)
        assert result.status == "success"
        assert result.rows_written == 2
        read_back = spark.read.parquet(str(tmp_path / "parquet_export"))
        assert read_back.count() == 2

    def test_write_csv(self, spark: SparkSession, tmp_path: Path):
        """Write export in CSV format."""
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        export = Export(
            name="csv_out",
            type="csv",
            path=str(tmp_path / "csv_export"),
            options={"header": "true"},
        )
        result = write_export(spark, df, export)
        assert result.status == "success"
        assert result.rows_written == 2
        read_back = spark.read.csv(str(tmp_path / "csv_export"), header=True)
        assert read_back.count() == 2

    def test_write_json(self, spark: SparkSession, tmp_path: Path):
        """Write export in JSON format."""
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        export = Export(name="json_out", type="json", path=str(tmp_path / "json_export"))
        result = write_export(spark, df, export)
        assert result.status == "success"
        assert result.rows_written == 2
        read_back = spark.read.json(str(tmp_path / "json_export"))
        assert read_back.count() == 2

    def test_write_orc(self, spark: SparkSession, tmp_path: Path):
        """Write export in ORC format."""
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        export = Export(name="orc_out", type="orc", path=str(tmp_path / "orc_export"))
        result = write_export(spark, df, export)
        assert result.status == "success"
        assert result.rows_written == 2
        read_back = spark.read.orc(str(tmp_path / "orc_export"))
        assert read_back.count() == 2

    def test_write_with_partition_by(self, spark: SparkSession, tmp_path: Path):
        """Write export with partition_by."""
        df = spark.createDataFrame([(1, "a", "us"), (2, "b", "uk")], ["id", "name", "region"])
        export = Export(
            name="partitioned",
            type="parquet",
            path=str(tmp_path / "part_export"),
            partition_by=["region"],
        )
        result = write_export(spark, df, export)
        assert result.status == "success"
        # Check partition directories exist
        assert (tmp_path / "part_export" / "region=us").exists()
        assert (tmp_path / "part_export" / "region=uk").exists()

    def test_write_failure_returns_error_result(self, spark: SparkSession):
        """Failed write returns ExportResult with error details."""
        df = spark.createDataFrame([(1,)], ["id"])
        # Use an invalid path that will fail
        export = Export(
            name="bad_export",
            type="parquet",
            path="/nonexistent/readonly/path/\x00invalid",
            on_failure="warn",
        )
        result = write_export(spark, df, export)
        assert result.status == "warned"
        assert result.error is not None
        assert result.rows_written == 0

    def test_abort_failure_status(self, spark: SparkSession):
        """on_failure=abort produces 'aborted' status."""
        df = spark.createDataFrame([(1,)], ["id"])
        export = Export(
            name="critical",
            type="parquet",
            path="/nonexistent/readonly/path/\x00invalid",
            on_failure="abort",
        )
        result = write_export(spark, df, export)
        assert result.status == "aborted"
        assert result.error is not None

    def test_csv_with_options(self, spark: SparkSession, tmp_path: Path):
        """CSV export with custom delimiter."""
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        export = Export(
            name="pipe_csv",
            type="csv",
            path=str(tmp_path / "pipe_csv"),
            options={"header": "true", "delimiter": "|"},
        )
        result = write_export(spark, df, export)
        assert result.status == "success"
        # Verify pipe delimiter was used
        read_back = spark.read.csv(str(tmp_path / "pipe_csv"), header=True, sep="|")
        assert read_back.count() == 2
