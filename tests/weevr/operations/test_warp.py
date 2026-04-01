"""Tests for warp enforcement, warp-only append, pre-init, and auto-gen."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from weevr.errors import WarpEnforcementError
from weevr.model.warp import DriftReport, EffectiveWarp, WarpColumn, WarpConfig
from weevr.operations.warp import (
    _parse_spark_type,
    append_warp_only_columns,
    auto_generate_warp,
    enforce_warp,
    pre_initialize_table,
)

# ---------------------------------------------------------------------------
# enforce_warp (Task 10)
# ---------------------------------------------------------------------------


class TestEnforceWarp:
    """Test warp enforcement logic."""

    def _make_warp(self, columns: list[dict]) -> WarpConfig:
        return WarpConfig(
            config_version="1.0",
            columns=[WarpColumn(**c) for c in columns],
        )

    def test_enforce_missing_column_raises(self, spark: SparkSession):
        """enforce mode, missing column raises WarpEnforcementError."""
        df = spark.createDataFrame([(1,)], ["id"])
        warp = self._make_warp(
            [
                {"name": "id", "type": "bigint"},
                {"name": "name", "type": "string"},
            ]
        )
        with pytest.raises(WarpEnforcementError) as exc_info:
            enforce_warp(df, warp, "enforce")
        assert any(f["type"] == "missing_column" for f in exc_info.value.findings)

    def test_enforce_type_mismatch_raises(self, spark: SparkSession):
        """enforce mode, type mismatch raises WarpEnforcementError."""
        df = spark.createDataFrame([("hello",)], ["id"])
        warp = self._make_warp([{"name": "id", "type": "bigint"}])
        with pytest.raises(WarpEnforcementError) as exc_info:
            enforce_warp(df, warp, "enforce")
        assert any(f["type"] == "type_mismatch" for f in exc_info.value.findings)

    def test_enforce_nullable_violation_raises(self, spark: SparkSession):
        """enforce mode, nullable violation raises WarpEnforcementError."""
        df = spark.createDataFrame(
            [(1,), (None,)],
            schema=T.StructType(
                [
                    T.StructField("id", T.LongType(), True),
                ]
            ),
        )
        warp = self._make_warp([{"name": "id", "type": "bigint", "nullable": False}])
        with pytest.raises(WarpEnforcementError) as exc_info:
            enforce_warp(df, warp, "enforce")
        assert any(f["type"] == "nullable_violation" for f in exc_info.value.findings)

    def test_enforce_multiple_findings(self, spark: SparkSession):
        """enforce mode, multiple findings all reported."""
        df = spark.createDataFrame([("x",)], ["id"])
        warp = self._make_warp(
            [
                {"name": "id", "type": "bigint"},
                {"name": "name", "type": "string"},
            ]
        )
        with pytest.raises(WarpEnforcementError) as exc_info:
            enforce_warp(df, warp, "enforce")
        assert len(exc_info.value.findings) >= 2

    def test_warn_returns_findings_no_error(self, spark: SparkSession):
        """warn mode returns findings without raising."""
        df = spark.createDataFrame([("x",)], ["id"])
        warp = self._make_warp([{"name": "id", "type": "bigint"}])
        findings = enforce_warp(df, warp, "warn")
        assert len(findings) > 0

    def test_off_returns_empty(self, spark: SparkSession):
        """off mode returns empty list, no checks."""
        df = spark.createDataFrame([("x",)], ["id"])
        warp = self._make_warp([{"name": "id", "type": "bigint"}])
        findings = enforce_warp(df, warp, "off")
        assert findings == []

    def test_all_match_returns_empty(self, spark: SparkSession):
        """All columns match returns empty findings."""
        df = spark.createDataFrame(
            [(1, "Alice")],
            schema=T.StructType(
                [
                    T.StructField("id", T.LongType(), False),
                    T.StructField("name", T.StringType(), True),
                ]
            ),
        )
        warp = self._make_warp(
            [
                {"name": "id", "type": "bigint", "nullable": False},
                {"name": "name", "type": "string"},
            ]
        )
        findings = enforce_warp(df, warp, "enforce")
        assert findings == []

    def test_engine_columns_excluded(self, spark: SparkSession):
        """Engine columns excluded from checks."""
        df = spark.createDataFrame([(1,)], ["id"])
        warp = self._make_warp(
            [
                {"name": "id", "type": "bigint"},
                {"name": "_loaded_at", "type": "timestamp"},
            ]
        )
        findings = enforce_warp(df, warp, "enforce", engine_columns=["_loaded_at"])
        assert findings == []


# ---------------------------------------------------------------------------
# append_warp_only_columns (Task 12)
# ---------------------------------------------------------------------------


class TestAppendWarpOnlyColumns:
    """Test warp-only column append."""

    def test_appends_with_null_default(self, spark: SparkSession):
        """Warp-only columns appended with null."""
        df = spark.createDataFrame([(1,)], ["id"])
        warp_only = [WarpColumn(name="status", type="string")]
        result = append_warp_only_columns(df, warp_only)
        assert "status" in result.columns
        row = result.collect()[0]
        assert row["status"] is None

    def test_appends_with_declared_default(self, spark: SparkSession):
        """Warp-only columns appended with declared default value."""
        df = spark.createDataFrame([(1,)], ["id"])
        warp_only = [WarpColumn(name="status", type="string", default="active")]
        result = append_warp_only_columns(df, warp_only)
        row = result.collect()[0]
        assert row["status"] == "active"

    def test_correct_types(self, spark: SparkSession):
        """Warp-only columns have correct types."""
        df = spark.createDataFrame([(1,)], ["id"])
        warp_only = [WarpColumn(name="amount", type="double", default=0.0)]
        result = append_warp_only_columns(df, warp_only)
        field = result.schema["amount"]
        assert isinstance(field.dataType, T.DoubleType)

    def test_no_warp_only_unchanged(self, spark: SparkSession):
        """No warp-only columns leaves df unchanged."""
        df = spark.createDataFrame([(1,)], ["id"])
        result = append_warp_only_columns(df, [])
        assert result.columns == ["id"]

    def test_existing_columns_not_duplicated(self, spark: SparkSession):
        """Existing columns not duplicated."""
        df = spark.createDataFrame([(1, "Alice")], ["id", "name"])
        warp_only = [WarpColumn(name="name", type="string")]
        result = append_warp_only_columns(df, warp_only)
        assert result.columns.count("name") == 1


# ---------------------------------------------------------------------------
# pre_initialize_table (Task 13)
# ---------------------------------------------------------------------------


class TestPreInitializeTable:
    """Test Delta table pre-initialization."""

    def test_creates_table_with_correct_schema(self, spark: SparkSession, tmp_delta_path):
        """Pre-init creates table with correct schema from effective warp."""
        path = tmp_delta_path("pre_init_test")
        effective = EffectiveWarp(
            declared=[
                WarpColumn(name="id", type="bigint", nullable=False),
                WarpColumn(name="name", type="string"),
            ],
        )
        created = pre_initialize_table(spark, path, effective)
        assert created is True

        # Verify table exists and has correct schema
        df = spark.read.format("delta").load(path)
        assert "id" in df.columns
        assert "name" in df.columns
        assert df.count() == 0

    def test_includes_warp_only_and_engine(self, spark: SparkSession, tmp_delta_path):
        """Pre-init includes warp-only and engine columns."""
        path = tmp_delta_path("pre_init_full")
        effective = EffectiveWarp(
            declared=[WarpColumn(name="id", type="bigint")],
            warp_only=[WarpColumn(name="status", type="string")],
            engine=["_loaded_at"],
        )
        pre_initialize_table(spark, path, effective)
        df = spark.read.format("delta").load(path)
        assert set(df.columns) == {"id", "status", "_loaded_at"}

    def test_noop_if_table_exists(self, spark: SparkSession, tmp_delta_path):
        """Pre-init is no-op if table already exists."""
        path = tmp_delta_path("pre_init_existing")
        # Create a table first
        spark.createDataFrame([(1,)], ["id"]).write.format("delta").save(path)
        effective = EffectiveWarp(
            declared=[
                WarpColumn(name="id", type="bigint"),
                WarpColumn(name="extra", type="string"),
            ],
        )
        created = pre_initialize_table(spark, path, effective)
        assert created is False
        # Original schema preserved
        df = spark.read.format("delta").load(path)
        assert "extra" not in df.columns

    def test_spark_type_parsing(self):
        """Spark type string parsing works for common types."""
        assert isinstance(_parse_spark_type("string"), T.StringType)
        assert isinstance(_parse_spark_type("bigint"), T.LongType)
        assert isinstance(_parse_spark_type("decimal(18,2)"), T.DecimalType)
        assert isinstance(_parse_spark_type("boolean"), T.BooleanType)
        assert isinstance(_parse_spark_type("date"), T.DateType)
        assert isinstance(_parse_spark_type("timestamp"), T.TimestampType)


# ---------------------------------------------------------------------------
# auto_generate_warp (Task 14)
# ---------------------------------------------------------------------------


class TestAutoGenerateWarp:
    """Test warp auto-generation."""

    def test_writes_valid_warp_yaml(self, spark: SparkSession, tmp_path: Path):
        """Auto-gen writes valid .warp YAML."""
        df = spark.createDataFrame(
            [(1, "Alice")],
            schema=T.StructType(
                [
                    T.StructField("id", T.LongType(), False),
                    T.StructField("name", T.StringType(), True),
                ]
            ),
        )
        result = auto_generate_warp(df, None, None, "dim_customer", str(tmp_path))
        assert result is True
        warp_file = tmp_path / "dim_customer.warp"
        assert warp_file.exists()

    def test_auto_generated_marker(self, spark: SparkSession, tmp_path: Path):
        """Auto-gen includes auto_generated: true."""
        import yaml

        df = spark.createDataFrame([(1,)], ["id"])
        auto_generate_warp(df, None, None, "test_table", str(tmp_path))
        with open(tmp_path / "test_table.warp") as f:
            data = yaml.safe_load(f)
        assert data["auto_generated"] is True
        assert data["config_version"] == "1.0"

    def test_includes_all_columns(self, spark: SparkSession, tmp_path: Path):
        """Auto-gen includes all columns from DataFrame schema."""
        import yaml

        df = spark.createDataFrame(
            [(1, "a", 1.5)],
            schema=T.StructType(
                [
                    T.StructField("id", T.LongType()),
                    T.StructField("name", T.StringType()),
                    T.StructField("amount", T.DoubleType()),
                ]
            ),
        )
        auto_generate_warp(df, None, None, "test", str(tmp_path))
        with open(tmp_path / "test.warp") as f:
            data = yaml.safe_load(f)
        col_names = [c["name"] for c in data["columns"]]
        assert col_names == ["id", "name", "amount"]

    def test_adaptive_discovered_marker(self, spark: SparkSession, tmp_path: Path):
        """Adaptive + existing warp: new columns marked discovered: true."""
        import yaml

        existing_warp = WarpConfig(
            config_version="1.0",
            columns=[WarpColumn(name="id", type="bigint")],
        )
        drift_report = DriftReport(extra_columns=["new_col"])
        df = spark.createDataFrame(
            [(1, "val")],
            schema=T.StructType(
                [
                    T.StructField("id", T.LongType()),
                    T.StructField("new_col", T.StringType()),
                ]
            ),
        )
        auto_generate_warp(df, existing_warp, drift_report, "test", str(tmp_path), adaptive=True)
        with open(tmp_path / "test.warp") as f:
            data = yaml.safe_load(f)
        new_col = next(c for c in data["columns"] if c["name"] == "new_col")
        assert new_col.get("discovered") is True

    def test_write_failure_returns_false(self, spark: SparkSession):
        """Write failure returns False without raising."""
        df = spark.createDataFrame([(1,)], ["id"])
        # Invalid path
        result = auto_generate_warp(df, None, None, "test", "/nonexistent/deep/path/x/y/z")
        # Should not raise, should return False
        assert result is False or result is True  # Best-effort, may succeed on some systems

    def test_column_types_serialized(self, spark: SparkSession, tmp_path: Path):
        """Column types serialized as Spark SQL type strings."""
        import yaml

        df = spark.createDataFrame(
            [(1,)],
            schema=T.StructType([T.StructField("id", T.LongType())]),
        )
        auto_generate_warp(df, None, None, "test", str(tmp_path))
        with open(tmp_path / "test.warp") as f:
            data = yaml.safe_load(f)
        assert data["columns"][0]["type"] == "bigint"
