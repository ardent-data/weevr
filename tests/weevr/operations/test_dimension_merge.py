"""Integration tests for the staged dimension merge."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from weevr.model.dimension import DimensionConfig
from weevr.operations.dimension import (
    DimensionMergeBuilder,
    execute_dimension_merge,
)
from weevr.operations.hashing import compute_dimension_keys


def _make_dim(**overrides):  # type: ignore[no-untyped-def]
    base = {
        "business_key": ["customer_id"],
        "surrogate_key": {"name": "_sk", "columns": ["customer_id"]},
    }
    return DimensionConfig(**{**base, **overrides})


def _prepare_source(spark, rows, dim_config, run_ts="2026-01-01"):
    """Create a source DataFrame with keys, hashes, and SCD columns."""
    df = spark.createDataFrame(rows)
    df = compute_dimension_keys(df, dim_config)
    builder = DimensionMergeBuilder(dim_config)
    df = builder.inject_scd_columns(df, run_ts)
    return df


@pytest.mark.spark
class TestFirstWrite:
    """First-write behavior: all rows inserted."""

    def test_all_rows_inserted(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_first")
        dim = _make_dim(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "cd": {"columns": "auto", "on_change": "version"},
            },
        )
        source = _prepare_source(
            spark,
            [
                {"customer_id": "C1", "name": "Alice"},
                {"customer_id": "C2", "name": "Bob"},
            ],
            dim,
        )
        builder = DimensionMergeBuilder(dim)
        result = execute_dimension_merge(spark, source, builder, path, "2026-01-01")
        assert result.rows_inserted == 2
        target = spark.read.format("delta").load(path)
        assert target.count() == 2


@pytest.mark.spark
class TestIdempotency:
    """Second run with same data: no-op."""

    def test_no_changes_on_rerun(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_idempotent")
        dim = _make_dim(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "cd": {"columns": "auto", "on_change": "version"},
            },
        )
        rows = [{"customer_id": "C1", "name": "Alice"}]

        # First run
        src1 = _prepare_source(spark, rows, dim, "2026-01-01")
        builder = DimensionMergeBuilder(dim)
        execute_dimension_merge(spark, src1, builder, path, "2026-01-01")

        # Second run — same data
        src2 = _prepare_source(spark, rows, dim, "2026-01-02")
        execute_dimension_merge(spark, src2, builder, path, "2026-01-02")

        target = spark.read.format("delta").load(path)
        # Should still have 1 current row + 1 closed row from rerun
        # (the merge inserts a new version even if hash matches because
        # the MERGE inserts unmatched source rows — but the BK matches,
        # so it should be just 1 row if no hash change)
        current = target.filter(F.col("_is_current") == True)  # noqa: E712
        assert current.count() == 1


@pytest.mark.spark
class TestVersioning:
    """Hash change triggers close + insert new version."""

    def test_changed_row_versioned(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_version")
        dim = _make_dim(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "cd": {"columns": "auto", "on_change": "version"},
            },
        )
        builder = DimensionMergeBuilder(dim)

        # First run
        src1 = _prepare_source(spark, [{"customer_id": "C1", "name": "Alice"}], dim, "2026-01-01")
        execute_dimension_merge(spark, src1, builder, path, "2026-01-01")

        # Second run — name changed
        src2 = _prepare_source(spark, [{"customer_id": "C1", "name": "Alicia"}], dim, "2026-02-01")
        execute_dimension_merge(spark, src2, builder, path, "2026-02-01")

        target = spark.read.format("delta").load(path)
        # Should have 2 rows: 1 closed + 1 current
        assert target.count() >= 2
        current = target.filter(F.col("_is_current") == True)  # noqa: E712
        assert current.count() == 1
        current_row = current.collect()[0]
        assert current_row["name"] == "Alicia"


@pytest.mark.spark
class TestNewEntity:
    """New entity (unmatched source) is inserted."""

    def test_new_entity_inserted(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_new_entity")
        dim = _make_dim(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "cd": {"columns": "auto", "on_change": "version"},
            },
        )
        builder = DimensionMergeBuilder(dim)

        # First run — C1 only
        src1 = _prepare_source(spark, [{"customer_id": "C1", "name": "Alice"}], dim, "2026-01-01")
        execute_dimension_merge(spark, src1, builder, path, "2026-01-01")

        # Second run — C1 + C2
        src2 = _prepare_source(
            spark,
            [
                {"customer_id": "C1", "name": "Alice"},
                {"customer_id": "C2", "name": "Bob"},
            ],
            dim,
            "2026-02-01",
        )
        execute_dimension_merge(spark, src2, builder, path, "2026-02-01")

        target = spark.read.format("delta").load(path)
        current = target.filter(F.col("_is_current") == True)  # noqa: E712
        assert current.count() == 2


@pytest.mark.spark
class TestSimpleDimension:
    """Non-versioned dimension (track_history=False) — standard merge."""

    def test_overwrite_on_change(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_simple")
        dim = _make_dim()  # track_history=False, default overwrite
        builder = DimensionMergeBuilder(dim)

        # First run
        src1 = _prepare_source(spark, [{"customer_id": "C1", "name": "Alice"}], dim, "2026-01-01")
        execute_dimension_merge(spark, src1, builder, path, "2026-01-01")

        # Second run — changed
        src2 = _prepare_source(spark, [{"customer_id": "C1", "name": "Alicia"}], dim, "2026-02-01")
        execute_dimension_merge(spark, src2, builder, path, "2026-02-01")

        target = spark.read.format("delta").load(path)
        assert target.count() == 1
        assert target.collect()[0]["name"] == "Alicia"


@pytest.mark.spark
class TestPreviousColumns:
    """previous_columns shift-and-update."""

    def test_previous_value_tracked_on_overwrite(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_prev")
        dim = _make_dim(
            previous_columns={"_prev_name": "name"},
        )
        builder = DimensionMergeBuilder(dim)

        schema = StructType(
            [
                StructField("customer_id", StringType()),
                StructField("name", StringType()),
                StructField("_prev_name", StringType()),
            ]
        )

        # First run
        src1 = spark.createDataFrame(
            [{"customer_id": "C1", "name": "Alice", "_prev_name": None}],
            schema=schema,
        )
        src1 = compute_dimension_keys(src1, dim)
        execute_dimension_merge(spark, src1, builder, path, "2026-01-01")

        # Second run — name changed
        src2 = spark.createDataFrame(
            [{"customer_id": "C1", "name": "Alicia", "_prev_name": None}],
            schema=schema,
        )
        src2 = compute_dimension_keys(src2, dim)
        execute_dimension_merge(spark, src2, builder, path, "2026-02-01")

        target = spark.read.format("delta").load(path)
        row = target.collect()[0]
        assert row["name"] == "Alicia"
        assert row["_prev_name"] == "Alice"


@pytest.mark.spark
class TestHistoryFilter:
    """history_filter toggle."""

    def test_history_filter_false_reads_all(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_no_filter")
        dim = _make_dim(
            track_history=True,
            history_filter=False,
            change_detection={  # type: ignore[arg-type]
                "cd": {"columns": "auto", "on_change": "version"},
            },
        )
        builder = DimensionMergeBuilder(dim)

        # First run
        src1 = _prepare_source(spark, [{"customer_id": "C1", "name": "Alice"}], dim, "2026-01-01")
        execute_dimension_merge(spark, src1, builder, path, "2026-01-01")

        # Merge still works with history_filter=False
        src2 = _prepare_source(spark, [{"customer_id": "C1", "name": "Alicia"}], dim, "2026-02-01")
        execute_dimension_merge(spark, src2, builder, path, "2026-02-01")

        target = spark.read.format("delta").load(path)
        assert target.count() >= 2  # Has history rows


@pytest.mark.spark
class TestAdditionalKeys:
    """Additional keys produce secondary hash columns."""

    def test_additional_key_present_in_output(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_addl_key")
        dim = _make_dim(
            additional_keys={  # type: ignore[arg-type]
                "version_sk": {
                    "name": "_version_sk",
                    "columns": ["customer_id", "name"],
                },
            },
        )
        source = _prepare_source(
            spark,
            [{"customer_id": "C1", "name": "Alice"}],
            dim,
        )
        builder = DimensionMergeBuilder(dim)
        execute_dimension_merge(spark, source, builder, path, "2026-01-01")

        target = spark.read.format("delta").load(path)
        assert "_version_sk" in target.columns
        assert "_sk" in target.columns
