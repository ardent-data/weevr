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
class TestVersionedLifecycle:
    """Three-plus-version lifecycles: merges never touch closed rows.

    Every scenario runs at least 3 writes because the close clause defect
    only becomes visible from the 3rd version on — with 2 writes there is
    no closed history row for a later merge to corrupt.
    """

    @staticmethod
    def _write(spark, path, dim, rows, run_ts):  # type: ignore[no-untyped-def]
        source = _prepare_source(spark, rows, dim, run_ts)
        builder = DimensionMergeBuilder(dim)
        execute_dimension_merge(spark, source, builder, path, run_ts)

    def _run_three_writes(self, spark, path, dim):  # type: ignore[no-untyped-def]
        for name, ts in [
            ("Alice", "2026-01-01"),
            ("Alicia", "2026-02-01"),
            ("Alize", "2026-03-01"),
        ]:
            self._write(spark, path, dim, [{"customer_id": "C1", "name": name}], ts)

    def test_third_version_leaves_closed_rows_untouched(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("dim_lifecycle")
        dim = _make_dim(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "names": {"columns": ["name"], "on_change": "version"},
            },
        )
        self._run_three_writes(spark, path, dim)

        rows = spark.read.format("delta").load(path).orderBy("_valid_from").collect()
        assert len(rows) == 3
        v1, v2, v3 = rows
        assert [r["_is_current"] for r in rows] == [False, False, True]
        # Closed rows keep the valid_to stamped when they were closed
        assert v1["_valid_to"] == "2026-02-01"
        assert v2["_valid_to"] == "2026-03-01"
        # Contiguous, non-overlapping validity intervals
        assert v1["_valid_to"] == v2["_valid_from"]
        assert v2["_valid_to"] == v3["_valid_from"]

    def test_overwrite_columns_on_closed_rows_stable(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("dim_lifecycle_overwrite")
        dim = _make_dim(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "names": {"columns": ["name"], "on_change": "version"},
                "contact": {"columns": ["email"], "on_change": "overwrite"},
            },
        )
        for name, email, ts in [
            ("Alice", "a1@x.com", "2026-01-01"),
            ("Alicia", "a2@x.com", "2026-02-01"),
            ("Alize", "a3@x.com", "2026-03-01"),
        ]:
            self._write(spark, path, dim, [{"customer_id": "C1", "name": name, "email": email}], ts)

        rows = spark.read.format("delta").load(path).orderBy("_valid_from").collect()
        assert len(rows) == 3
        v1, v2, v3 = rows
        # Overwrite values on closed rows come from the write that closed
        # them and are never re-stamped by later merges
        assert v1["email"] == "a2@x.com"
        assert v2["email"] == "a3@x.com"
        assert v3["email"] == "a3@x.com"

    def test_previous_columns_captured_only_on_closing_row(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("dim_lifecycle_prev")
        dim = _make_dim(
            track_history=True,
            previous_columns={"_prev_name": "name"},
            change_detection={  # type: ignore[arg-type]
                "names": {"columns": ["name"], "on_change": "version"},
            },
        )
        schema = StructType(
            [
                StructField("customer_id", StringType()),
                StructField("name", StringType()),
                StructField("_prev_name", StringType()),
            ]
        )
        builder = DimensionMergeBuilder(dim)
        for name, ts in [
            ("Alice", "2026-01-01"),
            ("Alicia", "2026-02-01"),
            ("Alize", "2026-03-01"),
        ]:
            src = spark.createDataFrame(
                [{"customer_id": "C1", "name": name, "_prev_name": None}], schema=schema
            )
            src = compute_dimension_keys(src, dim)
            src = builder.inject_scd_columns(src, ts)
            execute_dimension_merge(spark, src, builder, path, ts)

        rows = spark.read.format("delta").load(path).orderBy("_valid_from").collect()
        assert len(rows) == 3
        v1, v2, v3 = rows
        assert v1["_prev_name"] == "Alice"
        assert v2["_prev_name"] == "Alicia"
        assert v3["_prev_name"] is None

    def test_history_filter_false_matches_default_outcome(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("dim_lifecycle_nofilter")
        dim = _make_dim(
            track_history=True,
            history_filter=False,
            change_detection={  # type: ignore[arg-type]
                "names": {"columns": ["name"], "on_change": "version"},
            },
        )
        self._run_three_writes(spark, path, dim)

        rows = spark.read.format("delta").load(path).orderBy("_valid_from").collect()
        # history_filter must not change merge semantics: one row per
        # version, exactly one current — no duplicate current rows
        assert len(rows) == 3
        assert [r["_is_current"] for r in rows] == [False, False, True]
        assert [r["_valid_to"] for r in rows] == ["2026-02-01", "2026-03-01", "9999-12-31"]

    def test_history_filter_false_idempotent_rerun(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("dim_lifecycle_nofilter_rerun")
        dim = _make_dim(
            track_history=True,
            history_filter=False,
            change_detection={  # type: ignore[arg-type]
                "names": {"columns": ["name"], "on_change": "version"},
            },
        )
        self._run_three_writes(spark, path, dim)
        # Rerun the latest state unchanged — must be a no-op
        self._write(spark, path, dim, [{"customer_id": "C1", "name": "Alize"}], "2026-04-01")

        target = spark.read.format("delta").load(path)
        assert target.count() == 3
        current = target.filter(F.col("_is_current") == True)  # noqa: E712
        assert current.count() == 1
        assert current.collect()[0]["_valid_to"] == "9999-12-31"


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
                StructField("email", StringType()),
                StructField("_prev_name", StringType()),
            ]
        )

        # First run — email column ensures auto hash has data columns
        src1 = spark.createDataFrame(
            [{"customer_id": "C1", "name": "Alice", "email": "a@x.com", "_prev_name": None}],
            schema=schema,
        )
        src1 = compute_dimension_keys(src1, dim)
        execute_dimension_merge(spark, src1, builder, path, "2026-01-01")

        # Second run — name changed
        src2 = spark.createDataFrame(
            [{"customer_id": "C1", "name": "Alicia", "email": "a@x.com", "_prev_name": None}],
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
        # Include email so auto change detection has a data column
        source = _prepare_source(
            spark,
            [{"customer_id": "C1", "name": "Alice", "email": "a@x.com"}],
            dim,
        )
        builder = DimensionMergeBuilder(dim)
        execute_dimension_merge(spark, source, builder, path, "2026-01-01")

        target = spark.read.format("delta").load(path)
        assert "_version_sk" in target.columns
        assert "_sk" in target.columns
