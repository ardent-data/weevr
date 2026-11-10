"""Integration tests for the staged dimension merge."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from weevr.model.dimension import DimensionConfig
from weevr.model.write import WriteConfig
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


def _write_overrides(**kwargs):  # type: ignore[no-untyped-def]
    base = {"mode": "merge", "match_keys": ["customer_id"]}
    return WriteConfig(**{**base, **kwargs})


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

    def test_first_write_stamped_and_exact_under_interleaved_commit(
        self, spark: SparkSession, tmp_delta_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The dimension first write is a plain save — stamped and attributed
        like write_target's first write, exact when a foreign commit lands
        between the write and the metrics read."""
        import json

        from weevr import delta as delta_mod
        from weevr.operations.target_handle import CommitStamp

        path = tmp_delta_path("dim_first_stamped")
        dim = _make_dim()
        source = _prepare_source(
            spark,
            [
                {"customer_id": "C1", "name": "Alice"},
                {"customer_id": "C2", "name": "Bob"},
            ],
            dim,
        )
        builder = DimensionMergeBuilder(dim)
        stamp = CommitStamp(run_id="run-dim", thread="dim_thread", mode="merge")

        # A foreign commit lands after the engine write, before the metrics
        # read: intercept the history resolution to append it first.
        original_resolve = delta_mod.resolve_delta_table
        fired = {"done": False}

        def _foreign_then_resolve(spark_arg, path_arg):  # type: ignore[no-untyped-def]
            if not fired["done"]:
                fired["done"] = True
                frame = spark.createDataFrame([{"customer_id": "ZZ", "name": "foreign"}])
                frame.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
            return original_resolve(spark_arg, path_arg)

        monkeypatch.setattr(delta_mod, "resolve_delta_table", _foreign_then_resolve)
        result = execute_dimension_merge(spark, source, builder, path, "2026-01-01", stamp=stamp)
        monkeypatch.undo()

        assert result.rows_inserted == 2  # exact — not 0, not the foreign 1

        from weevr.delta import resolve_delta_table

        history = resolve_delta_table(spark, path).history().select("userMetadata").collect()
        stamped = [
            row[0] for row in history if row[0] and json.loads(row[0]).get("engine") == "weevr"
        ]
        assert len(stamped) == 1
        assert json.loads(stamped[0])["run_id"] == "run-dim"

    def test_first_write_unattributable_reports_unknown(
        self, spark: SparkSession, tmp_delta_path, monkeypatch: pytest.MonkeyPatch, caplog
    ) -> None:
        """A first-write count that cannot be attributed is None, never 0."""
        from weevr.operations.target_handle import TargetHandle

        path = tmp_delta_path("dim_first_unattr")
        dim = _make_dim()
        source = _prepare_source(spark, [{"customer_id": "C1", "name": "Alice"}], dim)
        builder = DimensionMergeBuilder(dim)
        monkeypatch.setattr(
            TargetHandle, "commit_metrics_after", lambda self, pre, stamp=None: None
        )

        result = execute_dimension_merge(spark, source, builder, path, "2026-01-01")
        assert result.rows_inserted is None
        assert "unavailable" in caplog.text

        monkeypatch.undo()
        assert spark.read.format("delta").load(path).count() == 1  # the write itself landed


@pytest.fixture
def eager_auto_compaction(spark: SparkSession):
    """Make auto compaction fire on small tables, restoring the session after."""
    key = "spark.databricks.delta.autoCompact.minNumFiles"
    spark.conf.set(key, "2")
    yield
    spark.conf.unset(key)


def _enable_auto_compaction(spark: SparkSession, path: str) -> None:
    spark.sql(
        f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')"
    )


def _operations(spark: SparkSession, path: str) -> list[str]:
    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").orderBy("version")
    return [row["operation"] for row in history.collect()]


@pytest.mark.spark
@pytest.mark.usefixtures("eager_auto_compaction")
class TestMergeCountsUnderAutoCompaction:
    """Auto compaction commits inside the merge call; counts must survive it."""

    def _merge(self, spark, path, dim, rows, ts):  # type: ignore[no-untyped-def]
        from weevr.operations.target_handle import TargetHandle

        source = _prepare_source(spark, rows, dim, ts).repartition(4)
        builder = DimensionMergeBuilder(dim)
        handle = TargetHandle(spark, path)
        return execute_dimension_merge(spark, source, builder, path, ts, handle=handle)

    def test_versioned_merge_counts(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_ac_versioned")
        dim = _make_dim(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "cd": {"columns": "auto", "on_change": "version"},
            },
        )
        first = [{"customer_id": f"C{i}", "name": "old"} for i in range(4)]
        self._merge(spark, path, dim, first, "2026-01-01")
        _enable_auto_compaction(spark, path)

        second = [
            {"customer_id": "C0", "name": "changed"},
            *[{"customer_id": f"N{i}", "name": "new"} for i in range(11)],
        ]
        result = self._merge(spark, path, dim, second, "2026-01-02")

        assert _operations(spark, path)[-1] == "OPTIMIZE"  # compaction actually fired
        assert result.rows_versioned == 1
        assert result.rows_inserted == 11

    def test_versioned_overwrite_merge_counts(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_ac_versioned_overwrite")
        dim = _make_dim(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "tracked": {"columns": ["tier"], "on_change": "version"},
            },
        )
        first = [{"customer_id": f"C{i}", "name": "old", "tier": "gold"} for i in range(4)]
        self._merge(spark, path, dim, first, "2026-01-01")
        _enable_auto_compaction(spark, path)

        second = [
            {"customer_id": "C0", "name": "old", "tier": "silver"},  # version
            {"customer_id": "C1", "name": "renamed", "tier": "gold"},  # unassigned → overwrite
            *[{"customer_id": f"N{i}", "name": "new", "tier": "gold"} for i in range(10)],
        ]
        result = self._merge(spark, path, dim, second, "2026-01-02")

        assert _operations(spark, path)[-1] == "OPTIMIZE"
        assert result.rows_versioned == 1
        assert result.rows_overwritten == 1
        assert result.rows_inserted == 10

    def test_overwrite_merge_counts(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_ac_overwrite")
        dim = _make_dim(track_history=False)
        first = [{"customer_id": f"C{i}", "name": "old"} for i in range(4)]
        self._merge(spark, path, dim, first, "2026-01-01")
        _enable_auto_compaction(spark, path)

        second = [{"customer_id": f"N{i}", "name": "new"} for i in range(12)]
        result = self._merge(spark, path, dim, second, "2026-01-02")

        assert _operations(spark, path)[-1] == "OPTIMIZE"
        assert result.rows_inserted == 12


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
        self._write(spark, path, dim, [{"customer_id": "C1", "name": "Alice"}], "2026-01-01")
        self._write(spark, path, dim, [{"customer_id": "C1", "name": "Alicia"}], "2026-02-01")
        v1_after_write2 = spark.read.format("delta").load(path).orderBy("_valid_from").collect()[0]
        self._write(spark, path, dim, [{"customer_id": "C1", "name": "Alize"}], "2026-03-01")

        rows = spark.read.format("delta").load(path).orderBy("_valid_from").collect()
        assert len(rows) == 3
        v1, v2, v3 = rows
        # The closed v1 row must be exactly what write 2 left behind —
        # write 3 may not touch it in any column
        assert v1.asDict() == v1_after_write2.asDict()
        assert [r["_is_current"] for r in rows] == [False, False, True]
        # Closed rows keep the valid_to stamped when they were closed
        assert v1["_valid_to"] == "2026-02-01"
        assert v2["_valid_to"] == "2026-03-01"
        # Contiguous, non-overlapping validity intervals
        assert v1["_valid_to"] == v2["_valid_from"]
        assert v2["_valid_to"] == v3["_valid_from"]

    def test_overwrite_columns_restamped_on_closed_rows(
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
        # Overwrite columns are updated in place across all versions: every
        # row of the key carries the latest value, including closed rows
        assert [r["email"] for r in rows] == ["a3@x.com"] * 3
        assert [r["_is_current"] for r in rows] == [False, False, True]

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

    def test_history_filter_false_matches_default_and_reruns_as_no_op(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        change_detection = {"names": {"columns": ["name"], "on_change": "version"}}
        default_path = tmp_delta_path("dim_lifecycle_default")
        nofilter_path = tmp_delta_path("dim_lifecycle_nofilter")
        default_dim = _make_dim(
            track_history=True,
            change_detection=change_detection,  # type: ignore[arg-type]
        )
        nofilter_dim = _make_dim(
            track_history=True,
            history_filter=False,
            change_detection=change_detection,  # type: ignore[arg-type]
        )
        self._run_three_writes(spark, default_path, default_dim)
        self._run_three_writes(spark, nofilter_path, nofilter_dim)

        scd_cols = ["customer_id", "name", "_sk", "_valid_from", "_valid_to", "_is_current"]

        def read_scd_rows(path):  # type: ignore[no-untyped-def]
            rows = spark.read.format("delta").load(path).orderBy("_valid_from").collect()
            return [{c: r[c] for c in scd_cols} for r in rows]

        nofilter_rows = read_scd_rows(nofilter_path)
        # history_filter must not change merge semantics: the lifecycle
        # outcome matches the default config row-for-row on SCD columns
        assert nofilter_rows == read_scd_rows(default_path)
        assert len(nofilter_rows) == 3
        assert [r["_is_current"] for r in nofilter_rows] == [False, False, True]
        assert [r["_valid_to"] for r in nofilter_rows] == ["2026-02-01", "2026-03-01", "9999-12-31"]
        # _sk is a business-key hash shared by every version; the RC5
        # fan-out precondition is multiple current rows on that one key,
        # ruled out by the single True above
        assert len({r["_sk"] for r in nofilter_rows}) == 1

        # Rerun the latest state unchanged — must be a no-op
        self._write(
            spark,
            nofilter_path,
            nofilter_dim,
            [{"customer_id": "C1", "name": "Alize"}],
            "2026-04-01",
        )
        assert read_scd_rows(nofilter_path) == nofilter_rows


_OW_SCHEMA = StructType(
    [
        StructField("customer_id", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("tier", StringType()),
    ]
)


def _ow_dim(attrs: dict | None = None, **overrides):  # type: ignore[no-untyped-def]
    """Versioned dimension: email/tier version, name per ``attrs`` (None = unassigned)."""
    groups: dict = {"tracked": {"columns": ["email", "tier"], "on_change": "version"}}
    if attrs is not None:
        groups["attrs"] = attrs
    return _make_dim(track_history=True, change_detection=groups, **overrides)


@pytest.mark.spark
class TestOverwriteInPlace:
    """Overwrite columns update in place across every version of the key."""

    @staticmethod
    def _write(spark, path, dim, rows, ts, schema=_OW_SCHEMA, evolve=False, overrides=None):  # type: ignore[no-untyped-def]
        from weevr.operations.target_handle import TargetHandle

        src = spark.createDataFrame(rows, schema=schema)
        src = compute_dimension_keys(src, dim)
        builder = DimensionMergeBuilder(dim, write_overrides=overrides)
        src = builder.inject_scd_columns(src, ts)
        handle = TargetHandle(spark, path)
        return execute_dimension_merge(spark, src, builder, path, ts, handle=handle, evolve=evolve)

    @staticmethod
    def _rows(spark, path):  # type: ignore[no-untyped-def]
        df = spark.read.format("delta").load(path)
        return df.orderBy("customer_id", "_valid_from").collect()

    @staticmethod
    def _row(cid, name, email="b@x.com", tier="gold"):  # type: ignore[no-untyped-def]
        return {"customer_id": cid, "name": name, "email": email, "tier": tier}

    def test_overwrite_only_change_updates_current_row(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("ow_repro")
        dim = _ow_dim({"columns": ["name"], "on_change": "overwrite"})
        self._write(spark, path, dim, [self._row("C1", "Bob")], "2026-01-01")
        result = self._write(spark, path, dim, [self._row("C1", "Bobby")], "2026-02-01")

        assert (result.rows_versioned, result.rows_overwritten, result.rows_inserted) == (0, 1, 0)
        rows = self._rows(spark, path)
        assert len(rows) == 1  # no new version
        (row,) = rows
        assert row["name"] == "Bobby"
        assert row["_is_current"] is True
        assert row["_valid_from"] == "2026-01-01"
        assert row["_valid_to"] == "9999-12-31"

    def test_overwrite_only_change_updates_closed_rows(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("ow_closed")
        dim = _ow_dim({"columns": ["name"], "on_change": "overwrite"})
        self._write(spark, path, dim, [self._row("C1", "Bob")], "2026-01-01")
        self._write(spark, path, dim, [self._row("C1", "Bob", email="c@x.com")], "2026-02-01")
        before = [
            (r["_valid_from"], r["_valid_to"], r["_is_current"]) for r in self._rows(spark, path)
        ]
        result = self._write(
            spark, path, dim, [self._row("C1", "Bobby", email="c@x.com")], "2026-03-01"
        )

        assert (result.rows_versioned, result.rows_overwritten, result.rows_inserted) == (0, 2, 0)
        rows = self._rows(spark, path)
        assert [r["name"] for r in rows] == ["Bobby", "Bobby"]
        # Validity is untouched by an in-place update
        assert [(r["_valid_from"], r["_valid_to"], r["_is_current"]) for r in rows] == before
        # The closed row keeps its own version columns
        assert [r["email"] for r in rows] == ["b@x.com", "c@x.com"]

    def test_version_and_overwrite_change_in_same_run(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("ow_mixed")
        dim = _ow_dim({"columns": ["name"], "on_change": "overwrite"})
        self._write(spark, path, dim, [self._row("C1", "Bob")], "2026-01-01")
        self._write(spark, path, dim, [self._row("C1", "Bob", email="c@x.com")], "2026-02-01")
        result = self._write(
            spark, path, dim, [self._row("C1", "Bobby", email="d@x.com")], "2026-03-01"
        )

        # The current row is closed (a version); the older closed row is
        # updated in place; the new version is not a new entity
        assert (result.rows_versioned, result.rows_overwritten, result.rows_inserted) == (1, 1, 0)
        rows = self._rows(spark, path)
        assert len(rows) == 3
        assert [r["name"] for r in rows] == ["Bobby"] * 3
        assert [r["email"] for r in rows] == ["b@x.com", "c@x.com", "d@x.com"]
        assert [r["_is_current"] for r in rows] == [False, False, True]
        assert [r["_valid_to"] for r in rows] == ["2026-02-01", "2026-03-01", "9999-12-31"]

    def test_auto_overwrite_group(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("ow_auto")
        dim = _ow_dim({"columns": "auto", "on_change": "overwrite"})
        self._write(spark, path, dim, [self._row("C1", "Bob")], "2026-01-01")
        self._write(spark, path, dim, [self._row("C1", "Bob", tier="silver")], "2026-02-01")
        self._write(spark, path, dim, [self._row("C1", "Bobby", tier="silver")], "2026-03-01")

        rows = self._rows(spark, path)
        assert len(rows) == 2
        assert [r["name"] for r in rows] == ["Bobby", "Bobby"]

    def test_unassigned_columns(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("ow_unassigned")
        dim = _ow_dim(None)
        self._write(spark, path, dim, [self._row("C1", "Bob")], "2026-01-01")
        self._write(spark, path, dim, [self._row("C1", "Bob", tier="silver")], "2026-02-01")
        self._write(spark, path, dim, [self._row("C1", "Bobby", tier="silver")], "2026-03-01")

        rows = self._rows(spark, path)
        assert len(rows) == 2
        assert [r["name"] for r in rows] == ["Bobby", "Bobby"]

    def test_overwrite_hash_matches_updated_values(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("ow_hash")
        dim = _ow_dim({"columns": ["name"], "on_change": "overwrite"})
        self._write(spark, path, dim, [self._row("C1", "Bob")], "2026-01-01")
        self._write(spark, path, dim, [self._row("C1", "Bob", email="c@x.com")], "2026-02-01")
        self._write(spark, path, dim, [self._row("C1", "Bobby", email="c@x.com")], "2026-03-01")

        expected = compute_dimension_keys(
            spark.createDataFrame([self._row("C1", "Bobby")], schema=_OW_SCHEMA), dim
        ).collect()[0]["attrs"]
        assert [r["attrs"] for r in self._rows(spark, path)] == [expected, expected]

    def test_static_and_previous_columns_keep_semantics(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("ow_static_prev")
        schema = StructType([*_OW_SCHEMA.fields, StructField("_prev_email", StringType())])
        dim = _make_dim(
            track_history=True,
            previous_columns={"_prev_email": "email"},
            change_detection={  # type: ignore[arg-type]
                "tracked": {"columns": ["email"], "on_change": "version"},
                "attrs": {"columns": ["name"], "on_change": "overwrite"},
                "fixed": {"columns": ["tier"], "on_change": "static"},
            },
        )

        def row(name, email, tier):  # type: ignore[no-untyped-def]
            return {**self._row("C1", name, email=email, tier=tier), "_prev_email": None}

        self._write(spark, path, dim, [row("Bob", "b@x.com", "gold")], "2026-01-01", schema)
        self._write(spark, path, dim, [row("Bob", "c@x.com", "gold")], "2026-02-01", schema)
        # Overwrite-only change plus a static change: static never moves
        self._write(spark, path, dim, [row("Bobby", "c@x.com", "platinum")], "2026-03-01", schema)

        rows = self._rows(spark, path)
        assert [r["name"] for r in rows] == ["Bobby", "Bobby"]
        assert [r["tier"] for r in rows] == ["gold", "gold"]
        # previous_columns set on the closing row only, not by the in-place update
        assert [r["_prev_email"] for r in rows] == ["b@x.com", None]

    def test_unchanged_rerun_updates_nothing(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("ow_rerun")
        dim = _ow_dim({"columns": ["name"], "on_change": "overwrite"})
        rows = [self._row("C1", None), self._row("C2", "Cara")]
        self._write(spark, path, dim, rows, "2026-01-01")
        self._write(
            spark, path, dim, [self._row("C1", None, email="c@x.com"), rows[1]], "2026-02-01"
        )
        before = self._rows(spark, path)
        result = self._write(
            spark, path, dim, [self._row("C1", None, email="c@x.com"), rows[1]], "2026-03-01"
        )

        assert (result.rows_versioned, result.rows_overwritten, result.rows_inserted) == (0, 0, 0)
        assert self._rows(spark, path) == before
        last = spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1").collect()[0]
        assert last["operationMetrics"]["numTargetRowsUpdated"] == "0"

    def test_counts_with_new_entities_and_versions(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("ow_counts")
        dim = _ow_dim({"columns": ["name"], "on_change": "overwrite"})
        self._write(
            spark, path, dim, [self._row("C1", "Ann"), self._row("C2", "Bob")], "2026-01-01"
        )
        result = self._write(
            spark,
            path,
            dim,
            [
                self._row("C1", "Ann", email="c@x.com"),  # version
                self._row("C2", "Bobby"),  # overwrite only
                self._row("C3", "Cara"),  # new entity
                self._row("C4", "Dan"),  # new entity
            ],
            "2026-02-01",
        )
        assert (result.rows_versioned, result.rows_overwritten, result.rows_inserted) == (1, 1, 2)
        assert len(self._rows(spark, path)) == 5

    def test_one_commit_per_merge(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("ow_commit")
        dim = _ow_dim({"columns": ["name"], "on_change": "overwrite"})
        self._write(spark, path, dim, [self._row("C1", "Bob")], "2026-01-01")
        self._write(spark, path, dim, [self._row("C1", "Bob", email="c@x.com")], "2026-02-01")
        version_before = spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1").collect()[0][
            "version"
        ]
        self._write(spark, path, dim, [self._row("C1", "Bobby", email="d@x.com")], "2026-03-01")
        last = spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1").collect()[0]
        assert last["version"] == version_before + 1
        assert last["operation"] == "MERGE"

    def test_adaptive_drift_new_unassigned_column(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("ow_adaptive")
        dim = _ow_dim(None)
        self._write(spark, path, dim, [self._row("C1", "Bob")], "2026-01-01")
        wide = StructType([*_OW_SCHEMA.fields, StructField("segment", StringType())])
        new_row = {**self._row("C1", "Bob"), "segment": "retail"}
        # The new column is not in the target yet: the write must succeed
        self._write(spark, path, dim, [new_row], "2026-02-01", schema=wide, evolve=True)
        assert "segment" in spark.read.format("delta").load(path).columns
        # Once the column exists, the next run fills it in place
        self._write(spark, path, dim, [new_row], "2026-03-01", schema=wide, evolve=True)
        rows = self._rows(spark, path)
        assert len(rows) == 1
        assert rows[0]["segment"] == "retail"

    def test_soft_delete_unaffected(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("ow_soft")
        schema = StructType([*_OW_SCHEMA.fields, StructField("deleted", BooleanType())])
        dim = _ow_dim({"columns": ["name"], "on_change": "overwrite"})
        overrides = _write_overrides(on_no_match_source="soft_delete", soft_delete_column="deleted")

        def row(cid, name, email="b@x.com"):  # type: ignore[no-untyped-def]
            return {**self._row(cid, name, email=email), "deleted": False}

        self._write(
            spark,
            path,
            dim,
            [row("C1", "Bob"), row("C2", "Cara")],
            "2026-01-01",
            schema,
            overrides=overrides,
        )
        self._write(
            spark,
            path,
            dim,
            [row("C1", "Bob", "c@x.com"), row("C2", "Cara")],
            "2026-02-01",
            schema,
            overrides=overrides,
        )
        # C2 vanishes; C1 gets an overwrite-only change
        self._write(
            spark,
            path,
            dim,
            [row("C1", "Bobby", "c@x.com")],
            "2026-03-01",
            schema,
            overrides=overrides,
        )

        rows = self._rows(spark, path)
        c1 = [r for r in rows if r["customer_id"] == "C1"]
        c2 = [r for r in rows if r["customer_id"] == "C2"]
        assert [r["name"] for r in c1] == ["Bobby", "Bobby"]
        assert [r["deleted"] for r in c1] == [False, False]
        assert [r["deleted"] for r in c2] == [True]


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
    """history_filter is accepted but inert.

    Detection and the close clause always operate on current rows; the
    flag is retained for configuration compatibility only. This guards
    the config surface: setting it must neither fail validation nor
    change versioning. Merge-semantics parity across the flag's values
    is covered by TestVersionedLifecycle.
    """

    def test_flag_accepted_and_versioning_unchanged(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
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

        # Second run — normal close-and-insert versioning
        src2 = _prepare_source(spark, [{"customer_id": "C1", "name": "Alicia"}], dim, "2026-02-01")
        execute_dimension_merge(spark, src2, builder, path, "2026-02-01")

        target = spark.read.format("delta").load(path)
        assert target.count() == 2
        assert target.filter(F.col("_is_current") == True).count() == 1  # noqa: E712


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


@pytest.mark.spark
class TestNoMatchSourceStandard:
    """on_no_match_source delete/soft_delete on the standard (Type 1) path."""

    def test_delete_removes_absent_entity(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_nms_delete")
        dim = _make_dim()
        builder = DimensionMergeBuilder(
            dim, write_overrides=_write_overrides(on_no_match_source="delete")
        )

        rows = [
            {"customer_id": "C1", "name": "Alice"},
            {"customer_id": "C2", "name": "Bob"},
        ]
        src1 = _prepare_source(spark, rows, dim, "2026-01-01")
        execute_dimension_merge(spark, src1, builder, path, "2026-01-01")

        # C2 disappears from the source — declared delete must remove it
        src2 = _prepare_source(spark, rows[:1], dim, "2026-01-02")
        execute_dimension_merge(spark, src2, builder, path, "2026-01-02")

        target = spark.read.format("delta").load(path)
        remaining = {r["customer_id"] for r in target.collect()}
        assert remaining == {"C1"}

    def test_soft_delete_flags_absent_entity(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_nms_soft")
        dim = _make_dim()
        builder = DimensionMergeBuilder(
            dim,
            write_overrides=_write_overrides(
                on_no_match_source="soft_delete", soft_delete_column="deleted"
            ),
        )

        rows = [
            {"customer_id": "C1", "name": "Alice", "deleted": False},
            {"customer_id": "C2", "name": "Bob", "deleted": False},
        ]
        src1 = _prepare_source(spark, rows, dim, "2026-01-01")
        execute_dimension_merge(spark, src1, builder, path, "2026-01-01")

        src2 = _prepare_source(spark, rows[:1], dim, "2026-01-02")
        execute_dimension_merge(spark, src2, builder, path, "2026-01-02")

        target = spark.read.format("delta").load(path)
        flags = {r["customer_id"]: r["deleted"] for r in target.collect()}
        assert flags == {"C1": False, "C2": True}

    @pytest.mark.parametrize("algorithm", ["sha256", "xxhash64"])
    def test_system_member_rows_survive(
        self, spark: SparkSession, tmp_delta_path, algorithm: str
    ) -> None:
        # sha256 → string SK (the default); xxhash64 → integer SK. The
        # exclusion must protect sentinels and still delete plain rows on both.
        path = tmp_delta_path(f"dim_nms_sysmember_{algorithm}")
        dim = _make_dim(
            surrogate_key={  # type: ignore[arg-type]
                "name": "_sk",
                "columns": ["customer_id"],
                "algorithm": algorithm,
            },
            seed_system_members=True,
            system_members=[  # type: ignore[arg-type]
                {"sk": -1, "code": "UNKNOWN", "label": "Unknown"},
            ],
        )
        builder = DimensionMergeBuilder(
            dim, write_overrides=_write_overrides(on_no_match_source="delete")
        )

        rows = [{"customer_id": "C1", "name": "Alice"}]
        src1 = _prepare_source(spark, rows, dim, "2026-01-01")
        execute_dimension_merge(spark, src1, builder, path, "2026-01-01")

        # Seed a sentinel row the way execute_seeds would: same schema, SK = -1
        sk_type = dict(src1.dtypes)["_sk"]
        sentinel = _prepare_source(
            spark, [{"customer_id": "__unknown__", "name": "Unknown"}], dim, "2026-01-01"
        ).withColumn("_sk", F.lit(-1).cast(sk_type))
        sentinel.write.format("delta").mode("append").save(path)

        # Neither the sentinel nor C1 is in the new source; only C1 may be deleted
        src2 = _prepare_source(spark, [{"customer_id": "C9", "name": "Nadia"}], dim, "2026-01-02")
        execute_dimension_merge(spark, src2, builder, path, "2026-01-02")

        target = spark.read.format("delta").load(path)
        remaining = {r["customer_id"] for r in target.collect()}
        assert "__unknown__" in remaining
        assert "C1" not in remaining
        assert "C9" in remaining

    def test_ignore_default_remains_noop(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_nms_ignore")
        dim = _make_dim()
        builder = DimensionMergeBuilder(dim)

        rows = [
            {"customer_id": "C1", "name": "Alice"},
            {"customer_id": "C2", "name": "Bob"},
        ]
        src1 = _prepare_source(spark, rows, dim, "2026-01-01")
        execute_dimension_merge(spark, src1, builder, path, "2026-01-01")

        src2 = _prepare_source(spark, rows[:1], dim, "2026-01-02")
        execute_dimension_merge(spark, src2, builder, path, "2026-01-02")

        target = spark.read.format("delta").load(path)
        remaining = {r["customer_id"] for r in target.collect()}
        assert remaining == {"C1", "C2"}


@pytest.mark.spark
class TestNoMatchSourceVersioned:
    """delete/soft_delete on the SCD2 path act on current rows only."""

    @staticmethod
    def _dim(**extra):  # type: ignore[no-untyped-def]
        return _make_dim(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "names": {"columns": ["name"], "on_change": "version"},
            },
            **extra,
        )

    def _seed_history(self, spark, path, dim, builder):  # type: ignore[no-untyped-def]
        """Two writes: C1 gains a closed v1 + current v2; C2 stays single-version."""
        rows1 = [
            {"customer_id": "C1", "name": "Alice", "deleted": False},
            {"customer_id": "C2", "name": "Bob", "deleted": False},
        ]
        src1 = _prepare_source(spark, rows1, dim, "2026-01-01")
        execute_dimension_merge(spark, src1, builder, path, "2026-01-01")

        rows2 = [
            {"customer_id": "C1", "name": "Alicia", "deleted": False},
            {"customer_id": "C2", "name": "Bob", "deleted": False},
        ]
        src2 = _prepare_source(spark, rows2, dim, "2026-02-01")
        execute_dimension_merge(spark, src2, builder, path, "2026-02-01")

    def test_delete_spares_closed_history_rows(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_nms_v_delete")
        dim = self._dim()
        builder = DimensionMergeBuilder(
            dim, write_overrides=_write_overrides(on_no_match_source="delete")
        )
        self._seed_history(spark, path, dim, builder)

        # C2 disappears; C1 continues unchanged
        src3 = _prepare_source(
            spark, [{"customer_id": "C1", "name": "Alicia", "deleted": False}], dim, "2026-03-01"
        )
        execute_dimension_merge(spark, src3, builder, path, "2026-03-01")

        target = spark.read.format("delta").load(path).collect()
        by_key = {}
        for r in target:
            by_key.setdefault(r["customer_id"], []).append(r)

        # C2's current row is gone entirely
        assert "C2" not in by_key
        # C1's closed v1 AND current v2 both survive
        c1_current = [r for r in by_key["C1"] if r["_is_current"]]
        c1_closed = [r for r in by_key["C1"] if not r["_is_current"]]
        assert len(c1_current) == 1
        assert len(c1_closed) == 1
        assert c1_closed[0]["name"] == "Alice"

    def test_soft_delete_stamps_current_only_and_is_idempotent(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("dim_nms_v_soft")
        dim = self._dim()
        builder = DimensionMergeBuilder(
            dim,
            write_overrides=_write_overrides(
                on_no_match_source="soft_delete", soft_delete_column="deleted"
            ),
        )
        self._seed_history(spark, path, dim, builder)

        def _merge_without_c2(run_ts: str) -> None:
            src = _prepare_source(
                spark,
                [{"customer_id": "C1", "name": "Alicia", "deleted": False}],
                dim,
                run_ts,
            )
            execute_dimension_merge(spark, src, builder, path, run_ts)

        _merge_without_c2("2026-03-01")
        _merge_without_c2("2026-04-01")  # rerun: closed rows must not be re-stamped

        target = spark.read.format("delta").load(path).collect()
        by_row = {(r["customer_id"], bool(r["_is_current"])): r for r in target}

        # C2's current row is stamped
        assert by_row[("C2", True)]["deleted"] is True
        # C1's closed row keeps its original flag across both merges
        assert by_row[("C1", False)]["deleted"] is False
        # C1's current row untouched
        assert by_row[("C1", True)]["deleted"] is False

    def test_system_member_current_row_survives_delete(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("dim_nms_v_sysmember")
        dim = self._dim(
            seed_system_members=True,
            system_members=[  # type: ignore[arg-type]
                {"sk": -1, "code": "UNKNOWN", "label": "Unknown"},
            ],
        )
        builder = DimensionMergeBuilder(
            dim, write_overrides=_write_overrides(on_no_match_source="delete")
        )
        self._seed_history(spark, path, dim, builder)

        src1 = _prepare_source(
            spark,
            [{"customer_id": "C1", "name": "Alicia", "deleted": False}],
            dim,
            "2026-02-15",
        )
        sk_type = dict(src1.dtypes)["_sk"]
        sentinel = _prepare_source(
            spark,
            [{"customer_id": "__unknown__", "name": "Unknown", "deleted": False}],
            dim,
            "2026-01-01",
        ).withColumn("_sk", F.lit(-1).cast(sk_type))
        sentinel.write.format("delta").mode("append").save(path)

        src3 = _prepare_source(
            spark, [{"customer_id": "C1", "name": "Alicia", "deleted": False}], dim, "2026-03-01"
        )
        execute_dimension_merge(spark, src3, builder, path, "2026-03-01")

        remaining = {r["customer_id"] for r in spark.read.format("delta").load(path).collect()}
        # Sentinel (current, sk=-1) survives; C2 (current, no source match) is gone
        assert "__unknown__" in remaining
        assert "C2" not in remaining
        assert "C1" in remaining


@pytest.mark.spark
class TestNoMatchSourceSystemMemberSoftDelete:
    """soft_delete never stamps system-member rows on either merge path."""

    def test_standard_path_flag_untouched(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("dim_nms_soft_sysmember")
        dim = _make_dim(
            seed_system_members=True,
            system_members=[  # type: ignore[arg-type]
                {"sk": -1, "code": "UNKNOWN", "label": "Unknown"},
            ],
        )
        builder = DimensionMergeBuilder(
            dim,
            write_overrides=_write_overrides(
                on_no_match_source="soft_delete", soft_delete_column="deleted"
            ),
        )

        rows = [{"customer_id": "C1", "name": "Alice", "deleted": False}]
        src1 = _prepare_source(spark, rows, dim, "2026-01-01")
        execute_dimension_merge(spark, src1, builder, path, "2026-01-01")

        sk_type = dict(src1.dtypes)["_sk"]
        sentinel = _prepare_source(
            spark,
            [{"customer_id": "__unknown__", "name": "Unknown", "deleted": False}],
            dim,
            "2026-01-01",
        ).withColumn("_sk", F.lit(-1).cast(sk_type))
        sentinel.write.format("delta").mode("append").save(path)

        # Neither C1 nor the system member is in the new source
        src2 = _prepare_source(
            spark, [{"customer_id": "C9", "name": "Nadia", "deleted": False}], dim, "2026-01-02"
        )
        execute_dimension_merge(spark, src2, builder, path, "2026-01-02")

        flags = {
            r["customer_id"]: r["deleted"] for r in spark.read.format("delta").load(path).collect()
        }
        assert flags["__unknown__"] is False  # system member never stamped
        assert flags["C1"] is True
        assert flags["C9"] is False

    def test_versioned_path_current_row_flag_untouched(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("dim_nms_v_soft_sysmember")
        dim = _make_dim(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "names": {"columns": ["name"], "on_change": "version"},
            },
            seed_system_members=True,
            system_members=[  # type: ignore[arg-type]
                {"sk": -1, "code": "UNKNOWN", "label": "Unknown"},
            ],
        )
        builder = DimensionMergeBuilder(
            dim,
            write_overrides=_write_overrides(
                on_no_match_source="soft_delete", soft_delete_column="deleted"
            ),
        )

        rows = [
            {"customer_id": "C1", "name": "Alice", "deleted": False},
            {"customer_id": "C2", "name": "Bob", "deleted": False},
        ]
        src1 = _prepare_source(spark, rows, dim, "2026-01-01")
        execute_dimension_merge(spark, src1, builder, path, "2026-01-01")

        sk_type = dict(src1.dtypes)["_sk"]
        sentinel = _prepare_source(
            spark,
            [{"customer_id": "__unknown__", "name": "Unknown", "deleted": False}],
            dim,
            "2026-01-01",
        ).withColumn("_sk", F.lit(-1).cast(sk_type))
        sentinel.write.format("delta").mode("append").save(path)

        # C2 and the system member both vanish from the source; only C2
        # (a plain entity's current row) may be stamped
        src2 = _prepare_source(
            spark, [{"customer_id": "C1", "name": "Alice", "deleted": False}], dim, "2026-02-01"
        )
        execute_dimension_merge(spark, src2, builder, path, "2026-02-01")

        current = {
            r["customer_id"]: r["deleted"]
            for r in spark.read.format("delta").load(path).collect()
            if r["_is_current"]
        }
        assert current["__unknown__"] is False  # system member's current row never stamped
        assert current["C2"] is True
        assert current["C1"] is False


@pytest.mark.spark
class TestMergeResultMetrics:
    """DimensionMergeResult populated from split commit-metric keys."""

    @staticmethod
    def _merge(spark, path, dim, rows, ts, overrides=None):  # type: ignore[no-untyped-def]
        from weevr.operations.target_handle import TargetHandle

        source = _prepare_source(spark, rows, dim, ts)
        builder = DimensionMergeBuilder(dim, write_overrides=overrides)
        handle = TargetHandle(spark, path)
        return execute_dimension_merge(spark, source, builder, path, ts, handle=handle)

    def test_versioned_split_consistent_with_table_state(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        path = tmp_delta_path("mm_versioned")
        dim = _make_dim(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "names": {"columns": ["name"], "on_change": "version"},
            },
        )
        r1 = self._merge(
            spark,
            path,
            dim,
            [
                {"customer_id": "C1", "name": "Alice"},
                {"customer_id": "C2", "name": "Bob"},
            ],
            "2026-01-01",
        )
        assert r1.rows_inserted == 2  # first write via commit metrics

        # C1 changes (close + new version), C3 arrives (new entity)
        r2 = self._merge(
            spark,
            path,
            dim,
            [
                {"customer_id": "C1", "name": "Alicia"},
                {"customer_id": "C2", "name": "Bob"},
                {"customer_id": "C3", "name": "Cara"},
            ],
            "2026-02-01",
        )
        assert r2.rows_versioned == 1  # C1 closed
        assert r2.rows_inserted == 1  # C3 only — new-version insert excluded
        assert r2.rows_deleted == 0
        # Consistency with observed table state
        target = spark.read.format("delta").load(path).collect()
        assert len(target) == 4  # C1 v1+v2, C2, C3

    def test_versioned_soft_delete_lands_in_rows_deleted(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        # track_history + on_no_match_source: soft_delete — the split keys
        # keep the soft-delete clause out of rows_versioned
        path = tmp_delta_path("mm_soft")
        dim = _make_dim(
            track_history=True,
            change_detection={  # type: ignore[arg-type]
                "names": {"columns": ["name"], "on_change": "version"},
            },
        )
        overrides = _write_overrides(on_no_match_source="soft_delete", soft_delete_column="deleted")
        rows = [
            {"customer_id": "C1", "name": "Alice", "deleted": False},
            {"customer_id": "C2", "name": "Bob", "deleted": False},
        ]
        self._merge(spark, path, dim, rows, "2026-01-01", overrides)

        # C2 vanishes; C1 unchanged → soft-delete stamp is the only update
        r2 = self._merge(spark, path, dim, rows[:1], "2026-02-01", overrides)
        assert r2.rows_versioned == 0  # stamp must NOT count as a close
        assert r2.rows_deleted == 1  # C2's current row stamped
        assert r2.rows_inserted == 0

    def test_standard_overwrite_split(self, spark: SparkSession, tmp_delta_path) -> None:
        path = tmp_delta_path("mm_standard")
        dim = _make_dim()
        self._merge(
            spark,
            path,
            dim,
            [
                {"customer_id": "C1", "name": "Alice"},
                {"customer_id": "C2", "name": "Bob"},
            ],
            "2026-01-01",
        )
        r2 = self._merge(
            spark,
            path,
            dim,
            [
                {"customer_id": "C1", "name": "Alicia"},
                {"customer_id": "C3", "name": "Cara"},
            ],
            "2026-02-01",
        )
        assert r2.rows_overwritten == 1  # C1 updated in place
        assert r2.rows_inserted == 1  # C3
        assert r2.rows_deleted == 0
