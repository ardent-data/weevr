"""End-to-end integration tests for the load_config → execute_thread pipeline."""

import csv
import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_helpers import create_delta_table
from weevr.config import load_config
from weevr.engine import ThreadResult, execute_thread
from weevr.errors.exceptions import ExecutionError
from weevr.model.keys import KeyConfig
from weevr.model.pipeline import (
    DedupParams,
    DedupStep,
    JoinKeyPair,
    JoinParams,
    JoinStep,
    SortParams,
    SortStep,
    UnionParams,
    UnionStep,
)
from weevr.model.source import DedupConfig, Source
from weevr.model.target import ColumnMapping, Target
from weevr.model.thread import Thread
from weevr.model.types import SparkExpr
from weevr.model.write import WriteConfig

pytestmark = pytest.mark.spark

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "threads"


def _thread(
    name: str,
    src: str,
    tgt: str,
    *,
    steps: list | None = None,
    write: WriteConfig | None = None,
    keys: KeyConfig | None = None,
    sources: dict[str, Source] | None = None,
    target: Target | None = None,
) -> Thread:
    """Construct a minimal Thread for integration testing."""
    return Thread(
        name=name,
        config_version="1",
        sources=sources or {"main": Source(type="delta", alias=src)},
        steps=steps or [],
        target=target or Target(path=tgt),
        write=write,
        keys=keys,
    )


class TestYamlLoadToExecute:
    """EC-001, EC-004, EC-010, EC-013, EC-014 — YAML config loaded via load_config()."""

    def test_simple_thread_content_correct(self, spark: SparkSession, tmp_delta_path) -> None:
        """EC-001: Delta source → overwrite → verify target content."""
        src = tmp_delta_path("ec001_src")
        tgt = tmp_delta_path("ec001_tgt")
        create_delta_table(spark, src, [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])

        thread = load_config(
            _FIXTURE_DIR / "thread_simple.yaml",
            runtime_params={"src_path": src, "tgt_path": tgt},
        )
        assert isinstance(thread, Thread)
        result = execute_thread(spark, thread)

        assert result.status == "success"
        written = spark.read.format("delta").load(tgt)
        assert written.count() == 2

    def test_transforms_thread_applies_all_steps(self, spark: SparkSession, tmp_delta_path) -> None:
        """EC-004: All declared transform steps execute in sequence (filter, derive,
        select, drop, rename, cast)."""
        src = tmp_delta_path("ec004_src")
        tgt = tmp_delta_path("ec004_tgt")
        create_delta_table(
            spark,
            src,
            [{"id": 1, "amount": 100}, {"id": 2, "amount": -5}],
        )

        thread = load_config(
            _FIXTURE_DIR / "thread_transforms.yaml",
            runtime_params={"src_path": src, "tgt_path": tgt},
        )
        assert isinstance(thread, Thread)
        result = execute_thread(spark, thread)

        assert result.status == "success"
        written = spark.read.format("delta").load(tgt)
        # filter(amount > 0) removes id=2; derive(doubled=amount*2); select/drop/rename/cast
        assert written.count() == 1
        row = written.collect()[0]
        assert row["id"] == 1
        assert row["total"] == "200"  # doubled=200 renamed to total, cast to string
        assert "amount" not in written.columns
        assert "doubled" not in written.columns

    def test_name_derived_from_file_path(self, spark: SparkSession, tmp_delta_path) -> None:
        """EC-014: Thread.name is derived from the YAML file path."""
        src = tmp_delta_path("ec014_src")
        tgt = tmp_delta_path("ec014_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        thread = load_config(
            _FIXTURE_DIR / "thread_simple.yaml",
            runtime_params={"src_path": src, "tgt_path": tgt},
        )
        assert isinstance(thread, Thread)
        # Fixture lives under threads/ → name = stem of file
        assert thread.name == "thread_simple"

    def test_result_fields_populated_from_yaml_execution(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """EC-013: ThreadResult carries correct status, name, rows, mode, and path."""
        src = tmp_delta_path("ec013_src")
        tgt = tmp_delta_path("ec013_tgt")
        create_delta_table(spark, src, [{"id": i} for i in range(3)])

        thread = load_config(
            _FIXTURE_DIR / "thread_simple.yaml",
            runtime_params={"src_path": src, "tgt_path": tgt},
        )
        assert isinstance(thread, Thread)
        result = execute_thread(spark, thread)

        assert isinstance(result, ThreadResult)
        assert result.status == "success"
        assert result.thread_name == "thread_simple"
        assert result.rows_written == 3
        assert result.write_mode == "overwrite"
        assert result.target_path == tgt

    def test_merge_thread_via_yaml(self, spark: SparkSession, tmp_delta_path) -> None:
        """EC-010: Merge write mode — update matched, insert new, ignore unmatched source."""
        src = tmp_delta_path("ec010_src")
        tgt = tmp_delta_path("ec010_tgt")
        # Pre-populate target
        create_delta_table(spark, tgt, [{"id": 1, "val": "old"}, {"id": 2, "val": "keep"}])
        # Source: updated id=1, new id=3
        create_delta_table(spark, src, [{"id": 1, "val": "new"}, {"id": 3, "val": "insert"}])

        thread = load_config(
            _FIXTURE_DIR / "thread_merge.yaml",
            runtime_params={"src_path": src, "tgt_path": tgt},
        )
        assert isinstance(thread, Thread)
        result = execute_thread(spark, thread)

        assert result.status == "success"
        written = spark.read.format("delta").load(tgt)
        rows = {r["id"]: r["val"] for r in written.collect()}
        assert rows[1] == "new"  # updated on match
        assert rows[2] == "keep"  # unchanged — not in source (on_no_match_source=ignore)
        assert rows[3] == "insert"  # inserted on no match in target


class TestFileSourceReaders:
    """EC-002: File-format sources (CSV, JSON, Parquet) are read correctly."""

    def test_csv_source_read(self, spark: SparkSession, tmp_path) -> None:
        csv_path = str(tmp_path / "data.csv")
        tgt = str(tmp_path / "tgt_csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name"])
            writer.writeheader()
            writer.writerows([{"id": "1", "name": "alice"}, {"id": "2", "name": "bob"}])

        thread = Thread(
            name="csv_thread",
            config_version="1",
            sources={"main": Source(type="csv", path=csv_path, options={"header": "true"})},
            steps=[],
            target=Target(path=tgt),
        )
        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.rows_written == 2

    def test_json_source_read(self, spark: SparkSession, tmp_path) -> None:
        json_path = str(tmp_path / "data.json")
        tgt = str(tmp_path / "tgt_json")
        rows = [{"id": 10, "val": "x"}, {"id": 20, "val": "y"}]
        with open(json_path, "w") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

        thread = Thread(
            name="json_thread",
            config_version="1",
            sources={"main": Source(type="json", path=json_path)},
            steps=[],
            target=Target(path=tgt),
        )
        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.rows_written == 2

    def test_parquet_source_read(self, spark: SparkSession, tmp_path) -> None:
        parquet_path = str(tmp_path / "data.parquet")
        tgt = str(tmp_path / "tgt_parquet")
        spark.createDataFrame([{"id": 5}, {"id": 6}]).write.parquet(parquet_path)

        thread = Thread(
            name="parquet_thread",
            config_version="1",
            sources={"main": Source(type="parquet", path=parquet_path)},
            steps=[],
            target=Target(path=tgt),
        )
        result = execute_thread(spark, thread)

        assert result.status == "success"
        assert result.rows_written == 2


class TestSourceDedup:
    """EC-003: Source-level deduplication removes duplicates before pipeline steps."""

    def test_source_dedup_removes_duplicates(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("ec003_src")
        tgt = tmp_delta_path("ec003_tgt")
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("ts", IntegerType()),
                StructField("val", StringType()),
            ]
        )
        create_delta_table(
            spark,
            src,
            [
                {"id": 1, "ts": 1, "val": "old"},
                {"id": 1, "ts": 2, "val": "new"},  # duplicate id, keep latest ts
                {"id": 2, "ts": 1, "val": "only"},
            ],
            schema=schema,
        )

        thread = Thread(
            name="dedup_thread",
            config_version="1",
            sources={
                "main": Source(
                    type="delta",
                    alias=src,
                    dedup=DedupConfig(keys=["id"], order_by="ts DESC"),
                )
            },
            steps=[],
            target=Target(path=tgt),
        )
        result = execute_thread(spark, thread)

        assert result.rows_written == 2
        written = spark.read.format("delta").load(tgt)
        id1_row = [r for r in written.collect() if r["id"] == 1][0]
        assert id1_row["val"] == "new"  # latest ts survives


class TestPipelineStepTypes:
    """EC-004 (remainder): dedup, sort, join, union pipeline steps via execute_thread."""

    def test_pipeline_dedup_step(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("dedup_src")
        tgt = tmp_delta_path("dedup_tgt")
        create_delta_table(spark, src, [{"id": 1, "v": 10}, {"id": 1, "v": 20}, {"id": 2, "v": 5}])

        # keep="last" sorts v descending; row_number=1 → highest v wins
        steps: list = [DedupStep(dedup=DedupParams(keys=["id"], order_by="v", keep="last"))]
        thread = _thread("dedup_pipe", src, tgt, steps=steps)
        result = execute_thread(spark, thread)

        assert result.rows_written == 2
        written = spark.read.format("delta").load(tgt)
        id1_vals = [r["v"] for r in written.collect() if r["id"] == 1]
        assert id1_vals == [20]

    def test_pipeline_sort_step(self, spark: SparkSession, tmp_delta_path) -> None:
        src = tmp_delta_path("sort_src")
        tgt = tmp_delta_path("sort_tgt")
        create_delta_table(spark, src, [{"id": 3}, {"id": 1}, {"id": 2}])

        steps: list = [SortStep(sort=SortParams(columns=["id"], ascending=True))]
        thread = _thread("sort_pipe", src, tgt, steps=steps)
        result = execute_thread(spark, thread)

        assert result.rows_written == 3
        written = spark.read.format("delta").load(tgt)
        ids = [r["id"] for r in written.collect()]
        assert ids == sorted(ids)

    def test_pipeline_union_step(self, spark: SparkSession, tmp_delta_path) -> None:
        src_a = tmp_delta_path("union_a")
        src_b = tmp_delta_path("union_b")
        tgt = tmp_delta_path("union_tgt")
        create_delta_table(spark, src_a, [{"id": 1}])
        create_delta_table(spark, src_b, [{"id": 2}])

        steps: list = [UnionStep(union=UnionParams(sources=["right"], mode="by_name"))]
        thread = Thread(
            name="union_pipe",
            config_version="1",
            sources={
                "main": Source(type="delta", alias=src_a),
                "right": Source(type="delta", alias=src_b),
            },
            steps=steps,
            target=Target(path=tgt),
        )
        result = execute_thread(spark, thread)

        assert result.rows_written == 2


class TestJoins:
    """EC-005: Join steps with null-safe and standard join modes.

    Uses semi joins so that only left-side columns appear in the result,
    avoiding the duplicate-column issue that arises from inner joins when
    both sides share a key column name.
    """

    def test_null_safe_semi_join_matches_null_keys(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """null_safe=True: NULL on left matches NULL on right (semi returns left cols only)."""
        src_left = tmp_delta_path("ns_left")
        src_right = tmp_delta_path("ns_right")
        tgt = tmp_delta_path("ns_tgt")

        schema = StructType(
            [
                StructField("k", StringType(), nullable=True),
                StructField("v", StringType()),
            ]
        )
        create_delta_table(
            spark,
            src_left,
            [{"k": None, "v": "left_null"}, {"k": "a", "v": "left_a"}],
            schema=schema,
        )
        create_delta_table(
            spark,
            src_right,
            [{"k": None, "v": "right_null"}, {"k": "b", "v": "right_b"}],
            schema=schema,
        )

        steps: list = [
            JoinStep(
                join=JoinParams(
                    source="right",
                    type="semi",
                    on=[JoinKeyPair(left="k", right="k")],
                    null_safe=True,
                )
            )
        ]
        thread = Thread(
            name="ns_join",
            config_version="1",
            sources={
                "main": Source(type="delta", alias=src_left),
                "right": Source(type="delta", alias=src_right),
            },
            steps=steps,
            target=Target(path=tgt),
        )
        result = execute_thread(spark, thread)

        # null_safe=True: left.k=NULL matches right.k=NULL → 1 row returned
        assert result.rows_written == 1

    def test_standard_semi_join_does_not_match_null_keys(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """null_safe=False: NULL on left does not match NULL on right."""
        src_left = tmp_delta_path("std_left")
        src_right = tmp_delta_path("std_right")
        tgt = tmp_delta_path("std_tgt")

        schema = StructType(
            [
                StructField("k", StringType(), nullable=True),
                StructField("v", StringType()),
            ]
        )
        create_delta_table(
            spark,
            src_left,
            [{"k": None, "v": "left_null"}, {"k": "a", "v": "left_a"}],
            schema=schema,
        )
        create_delta_table(
            spark,
            src_right,
            [{"k": None, "v": "right_null"}, {"k": "a", "v": "right_a"}],
            schema=schema,
        )

        steps: list = [
            JoinStep(
                join=JoinParams(
                    source="right",
                    type="semi",
                    on=[JoinKeyPair(left="k", right="k")],
                    null_safe=False,
                )
            )
        ]
        thread = Thread(
            name="std_join",
            config_version="1",
            sources={
                "main": Source(type="delta", alias=src_left),
                "right": Source(type="delta", alias=src_right),
            },
            steps=steps,
            target=Target(path=tgt),
        )
        result = execute_thread(spark, thread)

        # null_safe=False: NULL != NULL → only k="a" matches → 1 row
        assert result.rows_written == 1


class TestKeyGeneration:
    """EC-006, EC-007: Surrogate key and change detection hash via YAML config."""

    def test_surrogate_key_generated_via_yaml(self, spark: SparkSession, tmp_delta_path) -> None:
        """EC-006: Surrogate key column present with null-safe business key hashing."""
        src = tmp_delta_path("ec006_src")
        tgt = tmp_delta_path("ec006_tgt")
        create_delta_table(spark, src, [{"id": 1, "name": "alice"}, {"id": 2, "name": None}])

        thread = load_config(
            _FIXTURE_DIR / "thread_keys.yaml",
            runtime_params={"src_path": src, "tgt_path": tgt},
        )
        assert isinstance(thread, Thread)
        execute_thread(spark, thread)

        written = spark.read.format("delta").load(tgt)
        assert "sk" in written.columns
        # Both rows get a surrogate key (null is handled via sentinel)
        assert written.filter("sk IS NOT NULL").count() == 2

    def test_change_detection_hash_generated_via_yaml(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """EC-007: Change detection hash column is present and differs when data differs."""
        src = tmp_delta_path("ec007_src")
        tgt = tmp_delta_path("ec007_tgt")
        create_delta_table(spark, src, [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}])

        thread = load_config(
            _FIXTURE_DIR / "thread_keys.yaml",
            runtime_params={"src_path": src, "tgt_path": tgt},
        )
        assert isinstance(thread, Thread)
        execute_thread(spark, thread)

        written = spark.read.format("delta").load(tgt)
        assert "row_hash" in written.columns
        hashes = [r["row_hash"] for r in written.collect()]
        # Distinct data → distinct hashes
        assert len(set(hashes)) == 2


class TestTargetMapping:
    """EC-008, EC-009: Auto and explicit target mapping modes."""

    def test_auto_mapping_new_target_passes_all_columns(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """EC-008a: Auto mapping with no existing target — all columns pass through."""
        src = tmp_delta_path("auto_new_src")
        tgt = tmp_delta_path("auto_new_tgt")
        create_delta_table(spark, src, [{"id": 1, "extra": "keep"}])

        thread = _thread("auto_new", src, tgt, target=Target(path=tgt, mapping_mode="auto"))
        execute_thread(spark, thread)

        written = spark.read.format("delta").load(tgt)
        assert "id" in written.columns
        assert "extra" in written.columns

    def test_auto_mapping_existing_target_narrows_columns(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """EC-008b: Auto mapping with existing target — selects only matching columns."""
        src = tmp_delta_path("auto_exist_src")
        tgt = tmp_delta_path("auto_exist_tgt")
        # Existing target only has 'id'
        create_delta_table(spark, tgt, [{"id": 0}])
        # Source has 'id' + extra 'new_col'
        create_delta_table(spark, src, [{"id": 1, "new_col": "extra"}])

        thread = _thread("auto_exist", src, tgt, target=Target(path=tgt, mapping_mode="auto"))
        execute_thread(spark, thread)

        written = spark.read.format("delta").load(tgt)
        assert "id" in written.columns
        assert "new_col" not in written.columns

    def test_explicit_mapping_keeps_only_declared_columns(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """EC-009: Explicit mapping — only declared columns written."""
        src = tmp_delta_path("explicit_src")
        tgt = tmp_delta_path("explicit_tgt")
        create_delta_table(spark, src, [{"id": 1, "keep": "yes", "discard": "no"}])

        thread = _thread(
            "explicit_map",
            src,
            tgt,
            target=Target(
                path=tgt,
                mapping_mode="explicit",
                columns={"id": ColumnMapping(), "keep": ColumnMapping()},
            ),
        )
        execute_thread(spark, thread)

        written = spark.read.format("delta").load(tgt)
        assert "id" in written.columns
        assert "keep" in written.columns
        assert "discard" not in written.columns


class TestWriteModes:
    """EC-010, EC-011: Write mode integration (merge covered in YAML tests, append here)."""

    def test_append_accumulates_rows(self, spark: SparkSession, tmp_delta_path) -> None:
        """EC-011: Append mode adds rows to existing table."""
        src = tmp_delta_path("append_src")
        tgt = tmp_delta_path("append_tgt")
        create_delta_table(spark, tgt, [{"id": 0}])
        create_delta_table(spark, src, [{"id": 1}, {"id": 2}])

        thread = _thread("append_wrt", src, tgt, write=WriteConfig(mode="append"))
        execute_thread(spark, thread)

        written = spark.read.format("delta").load(tgt)
        assert written.count() == 3  # 1 existing + 2 appended

    def test_overwrite_replaces_existing_rows(self, spark: SparkSession, tmp_delta_path) -> None:
        """Overwrite mode replaces all existing content."""
        src = tmp_delta_path("ow_src")
        tgt = tmp_delta_path("ow_tgt")
        create_delta_table(spark, tgt, [{"id": 99}, {"id": 100}])
        create_delta_table(spark, src, [{"id": 1}])

        thread = _thread("ow_wrt", src, tgt, write=WriteConfig(mode="overwrite"))
        execute_thread(spark, thread)

        written = spark.read.format("delta").load(tgt)
        assert written.count() == 1
        assert written.collect()[0]["id"] == 1


class TestErrorWrapping:
    """EC-012: Errors are wrapped as ExecutionError with thread context."""

    def test_invalid_filter_expression_raises_execution_error(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Invalid Spark expression in a filter step → ExecutionError with thread_name."""
        from weevr.model.pipeline import FilterParams, FilterStep

        src = tmp_delta_path("err_src")
        tgt = tmp_delta_path("err_tgt")
        create_delta_table(spark, src, [{"id": 1}])

        steps: list = [
            FilterStep(filter=FilterParams(expr=SparkExpr("NOT_A_VALID_COLUMN_XYZ > 0")))
        ]
        thread = _thread("err_filter", src, tgt, steps=steps)

        with pytest.raises(ExecutionError) as exc_info:
            execute_thread(spark, thread)

        assert exc_info.value.thread_name == "err_filter"

    def test_missing_source_raises_execution_error_with_thread_name(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """Non-existent source path → ExecutionError carries thread_name."""
        tgt = tmp_delta_path("err_tgt2")
        thread = _thread("missing_src_thread", "/nonexistent/path/abc", tgt)

        with pytest.raises(ExecutionError) as exc_info:
            execute_thread(spark, thread)

        assert exc_info.value.thread_name == "missing_src_thread"
