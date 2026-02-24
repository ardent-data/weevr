"""Spark integration tests for CDC merge operations."""

import pytest
from pyspark.sql import SparkSession

from spark_helpers import create_delta_table
from weevr.model.load import CdcConfig
from weevr.model.write import WriteConfig
from weevr.operations.writers import execute_cdc_merge

pytestmark = pytest.mark.spark


class TestCdcGenericMerge:
    """CDC merge with generic (explicit) operation column."""

    def test_insert_update_delete(self, spark: SparkSession, tmp_delta_path) -> None:
        """Generic CDC with all three operation types applied correctly."""
        tgt = tmp_delta_path("cdc_iud_tgt")
        create_delta_table(
            spark,
            tgt,
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"},
            ],
        )

        cdc_df = spark.createDataFrame(
            [
                {"id": 4, "name": "Dave", "op": "I"},
                {"id": 2, "name": "Bobby", "op": "U"},
                {"id": 3, "name": "Charlie", "op": "D"},
            ]
        )

        cdc_config = CdcConfig(
            operation_column="op",
            insert_value="I",
            update_value="U",
            delete_value="D",
        )
        write_config = WriteConfig(mode="merge", match_keys=["id"])

        counts = execute_cdc_merge(spark, cdc_df, tgt, write_config, cdc_config)

        assert counts["inserts"] == 1
        assert counts["updates"] == 1
        assert counts["deletes"] == 1

        result = spark.read.format("delta").load(tgt)
        rows = {r["id"]: r["name"] for r in result.collect()}
        assert rows[1] == "Alice"  # unchanged
        assert rows[4] == "Dave"  # inserted
        assert rows[2] == "Bobby"  # updated
        assert 3 not in rows  # hard deleted

    def test_first_cdc_write_creates_table(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        """First CDC write to non-existent target creates the table (excluding deletes)."""
        tgt = tmp_delta_path("cdc_first_tgt")

        cdc_df = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice", "op": "I"},
                {"id": 2, "name": "Bob", "op": "D"},
            ]
        )

        cdc_config = CdcConfig(
            operation_column="op",
            insert_value="I",
            update_value="U",
            delete_value="D",
        )
        write_config = WriteConfig(mode="merge", match_keys=["id"])

        counts = execute_cdc_merge(spark, cdc_df, tgt, write_config, cdc_config)

        assert counts["inserts"] == 1
        assert counts["deletes"] == 1

        result = spark.read.format("delta").load(tgt)
        assert result.count() == 1
        assert result.collect()[0]["name"] == "Alice"


class TestCdcSoftDelete:
    """CDC with soft delete — delete rows get a flag column set."""

    def test_soft_delete_sets_flag(self, spark: SparkSession, tmp_delta_path) -> None:
        """on_delete=soft_delete sets the flag column instead of removing the row."""
        tgt = tmp_delta_path("cdc_soft_tgt")
        create_delta_table(
            spark,
            tgt,
            [
                {"id": 1, "name": "Alice", "is_deleted": "false"},
                {"id": 2, "name": "Bob", "is_deleted": "false"},
            ],
        )

        cdc_df = spark.createDataFrame(
            [{"id": 2, "name": "Bob", "op": "D", "is_deleted": "false"}]
        )

        cdc_config = CdcConfig(
            operation_column="op",
            insert_value="I",
            update_value="U",
            delete_value="D",
            on_delete="soft_delete",
        )
        write_config = WriteConfig(
            mode="merge",
            match_keys=["id"],
            soft_delete_column="is_deleted",
            soft_delete_value="true",
        )

        execute_cdc_merge(spark, cdc_df, tgt, write_config, cdc_config)

        result = spark.read.format("delta").load(tgt)
        rows = {r["id"]: r for r in result.collect()}
        assert result.count() == 2  # Row not physically removed
        assert rows[1]["is_deleted"] == "false"  # unchanged
        assert rows[2]["is_deleted"] == "true"  # soft deleted

    def test_hard_delete_removes_row(self, spark: SparkSession, tmp_delta_path) -> None:
        """on_delete=hard_delete physically removes the matching row."""
        tgt = tmp_delta_path("cdc_hard_tgt")
        create_delta_table(
            spark,
            tgt,
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        )

        cdc_df = spark.createDataFrame(
            [{"id": 2, "name": "Bob", "op": "D"}]
        )

        cdc_config = CdcConfig(
            operation_column="op",
            insert_value="I",
            update_value="U",
            delete_value="D",
            on_delete="hard_delete",
        )
        write_config = WriteConfig(mode="merge", match_keys=["id"])

        execute_cdc_merge(spark, cdc_df, tgt, write_config, cdc_config)

        result = spark.read.format("delta").load(tgt)
        assert result.count() == 1
        assert result.collect()[0]["id"] == 1


class TestCdcInsertOnly:
    """CDC with insert-only operations (no update/delete values)."""

    def test_insert_only_cdc(self, spark: SparkSession, tmp_delta_path) -> None:
        """CDC with only insert_value inserts new rows, ignores existing."""
        tgt = tmp_delta_path("cdc_ins_tgt")
        create_delta_table(spark, tgt, [{"id": 1, "val": "a"}])

        cdc_df = spark.createDataFrame(
            [
                {"id": 1, "val": "a_updated", "op": "I"},
                {"id": 2, "val": "b", "op": "I"},
            ]
        )

        cdc_config = CdcConfig(
            operation_column="op",
            insert_value="I",
            update_value="U",
        )
        write_config = WriteConfig(mode="merge", match_keys=["id"])

        counts = execute_cdc_merge(spark, cdc_df, tgt, write_config, cdc_config)

        assert counts["inserts"] == 2
        result = spark.read.format("delta").load(tgt)
        assert result.count() == 2
