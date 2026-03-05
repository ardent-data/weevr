"""Spark integration tests for CDC merge operations."""

import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from spark_helpers import create_delta_table
from weevr.model.load import CdcConfig
from weevr.model.write import WriteConfig
from weevr.operations.readers import read_cdc_source
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

    def test_first_cdc_write_creates_table(self, spark: SparkSession, tmp_delta_path) -> None:
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
                {"id": 1, "name": "Alice", "is_deleted": False},
                {"id": 2, "name": "Bob", "is_deleted": False},
            ],
        )

        cdc_df = spark.createDataFrame([{"id": 2, "name": "Bob", "op": "D", "is_deleted": False}])

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
        )

        execute_cdc_merge(spark, cdc_df, tgt, write_config, cdc_config)

        result = spark.read.format("delta").load(tgt)
        rows = {r["id"]: r for r in result.collect()}
        assert result.count() == 2  # Row not physically removed
        assert rows[1]["is_deleted"] is False  # unchanged
        assert rows[2]["is_deleted"] is True  # soft deleted

    def test_hard_delete_removes_row(self, spark: SparkSession, tmp_delta_path) -> None:
        """on_delete=hard_delete physically removes the matching row."""
        tgt = tmp_delta_path("cdc_hard_tgt")
        create_delta_table(
            spark,
            tgt,
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        )

        cdc_df = spark.createDataFrame([{"id": 2, "name": "Bob", "op": "D"}])

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


class TestCdcDeltaCdf:
    """CDC with Delta Change Data Feed preset."""

    @staticmethod
    def _enable_cdf(spark: SparkSession, path: str) -> None:
        """Enable Change Data Feed on an existing Delta table."""
        spark.sql(
            f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
        )

    def test_cdf_reads_changes(self, spark: SparkSession, tmp_delta_path) -> None:
        """Delta CDF preset reads insert/update/delete changes from CDF."""
        from weevr.model.source import Source

        src = tmp_delta_path("cdf_src")

        # Create source table and enable CDF
        create_delta_table(spark, src, [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        self._enable_cdf(spark, src)

        # Record the version after enabling CDF
        initial_version = (
            DeltaTable.forPath(spark, src).history(1).select("version").collect()[0]["version"]
        )

        # Write changes — these will be captured in CDF
        dt = DeltaTable.forPath(spark, src)
        # Update id=2
        dt.update(condition="id = 2", set={"name": "'Bobby'"})
        # Delete id=1
        dt.delete(condition="id = 1")
        # Insert id=3
        spark.createDataFrame([{"id": 3, "name": "Charlie"}]).write.format("delta").mode(
            "append"
        ).save(src)

        # Read CDF changes starting after our initial version
        source = Source(type="delta", alias=src)
        cdc_config = CdcConfig(preset="delta_cdf")
        cdf_df = read_cdc_source(spark, source, cdc_config, last_version=initial_version)

        # Verify CDF columns present
        assert "_change_type" in cdf_df.columns
        assert "_commit_version" in cdf_df.columns

        # Verify we got change rows
        change_types = {r["_change_type"] for r in cdf_df.collect()}
        assert "update_postimage" in change_types
        assert "delete" in change_types
        assert "insert" in change_types

    def test_cdf_merge_end_to_end(self, spark: SparkSession, tmp_delta_path) -> None:
        """Delta CDF preset end-to-end: read CDF changes and merge to target."""
        from weevr.model.source import Source

        src = tmp_delta_path("cdf_e2e_src")
        tgt = tmp_delta_path("cdf_e2e_tgt")

        # Create source and target
        create_delta_table(spark, src, [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        create_delta_table(spark, tgt, [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        self._enable_cdf(spark, src)

        initial_version = (
            DeltaTable.forPath(spark, src).history(1).select("version").collect()[0]["version"]
        )

        # Make changes in source
        dt = DeltaTable.forPath(spark, src)
        dt.update(condition="id = 2", set={"name": "'Bobby'"})

        # Read CDF
        source = Source(type="delta", alias=src)
        cdc_config = CdcConfig(preset="delta_cdf", on_delete="hard_delete")
        cdf_df = read_cdc_source(spark, source, cdc_config, last_version=initial_version)

        # Merge CDF changes to target
        write_config = WriteConfig(mode="merge", match_keys=["id"])
        counts = execute_cdc_merge(spark, cdf_df, tgt, write_config, cdc_config)

        assert counts["updates"] == 1

        result = spark.read.format("delta").load(tgt)
        rows = {r["id"]: r["name"] for r in result.collect()}
        assert rows[2] == "Bobby"
        assert rows[1] == "Alice"  # unchanged
