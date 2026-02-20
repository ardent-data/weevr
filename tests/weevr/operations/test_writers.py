"""Tests for target mapping (apply_target_mapping)."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from spark_helpers import create_delta_table
from weevr.model.target import ColumnMapping, Target
from weevr.model.types import SparkExpr
from weevr.operations.writers import apply_target_mapping


@pytest.fixture()
def source_df(spark: SparkSession):
    """Source DataFrame with mixed columns for mapping tests."""
    return spark.createDataFrame([
        {"id": 1, "name": "alice", "amount": 100, "status": "active"},
        {"id": 2, "name": "bob", "amount": 50, "status": "inactive"},
    ])


class TestAutoModeNewTarget:
    """auto mode when target does not yet exist — all columns pass through."""

    def test_auto_new_no_columns_config_passes_all(
        self, source_df, spark: SparkSession
    ) -> None:
        target = Target(path="/nonexistent/target")
        result = apply_target_mapping(source_df, target, spark)
        assert set(result.columns) == set(source_df.columns)

    def test_auto_new_with_drop_removes_column(
        self, source_df, spark: SparkSession
    ) -> None:
        target = Target(
            path="/nonexistent/target",
            columns={"status": ColumnMapping(drop=True)},
        )
        result = apply_target_mapping(source_df, target, spark)
        assert "status" not in result.columns
        assert "id" in result.columns

    def test_auto_new_with_expr_adds_column(
        self, source_df, spark: SparkSession
    ) -> None:
        target = Target(
            path="/nonexistent/target",
            columns={"doubled": ColumnMapping(expr=SparkExpr("amount * 2"))},
        )
        result = apply_target_mapping(source_df, target, spark)
        assert "doubled" in result.columns
        values = {r["doubled"] for r in result.collect()}
        assert 200 in values

    def test_auto_new_with_type_cast(self, source_df, spark: SparkSession) -> None:
        target = Target(
            path="/nonexistent/target",
            columns={"amount": ColumnMapping(type="string")},
        )
        result = apply_target_mapping(source_df, target, spark)
        assert result.schema["amount"].dataType == StringType()

    def test_auto_new_with_default_fills_nulls(
        self, spark: SparkSession
    ) -> None:
        schema = StructType([
            StructField("id", LongType()),
            StructField("val", StringType(), nullable=True),
        ])
        df = spark.createDataFrame([(1, None), (2, "existing")], schema=schema)
        target = Target(
            path="/nonexistent/target",
            columns={"val": ColumnMapping(default="fallback")},
        )
        result = apply_target_mapping(df, target, spark)
        values = {r["val"] for r in result.collect()}
        assert "fallback" in values
        assert "existing" in values
        assert None not in values

    def test_auto_new_preserves_row_count(self, source_df, spark: SparkSession) -> None:
        target = Target(path="/nonexistent/target")
        result = apply_target_mapping(source_df, target, spark)
        assert result.count() == source_df.count()


class TestAutoModeExistingTarget:
    """auto mode when target table already exists — select matching columns only."""

    def test_auto_existing_selects_matching_columns(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        # Create existing target with subset of columns
        existing_path = tmp_delta_path("existing_target")
        create_delta_table(spark, existing_path, [{"id": 0, "name": "seed"}])

        source_df = spark.createDataFrame([
            {"id": 1, "name": "alice", "extra_col": "ignored"},
        ])
        target = Target(alias=existing_path)
        result = apply_target_mapping(source_df, target, spark)

        # Only columns from existing target schema should remain
        assert "extra_col" not in result.columns
        assert "id" in result.columns
        assert "name" in result.columns

    def test_auto_existing_column_order_follows_existing_schema(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        existing_path = tmp_delta_path("ordered_target")
        # Use explicit schema to control column ordering in the existing table
        schema = StructType([StructField("b", LongType()), StructField("a", LongType())])
        spark.createDataFrame([(1, 2)], schema=schema).write.format("delta").save(existing_path)

        source_df = spark.createDataFrame([(10, 20, 30)], schema=StructType([
            StructField("a", LongType()),
            StructField("b", LongType()),
            StructField("c", LongType()),
        ]))
        target = Target(alias=existing_path)
        result = apply_target_mapping(source_df, target, spark)

        # Column order should match existing target schema (b, a), not source (a, b)
        assert result.columns == ["b", "a"]

    def test_auto_existing_with_expr_applied_before_selection(
        self, spark: SparkSession, tmp_delta_path
    ) -> None:
        existing_path = tmp_delta_path("expr_target")
        create_delta_table(spark, existing_path, [{"id": 0, "label": "x"}])

        source_df = spark.createDataFrame([{"id": 1, "name": "alice"}])
        target = Target(
            alias=existing_path,
            columns={"label": ColumnMapping(expr=SparkExpr("upper(name)"))},
        )
        result = apply_target_mapping(source_df, target, spark)

        assert "label" in result.columns
        assert result.collect()[0]["label"] == "ALICE"


class TestExplicitMode:
    """explicit mode — keep only declared non-dropped columns."""

    def test_explicit_selects_only_declared_columns(
        self, source_df, spark: SparkSession
    ) -> None:
        target = Target(
            mapping_mode="explicit",
            columns={
                "id": ColumnMapping(),
                "name": ColumnMapping(),
            },
        )
        result = apply_target_mapping(source_df, target, spark)
        assert set(result.columns) == {"id", "name"}
        assert "amount" not in result.columns

    def test_explicit_with_expr_applies_expression(
        self, source_df, spark: SparkSession
    ) -> None:
        target = Target(
            mapping_mode="explicit",
            columns={
                "id": ColumnMapping(),
                "upper_name": ColumnMapping(expr=SparkExpr("upper(name)")),
            },
        )
        result = apply_target_mapping(source_df, target, spark)
        assert set(result.columns) == {"id", "upper_name"}
        names = {r["upper_name"] for r in result.collect()}
        assert "ALICE" in names

    def test_explicit_with_type_cast(self, source_df, spark: SparkSession) -> None:
        target = Target(
            mapping_mode="explicit",
            columns={"amount": ColumnMapping(type="string")},
        )
        result = apply_target_mapping(source_df, target, spark)
        assert result.columns == ["amount"]
        assert result.schema["amount"].dataType == StringType()

    def test_explicit_excludes_dropped_columns(
        self, source_df, spark: SparkSession
    ) -> None:
        target = Target(
            mapping_mode="explicit",
            columns={
                "id": ColumnMapping(),
                "name": ColumnMapping(),
                "amount": ColumnMapping(drop=True),
            },
        )
        result = apply_target_mapping(source_df, target, spark)
        assert "amount" not in result.columns
        assert {"id", "name"} <= set(result.columns)

    def test_explicit_no_columns_returns_as_is(
        self, source_df, spark: SparkSession
    ) -> None:
        target = Target(mapping_mode="explicit")
        result = apply_target_mapping(source_df, target, spark)
        assert set(result.columns) == set(source_df.columns)

    def test_explicit_default_fills_nulls(self, spark: SparkSession) -> None:
        schema = StructType([
            StructField("id", LongType()),
            StructField("score", LongType(), nullable=True),
        ])
        df = spark.createDataFrame([(1, None), (2, 99)], schema=schema)
        target = Target(
            mapping_mode="explicit",
            columns={"score": ColumnMapping(default=0)},
        )
        result = apply_target_mapping(df, target, spark)
        values = {r["score"] for r in result.collect()}
        assert 0 in values
        assert 99 in values
        assert None not in values

    def test_explicit_preserves_row_count(self, source_df, spark: SparkSession) -> None:
        target = Target(
            mapping_mode="explicit",
            columns={"id": ColumnMapping()},
        )
        result = apply_target_mapping(source_df, target, spark)
        assert result.count() == source_df.count()
