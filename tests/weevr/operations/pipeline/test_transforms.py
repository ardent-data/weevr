"""Tests for pipeline transform step handlers."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from weevr.errors.exceptions import ConfigError
from weevr.model.pipeline import (
    CastParams,
    DeriveParams,
    DropParams,
    FilterParams,
    RenameParams,
    SelectParams,
)
from weevr.model.types import SparkExpr
from weevr.operations.pipeline.transforms import (
    apply_cast,
    apply_derive,
    apply_drop,
    apply_filter,
    apply_rename,
    apply_select,
)

pytestmark = pytest.mark.spark


@pytest.fixture()
def sample_df(spark: SparkSession):
    """Small DataFrame with mixed column types for transform tests."""
    data = [
        {"id": 1, "name": "alice", "amount": 100, "active": True},
        {"id": 2, "name": "bob", "amount": 50, "active": False},
        {"id": 3, "name": "carol", "amount": 200, "active": True},
    ]
    return spark.createDataFrame(data)


class TestApplyFilter:
    """Tests for the filter step handler."""

    def test_filter_keeps_matching_rows(self, sample_df) -> None:
        params = FilterParams(expr=SparkExpr("amount > 60"))
        result = apply_filter(sample_df, params)
        assert result.count() == 2

    def test_filter_excludes_all_rows(self, sample_df) -> None:
        params = FilterParams(expr=SparkExpr("amount > 9999"))
        result = apply_filter(sample_df, params)
        assert result.count() == 0

    def test_filter_keeps_all_rows(self, sample_df) -> None:
        params = FilterParams(expr=SparkExpr("amount >= 0"))
        result = apply_filter(sample_df, params)
        assert result.count() == 3

    def test_filter_on_string_column(self, sample_df) -> None:
        params = FilterParams(expr=SparkExpr("name = 'alice'"))
        result = apply_filter(sample_df, params)
        assert result.count() == 1
        assert result.collect()[0]["name"] == "alice"

    def test_filter_boolean_column(self, sample_df) -> None:
        params = FilterParams(expr=SparkExpr("active = true"))
        result = apply_filter(sample_df, params)
        assert result.count() == 2

    def test_filter_preserves_all_columns(self, sample_df) -> None:
        params = FilterParams(expr=SparkExpr("id = 1"))
        result = apply_filter(sample_df, params)
        assert set(result.columns) == set(sample_df.columns)


class TestApplyDerive:
    """Tests for the derive step handler."""

    def test_derive_adds_new_column(self, sample_df) -> None:
        params = DeriveParams(columns={"doubled": SparkExpr("amount * 2")})
        result = apply_derive(sample_df, params)
        assert "doubled" in result.columns
        values = {r["id"]: r["doubled"] for r in result.collect()}
        assert values[1] == 200
        assert values[2] == 100

    def test_derive_replaces_existing_column(self, sample_df) -> None:
        params = DeriveParams(columns={"name": SparkExpr("upper(name)")})
        result = apply_derive(sample_df, params)
        names = {r["name"] for r in result.collect()}
        assert names == {"ALICE", "BOB", "CAROL"}

    def test_derive_multiple_columns_at_once(self, sample_df) -> None:
        params = DeriveParams(
            columns={
                "doubled": SparkExpr("amount * 2"),
                "label": SparkExpr("concat(name, '_lbl')"),
            }
        )
        result = apply_derive(sample_df, params)
        assert "doubled" in result.columns
        assert "label" in result.columns
        assert result.count() == 3

    def test_derive_preserves_original_columns(self, sample_df) -> None:
        params = DeriveParams(columns={"extra": SparkExpr("1")})
        result = apply_derive(sample_df, params)
        for col in sample_df.columns:
            assert col in result.columns

    def test_derive_literal_expression(self, sample_df) -> None:
        params = DeriveParams(columns={"const": SparkExpr("'fixed'")})
        result = apply_derive(sample_df, params)
        values = {r["const"] for r in result.collect()}
        assert values == {"fixed"}


class TestApplySelect:
    """Tests for the select step handler."""

    def test_select_subset_of_columns(self, sample_df) -> None:
        params = SelectParams(columns=["id", "name"])
        result = apply_select(sample_df, params)
        assert result.columns == ["id", "name"]

    def test_select_single_column(self, sample_df) -> None:
        params = SelectParams(columns=["id"])
        result = apply_select(sample_df, params)
        assert result.columns == ["id"]
        assert result.count() == 3

    def test_select_all_columns_in_different_order(self, sample_df) -> None:
        params = SelectParams(columns=["name", "id", "amount", "active"])
        result = apply_select(sample_df, params)
        assert result.columns == ["name", "id", "amount", "active"]

    def test_select_preserves_row_count(self, sample_df) -> None:
        params = SelectParams(columns=["id"])
        result = apply_select(sample_df, params)
        assert result.count() == sample_df.count()


class TestApplyDrop:
    """Tests for the drop step handler."""

    def test_drop_single_column(self, sample_df) -> None:
        params = DropParams(columns=["active"])
        result = apply_drop(sample_df, params)
        assert "active" not in result.columns
        assert "id" in result.columns

    def test_drop_multiple_columns(self, sample_df) -> None:
        params = DropParams(columns=["name", "active"])
        result = apply_drop(sample_df, params)
        assert "name" not in result.columns
        assert "active" not in result.columns
        assert {"id", "amount"} <= set(result.columns)

    def test_drop_nonexistent_column_is_noop(self, sample_df) -> None:
        params = DropParams(columns=["nonexistent"])
        result = apply_drop(sample_df, params)
        assert set(result.columns) == set(sample_df.columns)

    def test_drop_preserves_row_count(self, sample_df) -> None:
        params = DropParams(columns=["name"])
        result = apply_drop(sample_df, params)
        assert result.count() == sample_df.count()


class TestApplyRename:
    """Tests for the rename step handler."""

    def test_rename_single_column(self, sample_df) -> None:
        params = RenameParams(columns={"name": "full_name"})
        result = apply_rename(sample_df, params)
        assert "full_name" in result.columns
        assert "name" not in result.columns

    def test_rename_multiple_columns(self, sample_df) -> None:
        params = RenameParams(columns={"id": "customer_id", "name": "customer_name"})
        result = apply_rename(sample_df, params)
        assert "customer_id" in result.columns
        assert "customer_name" in result.columns
        assert "id" not in result.columns
        assert "name" not in result.columns

    def test_rename_preserves_values(self, sample_df) -> None:
        params = RenameParams(columns={"amount": "value"})
        result = apply_rename(sample_df, params)
        values = {r["value"] for r in result.collect()}
        assert values == {100, 50, 200}

    def test_rename_preserves_row_count(self, sample_df) -> None:
        params = RenameParams(columns={"id": "pk"})
        result = apply_rename(sample_df, params)
        assert result.count() == sample_df.count()

    def test_rename_column_set_mapping_only(self, sample_df) -> None:
        params = RenameParams(columns={})
        result = apply_rename(
            sample_df,
            params,
            column_set_mapping={"id": "identifier", "name": "full_name"},
        )
        assert "identifier" in result.columns
        assert "full_name" in result.columns
        assert "id" not in result.columns
        assert "name" not in result.columns

    def test_rename_static_wins_over_column_set(self, sample_df) -> None:
        params = RenameParams(columns={"id": "override"})
        result = apply_rename(
            sample_df,
            params,
            column_set_mapping={"id": "from_set"},
        )
        assert "override" in result.columns
        assert "from_set" not in result.columns
        assert "id" not in result.columns

    def test_rename_on_unmapped_pass_through(self, sample_df) -> None:
        params = RenameParams(columns={})
        result = apply_rename(
            sample_df,
            params,
            column_set_mapping={"id": "identifier"},
            on_unmapped="pass_through",
        )
        # Unmapped columns preserved as-is
        assert "name" in result.columns
        assert "amount" in result.columns
        assert "active" in result.columns
        assert "identifier" in result.columns

    def test_rename_on_unmapped_error(self, sample_df) -> None:
        params = RenameParams(columns={})
        with pytest.raises(ConfigError, match="unmapped"):
            apply_rename(
                sample_df,
                params,
                column_set_mapping={"id": "identifier"},
                on_unmapped="error",
            )

    def test_rename_on_extra_warn(self, sample_df, caplog) -> None:
        import logging

        params = RenameParams(columns={})
        with caplog.at_level(logging.WARNING):
            apply_rename(
                sample_df,
                params,
                column_set_mapping={"id": "identifier", "nonexistent": "ghost"},
                on_extra="warn",
            )
        assert any("nonexistent" in r.message for r in caplog.records)

    def test_rename_on_extra_error(self, sample_df) -> None:
        params = RenameParams(columns={})
        with pytest.raises(ConfigError, match="nonexistent"):
            apply_rename(
                sample_df,
                params,
                column_set_mapping={"id": "identifier", "nonexistent": "ghost"},
                on_extra="error",
            )

    def test_rename_column_set_logs_counts(self, sample_df, caplog) -> None:
        """apply_rename with column_set_mapping logs loaded/applied/unmapped/extra counts."""
        import logging

        # sample_df has columns: id, name, amount, active (4 cols)
        # column_set: id->identifier, name->full_name (2 loaded)
        # static: amount->value (1 more)
        # merged has 3 entries; df has 4 cols
        # loaded = 3 (merged size), applied = 3 (id, name, amount all in df)
        # unmapped = 1 (active not in merged), extra = 0
        params = RenameParams(columns={"amount": "value"})
        with caplog.at_level(logging.INFO, logger="weevr.operations.pipeline.transforms"):
            apply_rename(
                sample_df,
                params,
                column_set_mapping={"id": "identifier", "name": "full_name"},
            )
        assert any("Column set rename" in r.message for r in caplog.records)
        log_msg = next(r.message for r in caplog.records if "Column set rename" in r.message)
        assert "3 loaded" in log_msg
        assert "3 applied" in log_msg
        assert "1 unmapped" in log_msg
        assert "0 extra" in log_msg


class TestApplyCast:
    """Tests for the cast step handler."""

    def test_cast_integer_to_string(self, spark: SparkSession) -> None:
        schema = StructType([StructField("id", LongType())])
        df = spark.createDataFrame([(1,), (2,)], schema=schema)

        params = CastParams(columns={"id": "string"})
        result = apply_cast(df, params)

        assert result.schema["id"].dataType == StringType()

    def test_cast_string_to_long(self, spark: SparkSession) -> None:
        schema = StructType([StructField("val", StringType())])
        df = spark.createDataFrame([("42",), ("99",)], schema=schema)

        params = CastParams(columns={"val": "long"})
        result = apply_cast(df, params)

        assert result.schema["val"].dataType == LongType()
        assert result.collect()[0]["val"] == 42

    def test_cast_multiple_columns(self, spark: SparkSession) -> None:
        schema = StructType(
            [
                StructField("a", LongType()),
                StructField("b", LongType()),
            ]
        )
        df = spark.createDataFrame([(1, 2)], schema=schema)

        params = CastParams(columns={"a": "string", "b": "string"})
        result = apply_cast(df, params)

        assert result.schema["a"].dataType == StringType()
        assert result.schema["b"].dataType == StringType()

    def test_cast_preserves_other_columns(self, sample_df) -> None:
        params = CastParams(columns={"amount": "string"})
        result = apply_cast(sample_df, params)
        assert "name" in result.columns
        assert "id" in result.columns
        assert result.schema["amount"].dataType == StringType()

    def test_cast_preserves_row_count(self, sample_df) -> None:
        params = CastParams(columns={"amount": "string"})
        result = apply_cast(sample_df, params)
        assert result.count() == sample_df.count()
