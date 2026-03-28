"""Tests for join and union step handlers."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from weevr.errors.exceptions import ExecutionError
from weevr.model.pipeline import JoinKeyPair, JoinParams, UnionParams
from weevr.operations.pipeline.joins import apply_join, apply_union

pytestmark = pytest.mark.spark


@pytest.fixture()
def left_df(spark: SparkSession):
    """Left-side DataFrame for join tests."""
    return spark.createDataFrame(
        [
            {"id": 1, "left_val": "a"},
            {"id": 2, "left_val": "b"},
            {"id": 3, "left_val": "c"},
        ]
    )


@pytest.fixture()
def right_df(spark: SparkSession):
    """Right-side DataFrame for join tests."""
    return spark.createDataFrame(
        [
            {"id": 1, "right_val": "x"},
            {"id": 2, "right_val": "y"},
            {"id": 4, "right_val": "z"},
        ]
    )


@pytest.fixture()
def sources(right_df):
    """Sources dict containing the right-side DataFrame."""
    return {"right": right_df}


class TestApplyJoin:
    """Tests for the join step handler."""

    def test_inner_join_returns_matching_rows(self, left_df, sources) -> None:
        params = JoinParams(
            source="right",
            type="inner",
            on=[JoinKeyPair(left="id", right="id")],
        )
        result = apply_join(left_df, params, sources)
        assert result.count() == 2

    def test_left_join_preserves_left_rows(self, left_df, sources) -> None:
        params = JoinParams(
            source="right",
            type="left",
            on=[JoinKeyPair(left="id", right="id")],
        )
        result = apply_join(left_df, params, sources)
        assert result.count() == 3

    def test_right_join_preserves_right_rows(self, left_df, sources) -> None:
        params = JoinParams(
            source="right",
            type="right",
            on=[JoinKeyPair(left="id", right="id")],
        )
        result = apply_join(left_df, params, sources)
        assert result.count() == 3

    def test_full_join_preserves_all_rows(self, left_df, sources) -> None:
        params = JoinParams(
            source="right",
            type="full",
            on=[JoinKeyPair(left="id", right="id")],
        )
        result = apply_join(left_df, params, sources)
        assert result.count() == 4

    def test_semi_join_returns_left_keys_in_right(self, left_df, sources) -> None:
        params = JoinParams(
            source="right",
            type="semi",
            on=[JoinKeyPair(left="id", right="id")],
        )
        result = apply_join(left_df, params, sources)
        assert result.count() == 2
        assert "right_val" not in result.columns

    def test_anti_join_returns_left_keys_not_in_right(self, left_df, sources) -> None:
        params = JoinParams(
            source="right",
            type="anti",
            on=[JoinKeyPair(left="id", right="id")],
        )
        result = apply_join(left_df, params, sources)
        assert result.count() == 1
        assert result.collect()[0]["id"] == 3

    def test_cross_join_produces_cartesian_product(self, left_df, sources) -> None:
        params = JoinParams(
            source="right",
            type="cross",
            on=[],
        )
        result = apply_join(left_df, params, sources)
        assert result.count() == 9  # 3 * 3

    def test_null_safe_join_default_is_true(self, spark: SparkSession) -> None:
        schema = StructType(
            [
                StructField("k", LongType(), nullable=True),
                StructField("v", StringType()),
            ]
        )
        left = spark.createDataFrame([(None, "left_null")], schema=schema)
        right = spark.createDataFrame([(None, "right_null")], schema=schema)
        src = {"right": right}

        params = JoinParams(
            source="right",
            type="inner",
            on=[JoinKeyPair(left="k", right="k")],
        )
        result = apply_join(left, params, src)
        # NULL eqNullSafe NULL → True, so inner join should return 1 row
        assert result.count() == 1

    def test_non_null_safe_join_excludes_null_matches(self, spark: SparkSession) -> None:
        schema = StructType(
            [
                StructField("k", LongType(), nullable=True),
                StructField("v", StringType()),
            ]
        )
        left = spark.createDataFrame([(None, "left_null")], schema=schema)
        right = spark.createDataFrame([(None, "right_null")], schema=schema)
        src = {"right": right}

        params = JoinParams(
            source="right",
            type="inner",
            on=[JoinKeyPair(left="k", right="k")],
            null_safe=False,
        )
        result = apply_join(left, params, src)
        # NULL == NULL is False in standard SQL, so no rows match
        assert result.count() == 0

    def test_explicit_key_pair_with_different_column_names(self, spark: SparkSession) -> None:
        left = spark.createDataFrame([{"customer_id": 1, "name": "alice"}])
        right = spark.createDataFrame([{"cid": 1, "score": 99}])
        src = {"scores": right}

        params = JoinParams(
            source="scores",
            type="inner",
            on=[JoinKeyPair(left="customer_id", right="cid")],
        )
        result = apply_join(left, params, src)
        assert result.count() == 1
        assert "score" in result.columns
        assert "name" in result.columns

    def test_same_name_join_keys_deduplicated(self, left_df, sources) -> None:
        """Join on same-name keys drops the right-side duplicate column."""
        params = JoinParams(
            source="right",
            type="inner",
            on=[JoinKeyPair(left="id", right="id")],
        )
        result = apply_join(left_df, params, sources)
        # 'id' should appear exactly once, not twice
        assert result.columns.count("id") == 1

    def test_missing_source_raises_execution_error(self, left_df, sources) -> None:
        params = JoinParams(
            source="nonexistent",
            type="inner",
            on=[JoinKeyPair(left="id", right="id")],
        )
        with pytest.raises(ExecutionError, match="nonexistent"):
            apply_join(left_df, params, sources)


class TestApplyUnion:
    """Tests for the union step handler."""

    def test_union_by_name(self, spark: SparkSession) -> None:
        df1 = spark.createDataFrame([{"id": 1, "val": "a"}])
        df2 = spark.createDataFrame([{"id": 2, "val": "b"}])
        src = {"other": df2}

        params = UnionParams(sources=["other"], mode="by_name")
        result = apply_union(df1, params, src)
        assert result.count() == 2

    def test_union_by_position(self, spark: SparkSession) -> None:
        df1 = spark.createDataFrame([{"id": 1, "val": "a"}])
        df2 = spark.createDataFrame([{"id": 2, "val": "b"}])
        src = {"other": df2}

        params = UnionParams(sources=["other"], mode="by_position")
        result = apply_union(df1, params, src)
        assert result.count() == 2

    def test_union_multiple_sources(self, spark: SparkSession) -> None:
        df1 = spark.createDataFrame([{"id": 1}])
        df2 = spark.createDataFrame([{"id": 2}])
        df3 = spark.createDataFrame([{"id": 3}])
        src = {"b": df2, "c": df3}

        params = UnionParams(sources=["b", "c"], mode="by_name")
        result = apply_union(df1, params, src)
        assert result.count() == 3

    def test_union_by_name_with_allow_missing(self, spark: SparkSession) -> None:
        df1 = spark.createDataFrame([{"id": 1, "extra": "x"}])
        df2 = spark.createDataFrame([{"id": 2}])
        src = {"other": df2}

        params = UnionParams(sources=["other"], mode="by_name", allow_missing=True)
        result = apply_union(df1, params, src)
        assert result.count() == 2
        assert "extra" in result.columns

    def test_union_missing_source_raises_execution_error(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([{"id": 1}])
        params = UnionParams(sources=["missing"], mode="by_name")
        with pytest.raises(ExecutionError, match="missing"):
            apply_union(df, params, {})

    def test_union_empty_sources_list_rejected_by_model(self, spark: SparkSession) -> None:
        """Empty sources list is rejected at model validation time."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="sources must not be empty"):
            UnionParams(sources=[], mode="by_name")
