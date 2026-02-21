"""Tests for the run_pipeline() dispatcher."""

import pytest
from pyspark.sql import SparkSession

from weevr.errors.exceptions import ExecutionError
from weevr.model.pipeline import (
    CastParams,
    CastStep,
    DedupParams,
    DedupStep,
    DeriveParams,
    DeriveStep,
    DropParams,
    DropStep,
    FilterParams,
    FilterStep,
    JoinKeyPair,
    JoinParams,
    JoinStep,
    RenameParams,
    RenameStep,
    SelectParams,
    SelectStep,
    SortParams,
    SortStep,
    Step,
    UnionParams,
    UnionStep,
)
from weevr.model.types import SparkExpr
from weevr.operations.pipeline import run_pipeline


@pytest.fixture()
def base_df(spark: SparkSession):
    """Base DataFrame for pipeline tests."""
    return spark.createDataFrame(
        [
            {"id": 1, "name": "alice", "amount": 100},
            {"id": 2, "name": "bob", "amount": 50},
            {"id": 3, "name": "carol", "amount": 200},
        ]
    )


class TestRunPipelineBasic:
    """Basic dispatcher behaviour."""

    def test_empty_pipeline_returns_original_df(self, base_df) -> None:
        result = run_pipeline(base_df, [], {})
        assert result.count() == base_df.count()
        assert set(result.columns) == set(base_df.columns)

    def test_single_filter_step(self, base_df) -> None:
        steps: list[Step] = [FilterStep(filter=FilterParams(expr=SparkExpr("amount > 60")))]
        result = run_pipeline(base_df, steps, {})
        assert result.count() == 2

    def test_sequential_steps_applied_in_order(self, base_df) -> None:
        steps: list[Step] = [
            FilterStep(filter=FilterParams(expr=SparkExpr("amount > 60"))),
            SelectStep(select=SelectParams(columns=["id", "name"])),
        ]
        result = run_pipeline(base_df, steps, {})
        assert result.count() == 2
        assert result.columns == ["id", "name"]

    def test_pipeline_with_derive_step(self, base_df) -> None:
        steps: list[Step] = [
            DeriveStep(derive=DeriveParams(columns={"doubled": SparkExpr("amount * 2")}))
        ]
        result = run_pipeline(base_df, steps, {})
        assert "doubled" in result.columns
        assert result.count() == 3

    def test_pipeline_with_drop_step(self, base_df) -> None:
        steps: list[Step] = [DropStep(drop=DropParams(columns=["amount"]))]
        result = run_pipeline(base_df, steps, {})
        assert "amount" not in result.columns

    def test_pipeline_with_rename_step(self, base_df) -> None:
        steps: list[Step] = [RenameStep(rename=RenameParams(columns={"name": "full_name"}))]
        result = run_pipeline(base_df, steps, {})
        assert "full_name" in result.columns
        assert "name" not in result.columns

    def test_pipeline_with_cast_step(self, base_df) -> None:
        steps: list[Step] = [CastStep(cast=CastParams(columns={"amount": "string"}))]
        result = run_pipeline(base_df, steps, {})
        assert result.schema["amount"].dataType.typeName() == "string"

    def test_pipeline_with_dedup_step(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [
                {"id": 1, "ts": 1},
                {"id": 1, "ts": 2},
                {"id": 2, "ts": 3},
            ]
        )
        steps: list[Step] = [DedupStep(dedup=DedupParams(keys=["id"], order_by="ts", keep="first"))]
        result = run_pipeline(df, steps, {})
        assert result.count() == 2

    def test_pipeline_with_sort_step(self, base_df) -> None:
        steps: list[Step] = [SortStep(sort=SortParams(columns=["amount"], ascending=False))]
        result = run_pipeline(base_df, steps, {})
        amounts = [r["amount"] for r in result.collect()]
        assert amounts == sorted(amounts, reverse=True)


class TestRunPipelineWithSources:
    """Dispatcher behaviour when steps require sources dict."""

    def test_pipeline_with_join_step(self, spark: SparkSession, base_df) -> None:
        right = spark.createDataFrame(
            [
                {"id": 1, "score": 99},
                {"id": 2, "score": 88},
            ]
        )
        steps: list[Step] = [
            JoinStep(
                join=JoinParams(
                    source="scores",
                    type="inner",
                    on=[JoinKeyPair(left="id", right="id")],
                )
            )
        ]
        result = run_pipeline(base_df, steps, {"scores": right})
        assert result.count() == 2
        assert "score" in result.columns

    def test_pipeline_with_union_step(self, spark: SparkSession, base_df) -> None:
        extra = spark.createDataFrame([{"id": 4, "name": "dave", "amount": 75}])
        steps: list[Step] = [UnionStep(union=UnionParams(sources=["extra"], mode="by_name"))]
        result = run_pipeline(base_df, steps, {"extra": extra})
        assert result.count() == 4


class TestRunPipelineErrorHandling:
    """Error wrapping in run_pipeline."""

    def test_spark_error_wrapped_with_step_context(self, base_df) -> None:
        steps: list[Step] = [FilterStep(filter=FilterParams(expr=SparkExpr("invalid!!!sql")))]
        with pytest.raises(ExecutionError) as exc_info:
            run_pipeline(base_df, steps, {}).count()

        err = exc_info.value
        assert err.step_index == 0
        assert err.step_type == "filter"

    def test_execution_error_passes_through_unwrapped(self, base_df) -> None:
        steps: list[Step] = [
            JoinStep(
                join=JoinParams(
                    source="nonexistent",
                    type="inner",
                    on=[JoinKeyPair(left="id", right="id")],
                )
            )
        ]
        with pytest.raises(ExecutionError) as exc_info:
            run_pipeline(base_df, steps, {})

        err = exc_info.value
        # ExecutionError from apply_join passes through — no double-wrapping
        assert "nonexistent" in str(err)

    def test_error_reports_correct_step_index(self, base_df) -> None:
        steps: list[Step] = [
            SelectStep(select=SelectParams(columns=["id", "name"])),
            FilterStep(filter=FilterParams(expr=SparkExpr("bad!!!expr"))),
        ]
        with pytest.raises(ExecutionError) as exc_info:
            run_pipeline(base_df, steps, {}).count()

        err = exc_info.value
        assert err.step_index == 1
