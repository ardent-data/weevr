"""Pipeline step dispatcher — routes each step to the appropriate handler."""

from collections.abc import Callable
from typing import Any

from pyspark.sql import DataFrame

from weevr.errors.exceptions import ExecutionError
from weevr.model.pipeline import (
    CastStep,
    DedupStep,
    DeriveStep,
    DropStep,
    FilterStep,
    JoinStep,
    RenameStep,
    SelectStep,
    SortStep,
    Step,
    UnionStep,
)
from weevr.operations.pipeline.joins import apply_join, apply_union
from weevr.operations.pipeline.reshaping import apply_dedup, apply_sort
from weevr.operations.pipeline.transforms import (
    apply_cast,
    apply_derive,
    apply_drop,
    apply_filter,
    apply_rename,
    apply_select,
)

# Handler signature: (df, step, sources) -> DataFrame
# All handlers receive the full step object and source dict for a uniform interface.
StepHandler = Callable[[DataFrame, Any, dict[str, DataFrame]], DataFrame]

_STEP_HANDLERS: dict[type, StepHandler] = {
    FilterStep: lambda df, step, _src: apply_filter(df, step.filter),
    DeriveStep: lambda df, step, _src: apply_derive(df, step.derive),
    SelectStep: lambda df, step, _src: apply_select(df, step.select),
    DropStep: lambda df, step, _src: apply_drop(df, step.drop),
    RenameStep: lambda df, step, _src: apply_rename(df, step.rename),
    CastStep: lambda df, step, _src: apply_cast(df, step.cast),
    DedupStep: lambda df, step, _src: apply_dedup(df, step.dedup),
    SortStep: lambda df, step, _src: apply_sort(df, step.sort),
    JoinStep: lambda df, step, src: apply_join(df, step.join, src),
    UnionStep: lambda df, step, src: apply_union(df, step.union, src),
}


def run_pipeline(
    df: DataFrame,
    steps: list[Step],
    sources: dict[str, DataFrame],
) -> DataFrame:
    """Execute a sequence of pipeline steps against a working DataFrame.

    Steps are applied in order. Join and union steps may reference other
    loaded source DataFrames via ``sources``. Any Spark exception raised
    during a step is wrapped in :class:`~weevr.errors.exceptions.ExecutionError`
    with step index and type context attached.

    Args:
        df: Initial working DataFrame (typically the primary source).
        steps: Ordered list of pipeline step configurations.
        sources: All loaded source DataFrames, keyed by alias. Required for
            join and union steps that reference secondary sources.

    Returns:
        Final DataFrame after all steps have been applied.

    Raises:
        ExecutionError: If any step fails, with step_index and step_type set.
    """
    result = df
    for i, step in enumerate(steps):
        try:
            handler = _STEP_HANDLERS.get(type(step))
            if handler is None:
                step_type = type(step).__name__.removesuffix("Step").lower()
                raise ExecutionError(
                    f"Pipeline step {i}: unrecognized step type '{step_type}'",
                    step_index=i,
                    step_type=step_type,
                )
            result = handler(result, step, sources)
        except ExecutionError:
            raise
        except Exception as exc:
            step_type = type(step).__name__.removesuffix("Step").lower()
            raise ExecutionError(
                f"Pipeline step {i} ({step_type}) failed",
                cause=exc,
                step_index=i,
                step_type=step_type,
            ) from exc
    return result
