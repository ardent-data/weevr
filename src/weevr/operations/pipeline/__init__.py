"""Pipeline step dispatcher — routes each step to the appropriate handler."""

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
            if isinstance(step, FilterStep):
                result = apply_filter(result, step.filter)
            elif isinstance(step, DeriveStep):
                result = apply_derive(result, step.derive)
            elif isinstance(step, SelectStep):
                result = apply_select(result, step.select)
            elif isinstance(step, DropStep):
                result = apply_drop(result, step.drop)
            elif isinstance(step, RenameStep):
                result = apply_rename(result, step.rename)
            elif isinstance(step, CastStep):
                result = apply_cast(result, step.cast)
            elif isinstance(step, DedupStep):
                result = apply_dedup(result, step.dedup)
            elif isinstance(step, SortStep):
                result = apply_sort(result, step.sort)
            elif isinstance(step, JoinStep):
                result = apply_join(result, step.join, sources)
            elif isinstance(step, UnionStep):
                result = apply_union(result, step.union, sources)
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
