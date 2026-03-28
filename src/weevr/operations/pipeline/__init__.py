"""Pipeline step dispatcher — routes each step to the appropriate handler."""

from collections.abc import Callable
from typing import Any

from pyspark.sql import DataFrame

from weevr.errors.exceptions import ExecutionError
from weevr.model.column_set import ColumnSet
from weevr.model.pipeline import (
    AggregateStep,
    CaseWhenStep,
    CastStep,
    CoalesceStep,
    ConcatStep,
    DateOpsStep,
    DedupStep,
    DeriveStep,
    DropStep,
    FillNullStep,
    FilterStep,
    FormatStep,
    JoinStep,
    MapStep,
    PivotStep,
    RenameStep,
    ResolveStep,
    SelectStep,
    SortStep,
    Step,
    StringOpsStep,
    UnionStep,
    UnpivotStep,
    WindowStep,
)
from weevr.operations.pipeline._result import StepResult
from weevr.operations.pipeline.analytical import (
    apply_aggregate,
    apply_pivot,
    apply_unpivot,
    apply_window,
)
from weevr.operations.pipeline.column_ops import apply_date_ops, apply_string_ops
from weevr.operations.pipeline.concat import apply_concat
from weevr.operations.pipeline.conditional import apply_case_when
from weevr.operations.pipeline.formatting import apply_format
from weevr.operations.pipeline.joins import apply_join, apply_union
from weevr.operations.pipeline.null_handling import apply_coalesce, apply_fill_null
from weevr.operations.pipeline.reshaping import apply_dedup, apply_sort
from weevr.operations.pipeline.resolve import apply_resolve, apply_resolve_batch
from weevr.operations.pipeline.transforms import (
    apply_cast,
    apply_derive,
    apply_drop,
    apply_filter,
    apply_rename,
    apply_select,
)
from weevr.operations.pipeline.value_mapping import apply_map

__all__ = ["StepResult", "run_pipeline"]

# Handler signature: (df, step, sources) -> StepResult
# All handlers receive the full step object and source dict for a uniform interface.
StepHandler = Callable[[DataFrame, Any, dict[str, DataFrame]], StepResult]

_STEP_HANDLERS: dict[type, StepHandler] = {
    # Original steps
    FilterStep: lambda df, step, _src: StepResult(apply_filter(df, step.filter)),
    DeriveStep: lambda df, step, _src: StepResult(apply_derive(df, step.derive)),
    SelectStep: lambda df, step, _src: StepResult(apply_select(df, step.select)),
    DropStep: lambda df, step, _src: StepResult(apply_drop(df, step.drop)),
    # RenameStep is dispatched separately in run_pipeline to support column sets
    CastStep: lambda df, step, _src: StepResult(apply_cast(df, step.cast)),
    DedupStep: lambda df, step, _src: StepResult(apply_dedup(df, step.dedup)),
    SortStep: lambda df, step, _src: StepResult(apply_sort(df, step.sort)),
    JoinStep: lambda df, step, src: StepResult(apply_join(df, step.join, src)),
    UnionStep: lambda df, step, src: StepResult(apply_union(df, step.union, src)),
    # Analytical steps (M08a)
    AggregateStep: lambda df, step, _src: StepResult(apply_aggregate(df, step.aggregate)),
    WindowStep: lambda df, step, _src: StepResult(apply_window(df, step.window)),
    PivotStep: lambda df, step, _src: StepResult(apply_pivot(df, step.pivot)),
    UnpivotStep: lambda df, step, _src: StepResult(apply_unpivot(df, step.unpivot)),
    # Conditional step (M08a)
    CaseWhenStep: lambda df, step, _src: StepResult(apply_case_when(df, step.case_when)),
    # Null-handling steps (M08a)
    FillNullStep: lambda df, step, _src: apply_fill_null(df, step.fill_null),
    CoalesceStep: lambda df, step, _src: apply_coalesce(df, step.coalesce),
    # Column-ops steps (M08a)
    StringOpsStep: lambda df, step, _src: StepResult(apply_string_ops(df, step.string_ops)),
    DateOpsStep: lambda df, step, _src: StepResult(apply_date_ops(df, step.date_ops)),
    # Transform step extensions (M115)
    ConcatStep: lambda df, step, _src: apply_concat(df, step.concat),
    MapStep: lambda df, step, _src: apply_map(df, step.map),
    FormatStep: lambda df, step, _src: apply_format(df, step.format),
}


def run_pipeline(
    df: DataFrame,
    steps: list[Step],
    sources: dict[str, DataFrame],
    column_sets: dict[str, dict[str, str]] | None = None,
    column_set_defs: dict[str, ColumnSet] | None = None,
    lookups: dict[str, DataFrame] | None = None,
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
        column_sets: Pre-resolved column set mappings keyed by name. When a
            rename step references a column set, the resolved dict is looked up
            here and passed to ``apply_rename``.
        column_set_defs: Column set model instances keyed by name. Used to read
            ``on_unmapped`` and ``on_extra`` behaviour settings for rename steps
            that reference a column set.
        lookups: Cached lookup DataFrames keyed by name. Required for
            resolve steps that reference named lookups.

    Returns:
        Final DataFrame after all steps have been applied.

    Raises:
        ExecutionError: If any step fails, with step_index and step_type set.
    """

    def _dispatch_rename(result: DataFrame, step: RenameStep) -> StepResult:
        cs_name = step.rename.column_set
        if cs_name is not None and column_sets is not None and cs_name in column_sets:
            cs_def = (column_set_defs or {}).get(cs_name)
            return StepResult(
                apply_rename(
                    result,
                    step.rename,
                    column_set_mapping=column_sets[cs_name],
                    on_unmapped=cs_def.on_unmapped if cs_def is not None else "pass_through",
                    on_extra=cs_def.on_extra if cs_def is not None else "ignore",
                )
            )
        return StepResult(apply_rename(result, step.rename))

    def _dispatch_resolve(result: DataFrame, step: ResolveStep) -> StepResult:
        resolve_lookups = lookups or {}
        params = step.resolve
        if params.batch is not None:
            return apply_resolve_batch(result, params, resolve_lookups)
        # Single mode — look up the named lookup DataFrame
        lookup_name = params.lookup
        assert lookup_name is not None
        lookup_df = resolve_lookups.get(lookup_name)
        if lookup_df is None:
            raise ExecutionError(
                f"Lookup '{lookup_name}' not found for resolve step '{params.name}'",
            )
        return apply_resolve(result, params, lookup_df)

    result = df
    for i, step in enumerate(steps):
        try:
            if isinstance(step, RenameStep):
                step_result = _dispatch_rename(result, step)
                result = step_result.df
            elif isinstance(step, ResolveStep):
                step_result = _dispatch_resolve(result, step)
                result = step_result.df
            else:
                handler = _STEP_HANDLERS.get(type(step))
                if handler is None:
                    step_type = type(step).__name__.removesuffix("Step").lower()
                    raise ExecutionError(
                        f"Pipeline step {i}: unrecognized step type '{step_type}'",
                        step_index=i,
                        step_type=step_type,
                    )
                step_result = handler(result, step, sources)
                result = step_result.df
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

    # Strip internal flag columns added by steps (e.g. __map_unmapped).
    # These are used within the pipeline for downstream step coordination
    # but must not surface in the output DataFrame.
    flag_cols = [c for c in result.columns if c.startswith("__map_")]
    if flag_cols:
        result = result.drop(*flag_cols)

    return result
