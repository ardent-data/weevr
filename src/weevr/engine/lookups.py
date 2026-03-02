"""Lookup materializer — pre-reads and caches lookup DataFrames for weave execution."""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from weevr.errors.exceptions import LookupResolutionError
from weevr.model.base import FrozenBase
from weevr.model.lookup import Lookup
from weevr.model.source import Source
from weevr.operations.readers import read_source
from weevr.telemetry.span import SpanStatus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from weevr.telemetry.collector import SpanCollector

logger = logging.getLogger("weevr.engine.lookups")


class LookupResult(FrozenBase):
    """Result of a single lookup materialization.

    Attributes:
        name: Lookup name.
        materialized: Whether the lookup was pre-materialized.
        strategy: Materialization strategy used (broadcast or cache).
        row_count: Number of rows in the materialized DataFrame.
        duration_ms: Materialization time in milliseconds.
        key_columns: Key columns declared on the lookup, if any.
        value_columns: Value columns declared on the lookup, if any.
        filter_applied: Whether a filter expression was applied.
        unique_key_checked: Whether a unique-key check was performed.
        unique_key_passed: Result of the unique-key check, if performed.
    """

    name: str
    materialized: bool = False
    strategy: str = "cache"
    row_count: int = 0
    duration_ms: float = 0.0
    key_columns: list[str] | None = None
    value_columns: list[str] | None = None
    filter_applied: bool = False
    unique_key_checked: bool = False
    unique_key_passed: bool | None = None


def _validate_columns(df: DataFrame, columns: list[str], lookup_name: str) -> None:
    """Validate that all declared columns exist in the DataFrame.

    Args:
        df: DataFrame to check.
        columns: Column names that must be present.
        lookup_name: Lookup name for error messages.

    Raises:
        LookupResolutionError: If any column is missing.
    """
    available = set(df.columns)
    for col in columns:
        if col not in available:
            raise LookupResolutionError(
                f"Lookup '{lookup_name}': column '{col}' not found in source. "
                f"Available: {sorted(available)}"
            )


def _check_unique_key(
    df: DataFrame,
    key_columns: list[str],
    lookup_name: str,
    on_failure: str,
) -> bool:
    """Validate that key columns form a unique key.

    Args:
        df: DataFrame to check.
        key_columns: Columns that should form a unique key.
        lookup_name: Lookup name for error messages.
        on_failure: Behavior on duplicate keys (``"abort"`` or ``"warn"``).

    Returns:
        True if keys are unique, False if duplicates found (and on_failure is warn).

    Raises:
        LookupResolutionError: If duplicates found and on_failure is ``"abort"``.
    """
    dup_count = df.groupBy(*key_columns).count().filter("count > 1").count()
    if dup_count > 0:
        msg = (
            f"Lookup '{lookup_name}': unique_key check failed — "
            f"{dup_count} duplicate key group(s) found in {key_columns}"
        )
        if on_failure == "abort":
            raise LookupResolutionError(msg)
        logger.warning(msg)
        return False
    return True


def _apply_narrow_pipeline(
    df: DataFrame,
    lookup: Lookup,
    name: str,
) -> tuple[DataFrame, bool, bool, bool | None]:
    """Apply filter, projection, and unique-key check to a lookup DataFrame.

    Called by both :func:`materialize_lookups` and :func:`resolve_thread_lookups`
    to ensure consistent narrow semantics.

    Args:
        df: Raw DataFrame from source read.
        lookup: Lookup definition with optional narrow fields.
        name: Lookup name for error messages.

    Returns:
        Tuple of (narrowed DataFrame, filter_applied, unique_key_checked,
        unique_key_passed).

    Raises:
        LookupResolutionError: On missing columns or unique-key abort.
    """
    filter_applied = False
    unique_key_checked = False
    unique_key_passed: bool | None = None

    # 1. Filter
    if lookup.filter:
        df = df.filter(F.expr(lookup.filter))
        filter_applied = True

    # 2. Validate + project
    if lookup.values:
        assert lookup.key is not None  # enforced by model validator
        _validate_columns(df, lookup.key + lookup.values, name)
        df = df.select(*lookup.key, *lookup.values)
    elif lookup.key:
        _validate_columns(df, lookup.key, name)

    # 3. Unique-key check
    if lookup.unique_key:
        assert lookup.key is not None  # unique_key without key is meaningless
        unique_key_checked = True
        unique_key_passed = _check_unique_key(df, lookup.key, name, lookup.on_failure)

    return df, filter_applied, unique_key_checked, unique_key_passed


def materialize_lookups(
    spark: SparkSession,
    lookups: dict[str, Lookup],
    collector: SpanCollector | None = None,
    parent_span_id: str | None = None,
) -> tuple[dict[str, DataFrame], list[LookupResult]]:
    """Pre-read and cache/broadcast lookups marked for materialization.

    Only lookups with ``materialize=True`` are read and cached. Others
    are resolved on-demand by threads via :func:`resolve_thread_lookups`.

    Args:
        spark: Active SparkSession.
        lookups: Weave-level lookup definitions keyed by name.
        collector: Optional span collector for telemetry.
        parent_span_id: Optional parent span ID for hierarchy.

    Returns:
        Tuple of (cached DataFrames mapping, list of LookupResult).
    """
    cached: dict[str, DataFrame] = {}
    results: list[LookupResult] = []

    for name, lookup in lookups.items():
        if not lookup.materialize:
            results.append(LookupResult(name=name, materialized=False))
            continue

        span = None
        if collector:
            span = collector.start_span(f"lookup:materialize:{name}", parent_span_id=parent_span_id)
            span.set_attribute("lookup.name", name)
            span.set_attribute("lookup.strategy", lookup.strategy)

        start = time.monotonic()
        try:
            df = read_source(spark, name, lookup.source)

            # Apply narrow pipeline (filter → project → unique_key)
            df, filter_applied, uk_checked, uk_passed = _apply_narrow_pipeline(df, lookup, name)

            if lookup.strategy == "broadcast":
                df = F.broadcast(df)
            else:
                from pyspark import StorageLevel

                df.persist(StorageLevel.MEMORY_AND_DISK)

            # Trigger materialization and get count
            row_count = df.count()

            cached[name] = df
            elapsed_ms = (time.monotonic() - start) * 1000

            results.append(
                LookupResult(
                    name=name,
                    materialized=True,
                    strategy=lookup.strategy,
                    row_count=row_count,
                    duration_ms=elapsed_ms,
                    key_columns=lookup.key,
                    value_columns=lookup.values,
                    filter_applied=filter_applied,
                    unique_key_checked=uk_checked,
                    unique_key_passed=uk_passed,
                )
            )

            if span:
                span.set_attribute("lookup.row_count", row_count)
                if lookup.key:
                    span.set_attribute("lookup.key_columns", ",".join(lookup.key))
                if lookup.values:
                    span.set_attribute("lookup.value_columns", ",".join(lookup.values))
                span.set_attribute("lookup.filter_applied", filter_applied)
                span.set_attribute("lookup.unique_key_checked", uk_checked)
                collector.add_span(span.finish(SpanStatus.OK))  # type: ignore[union-attr]

            logger.info(
                "Materialized lookup '%s' (%s): %d rows in %.1fms",
                name,
                lookup.strategy,
                row_count,
                elapsed_ms,
            )

        except Exception as exc:
            if span:
                span.add_event("error", {"message": str(exc)})
                collector.add_span(span.finish(SpanStatus.ERROR))  # type: ignore[union-attr]
            if isinstance(exc, LookupResolutionError):
                raise
            raise LookupResolutionError(
                f"Failed to materialize lookup '{name}': {exc}",
                cause=exc,
            ) from exc

    return cached, results


def cleanup_lookups(cached: dict[str, DataFrame]) -> None:
    """Unpersist all cached lookup DataFrames (best-effort).

    Swallows exceptions during cleanup to avoid masking the original
    execution error.

    Args:
        cached: Mapping of lookup name to cached DataFrame.
    """
    for name, df in cached.items():
        try:
            df.unpersist()
        except Exception:
            logger.warning("Failed to unpersist lookup '%s'", name, exc_info=True)


def resolve_thread_lookups(
    thread_sources: Mapping[str, Source],
    weave_lookups: dict[str, Lookup],
    cached_dfs: dict[str, DataFrame],
    spark: SparkSession,
) -> dict[str, DataFrame]:
    """Resolve thread source lookup references to DataFrames.

    For each thread source that has a ``lookup`` field, resolves it to
    either a pre-cached DataFrame (materialized) or reads it on-demand
    (non-materialized) with the narrow pipeline applied.

    Args:
        thread_sources: Thread source definitions with optional lookup field.
        weave_lookups: Weave-level lookup definitions.
        cached_dfs: Pre-materialized DataFrames from :func:`materialize_lookups`.
        spark: Active SparkSession for on-demand reads.

    Returns:
        Mapping of source alias to resolved DataFrame.

    Raises:
        LookupResolutionError: If a referenced lookup is not defined in the weave.
    """
    resolved: dict[str, DataFrame] = {}

    for alias, source in thread_sources.items():
        lookup_name = source.lookup
        if lookup_name is None:
            continue

        if lookup_name not in weave_lookups:
            raise LookupResolutionError(
                f"Thread source '{alias}' references undefined lookup '{lookup_name}'"
            )

        if lookup_name in cached_dfs:
            resolved[alias] = cached_dfs[lookup_name]
        else:
            lookup_def = weave_lookups[lookup_name]
            df = read_source(spark, lookup_name, lookup_def.source)
            # Apply narrow pipeline for on-demand reads (DEC-001)
            df, _, _, _ = _apply_narrow_pipeline(df, lookup_def, lookup_name)
            resolved[alias] = df

    return resolved
