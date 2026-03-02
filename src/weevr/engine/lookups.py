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
    """

    name: str
    materialized: bool = False
    strategy: str = "cache"
    row_count: int = 0
    duration_ms: float = 0.0


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
                )
            )

            if span:
                span.set_attribute("lookup.row_count", row_count)
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
    (non-materialized).

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
            resolved[alias] = read_source(spark, lookup_name, lookup_def.source)

    return resolved
