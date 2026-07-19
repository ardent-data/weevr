"""Lookup materializer — pre-reads and caches lookup DataFrames for weave execution."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from weevr.errors.exceptions import LookupResolutionError
from weevr.model.base import FrozenBase
from weevr.model.connection import OneLakeConnection
from weevr.model.lookup import Lookup
from weevr.model.source import Source
from weevr.operations.pipeline._lookup_meta import LookupMeta
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
        source_size_bytes: Delta snapshot size of the source table, when
            resolvable — evidence for the broadcast hint policy.
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
    source_size_bytes: int | None = None


def _delta_source_size(
    spark: SparkSession,
    source: Source,
    connections: dict[str, OneLakeConnection] | None,
) -> int | None:
    """Best-effort Delta snapshot size for a lookup source, in bytes.

    A driver-side Delta log read — no Spark jobs. Returns None for
    non-Delta sources or any resolution failure; absence of evidence
    simply means no size-based broadcast hint.
    """
    try:
        from delta.tables import DeltaTable

        if source.connection:
            if not connections or source.connection not in connections:
                return None
            from weevr.config.paths import build_onelake_path

            conn = connections[source.connection]
            if source.table is None:
                return None
            location = build_onelake_path(conn, source.schema_override, source.table)
            table = DeltaTable.forPath(spark, location)
        elif source.type == "delta" and source.alias is not None:
            from weevr.config.paths import resolve_fuse_path
            from weevr.delta import is_table_alias

            if is_table_alias(source.alias):
                table = DeltaTable.forName(spark, source.alias)
            else:
                table = DeltaTable.forPath(spark, resolve_fuse_path(source.alias, spark))
        else:
            return None
        detail = table.detail().collect()[0]
        size = detail["sizeInBytes"]
        return int(size) if size is not None else None
    except Exception:
        logger.debug("Could not resolve Delta size for lookup source", exc_info=True)
        return None


class UniqueKeyMemo:
    """Validate-exactly-once memo for non-materialized lookups, per weave.

    ``materialize: false`` keeps the user's per-thread re-read, but the
    unique-key validation memoizes by lookup name: the first consumer
    validates, the rest reuse the outcome — including a failure outcome.
    Same-group threads run concurrently, so the check-and-set holds a
    lock for the duration of the winner's validation; losers block on it
    and then observe the settled outcome (a warn is logged once, an abort
    raises consistently for every consumer).
    """

    def __init__(self) -> None:
        """Create an empty memo (one per weave execution)."""
        self._lock = threading.Lock()
        self._outcomes: dict[str, Exception | bool | None] = {}

    def validate(self, name: str, df: DataFrame, lookup: Lookup) -> None:
        """Run or reuse the unique-key validation for one lookup.

        Raises:
            LookupResolutionError: The winner's abort, re-raised for every
                consumer that reuses the memoized failure.
        """
        if not lookup.unique_key or lookup.key is None:
            return
        with self._lock:
            if name not in self._outcomes:
                try:
                    self._outcomes[name] = _check_unique_key(
                        df, lookup.key, name, lookup.on_failure
                    )
                except Exception as exc:
                    self._outcomes[name] = exc
                    raise
            outcome = self._outcomes[name]
        if isinstance(outcome, Exception):
            # A fresh exception per consumer: re-raising one instance from
            # several threads mutates its traceback concurrently
            raise LookupResolutionError(str(outcome)) from outcome


def build_lookup_meta(
    results: list[LookupResult],
    base: dict[str, LookupMeta] | None = None,
) -> dict[str, LookupMeta]:
    """Reduce materialization results to per-lookup facts for resolve steps.

    Args:
        results: Materialization results to convert, keyed into the map by
            lookup name (later entries win on collision).
        base: Facts inherited from a parent scope (e.g. loom-level), copied
            first so scope precedence matches the cached-DataFrame maps.

    Returns:
        Mapping of lookup name to reduced facts.
    """
    meta: dict[str, LookupMeta] = dict(base or {})
    for r in results:
        meta[r.name] = LookupMeta(
            key_columns=tuple(r.key_columns) if r.key_columns else None,
            unique_key_checked=r.unique_key_checked,
            unique_key_passed=r.unique_key_passed,
            size_in_bytes=r.source_size_bytes,
            broadcast_declared=r.materialized and r.strategy == "broadcast",
        )
    return meta


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
    *,
    run_unique_check: bool = True,
) -> tuple[DataFrame, bool, bool, bool | None]:
    """Apply filter, projection, and unique-key check to a lookup DataFrame.

    Called by both :func:`materialize_lookups` and :func:`resolve_thread_lookups`
    to ensure consistent narrow semantics.

    Args:
        df: Raw DataFrame from source read.
        lookup: Lookup definition with optional narrow fields.
        name: Lookup name for error messages.
        run_unique_check: The materialized path passes False and runs the
            check itself AGAINST THE CACHE after persisting — checking here
            would read the source once for the check and again for the
            cache fill. The on-demand path keeps the inline check.

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
        if lookup.key is None:
            raise LookupResolutionError(f"Lookup '{name}': 'values' requires 'key' to be set")
        _validate_columns(df, lookup.key + lookup.values, name)
        df = df.select(*lookup.key, *lookup.values)
    elif lookup.key:
        _validate_columns(df, lookup.key, name)

    # 3. Unique-key check
    if lookup.unique_key and run_unique_check:
        if lookup.key is None:
            raise LookupResolutionError(f"Lookup '{name}': 'unique_key' requires 'key' to be set")
        unique_key_checked = True
        unique_key_passed = _check_unique_key(df, lookup.key, name, lookup.on_failure)

    return df, filter_applied, unique_key_checked, unique_key_passed


def materialize_lookups(
    spark: SparkSession,
    lookups: dict[str, Lookup],
    collector: SpanCollector | None = None,
    parent_span_id: str | None = None,
    connections: dict[str, OneLakeConnection] | None = None,
) -> tuple[dict[str, DataFrame], list[LookupResult]]:
    """Pre-read and cache/broadcast lookups marked for materialization.

    Only lookups with ``materialize=True`` are read and cached. Others
    are resolved on-demand by threads via :func:`resolve_thread_lookups`.

    Args:
        spark: Active SparkSession.
        lookups: Weave-level lookup definitions keyed by name.
        collector: Optional span collector for telemetry.
        parent_span_id: Optional parent span ID for hierarchy.
        connections: Named connection declarations forwarded to each source read.

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
            df = read_source(spark, name, lookup.source, connections=connections)
            source_size = _delta_source_size(spark, lookup.source, connections)

            # Narrow first (filter → project), check LATER against the
            # cache: persist-before-check means the source is read exactly
            # once — the warming count fills the cache and the unique-key
            # check reuses it
            df, filter_applied, _, _ = _apply_narrow_pipeline(
                df, lookup, name, run_unique_check=False
            )

            from pyspark import StorageLevel

            if lookup.strategy == "broadcast":
                # Persist AND hint — they compose: the warming count fills
                # the cache, consuming joins build their broadcast exchange
                # from cache instead of re-reading the source per join.
                # Hint before persist so unpersist targets the cached plan.
                df = F.broadcast(df)
            df.persist(StorageLevel.MEMORY_AND_DISK)

            # Trigger materialization and get count — the single source read
            row_count = df.count()

            uk_checked = False
            uk_passed: bool | None = None
            if lookup.unique_key:
                if lookup.key is None:
                    raise LookupResolutionError(
                        f"Lookup '{name}': 'unique_key' requires 'key' to be set"
                    )
                uk_checked = True
                uk_passed = _check_unique_key(df, lookup.key, name, lookup.on_failure)

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
                    source_size_bytes=source_size,
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
    connections: dict[str, OneLakeConnection] | None = None,
    uk_memo: UniqueKeyMemo | None = None,
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
        connections: Named connection declarations forwarded to on-demand reads.
        uk_memo: Per-weave validate-exactly-once memo for non-materialized
            ``unique_key`` lookups; when absent (direct callers) the check
            runs inline per call, as before.

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
            df = read_source(spark, lookup_name, lookup_def.source, connections=connections)
            if uk_memo is not None:
                # Narrow without the inline check; the memo validates
                # exactly once per weave (first consumer wins, others —
                # including concurrent group-mates — reuse the outcome)
                df, _, _, _ = _apply_narrow_pipeline(
                    df, lookup_def, lookup_name, run_unique_check=False
                )
                uk_memo.validate(lookup_name, df, lookup_def)
            else:
                # Direct callers keep today's inline per-call check
                df, _, _, _ = _apply_narrow_pipeline(df, lookup_def, lookup_name)
            resolved[alias] = df

    return resolved
