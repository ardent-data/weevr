"""Cache manager — tracks cached DataFrames and handles persist/unpersist lifecycle."""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.storagelevel import StorageLevel

from weevr.delta import read_delta

logger = logging.getLogger(__name__)


class CacheManager:
    """Manages the lifecycle of cached DataFrames within a weave execution.

    After a thread that is a cache target completes, the manager persists
    its output DataFrame so that multiple downstream consumers can read from
    memory/disk rather than re-scanning the Delta table. When all consumers
    of a cached thread have finished, the DataFrame is automatically unpersisted.

    Cache failures are non-fatal — they degrade performance but never
    affect correctness (consumers fall back to reading from Delta directly).

    Args:
        cache_targets: Thread names whose outputs should be cached.
        dependents: Maps each thread name to the list of threads that depend on it.
    """

    def __init__(
        self,
        cache_targets: list[str],
        dependents: dict[str, list[str]],
    ) -> None:
        """Initialize with target thread names and their dependency map."""
        self._cache_targets: set[str] = set(cache_targets)
        self._dependents = dependents
        self._cached: dict[str, DataFrame] = {}
        self._by_identity: dict[str, str] = {}
        self._remaining_consumers: dict[str, int] = {
            name: len(dependents.get(name, [])) for name in cache_targets
        }

    def is_cache_target(self, thread_name: str) -> bool:
        """Return True if the thread's output should be cached."""
        return thread_name in self._cache_targets

    def persist(
        self,
        thread_name: str,
        spark: SparkSession,
        target_path: str,
        identity: str | None = None,
    ) -> None:
        """Read, persist, and register the thread's target DataFrame.

        The DataFrame is read from ``target_path``, persisted at
        ``MEMORY_AND_DISK``, eagerly materialized, and registered under the
        target's canonical identity so consumers fetch it explicitly. If
        the operation fails for any reason, a warning is logged and
        execution continues without caching.

        Args:
            thread_name: Name of the thread whose output to cache.
            spark: Active SparkSession.
            target_path: Delta path or alias to read the DataFrame from.
            identity: Canonical target identity for registry lookups; when
                None the entry participates in lifecycle but is not
                fetchable by identity.
        """
        if thread_name not in self._cache_targets:
            return
        try:
            df = read_delta(spark, target_path)
            df.persist(StorageLevel.MEMORY_AND_DISK)
            # Materialize eagerly: parallel same-group consumers must never
            # race to compute the cache — by the time any consumer fetches,
            # the partitions exist
            df.count()
            self._cached[thread_name] = df
            if identity:
                self._by_identity[identity] = thread_name
            logger.debug("Cached output of thread '%s' from path '%s'", thread_name, target_path)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to cache output of thread '%s' (path: '%s'): %s — "
                "consumers will read from Delta directly",
                thread_name,
                target_path,
                exc,
            )

    def get(self, identity: str | None) -> DataFrame | None:
        """The registered post-write snapshot for a canonical identity.

        A miss IS the fallback — the caller reads from storage directly,
        so cache failures stay non-fatal by construction.
        """
        if identity is None:
            return None
        thread_name = self._by_identity.get(identity)
        if thread_name is None:
            return None
        return self._cached.get(thread_name)

    def notify_complete(self, consumer_name: str) -> None:
        """Notify the manager that a consumer thread has completed.

        When all consumers of a cached thread have finished, its cached
        DataFrame is automatically unpersisted. If unpersist fails, a warning
        is logged and the memory is eventually reclaimed by Spark's GC.

        Args:
            consumer_name: Name of the thread that just completed.
        """
        for producer_name in list(self._cached.keys()):
            if consumer_name not in self._dependents.get(producer_name, []):
                continue
            self._remaining_consumers[producer_name] -= 1
            if self._remaining_consumers[producer_name] <= 0:
                self._unpersist(producer_name)

    def cleanup(self) -> None:
        """Force-unpersist all remaining cached DataFrames.

        Called in a ``finally`` block to ensure caches are always released,
        even when execution fails mid-way.
        """
        for name in list(self._cached.keys()):
            self._unpersist(name)

    def _unpersist(self, thread_name: str) -> None:
        df = self._cached.pop(thread_name, None)
        self._by_identity = {k: v for k, v in self._by_identity.items() if v != thread_name}
        if df is None:
            return
        try:
            df.unpersist()
            logger.debug("Unpersisted cached output of thread '%s'", thread_name)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to unpersist cached output of thread '%s': %s",
                thread_name,
                exc,
            )
