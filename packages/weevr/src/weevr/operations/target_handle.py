"""Target handle — one existence/identity resolution per thread.

The executor resolves a thread's target once and threads the handle to
every consumer (target mapping, seeding, writers, merge routing). The
handle caches the *resolution mechanics*, not a frozen snapshot:
existence is a refreshable query, explicitly invalidated after any
engine-internal commit the thread itself makes (warp pre-init, seeds),
so downstream consumers observe post-commit state exactly as their
previous independent probes did — with one probe mechanism instead of
three to five per thread.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from weevr import delta as _delta

logger = logging.getLogger(__name__)


class TargetHandle:
    """Cached target existence resolution for one thread execution.

    Args:
        spark: Active SparkSession.
        path: Resolved target path or table alias.
    """

    def __init__(self, spark: SparkSession, path: str) -> None:
        """Bind the handle to a resolved target path or alias."""
        self._spark = spark
        self.path = path
        self._exists: bool | None = None

    def exists(self) -> bool:
        """Whether a Delta table exists at the target (cached until refresh)."""
        if self._exists is None:
            self._exists = _delta.delta_table_exists(self._spark, self.path)
        return self._exists

    def refresh(self) -> None:
        """Invalidate the cached existence after an engine-internal commit.

        Must be called after warp pre-init and after seeds — the commits a
        thread makes to its own target before the primary write.
        """
        self._exists = None

    def mark_exists(self) -> None:
        """Record that the target now exists without re-probing.

        Used after a commit the engine itself just made successfully —
        existence is monotonic within a run.
        """
        self._exists = True
