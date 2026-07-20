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
        self._exists_uncertain = False
        self._version_read_failed = False

    def exists(self) -> bool:
        """Whether a Delta table exists at the target (cached until refresh).

        A probe *failure* is folded into False for this method's bool
        contract, but recorded on :meth:`exists_uncertain` — the write
        path must not treat "could not ask" as "confirmed absent".
        """
        if self._exists is None:
            self._exists_uncertain = False
            try:
                self._exists = _delta.probe_delta_table_exists(self._spark, self.path)
            except Exception as exc:
                logger.warning("Existence probe failed for '%s': %s", self.path, exc)
                self._exists = False
                self._exists_uncertain = True
        return self._exists

    def exists_uncertain(self) -> bool:
        """True when the cached False came from a failed probe, not a clean answer."""
        self.exists()
        return self._exists_uncertain

    def refresh(self) -> None:
        """Invalidate the cached existence after an engine-internal commit.

        Must be called after warp pre-init and after seeds — the commits a
        thread makes to its own target before the primary write.
        """
        self._exists = None
        self._exists_uncertain = False

    def mark_exists(self) -> None:
        """Record that the target now exists without re-probing.

        Used after a commit the engine itself just made successfully —
        existence is monotonic within a run.
        """
        self._exists = True
        self._exists_uncertain = False

    def current_version(self) -> int | None:
        """The target's latest Delta commit version, or None when absent.

        A driver-side log read — no Spark jobs. Captured immediately
        before a counted write so the engine's own earlier commits to the
        same target (warp pre-init, seeds) are already included.
        """
        self._version_read_failed = False
        if not self.exists():
            return None
        try:
            table = _delta.resolve_delta_table(self._spark, self.path)
            row = table.history(1).select("version").collect()
            return int(row[0][0]) if row else None
        except Exception:
            # An EXISTING table whose history could not be read is not a
            # fresh table — remember the difference so the metrics guard
            # reports the right cause instead of a phantom foreign commit
            self._version_read_failed = True
            logger.warning("Could not read current version for '%s'", self.path)
            return None

    def commit_metrics_after(self, pre_version: int | None) -> dict[str, str] | None:
        """Operation metrics of the commit that followed ``pre_version``.

        Accepts the latest commit ONLY when its version is exactly
        ``pre_version + 1`` (or 0 for a fresh table) — a foreign commit
        landing between the engine's write and this read makes the
        version jump, and the metrics are then reported absent rather
        than misattributed. Degrade, never misattribute.
        """
        if self._version_read_failed:
            logger.warning(
                "Commit metrics for '%s' skipped: pre-write version was "
                "unreadable, so the commit cannot be attributed",
                self.path,
            )
            return None
        expected = 0 if pre_version is None else pre_version + 1
        try:
            table = _delta.resolve_delta_table(self._spark, self.path)
            row = table.history(1).select("version", "operationMetrics").collect()[0]
        except Exception:
            logger.warning("Commit metrics unavailable for '%s'", self.path)
            return None
        if int(row[0]) != expected:
            logger.warning(
                "Commit metrics for '%s' skipped: expected version %d, found %d "
                "(foreign commit interleaved)",
                self.path,
                expected,
                int(row[0]),
            )
            return None
        return dict(row[1])
