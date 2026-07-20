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

import json
import logging
from dataclasses import dataclass
from typing import Any

from pyspark.sql import SparkSession

from weevr import delta as _delta

logger = logging.getLogger(__name__)

STAMP_ENGINE_MARKER = "weevr"
STAMP_VERSION = 1


@dataclass(frozen=True)
class CommitStamp:
    """Lineage stamp written as Delta commit ``userMetadata`` on engine writes.

    Visible in ``DESCRIBE HISTORY`` and joinable to telemetry via
    ``run_id``. The stable core is the engine marker, ``run_id``, and
    ``thread`` — attribution matches on exactly that; the remaining
    fields are best-effort context and may evolve.

    Attributes:
        run_id: Run identity shared with audit columns and exports.
        thread: Name of the thread that made the commit.
        loom: Loom name, when the thread ran under one.
        weave: Weave name, when the thread ran under one.
        mode: Write mode of the commit (overwrite/append/merge).
    """

    run_id: str
    thread: str
    loom: str | None = None
    weave: str | None = None
    mode: str | None = None

    def to_json(self) -> str:
        """Serialize to the compact JSON written as commit userMetadata."""
        payload: dict[str, Any] = {
            "engine": STAMP_ENGINE_MARKER,
            "version": STAMP_VERSION,
            "run_id": self.run_id,
            "thread": self.thread,
        }
        if self.loom:
            payload["loom"] = self.loom
        if self.weave:
            payload["weave"] = self.weave
        if self.mode:
            payload["mode"] = self.mode
        return json.dumps(payload, separators=(",", ":"))

    def matches(self, user_metadata: str | None) -> bool:
        """Whether *user_metadata* is this write's own stamp.

        Matches on the stable core only (engine marker + ``run_id`` +
        ``thread``), so an engine-shaped stamp from another run in the
        same history window is never claimed.
        """
        if not user_metadata:
            return False
        try:
            data = json.loads(user_metadata)
        except ValueError:
            return False
        if not isinstance(data, dict):
            return False
        return (
            data.get("engine") == STAMP_ENGINE_MARKER
            and data.get("run_id") == self.run_id
            and data.get("thread") == self.thread
        )


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

    def commit_metrics_after(
        self, pre_version: int | None, stamp: CommitStamp | None = None
    ) -> dict[str, str] | None:
        """Operation metrics of the engine's own commit after ``pre_version``.

        With a *stamp* (single-pass writes), the history window since
        ``pre_version`` is scanned for the commit carrying the write's
        own stamp — exact regardless of interleaved foreign commits.

        Without a stamp (merge paths, where per-write userMetadata is
        not honored), the latest commit is accepted ONLY when its
        version is exactly ``pre_version + 1`` (or 0 for a fresh
        table) — a foreign commit landing between the engine's write
        and this read makes the version jump, and the metrics are then
        reported absent rather than misattributed. Either way: degrade,
        never misattribute.
        """
        if stamp is not None:
            return self._stamped_commit_metrics(pre_version, stamp)
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

    def _stamped_commit_metrics(
        self, pre_version: int | None, stamp: CommitStamp
    ) -> dict[str, str] | None:
        """Find the own-stamped commit in the window after ``pre_version``.

        A driver-side log read. The window is bounded below by
        ``pre_version`` (full history when the pre-write version was
        unreadable — the stamp still identifies the own commit exactly).
        """
        floor = -1 if pre_version is None else pre_version
        try:
            table = _delta.resolve_delta_table(self._spark, self.path)
            rows = (
                table.history()
                .select("version", "operationMetrics", "userMetadata")
                .where(f"version > {floor}")
                .collect()
            )
        except Exception:
            logger.warning("Commit metrics unavailable for '%s'", self.path)
            return None
        own = [row for row in rows if stamp.matches(row["userMetadata"])]
        if not own:
            if not rows and pre_version is not None:
                # No commit of ANY kind landed after pre_version: Delta
                # skips the commit entirely for a zero-row single-pass
                # write, so the engine provably wrote nothing — an exact
                # zero, not an unknown.
                return {"numOutputRows": "0"}
            logger.warning(
                "Commit metrics for '%s' skipped: no commit stamped by this "
                "write found after version %s (own commit unattributable)",
                self.path,
                "?" if pre_version is None else pre_version,
            )
            return None
        latest = max(own, key=lambda row: int(row["version"]))
        return dict(latest["operationMetrics"])
