"""Observation registry — lazy per-step statistics collection.

Step handlers attach :class:`pyspark.sql.Observation` objects to their
output DataFrames instead of running eager fact-scale aggregations. The
observations are fulfilled as a byproduct of whatever action later
executes the plan (normally the target write), and the executor harvests
them afterward into thread telemetry.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import Observation

logger = logging.getLogger(__name__)

# Post-write the observations are already fulfilled, so `.get` returns
# immediately; the timeout only guards pathological callers that harvest
# before any action executed the observed plan.
_HARVEST_TIMEOUT_S = 30.0


def _get_with_timeout(observation: Observation, timeout: float) -> dict[str, Any]:
    """Read ``observation.get`` with a timeout.

    ``get`` blocks until fulfilled and the JVM side exposes no
    non-blocking probe on this Spark line, so the read runs on a daemon
    thread — on timeout the thread stays parked but, being a daemon,
    can never hang interpreter shutdown.

    Residual risk, accepted deliberately: each timed-out read leaks one
    parked daemon thread for the life of the process, and harvest time
    is bounded only per-entry. This is acceptable ONLY because every
    harvest call site guarantees a prior action over every observed node
    (executor: the rows_after_transforms count; CTEs: the per-CTE count;
    preview: harvest is skipped unless the sampling action succeeded) —
    a new call path must preserve that invariant or bound the harvest.
    """
    result: list[dict[str, Any]] = []

    def _reader() -> None:
        result.append(dict(observation.get))

    reader = threading.Thread(target=_reader, daemon=True, name="weevr-obs-harvest")
    reader.start()
    reader.join(timeout)
    if not result:
        raise TimeoutError("observation not fulfilled")
    return result[0]


def read_observation(observation: Observation) -> dict[str, Any] | None:
    """Best-effort read of one observation's values.

    Same posture as registry harvest: timeout-guarded, never raises —
    an unfulfilled observation yields None.
    """
    try:
        return _get_with_timeout(observation, _HARVEST_TIMEOUT_S)
    except Exception:
        return None


@dataclass
class _Entry:
    key: str
    observation: Observation
    extras: dict[str, Any] = field(default_factory=dict)
    derive: Callable[[dict[str, Any]], dict[str, Any]] | None = None


class ObservationRegistry:
    """Collects step observations for post-action harvest.

    Observation names are namespaced ``__weevr_obs_<scope>_<step>_<name>``
    so that main-pipeline, CTE sub-pipeline, and batch-item observations
    never collide within one thread's plan.

    Args:
        scope: Pipeline scope label — ``"main"`` for the primary pipeline,
            the CTE name for ``with:`` sub-pipelines.
    """

    def __init__(self, scope: str = "main") -> None:
        self._scope = scope
        self._entries: list[_Entry] = []
        self._names: set[str] = set()

    def create(
        self,
        step: str,
        name: str,
        *,
        extras: dict[str, Any] | None = None,
        derive: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    ) -> Observation:
        """Create and register a uniquely named Observation.

        Args:
            step: Step kind (e.g. ``"resolve"``, ``"map"``).
            name: Step-local stat name (e.g. the FK column name).
            extras: Values known at attach time, merged into the harvested
                row before ``derive`` runs.
            derive: Optional finalizer mapping the raw harvested values to
                the published stats dict.

        Returns:
            The Observation to pass to ``DataFrame.observe``.
        """
        base = f"__weevr_obs_{self._scope}_{step}_{name}"
        obs_name = base
        key = f"{self._scope}.{step}.{name}"
        suffix = 1
        while obs_name in self._names:
            obs_name = f"{base}_{suffix}"
            key = f"{self._scope}.{step}.{name}_{suffix}"
            suffix += 1
        self._names.add(obs_name)

        obs = Observation(obs_name)
        self._entries.append(
            _Entry(
                key=key,
                observation=obs,
                extras=dict(extras or {}),
                derive=derive,
            )
        )
        return obs

    def __bool__(self) -> bool:
        return bool(self._entries)

    def merge(self, other: ObservationRegistry) -> None:
        """Absorb another registry's entries (e.g. a CTE sub-pipeline's)."""
        self._entries.extend(other._entries)
        self._names.update(other._names)

    def harvest(self) -> dict[str, dict[str, Any]]:
        """Read every registered observation into a stats mapping.

        Best-effort: a failed or timed-out read logs a warning and yields
        an absent entry — harvest never raises. On timeout the reader
        thread stays parked on the blocking ``get``; acceptable because
        the normal call point (after the terminal action) always finds
        the observation already fulfilled.
        """
        stats: dict[str, dict[str, Any]] = {}
        for entry in self._entries:
            try:
                values = _get_with_timeout(entry.observation, _HARVEST_TIMEOUT_S)
            except Exception:
                logger.warning(
                    "Step statistics harvest failed for '%s'; stats absent.",
                    entry.key,
                )
                continue
            values.update(entry.extras)
            if entry.derive is not None:
                values = entry.derive(values)
            stats[entry.key] = values
        return stats
