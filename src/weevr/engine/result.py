"""Execution result models for threads, weaves, and looms."""

from __future__ import annotations

from typing import Any, Literal

from weevr.model.base import FrozenBase
from weevr.telemetry.results import LoomTelemetry, ThreadTelemetry, WeaveTelemetry


class ThreadResult(FrozenBase):
    """Immutable record of a completed thread execution.

    Attributes:
        status: Outcome of the execution — ``"success"``, ``"failure"``, or
            ``"skipped"`` (when a condition evaluated to False).
        thread_name: Name of the thread that was executed.
        rows_written: Number of rows in the DataFrame at write time.
        write_mode: The write mode used (``"overwrite"``, ``"append"``, or ``"merge"``).
        target_path: Physical path of the Delta table that was written.
        telemetry: Thread-level telemetry with validation/assertion results and row counts.
        skip_reason: The condition expression that caused the thread to be skipped.
        error: Error message when the thread failed, ``None`` on success or skip.
        output_schema: Column names and Spark data types captured from the output
            DataFrame before write. Each tuple is ``(column_name, type_string)``.
        samples: Data samples captured before write, keyed by category. Contains
            ``"output"`` (up to 10 rows) and optionally ``"quarantine"`` (up to 10
            rows from the quarantine DataFrame).
    """

    status: Literal["success", "failure", "skipped"]
    thread_name: str
    rows_written: int
    write_mode: str
    target_path: str
    telemetry: ThreadTelemetry | None = None
    skip_reason: str | None = None
    error: str | None = None
    output_schema: list[tuple[str, str]] | None = None
    samples: dict[str, list[dict[str, Any]]] | None = None
    drift_report: dict[str, Any] | None = None
    warp_findings: list[dict[str, str]] | None = None


class WeaveResult(FrozenBase):
    """Immutable record of a completed weave execution.

    Attributes:
        status: Aggregate outcome — ``"success"`` if all threads succeeded
            or were conditionally skipped (no failures), ``"failure"`` if
            all threads failed or were skipped due to upstream failure,
            ``"partial"`` if some threads succeeded and some failed,
            ``"skipped"`` if the weave was conditionally skipped at loom
            level.
        weave_name: Name of the weave that was executed.
        thread_results: Results for each thread that was executed (not skipped).
        threads_skipped: Names of threads that were skipped due to upstream failure.
        duration_ms: Wall-clock duration of the weave execution in milliseconds.
        telemetry: Weave-level telemetry aggregated from thread telemetry.
        skip_reason: The condition expression that caused the weave to be skipped.
    """

    status: Literal["success", "failure", "partial", "skipped"]
    weave_name: str
    thread_results: list[ThreadResult]
    threads_skipped: list[str]
    duration_ms: int
    telemetry: WeaveTelemetry | None = None
    skip_reason: str | None = None


class LoomResult(FrozenBase):
    """Immutable record of a completed loom execution.

    Attributes:
        status: Aggregate outcome — ``"success"`` if all weaves succeeded,
            ``"failure"`` if loom stopped after a weave failure, ``"partial"``
            if some weaves succeeded before a failure.
        loom_name: Name of the loom that was executed.
        weave_results: Results for each weave that was executed.
        duration_ms: Wall-clock duration of the loom execution in milliseconds.
        telemetry: Loom-level telemetry aggregated from weave telemetry.
    """

    status: Literal["success", "failure", "partial"]
    loom_name: str
    weave_results: list[WeaveResult]
    duration_ms: int
    telemetry: LoomTelemetry | None = None
