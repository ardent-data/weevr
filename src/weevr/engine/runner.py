"""Weave and loom execution runners."""

import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Literal

from pyspark.sql import SparkSession

from weevr.engine.cache_manager import CacheManager
from weevr.engine.executor import execute_thread
from weevr.engine.planner import ExecutionPlan, build_plan
from weevr.engine.result import LoomResult, ThreadResult, WeaveResult
from weevr.model.loom import Loom
from weevr.model.thread import Thread
from weevr.model.weave import Weave

logger = logging.getLogger(__name__)

_ThreadState = Literal["pending", "running", "succeeded", "failed", "skipped"]


def _get_transitive_dependents(thread_name: str, dependents: dict[str, list[str]]) -> set[str]:
    """Return all transitive downstream threads of ``thread_name``."""
    visited: set[str] = set()
    queue = list(dependents.get(thread_name, []))
    while queue:
        current = queue.pop()
        if current not in visited:
            visited.add(current)
            queue.extend(dependents.get(current, []))
    return visited


def _compute_weave_status(
    thread_states: dict[str, _ThreadState],
) -> Literal["success", "failure", "partial"]:
    """Compute aggregate weave status from individual thread states."""
    states = set(thread_states.values())
    if states <= {"succeeded"}:
        return "success"
    if states <= {"failed", "skipped"}:
        return "failure"
    return "partial"


def execute_weave(
    spark: SparkSession,
    plan: ExecutionPlan,
    threads: dict[str, Thread],
) -> WeaveResult:
    """Execute threads according to the execution plan.

    Processes each parallel group sequentially, submitting threads within a
    group to a :class:`~concurrent.futures.ThreadPoolExecutor` for concurrent
    execution. Respects ``on_failure`` config on each thread and manages the
    cache lifecycle via :class:`~weevr.engine.cache_manager.CacheManager`.

    Args:
        spark: Active SparkSession.
        plan: Immutable :class:`~weevr.engine.planner.ExecutionPlan` produced
            by :func:`~weevr.engine.planner.build_plan`.
        threads: Mapping of thread name to :class:`~weevr.model.thread.Thread` config.

    Returns:
        :class:`~weevr.engine.result.WeaveResult` with aggregate status and
        per-thread results.
    """
    start_ns = time.monotonic_ns()
    thread_states: dict[str, _ThreadState] = {name: "pending" for name in plan.threads}
    thread_results: list[ThreadResult] = []
    threads_skipped: list[str] = []
    cache = CacheManager(plan.cache_targets, plan.dependents)
    aborted = False

    logger.debug("Starting weave '%s' — %d threads", plan.weave_name, len(plan.threads))

    try:
        for group in plan.execution_order:
            if aborted:
                for name in group:
                    if thread_states[name] == "pending":
                        thread_states[name] = "skipped"
                        threads_skipped.append(name)
                continue

            # Separate threads that should run vs those already skipped
            to_run = [n for n in group if thread_states[n] == "pending"]
            for name in group:
                if thread_states[name] == "skipped":
                    threads_skipped.append(name)

            if not to_run:
                continue

            logger.debug(
                "Weave '%s' — executing group: %s",
                plan.weave_name,
                to_run,
            )

            future_to_name: dict[Future[ThreadResult], str] = {}
            with ThreadPoolExecutor(max_workers=len(to_run)) as executor:
                for name in to_run:
                    thread_states[name] = "running"
                    future_to_name[executor.submit(execute_thread, spark, threads[name])] = name

                for future in as_completed(future_to_name):
                    name = future_to_name[future]
                    try:
                        result = future.result()
                        thread_states[name] = "succeeded"
                        thread_results.append(result)
                        logger.debug("Thread '%s' succeeded", name)

                        # Persist cache if this thread is a cache target
                        if cache.is_cache_target(name):
                            target_path = threads[name].target.alias or threads[name].target.path
                            if target_path:
                                cache.persist(name, spark, target_path)

                        # Notify cache manager so it can unpersist when all consumers done
                        cache.notify_complete(name)

                    except Exception as exc:
                        thread_states[name] = "failed"
                        # Record a failure result
                        thread_results.append(
                            ThreadResult(
                                status="failure",
                                thread_name=name,
                                rows_written=0,
                                write_mode="",
                                target_path="",
                            )
                        )
                        logger.debug("Thread '%s' failed: %s", name, exc)

                        # Resolve on_failure: use thread config, fall back to abort_weave
                        thread_cfg = threads[name]
                        on_failure = (
                            thread_cfg.failure.on_failure
                            if thread_cfg.failure is not None
                            else "abort_weave"
                        )

                        downstream = _get_transitive_dependents(name, plan.dependents)

                        if on_failure == "abort_weave":
                            # Mark all remaining pending threads as skipped
                            for t in plan.threads:
                                if thread_states[t] == "pending":
                                    thread_states[t] = "skipped"
                            aborted = True
                            logger.debug(
                                "Thread '%s' abort_weave — remaining threads skipped", name
                            )
                        else:
                            # skip_downstream or continue: skip transitive dependents only
                            for dep in downstream:
                                if thread_states[dep] == "pending":
                                    thread_states[dep] = "skipped"
                            logger.debug(
                                "Thread '%s' %s — dependents skipped: %s",
                                name,
                                on_failure,
                                sorted(downstream),
                            )

    finally:
        cache.cleanup()

    # Collect any pending threads that weren't processed (shouldn't happen, but be safe)
    for name, state in thread_states.items():
        if state == "skipped" and name not in threads_skipped:
            threads_skipped.append(name)

    status = _compute_weave_status(thread_states)
    duration_ms = (time.monotonic_ns() - start_ns) // 1_000_000

    logger.debug(
        "Weave '%s' complete — status=%s, duration=%dms",
        plan.weave_name,
        status,
        duration_ms,
    )

    return WeaveResult(
        status=status,
        weave_name=plan.weave_name,
        thread_results=thread_results,
        threads_skipped=threads_skipped,
        duration_ms=duration_ms,
    )


def execute_loom(
    spark: SparkSession,
    loom: Loom,
    weaves: dict[str, Weave],
    threads: dict[str, dict[str, Thread]],
) -> LoomResult:
    """Execute weaves sequentially in declared order.

    Iterates the weaves listed in ``loom.weaves``, builds an
    :class:`~weevr.engine.planner.ExecutionPlan` for each, and executes them
    via :func:`execute_weave`. Stops on the first weave failure (no
    ``on_failure`` at loom level in M04).

    Args:
        spark: Active SparkSession.
        loom: Loom configuration declaring the ordered list of weave names.
        weaves: Mapping of weave name to :class:`~weevr.model.weave.Weave` config.
        threads: Nested mapping of ``weave_name → thread_name → Thread`` config.

    Returns:
        :class:`~weevr.engine.result.LoomResult` with aggregate status and
        per-weave results.
    """
    start_ns = time.monotonic_ns()
    weave_results: list[WeaveResult] = []

    logger.debug("Starting loom '%s' — %d weaves", loom.name, len(loom.weaves))

    for weave_name in loom.weaves:
        weave = weaves[weave_name]
        weave_threads = threads[weave_name]

        plan = build_plan(
            weave_name=weave_name,
            threads=weave_threads,
            thread_entries=list(weave.threads),
        )

        result = execute_weave(spark, plan, weave_threads)
        weave_results.append(result)

        if result.status == "failure":
            logger.debug(
                "Loom '%s' — weave '%s' failed, stopping loom execution",
                loom.name,
                weave_name,
            )
            break

    duration_ms = (time.monotonic_ns() - start_ns) // 1_000_000

    # Compute aggregate loom status
    statuses = {r.status for r in weave_results}
    if statuses <= {"success"}:
        loom_status: Literal["success", "failure", "partial"] = "success"
    elif "success" in statuses or "partial" in statuses:
        loom_status = "partial"
    else:
        loom_status = "failure"

    logger.debug(
        "Loom '%s' complete — status=%s, duration=%dms",
        loom.name,
        loom_status,
        duration_ms,
    )

    return LoomResult(
        status=loom_status,
        loom_name=loom.name,
        weave_results=weave_results,
        duration_ms=duration_ms,
    )
