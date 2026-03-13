"""Weave and loom execution runners."""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Literal

from pyspark.sql import SparkSession

from weevr.engine.cache_manager import CacheManager
from weevr.engine.conditions import evaluate_condition
from weevr.engine.executor import execute_thread
from weevr.engine.hooks import HookResult, run_hook_steps
from weevr.engine.lookups import LookupResult, cleanup_lookups, materialize_lookups
from weevr.engine.planner import ExecutionPlan, build_plan
from weevr.engine.result import LoomResult, ThreadResult, WeaveResult
from weevr.engine.variables import VariableContext
from weevr.errors.exceptions import HookError
from weevr.model.hooks import HookStep
from weevr.model.lookup import Lookup
from weevr.model.loom import Loom
from weevr.model.thread import Thread
from weevr.model.variable import VariableSpec
from weevr.model.weave import ConditionSpec, Weave
from weevr.telemetry.collector import SpanCollector
from weevr.telemetry.results import LoomTelemetry, ThreadTelemetry, WeaveTelemetry
from weevr.telemetry.span import SpanStatus, generate_trace_id

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
    collector: SpanCollector | None = None,
    parent_span_id: str | None = None,
    thread_conditions: dict[str, ConditionSpec] | None = None,
    params: dict[str, Any] | None = None,
    weave_span_label: str | None = None,
    pre_steps: Sequence[HookStep] | None = None,
    post_steps: Sequence[HookStep] | None = None,
    lookups: dict[str, Lookup] | None = None,
    variables: dict[str, VariableSpec] | None = None,
) -> WeaveResult:
    """Execute threads according to the execution plan.

    Processes each parallel group sequentially, submitting threads within a
    group to a :class:`~concurrent.futures.ThreadPoolExecutor` for concurrent
    execution. Respects ``on_failure`` config on each thread and manages the
    cache lifecycle via :class:`~weevr.engine.cache_manager.CacheManager`.

    When hooks are provided, the lifecycle is:
    1. Initialize ``VariableContext`` from ``variables``.
    2. Materialize lookups (``materialize=True``).
    3. Execute ``pre_steps``.
    4. Execute threads (with cached lookup DataFrames).
    5. Execute ``post_steps``.
    6. Cleanup lookups.

    Args:
        spark: Active SparkSession.
        plan: Immutable :class:`~weevr.engine.planner.ExecutionPlan` produced
            by :func:`~weevr.engine.planner.build_plan`.
        threads: Mapping of thread name to :class:`~weevr.model.thread.Thread` config.
        collector: Optional span collector for telemetry. When provided,
            per-thread collectors are created and merged after execution.
        parent_span_id: Optional parent span ID for trace tree linkage.
        thread_conditions: Mapping of thread name to condition spec. Threads
            with a condition that evaluates to False are skipped.
        params: Parameters for condition evaluation.
        weave_span_label: Label for the weave telemetry span. When set, used
            instead of ``plan.weave_name``. Typically the weave's qualified key.
        pre_steps: Optional pre-execution hook steps.
        post_steps: Optional post-execution hook steps.
        lookups: Optional weave-level lookup definitions.
        variables: Optional weave-level variable specs.

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

    # Create weave span if collector is active
    weave_span_builder = None
    weave_span_id = None
    span_name = weave_span_label or plan.weave_name
    if collector is not None:
        weave_span_builder = collector.start_span(
            f"weave:{span_name}", parent_span_id=parent_span_id
        )
        weave_span_id = weave_span_builder.span_id

    logger.debug("Starting weave '%s' — %d threads", plan.weave_name, len(plan.threads))

    # Initialize hook lifecycle components
    variable_ctx = VariableContext(variables)
    cached_lookup_dfs: dict[str, Any] = {}
    all_hook_results: list[HookResult] = []
    all_lookup_results: list[LookupResult] = []

    def _materialize_scheduled(sched_key: int) -> None:
        """Materialize lookups assigned to ``sched_key`` in the lookup schedule."""
        if not lookups or plan.lookup_schedule is None or sched_key not in plan.lookup_schedule:
            return
        subset = {k: v for k, v in lookups.items() if k in plan.lookup_schedule[sched_key]}
        if not subset:
            return
        new_cached, new_results = materialize_lookups(
            spark, subset, collector=collector, parent_span_id=weave_span_id
        )
        cached_lookup_dfs.update(new_cached)
        all_lookup_results.extend(new_results)

    try:
        # Materialize lookups: when a lookup schedule is available, defer
        # internal lookups to the correct group boundary. Otherwise (backward
        # compat) materialize everything upfront.
        if lookups and plan.lookup_schedule is not None:
            # Materialize only lookups scheduled at group 0 (external / pre-thread)
            _materialize_scheduled(0)
        elif lookups:
            cached_lookup_dfs, all_lookup_results = materialize_lookups(
                spark, lookups, collector=collector, parent_span_id=weave_span_id
            )

        # Execute pre-steps
        if pre_steps:
            try:
                pre_results = run_hook_steps(
                    spark,
                    pre_steps,
                    "pre",
                    variable_ctx,
                    params=params,
                    collector=collector,
                    parent_span_id=weave_span_id,
                )
                all_hook_results.extend(pre_results)
            except HookError:
                cleanup_lookups(cached_lookup_dfs)
                raise

        for group_idx, group in enumerate(plan.execution_order):
            # Materialize lookups whose producers completed in prior groups.
            # Schedule key N (N>=1) means "after group N-1 finishes" — at the
            # top of group N's iteration all prior groups have already executed.
            # Key 0 is handled before the loop (pre-thread external lookups).
            if group_idx > 0:
                _materialize_scheduled(group_idx)

            if aborted:
                for name in group:
                    if thread_states[name] == "pending":
                        thread_states[name] = "skipped"
                        threads_skipped.append(name)
                continue

            # Evaluate conditions for pending threads before execution
            conditions = thread_conditions or {}
            for name in group:
                if (
                    thread_states[name] == "pending"
                    and name in conditions
                    and not evaluate_condition(conditions[name], spark=spark, params=params)
                ):
                    thread_states[name] = "skipped"
                    threads_skipped.append(name)
                    thread_results.append(
                        ThreadResult(
                            status="skipped",
                            thread_name=name,
                            rows_written=0,
                            write_mode="",
                            target_path="",
                            skip_reason=conditions[name].when,
                        )
                    )
                    logger.debug(
                        "Thread '%s' skipped — condition '%s' is False",
                        name,
                        conditions[name].when,
                    )

            # Separate threads that should run vs those already skipped
            to_run = [n for n in group if thread_states[n] == "pending"]
            for name in group:
                if thread_states[name] == "skipped" and name not in threads_skipped:
                    threads_skipped.append(name)

            if not to_run:
                continue

            logger.debug(
                "Weave '%s' — executing group: %s",
                plan.weave_name,
                to_run,
            )

            future_to_name: dict[Future[ThreadResult], str] = {}
            # Per-thread collectors for isolation during concurrent execution
            thread_collectors: dict[str, SpanCollector] = {}

            with ThreadPoolExecutor(max_workers=len(to_run)) as executor:
                for name in to_run:
                    thread_states[name] = "running"
                    if collector is not None:
                        tc = SpanCollector(collector.trace_id)
                        thread_collectors[name] = tc
                        future_to_name[
                            executor.submit(
                                execute_thread,
                                spark,
                                threads[name],
                                collector=tc,
                                parent_span_id=weave_span_id,
                                cached_lookups=cached_lookup_dfs or None,
                                weave_lookups=lookups,
                            )
                        ] = name
                    else:
                        future_to_name[
                            executor.submit(
                                execute_thread,
                                spark,
                                threads[name],
                                cached_lookups=cached_lookup_dfs or None,
                                weave_lookups=lookups,
                            )
                        ] = name

                for future in as_completed(future_to_name):
                    name = future_to_name[future]
                    try:
                        result = future.result()
                        thread_states[name] = "succeeded"
                        thread_results.append(result)
                        logger.debug("Thread '%s' succeeded", name)

                        # Merge thread collector into weave collector
                        if name in thread_collectors and collector is not None:
                            collector.merge(thread_collectors[name])

                        # Persist cache if this thread is a cache target
                        if cache.is_cache_target(name):
                            target_path = threads[name].target.alias or threads[name].target.path
                            if target_path:
                                cache.persist(name, spark, target_path)

                        # Notify cache manager so it can unpersist when all consumers done
                        cache.notify_complete(name)

                    except Exception as exc:
                        thread_states[name] = "failed"
                        # Merge thread collector even on failure
                        if name in thread_collectors and collector is not None:
                            collector.merge(thread_collectors[name])

                        # Record a failure result with the error message
                        error_msg = f"{type(exc).__name__}: {exc}"
                        thread_results.append(
                            ThreadResult(
                                status="failure",
                                thread_name=name,
                                rows_written=0,
                                write_mode="",
                                target_path="",
                                error=error_msg,
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

        # Execute post-steps after all threads complete.
        # Note: row_counts is not populated, so row_count_delta checks in post-hooks
        # compare against before_count=0. Use row_count (absolute bounds) in post-hooks
        # for target validation instead.
        if post_steps:
            post_results = run_hook_steps(
                spark,
                post_steps,
                "post",
                variable_ctx,
                params=params,
                collector=collector,
                parent_span_id=weave_span_id,
            )
            all_hook_results.extend(post_results)

    finally:
        cache.cleanup()
        cleanup_lookups(cached_lookup_dfs)

    # Collect any pending threads that weren't processed (shouldn't happen, but be safe)
    for name, state in thread_states.items():
        if state == "skipped" and name not in threads_skipped:
            threads_skipped.append(name)

    status = _compute_weave_status(thread_states)
    duration_ms = (time.monotonic_ns() - start_ns) // 1_000_000

    # Finalize weave span and add to collector
    if weave_span_builder is not None and collector is not None:
        weave_span_status = SpanStatus.OK if status == "success" else SpanStatus.ERROR
        weave_span = weave_span_builder.finish(status=weave_span_status)
        collector.add_span(weave_span)

    # Build weave telemetry from thread results
    weave_telemetry = _build_weave_telemetry(
        span_name,
        thread_results,
        collector,
        hook_results=all_hook_results,
        lookup_results=all_lookup_results,
        variables=variable_ctx.snapshot(),
        resolved_params=params,
    )

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
        telemetry=weave_telemetry,
    )


def _build_weave_telemetry(
    weave_name: str,
    thread_results: list[ThreadResult],
    collector: SpanCollector | None,
    hook_results: list[HookResult] | None = None,
    lookup_results: list[LookupResult] | None = None,
    variables: dict[str, Any] | None = None,
    resolved_params: dict[str, Any] | None = None,
) -> WeaveTelemetry | None:
    """Build WeaveTelemetry from thread results."""
    if collector is None:
        return None

    from weevr.telemetry.span import ExecutionSpan, generate_span_id

    # Find the weave span from the collector
    spans = collector.get_spans()
    weave_span = None
    for s in spans:
        if s.name == f"weave:{weave_name}":
            weave_span = s
            break

    if weave_span is None:
        weave_span = ExecutionSpan(
            trace_id=collector.trace_id,
            span_id=generate_span_id(),
            name=f"weave:{weave_name}",
            status=SpanStatus.OK,
        )

    thread_telemetry: dict[str, ThreadTelemetry] = {}
    for tr in thread_results:
        if tr.telemetry is not None:
            thread_telemetry[tr.thread_name] = tr.telemetry

    return WeaveTelemetry(
        span=weave_span,
        thread_telemetry=thread_telemetry,
        hook_results=hook_results or [],
        lookup_results=lookup_results or [],
        variables=variables or {},
        resolved_params=resolved_params,
    )


def execute_loom(
    spark: SparkSession,
    loom: Loom,
    weaves: dict[str, Weave],
    threads: dict[str, dict[str, Thread]],
    params: dict[str, Any] | None = None,
) -> LoomResult:
    """Execute weaves sequentially in declared order.

    Iterates the weaves listed in ``loom.weaves``, builds an
    :class:`~weevr.engine.planner.ExecutionPlan` for each, and executes them
    via :func:`execute_weave`. Creates a root span collector for telemetry
    and passes it through the execution tree.

    Args:
        spark: Active SparkSession.
        loom: Loom configuration declaring the ordered list of weave names.
        weaves: Mapping of weave name to :class:`~weevr.model.weave.Weave` config.
        threads: Nested mapping of ``weave_name → thread_name → Thread`` config.
        params: Parameters for condition evaluation at weave and thread levels.

    Returns:
        :class:`~weevr.engine.result.LoomResult` with aggregate status and
        per-weave results.
    """
    start_ns = time.monotonic_ns()
    weave_results: list[WeaveResult] = []

    # Create root collector and loom span
    trace_id = generate_trace_id()
    collector = SpanCollector(trace_id)
    loom_span_label = loom.qualified_key or loom.name
    loom_span_builder = collector.start_span(f"loom:{loom_span_label}")
    loom_span_id = loom_span_builder.span_id

    logger.debug("Starting loom '%s' — %d weaves", loom.name, len(loom.weaves))

    for weave_entry in loom.weaves:
        weave_name = weave_entry.name or (Path(weave_entry.ref).stem if weave_entry.ref else "")

        # Evaluate weave-level condition
        if weave_entry.condition is not None and not evaluate_condition(
            weave_entry.condition, spark=spark, params=params
        ):
            logger.debug(
                "Loom '%s' — weave '%s' skipped (condition: '%s')",
                loom.name,
                weave_name,
                weave_entry.condition.when,
            )
            weave_results.append(
                WeaveResult(
                    status="skipped",
                    weave_name=weave_name,
                    thread_results=[],
                    threads_skipped=[],
                    duration_ms=0,
                    skip_reason=weave_entry.condition.when,
                )
            )
            continue

        weave = weaves[weave_name]
        weave_threads = threads[weave_name]

        weave_lookups = dict(weave.lookups) if weave.lookups else None
        plan = build_plan(
            weave_name=weave_name,
            threads=weave_threads,
            thread_entries=list(weave.threads),
            lookups=weave_lookups,
        )

        # Build thread condition map from ThreadEntry conditions
        thread_conditions: dict[str, ConditionSpec] = {}
        for te in weave.threads:
            if te.condition is not None:
                thread_conditions[te.name] = te.condition

        result = execute_weave(
            spark,
            plan,
            weave_threads,
            collector=collector,
            parent_span_id=loom_span_id,
            thread_conditions=thread_conditions if thread_conditions else None,
            params=params,
            weave_span_label=weave.qualified_key or weave_name,
            pre_steps=list(weave.pre_steps) if weave.pre_steps else None,
            post_steps=list(weave.post_steps) if weave.post_steps else None,
            lookups=dict(weave.lookups) if weave.lookups else None,
            variables=dict(weave.variables) if weave.variables else None,
        )
        weave_results.append(result)

        if result.status == "failure":
            logger.debug(
                "Loom '%s' — weave '%s' failed, stopping loom execution",
                loom.name,
                weave_name,
            )
            break

    duration_ms = (time.monotonic_ns() - start_ns) // 1_000_000

    # Compute aggregate loom status (skipped weaves don't count as failures)
    statuses = {r.status for r in weave_results}
    if statuses <= {"success", "skipped"}:
        loom_status: Literal["success", "failure", "partial"] = "success"
    elif "success" in statuses or "partial" in statuses:
        loom_status = "partial"
    else:
        loom_status = "failure"

    # Finalize loom span and add to collector
    loom_span_status = SpanStatus.OK if loom_status == "success" else SpanStatus.ERROR
    loom_span = loom_span_builder.finish(status=loom_span_status)
    collector.add_span(loom_span)

    # Build loom telemetry
    loom_telemetry = _build_loom_telemetry(
        loom_span_label, weave_results, collector, resolved_params=params
    )

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
        telemetry=loom_telemetry,
    )


def _build_loom_telemetry(
    loom_name: str,
    weave_results: list[WeaveResult],
    collector: SpanCollector,
    resolved_params: dict[str, Any] | None = None,
) -> LoomTelemetry:
    """Build LoomTelemetry from weave results."""
    from weevr.telemetry.span import ExecutionSpan, generate_span_id

    # Find the loom span from the collector
    spans = collector.get_spans()
    loom_span = None
    for s in spans:
        if s.name == f"loom:{loom_name}":
            loom_span = s
            break

    if loom_span is None:
        loom_span = ExecutionSpan(
            trace_id=collector.trace_id,
            span_id=generate_span_id(),
            name=f"loom:{loom_name}",
            status=SpanStatus.OK,
        )

    weave_telemetry: dict[str, WeaveTelemetry] = {}
    for wr in weave_results:
        if wr.telemetry is not None:
            weave_telemetry[wr.weave_name] = wr.telemetry

    return LoomTelemetry(
        span=loom_span,
        weave_telemetry=weave_telemetry,
        resolved_params=resolved_params,
    )
