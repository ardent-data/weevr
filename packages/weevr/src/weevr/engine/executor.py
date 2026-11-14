"""Thread executor — orchestrates the full read → transform → write pipeline."""

from __future__ import annotations

import contextlib
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import Observation, SparkSession
from pyspark.sql import functions as F

from weevr.config.audit_templates import AuditContext
from weevr.delta import read_delta
from weevr.engine.cache_manager import CacheManager
from weevr.engine.column_sets import materialize_column_sets
from weevr.engine.hooks import run_hook_steps, snapshot_row_counts
from weevr.engine.lookups import (
    UniqueKeyMemo,
    build_lookup_meta,
    cleanup_lookups,
    materialize_lookups,
    resolve_lookup_frames,
)
from weevr.engine.resources import merge_resource_dicts
from weevr.engine.result import ThreadResult
from weevr.engine.target_identity import resolve_target_identity
from weevr.engine.variables import VariableContext
from weevr.errors.exceptions import (
    DataValidationError,
    ExecutionError,
    ExportError,
    SchemaDriftError,
    StateError,
    WarpEnforcementError,
)
from weevr.model.column_set import ColumnSet
from weevr.model.connection import OneLakeConnection
from weevr.model.load import CdcConfig, LoadConfig
from weevr.model.lookup import Lookup
from weevr.model.seed import SeedConfig
from weevr.model.thread import Thread
from weevr.model.warp import DriftReport, WarpConfig
from weevr.model.write import WriteConfig
from weevr.operations.assertions import evaluate_assertions
from weevr.operations.audit import build_sources_json, inject_audit_columns
from weevr.operations.dimension import DimensionMergeBuilder, execute_dimension_merge
from weevr.operations.drift import handle_drift
from weevr.operations.exports import resolve_exports, write_export
from weevr.operations.fact import check_fact_sentinels, validate_fact_schema
from weevr.operations.hashing import compute_dimension_keys, compute_keys
from weevr.operations.naming import normalize_columns
from weevr.operations.pipeline import LookupMeta, ObservationRegistry, run_pipeline
from weevr.operations.pipeline._observations import read_observation
from weevr.operations.quarantine import write_quarantine
from weevr.operations.readers import (
    apply_source_dedup,
    compute_generic_cdc_hwm,
    read_cdc_source,
    read_source,
    read_source_incremental,
)
from weevr.operations.seeding import build_system_member_rows, execute_seeds
from weevr.operations.target_handle import CommitStamp, TargetHandle
from weevr.operations.validation import validate_dataframe
from weevr.operations.warp import (
    append_warp_only_columns,
    auto_generate_warp,
    enforce_warp,
    pre_initialize_table,
)
from weevr.operations.writers import (
    apply_target_mapping,
    execute_cdc_merge,
    existing_target_columns,
    narrow_to_target,
    seed_cdc_target,
    write_target,
)
from weevr.state import WatermarkState, WatermarkStore, resolve_store
from weevr.telemetry.collector import SpanBuilder, SpanCollector
from weevr.telemetry.results import ExportResult, ThreadTelemetry
from weevr.telemetry.span import ExecutionSpan, SpanStatus, generate_span_id, generate_trace_id

logger = logging.getLogger(__name__)


@dataclass
class _ThreadCleanup:
    """Resources a thread run must release, registered the moment they exist.

    Phase helpers record what they create here, so a helper that fails after
    persisting a frame still has it released by ``execute_thread``'s
    ``finally``.
    """

    thread_lookups: dict[str, Any] = field(default_factory=dict)
    cdc_window: Any = None
    working_frame: Any = None
    quarantine_frame: Any = None

    def release(self) -> None:
        """Release thread-level lookups, then every persisted frame.

        Lookup cleanup is not suppressed; each frame release suppresses its
        own failure.
        """
        if self.thread_lookups:
            cleanup_lookups(self.thread_lookups)
        for frame in (self.cdc_window, self.working_frame, self.quarantine_frame):
            if frame is not None:
                with contextlib.suppress(Exception):
                    frame.unpersist()


@dataclass
class _ThreadResources:
    """Weave-level resources merged with the thread's own (thread wins)."""

    cached_lookups: dict[str, Any] | None
    weave_lookups: dict[str, Lookup] | None
    column_sets: dict[str, dict[str, str]] | None
    column_set_defs: dict[str, ColumnSet] | None
    lookup_meta: dict[str, LookupMeta] | None


def execute_thread(
    spark: SparkSession,
    thread: Thread,
    collector: SpanCollector | None = None,
    parent_span_id: str | None = None,
    cached_lookups: dict[str, Any] | None = None,
    weave_lookups: dict[str, Lookup] | None = None,
    resolved_params: dict[str, Any] | None = None,
    loom_name: str = "",
    weave_name: str = "",
    column_sets: dict[str, dict[str, str]] | None = None,
    column_set_defs: dict[str, ColumnSet] | None = None,
    connections: dict[str, OneLakeConnection] | None = None,
    lookup_meta: dict[str, LookupMeta] | None = None,
    capture_samples: bool = False,
    cache_registry: CacheManager | None = None,
    uk_memo: UniqueKeyMemo | None = None,
) -> ThreadResult:
    """Execute a single thread from sources through transforms to a Delta target.

    Execution order:
    1. Initialize thread-level resources (variables, lookups, column_sets).
    2. Run thread-level pre_steps.
    3. Resolve the target write path and Warp (pre-initializing the table
       when ``warp_init`` is set), and load prior watermark state.
    4. Resolve lookup-based sources and read all remaining declared sources.
       - For ``incremental_watermark``: apply the prior HWM filter, capture new HWM.
       - For ``cdc``: read via CDF or generic CDC source.
    5. Set the primary (first) source as the working DataFrame.
    6. Run pipeline steps against the working DataFrame.
    7. Run validation rules (if configured) — quarantine or abort on failures.
    8. Apply naming normalization (if configured).
    9. Compute business keys and hashes if configured.
    10. Apply target column mapping.
    11. Inject audit columns (if configured; bypasses mapping mode).
    12. Seed and write to the Delta target.
        - For dimensions: dimension merge; for ``cdc``: CDC merge routing.
    13. Run the fact sentinel advisory and auto warp generation.
    14. Persist watermark state (if applicable).
    15. Run post-write assertions (if configured).
    16. Write exports (secondary outputs, if configured).
    17. Run thread-level post_steps.
    18. Build telemetry and return ThreadResult.

    Args:
        spark: Active SparkSession.
        thread: Thread configuration to execute.
        collector: Optional span collector for telemetry. When provided,
            a thread span is created and finalized.
        parent_span_id: Optional parent span ID for trace tree linkage.
        cached_lookups: Pre-materialized lookup DataFrames keyed by lookup name.
            Runtime type is ``dict[str, DataFrame]``; ``Any`` avoids coupling
            the signature to PySpark types.
        weave_lookups: Weave-level lookup definitions for on-demand resolution
            of non-materialized lookups.
        resolved_params: Runtime parameter values for telemetry capture.
            Stored on ThreadTelemetry for thread-level runs.
        loom_name: Loom name for audit column context variables.
        weave_name: Weave name for audit column context variables.
            Derived from the prefix of ``thread.qualified_key`` when not
            provided. Falls back to empty string if no dot separator exists.
        column_sets: Pre-resolved column set mappings keyed by name. Passed
            through to ``run_pipeline`` so rename steps can look up their
            resolved dictionaries.
        column_set_defs: Column set model instances keyed by name. Passed
            through to ``run_pipeline`` so rename steps can read
            ``on_unmapped`` and ``on_extra`` settings.
        connections: Named connection declarations keyed by connection name.
            When provided, source reads, lookup materialization, target path
            resolution, and export writes use connection-based paths where
            configured.
        lookup_meta: Materialization-time lookup facts inherited from the
            weave/loom scope; thread-level facts merge on top.
        capture_samples: Whether to capture 10-row output/quarantine
            samples for display (effective ``execution.capture_samples``).
            Off by default — sampling re-executes the pipeline lineage.
        cache_registry: Weave-level cache registry; Delta-backed sources
            whose canonical identity matches a registered producer are
            served the post-write snapshot instead of a storage read.
            A miss falls back to a direct read.
        uk_memo: Per-weave validate-exactly-once memo for non-materialized
            ``unique_key`` lookups.

    Returns:
        :class:`ThreadResult` with status, row count, write mode, target path,
        and optional telemetry.

    Raises:
        ExecutionError: If any step fails, with ``thread_name`` set on the error.
        DataValidationError: If a fatal-severity validation rule fails.
        ExportError: If an export with ``on_failure: abort`` fails.
    """
    # Generate per-execution run context (shared across audit columns and exports)
    run_timestamp = datetime.now(UTC).isoformat()
    run_id = str(uuid.uuid4())

    span_builder = None
    if collector is not None:
        # For aliased template threads, prefer the effective name (alias)
        # over qualified_key (which retains the raw ref path).
        span_label = thread.name if thread.template_ref else thread.qualified_key or thread.name
        span_builder = collector.start_span(f"thread:{span_label}", parent_span_id=parent_span_id)
        if thread.template_ref:
            span_builder.set_attribute("thread.template_ref", thread.template_ref)

    load_mode = thread.load.mode if thread.load else "full"

    # Fail-fast: validate incremental config cross-cutting constraints
    if load_mode != "full":
        _validate_incremental(thread, load_mode)

    # --- Thread-level resource lifecycle ---
    thread_variable_ctx = _build_thread_variable_context(thread)
    cleanup = _ThreadCleanup()

    try:
        resources = _prepare_thread_resources(
            spark,
            thread,
            cleanup,
            cached_lookups=cached_lookups,
            weave_lookups=weave_lookups,
            column_sets=column_sets,
            column_set_defs=column_set_defs,
            lookup_meta=lookup_meta,
            resolved_params=resolved_params,
            connections=connections,
        )

        # Run thread-level pre_steps
        if thread.pre_steps:
            run_hook_steps(
                spark,
                thread.pre_steps,
                "pre",
                thread_variable_ctx,
                params=resolved_params,
            )

        # Baseline for row_count_delta post-steps, before the core pipeline runs.
        post_row_counts = snapshot_row_counts(spark, thread.post_steps)

        # --- Core execution pipeline ---
        try:
            # Resolve target path early (needed for watermark store resolution)
            target_path = _resolve_target_path(thread, connections)

            # One existence/identity resolution per thread. Refreshed after
            # every engine-internal commit (warp pre-init, seeds) so
            # downstream consumers observe post-commit state exactly as
            # their previous independent probes did.
            target_handle = TargetHandle(spark, target_path)

            resolved_warp = _resolve_warp(spark, thread, target_path, target_handle)
            watermark_store, prior_state = _load_watermark_state(
                spark, thread, load_mode, target_path, target_handle
            )

            # --- Steps 3-4: Resolve lookup-based sources, read the rest ---
            source_read = _read_sources(
                spark,
                thread,
                load_mode,
                prior_state,
                cleanup,
                cached_lookups=resources.cached_lookups,
                weave_lookups=resources.weave_lookups,
                lookup_meta=resources.lookup_meta,
                connections=connections,
                cache_registry=cache_registry,
                uk_memo=uk_memo,
            )
            new_hwm = source_read.new_hwm

            # Step statistics attach as Observations (harvested after the
            # write); CTE sub-pipelines contribute under their own scopes.
            obs_registry = ObservationRegistry(scope="main")

            # Merge-terminal paths (dimension, CDC, merge write mode) fix
            # observations with ZERO rows — the merge's internal execution
            # fires observed nodes without flowing source rows through them
            # (locked in test_merge_observation_semantics). Those paths keep
            # eager counts, which double as the action that fixes the step
            # observations with true full-scale values. Single-pass write
            # paths (overwrite/append) go fully lazy.
            merge_terminal = (
                thread.target.dimension is not None
                or load_mode == "cdc"
                or (thread.write is not None and thread.write.mode == "merge")
            )

            # Steps 5-6 — CTEs, primary DataFrame, pipeline steps, counts
            transformed = _run_transforms(
                thread,
                source_read.sources_map,
                merge_terminal,
                obs_registry,
                column_sets=resources.column_sets,
                column_set_defs=resources.column_set_defs,
                resolve_lookups=source_read.resolve_lookups,
                resolve_meta=source_read.resolve_meta,
                collector=collector,
                span_builder=span_builder,
            )

            # Steps 7-9b — validation and quarantine, naming, keys, SCD columns,
            # fact schema check
            shaped = _validate_and_key(
                spark, thread, transformed.df, target_path, cleanup, capture_samples, run_timestamp
            )

            # Step 10 — resolve and prepare audit columns
            audit_columns = thread.target.audit_columns or {}
            audit_ctx: AuditContext | None = None
            if audit_columns:
                audit_ctx = _build_audit_context(
                    thread, weave_name, loom_name, run_timestamp, run_id
                )

            # Step 11/12/13 — write to Delta
            write_mode = thread.write.mode if thread.write else "overwrite"

            # Lineage stamp for the counted write's commit — carries the
            # same run identity as audit columns and exports, so table
            # history joins to telemetry via run_id.
            commit_stamp = CommitStamp(
                run_id=run_id,
                thread=thread.name,
                loom=loom_name or None,
                weave=weave_name or None,
                mode=write_mode,
                # An incremental append records its new HWM atomically with the
                # data, so a failed state save is recoverable from the commit
                hwm=new_hwm
                if load_mode == "incremental_watermark" and write_mode == "append"
                else None,
            )

            write_ctx = _WriteContext(
                target_path=target_path,
                target_handle=target_handle,
                resolved_warp=resolved_warp,
                audit_columns=audit_columns,
                audit_ctx=audit_ctx,
                commit_stamp=commit_stamp,
                run_timestamp=run_timestamp,
                capture_samples=capture_samples,
                cleanup=cleanup,
            )
            if thread.target.dimension is not None and shaped.dim_builder is not None:
                written = _write_dimension(
                    spark,
                    thread,
                    shaped.df,
                    shaped.dim_builder,
                    write_ctx,
                    rows_quarantined=shaped.rows_quarantined,
                    rows_after_transforms=transformed.rows_after_transforms,
                )
            elif load_mode == "cdc" and thread.load is not None and thread.load.cdc is not None:
                written = _write_cdc(
                    spark, thread, shaped.df, thread.load, thread.load.cdc, write_ctx
                )
            else:
                written = _write_standard(spark, thread, shaped.df, write_ctx)

            # A successful write whose count could not be attributed reads
            # as unknown, never as zero — surface it to the caller.
            thread_warnings: list[str] = []
            if written.rows_written is None:
                thread_warnings.append(
                    f"Thread '{thread.name}': rows written to '{target_path}' "
                    f"could not be attributed to the engine's commit; "
                    f"reported as unknown"
                )

            # Steps 13b-13c — fact sentinel advisory, auto warp generation
            _run_post_write_checks(spark, thread, written, target_path, resolved_warp)

            # Step 14 — persist watermark state
            watermark_persisted = _persist_watermark(
                spark, thread, load_mode, watermark_store, new_hwm
            )

            # Step 15 — run post-write assertions
            assertion_results: list[Any] = []
            if thread.assertions:
                assertion_results = evaluate_assertions(spark, thread.assertions, target_path)

            # Step 16 — write exports (secondary outputs)
            export_results: list[ExportResult] = []
            if thread.exports:
                # Build AuditContext for export path resolution if not already built
                export_ctx = (
                    audit_ctx
                    if audit_ctx is not None
                    else _build_audit_context(thread, weave_name, loom_name, run_timestamp, run_id)
                )
                export_results = _write_exports(spark, thread, written, export_ctx, connections)

            # Run thread-level post_steps (after core execution, before result)
            if thread.post_steps:
                run_hook_steps(
                    spark,
                    thread.post_steps,
                    "post",
                    thread_variable_ctx,
                    params=resolved_params,
                    row_counts=post_row_counts,
                )

            # Step 18 — build result. Harvest step observations — fulfilled
            # by the write (or the first action that executed the plan).
            # Best-effort by design.
            step_stats = obs_registry.harvest() or None
            rows_read, rows_after_transforms = _harvest_row_counts(transformed)
            drift_report = written.drift_report
            telemetry = _build_telemetry(
                span_builder,
                shaped.validation_results,
                assertion_results,
                rows_read,
                written.rows_written,
                shaped.rows_quarantined,
                collector=collector,
                step_stats=step_stats,
                load_mode=load_mode,
                watermark_column=thread.load.watermark_column if thread.load else None,
                watermark_previous_value=prior_state.last_value if prior_state else None,
                watermark_new_value=new_hwm,
                watermark_persisted=watermark_persisted,
                watermark_first_run=watermark_store is not None and prior_state is None,
                cdc_counts=written.cdc_counts,
                rows_after_transforms=rows_after_transforms,
                resolved_params=resolved_params,
                audit_columns_applied=written.audit_columns_applied,
                export_results=export_results,
                warp_name=resolved_warp.description if resolved_warp else None,
                warp_source="explicit"
                if isinstance(thread.target.warp, str)
                else ("auto" if resolved_warp is not None else None),
                warp_enforcement_mode=thread.target.warp_enforcement if resolved_warp else None,
                drift_detected=drift_report.has_drift if drift_report else False,
                drift_columns=drift_report.extra_columns if drift_report else None,
                drift_missing_columns=drift_report.missing_columns if drift_report else None,
                drift_mode=drift_report.drift_mode if drift_report else None,
                drift_action_taken=drift_report.action_taken if drift_report else None,
            )

            return ThreadResult(
                status="success",
                thread_name=thread.name,
                rows_written=written.rows_written,
                write_mode=write_mode,
                target_path=target_path,
                telemetry=telemetry,
                output_schema=written.output_schema,
                samples=_result_samples(written.output_sample, shaped.quarantine_sample),
                drift_report=_drift_report_dict(drift_report),
                warp_findings=written.warp_findings or None,
                warnings=thread_warnings or None,
            )

        except DataValidationError:
            _finish_error_span(span_builder, collector)
            raise

        except (WarpEnforcementError, SchemaDriftError) as exc:
            _finish_error_span(span_builder, collector)
            if exc.thread_name is None:
                exc.thread_name = thread.name
            raise

        except ExecutionError as exc:
            _finish_error_span(span_builder, collector)
            if exc.thread_name is None:
                raise ExecutionError(
                    exc.message,
                    cause=exc.cause,
                    thread_name=thread.name,
                    step_index=exc.step_index,
                    step_type=exc.step_type,
                    source_name=exc.source_name,
                ) from exc
            raise
        except Exception as exc:
            _finish_error_span(span_builder, collector)
            raise ExecutionError(
                f"Thread '{thread.name}' failed unexpectedly",
                cause=exc,
                thread_name=thread.name,
            ) from exc

    finally:
        # Thread-level resources only; weave-level cleanup is the caller's job
        cleanup.release()


@dataclass
class _SourceRead:
    """Sources read for a thread run, keyed by alias, plus resolve-step context."""

    sources_map: dict[str, Any]
    new_hwm: str | None
    resolve_lookups: dict[str, Any]
    resolve_meta: dict[str, LookupMeta] | None


@dataclass
class _Transformed:
    """The working DataFrame after pipeline steps, with its row counts.

    Merge-terminal paths count eagerly; single-pass paths carry
    observations that the write fulfils.
    """

    df: Any
    rows_read: int
    rows_read_obs: Observation | None
    rows_after_transforms: int
    rows_after_transforms_obs: Observation | None


@dataclass
class _Shaped:
    """The working DataFrame after validation, naming, keys, and SCD columns."""

    df: Any
    validation_results: list[Any]
    rows_quarantined: int
    quarantine_sample: list[dict[str, Any]] | None
    dim_builder: DimensionMergeBuilder | None


@dataclass
class _WriteContext:
    """What every write route needs besides the working DataFrame."""

    target_path: str
    target_handle: TargetHandle
    resolved_warp: WarpConfig | None
    audit_columns: dict[str, str]
    audit_ctx: AuditContext | None
    commit_stamp: CommitStamp
    run_timestamp: str
    capture_samples: bool
    cleanup: _ThreadCleanup


@dataclass
class _WriteOutcome:
    """The result of one write route."""

    df: Any
    rows_written: int | None
    warp_findings: list[dict[str, str]]
    drift_report: DriftReport | None
    audit_columns_applied: list[str]
    output_schema: list[tuple[str, str]]
    output_sample: list[dict[str, Any]] | None
    cdc_counts: dict[str, int] | None = None


def _prepare_thread_resources(
    spark: SparkSession,
    thread: Thread,
    cleanup: _ThreadCleanup,
    *,
    cached_lookups: dict[str, Any] | None,
    weave_lookups: dict[str, Lookup] | None,
    column_sets: dict[str, dict[str, str]] | None,
    column_set_defs: dict[str, ColumnSet] | None,
    lookup_meta: dict[str, LookupMeta] | None,
    resolved_params: dict[str, Any] | None,
    connections: dict[str, OneLakeConnection] | None,
) -> _ThreadResources:
    """Materialize the thread's own lookups and column sets over the weave's.

    Materialized thread lookups are registered with ``cleanup`` before any
    later step can fail.
    """
    resources = _ThreadResources(
        cached_lookups=cached_lookups,
        weave_lookups=weave_lookups,
        column_sets=column_sets,
        column_set_defs=column_set_defs,
        lookup_meta=lookup_meta,
    )

    # Materialize thread-level lookups and merge with weave-level
    if thread.lookups:
        thread_cached_lookups, thread_lookup_results = materialize_lookups(
            spark, thread.lookups, connections=connections
        )
        cleanup.thread_lookups = thread_cached_lookups
        all_cached: dict[str, Any] = {}
        if cached_lookups:
            all_cached.update(cached_lookups)
        all_cached.update(thread_cached_lookups)  # thread wins
        # A thread lookup that isn't materialized must not be served an
        # inherited frame of the same name
        for name in thread.lookups:
            if name not in thread_cached_lookups:
                all_cached.pop(name, None)
        resources.cached_lookups = all_cached or None
        # Thread-scoped materialization facts join the inherited map so
        # resolve steps can reuse unique-key outcomes at every scope
        resources.lookup_meta = build_lookup_meta(thread_lookup_results, base=lookup_meta)
        # Merge lookup definitions so resolve_thread_lookups sees both
        resources.weave_lookups = merge_resource_dicts(
            weave_lookups, thread.lookups, "lookup", "weave", "thread"
        )

    # Merge thread-level column_sets with weave-level (thread wins).
    # Only the THREAD's defs materialize here — the weave's mappings
    # arrive already resolved and merge as plain dicts, so a weave set
    # resolves once per weave regardless of thread count
    if thread.column_sets:
        merged_cs_defs = merge_resource_dicts(
            column_set_defs, thread.column_sets, "column_set", "weave", "thread"
        )
        if merged_cs_defs:
            resources.column_set_defs = merged_cs_defs
        thread_resolved_cs, _ = materialize_column_sets(
            spark,
            dict(thread.column_sets),
            resolved_params or {},
            connections=connections,
        )
        resources.column_sets = {**(column_sets or {}), **thread_resolved_cs}

    return resources


def _resolve_target_path(thread: Thread, connections: dict[str, OneLakeConnection] | None) -> str:
    """Resolve the Target's write location from alias, path, or connection.

    Raises:
        ExecutionError: If connection resolution fails or no location is set.
    """
    target_path = thread.target.alias or thread.target.path
    if target_path is None and thread.target.connection:
        conn_name = thread.target.connection
        from weevr.config.paths import resolve_connection_path

        try:
            target_path = resolve_connection_path(
                conn_name,
                thread.target.table,
                thread.target.schema_override,
                connections,
            )
        except ValueError as exc:
            raise ExecutionError(
                f"Target connection resolution failed: {exc}",
                thread_name=thread.name,
            ) from exc
        logger.debug(
            "Thread '%s': resolved target connection '%s' → %s",
            thread.name,
            conn_name,
            target_path,
        )
    if target_path is None:
        raise ExecutionError(
            "Target has no 'alias', 'path', or 'connection' — cannot resolve write location",
            thread_name=thread.name,
        )
    return target_path


def _resolve_warp(
    spark: SparkSession, thread: Thread, target_path: str, target_handle: TargetHandle
) -> WarpConfig | None:
    """Resolve the Thread's Warp and pre-initialize the target when asked.

    The loader resolves warps from the thread file and project root. A
    thread built without the loader has no binding; it keeps the original
    lookup beside the target, explicit references only.
    """
    resolved_warp: WarpConfig | None = None
    if thread.warp_binding is not None:
        resolved_warp = thread.warp_binding.warp
    elif isinstance(thread.target.warp, str):
        from weevr.config.warp import resolve_warp

        thread_dir = Path(target_path).parent if target_path else Path(".")
        config_root = thread_dir
        resolved_warp = resolve_warp(
            warp_ref=thread.target.warp,
            thread_dir=thread_dir,
            config_root=config_root,
            target_alias=thread.target.alias,
        )

    # --- Warp pre-initialization ---
    # Note: SCD and soft-delete columns are not yet known at this
    # point. Dimension targets using warp_init should declare SCD
    # columns in the warp itself.
    if resolved_warp is not None and thread.target.warp_init:
        from weevr.config.warp import build_effective_warp

        engine_col_names = list((thread.target.audit_columns or {}).keys())
        effective_warp = build_effective_warp(resolved_warp.columns, [], engine_col_names)
        pre_initialize_table(spark, target_path, effective_warp)
        target_handle.refresh()

    return resolved_warp


def _load_watermark_state(
    spark: SparkSession,
    thread: Thread,
    load_mode: str,
    target_path: str,
    target_handle: TargetHandle,
) -> tuple[WatermarkStore | None, WatermarkState | None]:
    """Load the prior Watermark state for incremental and CDC threads.

    Returns ``(None, None)`` for load modes that keep no state.
    """
    if load_mode not in ("incremental_watermark", "cdc") or thread.load is None:
        return None, None
    watermark_store = resolve_store(thread.load, target_path)
    prior_state = watermark_store.read(spark, thread.name)
    if (
        load_mode == "incremental_watermark"
        and thread.write is not None
        and thread.write.mode == "append"
    ):
        prior_state = _reconcile_stamped_hwm(spark, thread, prior_state, target_handle, target_path)
    logger.debug(
        "Thread '%s': load_mode=%s, first_run=%s, prior_value=%s",
        thread.name,
        load_mode,
        prior_state is None,
        prior_state.last_value if prior_state else None,
    )
    return watermark_store, prior_state


def _read_sources(
    spark: SparkSession,
    thread: Thread,
    load_mode: str,
    prior_state: WatermarkState | None,
    cleanup: _ThreadCleanup,
    *,
    cached_lookups: dict[str, Any] | None,
    weave_lookups: dict[str, Lookup] | None,
    lookup_meta: dict[str, LookupMeta] | None,
    connections: dict[str, OneLakeConnection] | None,
    cache_registry: CacheManager | None,
    uk_memo: UniqueKeyMemo | None,
) -> _SourceRead:
    """Resolve lookup-bound sources and read every other declared Source.

    The primary (first) Source is read per load mode — watermark-filtered,
    as a CDC change window, or in full; the new high-water mark is captured
    alongside.
    """
    # lookup_dfs: frames for lookup-bound sources, by source alias.
    # resolve_lookups: what resolve steps see — every visible lookup
    # they name, by lookup name, with bound aliases winning a clash.
    lookup_dfs: dict[str, Any] = {}
    resolve_lookups: dict[str, Any] = {}
    resolve_meta = lookup_meta
    if cached_lookups is not None or weave_lookups is not None:
        lookup_dfs, resolve_lookups = resolve_lookup_frames(
            thread,
            weave_lookups or {},
            cached_lookups or {},
            spark,
            connections=connections,
            uk_memo=uk_memo,
        )
        resolve_meta = _meta_for_resolve(thread, lookup_meta)

    new_hwm: str | None = None
    if load_mode == "incremental_watermark" and thread.load is not None:
        # Read primary source with watermark filter
        primary_alias = next(iter(thread.sources))
        primary_df, new_hwm = read_source_incremental(
            spark,
            primary_alias,
            thread.sources[primary_alias],
            thread.load,
            prior_state,
            connections=connections,
        )
        sources_map = _with_secondary_sources(
            spark, thread, primary_alias, primary_df, lookup_dfs, connections, cache_registry
        )
    elif load_mode == "cdc" and thread.load is not None and thread.load.cdc is not None:
        primary_alias = next(iter(thread.sources))
        primary_df, new_hwm = _read_cdc_window(
            spark, thread, thread.load, thread.load.cdc, prior_state, cleanup, connections
        )
        sources_map = _with_secondary_sources(
            spark, thread, primary_alias, primary_df, lookup_dfs, connections, cache_registry
        )
    else:
        # Full mode or incremental_parameter — read non-lookup
        # sources, serving registered cache snapshots when the
        # source's identity matches a same-run producer
        normal_sources = {k: v for k, v in thread.sources.items() if k not in lookup_dfs}
        sources_map = {
            alias_name: _read_source_via_registry(
                spark, alias_name, src, connections, cache_registry
            )
            for alias_name, src in normal_sources.items()
        }
        sources_map.update(lookup_dfs)

    return _SourceRead(
        sources_map=sources_map,
        new_hwm=new_hwm,
        resolve_lookups=resolve_lookups,
        resolve_meta=resolve_meta,
    )


def _with_secondary_sources(
    spark: SparkSession,
    thread: Thread,
    primary_alias: str,
    primary_df: Any,
    lookup_dfs: dict[str, Any],
    connections: dict[str, OneLakeConnection] | None,
    cache_registry: CacheManager | None,
) -> dict[str, Any]:
    """Build the sources map around an already-read primary Source.

    Secondary sources (the primary keeps its filtered read — snapshot
    semantics don't apply to it) consult the cache registry like any
    full-mode source; lookup-bound frames are added last.
    """
    sources_map = {primary_alias: primary_df}
    for alias, source in thread.sources.items():
        if alias != primary_alias and alias not in lookup_dfs:
            sources_map[alias] = _read_source_via_registry(
                spark, alias, source, connections, cache_registry
            )
    sources_map.update(lookup_dfs)
    return sources_map


def _read_cdc_window(
    spark: SparkSession,
    thread: Thread,
    load: LoadConfig,
    cdc: CdcConfig,
    prior_state: WatermarkState | None,
    cleanup: _ThreadCleanup,
    connections: dict[str, OneLakeConnection] | None,
) -> tuple[Any, str | None]:
    """Read and persist the primary Source's CDC change window.

    The window is registered with ``cleanup`` as soon as it is persisted.

    Returns:
        The persisted change window and the new high-water mark (``None``
        when the window yields none).

    Raises:
        StateError: If a stored Delta CDF version is not an integer.
    """
    primary_source = thread.sources[next(iter(thread.sources))]

    # last_version is only meaningful for the CDF preset path,
    # where prior_state.last_value is a stringified commit version.
    # For generic CDC, the persisted value is a watermark string
    # (timestamp / date / int / long) and must not be coerced to int.
    last_version: int | None = None
    if cdc.preset == "delta_cdf" and prior_state is not None:
        try:
            last_version = int(prior_state.last_value)
        except (ValueError, TypeError) as exc:
            raise StateError(
                f"Stored CDF version {prior_state.last_value!r} is not an "
                f"integer; seed or reset the watermark",
                cause=exc,
                thread_name=thread.name,
            ) from exc

    primary_df, cdc_new_hwm = read_cdc_source(
        spark,
        primary_source,
        cdc,
        last_version,
        load_config=load,
        prior_state=prior_state,
        connections=connections,
        compute_hwm=False,
    )
    # Persist the change window: the version capture, the
    # op-code counts, and the merge itself all execute this
    # lineage — window-scale, cached once, released in the
    # thread's finally block
    primary_df = primary_df.persist(StorageLevel.MEMORY_AND_DISK)
    cleanup.cdc_window = primary_df
    # Generic-preset HWM computes AGAINST the persisted window,
    # sharing one lineage with the op counts and the merge — the
    # captured value and the applied rows cannot diverge. (The
    # old in-read capture ran on a separate execution: a commit
    # between it and the merge silently decoupled persisted
    # state from applied state.)
    if cdc.preset != "delta_cdf":
        cdc_new_hwm = compute_generic_cdc_hwm(primary_df, load)

    new_hwm: str | None = None
    # Capture new CDF version if Delta CDF
    if cdc.preset == "delta_cdf" and "_commit_version" in primary_df.columns:
        max_row = primary_df.agg(F.max("_commit_version").alias("mv")).collect()
        if max_row and max_row[0]["mv"] is not None:
            new_hwm = str(max_row[0]["mv"])
    elif cdc_new_hwm is not None:
        # Generic CDC + watermark composition: HWM was captured
        # from the filtered DataFrame at read time. Persistence
        # still goes through the save_watermark path, which keys on
        # `new_hwm is not None`.
        new_hwm = cdc_new_hwm
        logger.debug(
            "Thread '%s': CDC watermark captured on %s (prior=%s, new=%s, filter_applied=%s)",
            thread.name,
            load.watermark_column,
            prior_state.last_value if prior_state is not None else None,
            cdc_new_hwm,
            prior_state is not None,
        )
    return primary_df, new_hwm


def _run_transforms(
    thread: Thread,
    sources_map: dict[str, Any],
    merge_terminal: bool,
    obs_registry: ObservationRegistry,
    *,
    column_sets: dict[str, dict[str, str]] | None,
    column_set_defs: dict[str, ColumnSet] | None,
    resolve_lookups: dict[str, Any],
    resolve_meta: dict[str, LookupMeta] | None,
    collector: SpanCollector | None,
    span_builder: SpanBuilder | None,
) -> _Transformed:
    """Resolve ``with`` CTEs, run the pipeline steps, and count rows."""
    # Resolve with: block CTEs before the primary DataFrame is set
    if thread.with_:
        sources_map = resolve_with_block(
            thread,
            sources_map,
            collector=collector if span_builder else None,
            parent_span_id=span_builder.span_id if span_builder else None,
            observations=obs_registry,
            lookup_meta=resolve_meta,
            lookups=resolve_lookups or None,
        )

    # Step 5 — primary DataFrame is the first declared source
    df = next(iter(sources_map.values()))
    rows_read = 0
    rows_read_obs: Observation | None = None
    if merge_terminal:
        rows_read = df.count()
    else:
        rows_read_obs = Observation()
        df = df.observe(rows_read_obs, F.count(F.lit(1)).alias("n"))

    # Step 6 — run pipeline steps
    df = run_pipeline(
        df,
        thread.steps,
        sources_map,
        column_sets,
        column_set_defs,
        lookups=resolve_lookups or None,
        observations=obs_registry,
        lookup_meta=resolve_meta,
    )

    # Intermediate row count for waterfall visualization. On
    # merge-terminal paths this eager count is ALSO the first action
    # over the plan — the one that fixes every observation with true
    # full-scale values before the merge can zero-fire them. On
    # single-pass paths it becomes an observation fixed by the write.
    rows_after_transforms = 0
    rows_after_transforms_obs: Observation | None = None
    if merge_terminal:
        rows_after_transforms = df.count()
    else:
        observation = Observation()
        df = df.observe(observation, F.count(F.lit(1)).alias("n"))
        rows_after_transforms_obs = observation

    return _Transformed(
        df=df,
        rows_read=rows_read,
        rows_read_obs=rows_read_obs,
        rows_after_transforms=rows_after_transforms,
        rows_after_transforms_obs=rows_after_transforms_obs,
    )


def _validate_and_key(
    spark: SparkSession,
    thread: Thread,
    df: Any,
    target_path: str,
    cleanup: _ThreadCleanup,
    capture_samples: bool,
    run_timestamp: str,
) -> _Shaped:
    """Apply ValidationRules, naming, keys, and SCD columns; check fact schema.

    Raises:
        DataValidationError: If a fatal-severity rule fails (before any write).
        ExecutionError: If a fact target's schema is invalid.
    """
    validation_results: list[Any] = []
    rows_quarantined = 0
    quarantine_sample: list[dict[str, Any]] | None = None

    # Step 7 — run validation rules
    if thread.validations:
        outcome = validate_dataframe(df, thread.validations)
        validation_results = outcome.validation_results

        if outcome.has_fatal:
            raise DataValidationError(
                f"Fatal validation failure in thread '{thread.name}'",
            )

        # Write quarantine rows if present. The union computes once:
        # persist, count from cache, write only when rows exist (an
        # all-clean run leaves the prior quarantine table untouched),
        # sample from cache. Released in finally.
        if outcome.quarantine_df is not None:
            quarantine_df = outcome.quarantine_df.persist(StorageLevel.MEMORY_AND_DISK)
            cleanup.quarantine_frame = quarantine_df
            rows_quarantined = write_quarantine(spark, quarantine_df, target_path)
            if capture_samples:
                with contextlib.suppress(Exception):
                    quarantine_sample = quarantine_df.limit(10).toPandas().to_dict("records")

        # Continue with clean_df
        df = outcome.clean_df

    # Step 8 — apply naming normalization (before target mapping)
    if thread.target.naming is not None:
        df = normalize_columns(df, thread.target.naming)

    # Step 9 — compute keys and hashes
    dim_config = thread.target.dimension
    # Audit column names are engine-managed: auto hashes and the overwrite
    # column set both exclude them.
    audit_col_names = (
        set(thread.target.audit_columns.keys()) if thread.target.audit_columns else None
    )
    if dim_config is not None:
        # Dimension mode: use dimension-aware key computation.
        df = compute_dimension_keys(df, dim_config, audit_columns=audit_col_names)
    elif thread.keys is not None:
        df = compute_keys(df, thread.keys)

    # Step 9b — inject SCD columns for versioned dimensions
    dim_builder: DimensionMergeBuilder | None = None
    if dim_config is not None:
        dim_builder = DimensionMergeBuilder(dim_config, thread.write, audit_columns=audit_col_names)
        df = dim_builder.inject_scd_columns(df, run_timestamp)

    # Step 10b — validate fact target schema (if fact mode).
    # Schema-only, no Spark actions; the sentinel advisory runs
    # post-write against the written table (step 13b).
    if thread.target.fact is not None:
        fact_diags = validate_fact_schema(df, thread.target.fact)
        if fact_diags:
            raise ExecutionError(fact_diags[0])

    return _Shaped(
        df=df,
        validation_results=validation_results,
        rows_quarantined=rows_quarantined,
        quarantine_sample=quarantine_sample,
        dim_builder=dim_builder,
    )


def _capture_output(
    df: Any, thread: Thread, ctx: _WriteContext
) -> tuple[Any, list[tuple[str, str]], list[dict[str, Any]] | None]:
    """Persist the write-ready frame for exports and capture its schema and sample.

    Exports reuse one compute of the working DataFrame: the persist anchors
    after warp/drift, before the route-specific write — the point common to
    all routes (the CDC route deliberately skips target mapping, so mapping
    is not a valid anchor). Seeding only reads the frame's schema, so routes
    that seed call this after seeding. Released in ``execute_thread``'s
    finally.

    Returns:
        The (possibly persisted) frame, its output schema, and the optional
        10-row sample.
    """
    if thread.exports:
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        ctx.cleanup.working_frame = df
    output_schema = [(name, str(dtype)) for name, dtype in df.dtypes]
    output_sample: list[dict[str, Any]] | None = None
    if ctx.capture_samples:
        with contextlib.suppress(Exception):
            output_sample = df.limit(10).toPandas().to_dict("records")
    return df, output_schema, output_sample


def _write_dimension(
    spark: SparkSession,
    thread: Thread,
    df: Any,
    dim_builder: DimensionMergeBuilder,
    ctx: _WriteContext,
    *,
    rows_quarantined: int,
    rows_after_transforms: int,
) -> _WriteOutcome:
    """Shape, seed, and merge into a dimension target."""
    dim_config = dim_builder.config
    seed_config = thread.target.seed
    df, warp_findings, drift_report, audit_columns_applied = _shape_for_target(
        df,
        thread,
        spark,
        ctx.target_handle,
        ctx.target_path,
        ctx.resolved_warp,
        ctx.audit_columns,
        ctx.audit_ctx,
        _writer_managed_columns(thread, dim_builder),
    )

    # Seed phase (before merge)
    if seed_config is not None or dim_config.seed_system_members:
        target_schema = df.schema
        all_seed_rows: list[dict[str, Any]] = []
        if seed_config is not None:
            all_seed_rows.extend(seed_config.rows)
        if dim_config.seed_system_members:
            all_seed_rows.extend(build_system_member_rows(dim_config, df))
        if all_seed_rows:
            combined_seed = SeedConfig(
                on=seed_config.on if seed_config else "first_write",
                rows=all_seed_rows,
            )
            execute_seeds(
                spark, combined_seed, ctx.target_path, target_schema, handle=ctx.target_handle
            )
            ctx.target_handle.refresh()

    df, output_schema, output_sample = _capture_output(df, thread, ctx)
    was_first_write = not ctx.target_handle.exists()
    merge_result = execute_dimension_merge(
        spark,
        df,
        dim_builder,
        ctx.target_path,
        ctx.run_timestamp,
        handle=ctx.target_handle,
        stamp=ctx.commit_stamp,
        evolve=thread.target.schema_drift == "adaptive",
    )
    # Input-rows semantic. Quarantine-free merges need no new
    # action: transforms after the eager rows_after_transforms
    # count are column-only, so write-input rows equal it. With
    # quarantined rows the subtraction would lie — quarantine
    # frames are exploded one row per FAILED RULE, so a source
    # row failing two rules subtracts twice — and the input df
    # must be counted directly (exact beats cheap here). First
    # writes report the commit's own numOutputRows.
    if was_first_write:
        rows_written: int | None = merge_result.rows_inserted
    elif rows_quarantined:
        rows_written = df.count()
    else:
        rows_written = rows_after_transforms

    return _WriteOutcome(
        df=df,
        rows_written=rows_written,
        warp_findings=warp_findings,
        drift_report=drift_report,
        audit_columns_applied=audit_columns_applied,
        output_schema=output_schema,
        output_sample=output_sample,
    )


def _write_cdc(
    spark: SparkSession,
    thread: Thread,
    df: Any,
    load: LoadConfig,
    cdc: CdcConfig,
    ctx: _WriteContext,
) -> _WriteOutcome:
    """Apply a CDC change window to the target by merge.

    Skips target column mapping because ``execute_cdc_merge`` handles
    column selection internally (it retains the operation column).
    """
    audit_columns_applied: list[str] = []
    if ctx.audit_columns and ctx.audit_ctx is not None:
        df = inject_audit_columns(df, ctx.audit_columns, ctx.audit_ctx)
        audit_columns_applied = list(ctx.audit_columns)

    # Warp enforcement, drift detection, warp-only append
    engine_col_names = audit_columns_applied + _writer_managed_columns(thread, None)
    df, warp_findings, drift_report = _apply_warp_and_drift(
        df,
        ctx.resolved_warp,
        thread,
        engine_col_names,
        table_columns=existing_target_columns(spark, ctx.target_path, ctx.target_handle),
    )

    df, output_schema, output_sample = _capture_output(df, thread, ctx)
    seed_config = thread.target.seed
    cdc_write = _validate_cdc_write_config(thread)
    seeded_first_write = seed_config is not None and seed_cdc_target(
        spark,
        df,
        seed_config,
        ctx.target_path,
        cdc_write,
        cdc,
        ctx.target_handle,
    )
    cdc_counts = execute_cdc_merge(
        spark,
        df,
        ctx.target_path,
        cdc_write,
        cdc,
        load_config=load,
        handle=ctx.target_handle,
        evolve=thread.target.schema_drift == "adaptive",
        seeded_first_write=seeded_first_write,
    )
    return _WriteOutcome(
        df=df,
        rows_written=sum(cdc_counts.values()),
        warp_findings=warp_findings,
        drift_report=drift_report,
        audit_columns_applied=audit_columns_applied,
        output_schema=output_schema,
        output_sample=output_sample,
        cdc_counts=cdc_counts,
    )


def _write_standard(
    spark: SparkSession, thread: Thread, df: Any, ctx: _WriteContext
) -> _WriteOutcome:
    """Shape, seed, and write the target with the thread's write mode."""
    df, warp_findings, drift_report, audit_columns_applied = _shape_for_target(
        df,
        thread,
        spark,
        ctx.target_handle,
        ctx.target_path,
        ctx.resolved_warp,
        ctx.audit_columns,
        ctx.audit_ctx,
        _writer_managed_columns(thread, None),
    )

    # General seed phase (non-dimension)
    seed_config = thread.target.seed
    if seed_config is not None:
        execute_seeds(spark, seed_config, ctx.target_path, df.schema, handle=ctx.target_handle)
        ctx.target_handle.refresh()

    df, output_schema, output_sample = _capture_output(df, thread, ctx)
    rows_written = write_target(
        spark,
        df,
        thread.target,
        thread.write,
        ctx.target_path,
        handle=ctx.target_handle,
        stamp=ctx.commit_stamp,
    )
    return _WriteOutcome(
        df=df,
        rows_written=rows_written,
        warp_findings=warp_findings,
        drift_report=drift_report,
        audit_columns_applied=audit_columns_applied,
        output_schema=output_schema,
        output_sample=output_sample,
    )


def _run_post_write_checks(
    spark: SparkSession,
    thread: Thread,
    written: _WriteOutcome,
    target_path: str,
    resolved_warp: WarpConfig | None,
) -> None:
    """Run the fact sentinel advisory and auto Warp generation after a write."""
    # Step 13b — post-write fact sentinel advisory. Runs against
    # the written table, where columnar stats make per-column
    # scans cheap. Advisory only: failures never block the run.
    fact_config = thread.target.fact
    if fact_config is not None and target_path:
        try:
            written_df = read_delta(spark, target_path)
            # Return value intentionally unused: the check logs its
            # own WARNs, and advisory diagnostics are not stored.
            check_fact_sentinels(written_df, fact_config)
        except Exception:
            logger.debug(
                "Post-write fact sentinel advisory failed for thread '%s'",
                thread.name,
                exc_info=True,
            )

    # Auto-generate warp after successful write (all paths)
    if thread.target.warp_mode == "auto":
        destination = _auto_warp_destination(thread, target_path)
        if destination is not None:
            auto_generate_warp(
                written.df,
                resolved_warp,
                written.drift_report,
                destination[1],
                destination[0],
                adaptive=thread.target.schema_drift == "adaptive",
            )


def _persist_watermark(
    spark: SparkSession,
    thread: Thread,
    load_mode: str,
    watermark_store: WatermarkStore | None,
    new_hwm: str | None,
) -> bool:
    """Persist the new high-water mark after a successful write.

    Returns:
        Whether a Watermark state was written.

    Raises:
        StateError: If the store write fails.
    """
    if (
        watermark_store is None
        or load_mode not in ("incremental_watermark", "cdc")
        or thread.load is None
        or new_hwm is None
    ):
        return False
    try:
        wm_type = thread.load.watermark_type or "timestamp"
        wm_col = thread.load.watermark_column or "_commit_version"

        new_state = WatermarkState(
            thread_name=thread.name,
            watermark_column=wm_col,
            watermark_type=wm_type,
            last_value=new_hwm,
            last_updated=datetime.now(UTC),
        )
        watermark_store.write(spark, new_state)
        logger.debug(
            "Thread '%s': persisted HWM %s=%s",
            thread.name,
            wm_col,
            new_hwm,
        )
    except StateError:
        raise
    except Exception as e:
        raise StateError(
            f"Failed to persist watermark for thread '{thread.name}'",
            cause=e,
            thread_name=thread.name,
        ) from e
    return True


def _write_exports(
    spark: SparkSession,
    thread: Thread,
    written: _WriteOutcome,
    audit_ctx: AuditContext,
    connections: dict[str, OneLakeConnection] | None,
) -> list[ExportResult]:
    """Write the thread's exports from the write-ready frame.

    Raises:
        ExportError: If an export names an undefined connection, lacks a
            table for its connection, or aborts on failure.
    """
    export_results: list[ExportResult] = []
    for export in resolve_exports(thread.exports or [], audit_ctx):
        exp = export
        if export.connection:
            conn_name = export.connection
            if not connections or conn_name not in connections:
                raise ExportError(
                    f"Export '{export.name}' references undefined connection '{conn_name}'",
                    thread_name=thread.name,
                    export_name=export.name,
                    export_type=export.type,
                )
            from weevr.config.paths import build_onelake_path

            conn = connections[conn_name]
            if export.table is None:
                raise ExportError(
                    f"Export '{export.name}' connection requires 'table'",
                    thread_name=thread.name,
                    export_name=export.name,
                )
            path = build_onelake_path(conn, export.schema_override, export.table)
            exp = export.model_copy(update={"path": path})
        result = write_export(spark, written.df, exp, row_count=written.rows_written)
        export_results.append(result)
        if result.status == "aborted":
            raise ExportError(
                f"Export '{export.name}' failed: {result.error}",
                thread_name=thread.name,
                export_name=export.name,
                export_type=export.type,
            )
    return export_results


def _harvest_row_counts(transformed: _Transformed) -> tuple[int, int]:
    """Rows read and rows after transforms, from eager counts or observations."""
    rows_read = transformed.rows_read
    rows_after_transforms = transformed.rows_after_transforms
    if transformed.rows_read_obs is not None:
        rr = read_observation(transformed.rows_read_obs)
        rows_read = int(rr["n"]) if rr and rr.get("n") is not None else 0
    if transformed.rows_after_transforms_obs is not None:
        rat = read_observation(transformed.rows_after_transforms_obs)
        rows_after_transforms = int(rat["n"]) if rat and rat.get("n") is not None else 0
    return rows_read, rows_after_transforms


def _result_samples(
    output_sample: list[dict[str, Any]] | None,
    quarantine_sample: list[dict[str, Any]] | None,
) -> dict[str, list[dict[str, Any]]] | None:
    """Samples for display: output first, quarantine only alongside output."""
    if not output_sample:
        return None
    samples = {"output": output_sample}
    if quarantine_sample:
        samples["quarantine"] = quarantine_sample
    return samples


def _drift_report_dict(drift_report: DriftReport | None) -> dict[str, Any] | None:
    """The ThreadResult drift summary, present only when drift was detected."""
    if drift_report is None or not drift_report.has_drift:
        return None
    return {
        "extra_columns": drift_report.extra_columns,
        "missing_columns": drift_report.missing_columns,
        "action_taken": drift_report.action_taken,
        "drift_mode": drift_report.drift_mode,
        "baseline_source": drift_report.baseline_source,
    }


def _finish_error_span(span_builder: SpanBuilder | None, collector: SpanCollector | None) -> None:
    """Close the thread span as ERROR and record it with the collector."""
    if span_builder is not None:
        span = span_builder.finish(status=SpanStatus.ERROR)
        if collector is not None:
            collector.add_span(span)


def _meta_for_resolve(
    thread: Thread, lookup_meta: dict[str, LookupMeta] | None
) -> dict[str, LookupMeta] | None:
    """Lookup facts keyed the way resolve steps look them up.

    Resolve steps fetch facts by the name they reference. A lookup-bound
    source alias that shadows a definition of the same name serves a
    different lookup's frame, so the definition's facts must not apply.
    """
    if not lookup_meta:
        return lookup_meta
    shadowing = {
        alias
        for alias, src in thread.sources.items()
        if src.lookup is not None and src.lookup != alias and alias in lookup_meta
    }
    if not shadowing:
        return lookup_meta
    return {name: meta for name, meta in lookup_meta.items() if name not in shadowing}


def resolve_with_block(
    thread: Thread,
    sources_map: dict[str, Any],
    collector: SpanCollector | None = None,
    parent_span_id: str | None = None,
    observations: ObservationRegistry | None = None,
    lookup_meta: dict[str, LookupMeta] | None = None,
    lookups: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve with: block CTEs, registering each result in the source registry.

    Each CTE is evaluated in declaration order. The result of each CTE is
    added to the registry so that later CTEs (and the main pipeline) can
    reference it by name.

    When a ``collector`` is provided, a child span is emitted per CTE with
    ``row_count``, ``from``, and ``step_count`` attributes recorded on each.

    Args:
        thread: Thread configuration with a populated ``with_`` block.
        sources_map: Current source registry (sources + lookup DataFrames).
        collector: Optional span collector for telemetry.
        parent_span_id: Parent span ID to link CTE spans into the thread span.
        observations: Thread-level registry; each CTE's observations attach
            under its own scope and merge in for plan-wide unique names.
        lookup_meta: Materialization-time lookup facts for resolve steps.
        lookups: Lookup DataFrames visible to resolve steps, by name.

    Returns:
        An enriched copy of ``sources_map`` with each CTE result registered
        under its declared name.
    """
    enriched: dict[str, Any] = dict(sources_map)
    for cte_name, cte in thread.with_.items():  # type: ignore[union-attr]
        cte_builder = None
        if collector is not None:
            cte_builder = collector.start_span(f"cte:{cte_name}", parent_span_id=parent_span_id)

        if cte.from_ not in enriched:
            raise ExecutionError(
                f"with: CTE '{cte_name}' references '{cte.from_}' "
                f"which is not in loaded sources. "
                f"Available: {sorted(enriched)}"
            )
        cte_df = enriched[cte.from_]
        cte_registry = ObservationRegistry(scope=cte_name) if observations is not None else None
        cte_df = run_pipeline(
            cte_df,
            cte.steps,
            enriched,
            lookups=lookups,
            observations=cte_registry,
            lookup_meta=lookup_meta,
        )
        if observations is not None and cte_registry is not None:
            observations.merge(cte_registry)
        enriched[cte_name] = cte_df

        if cte_builder is not None:
            # Collector-gated: the count exists only for the CTE span
            # attribute — collector-less runs pay no CTE action at all
            cte_builder.set_attribute("row_count", cte_df.count())
            cte_builder.set_attribute("from", cte.from_)
            cte_builder.set_attribute("step_count", len(cte.steps))
            span = cte_builder.finish()
            collector.add_span(span)  # type: ignore[union-attr]

    return enriched


SEED_WATERMARK_DOCS = (
    "https://ardent-data.github.io/weevr/concepts/idempotency/#seed-or-reset-a-watermark"
)


def _reconcile_stamped_hwm(
    spark: SparkSession,
    thread: Thread,
    prior_state: WatermarkState | None,
    target_handle: TargetHandle,
    target_path: str,
) -> WatermarkState | None:
    """Recover the HWM from the target's commit when the state store lags it.

    An incremental append stamps its new HWM into the commit. When the
    state save after that commit failed, the store is older than the commit
    and its value is behind; the stamped value is used instead. A store
    updated after the commit (a normal run, or a manual reset) wins.

    A first run with no stored and no stamped HWM into a target that already
    holds rows would re-append everything, so it fails before writing.

    Args:
        spark: Active SparkSession.
        thread: Thread configuration (incremental_watermark, append).
        prior_state: The state store's value, or None.
        target_handle: Pre-resolved target handle.
        target_path: Resolved target path or alias.

    Returns:
        The state to read from.

    Raises:
        StateError: On a first run into a non-empty target with no HWM.
    """
    from weevr.state.hwm import as_utc, hwm_is_newer

    load = thread.load
    if load is None or not target_handle.exists():
        return prior_state
    wm_type = load.watermark_type or "timestamp"
    stamped = target_handle.latest_stamped_hwm(thread.name)
    if stamped is not None:
        value, committed_at = stamped
        if prior_state is None or (
            as_utc(committed_at) > as_utc(prior_state.last_updated)
            and hwm_is_newer(value, prior_state.last_value, wm_type)
        ):
            logger.info(
                "Thread '%s': watermark recovered from the target commit at %s: %s "
                "(state store had %s)",
                thread.name,
                committed_at,
                value,
                prior_state.last_value if prior_state else None,
            )
            return WatermarkState(
                thread_name=thread.name,
                watermark_column=load.watermark_column or "",
                watermark_type=wm_type,
                last_value=value,
                last_updated=as_utc(committed_at),
            )
        return prior_state
    if prior_state is None and _target_has_rows(spark, target_path):
        history_note = (
            " Its commit history can't be read, so a stamped watermark may exist; retry "
            "once the history is readable. Otherwise:"
            if target_handle.history_read_failed()
            else ""
        )
        raise StateError(
            f"Thread '{thread.name}' has no stored watermark, but its append target "
            f"'{target_path}' already holds rows; a first run would re-append the whole "
            f"source.{history_note} Seed the watermark, or truncate the target for a full "
            f"reload: {SEED_WATERMARK_DOCS}",
            thread_name=thread.name,
        )
    return prior_state


def _target_has_rows(spark: SparkSession, target_path: str) -> bool:
    """Whether the target table holds at least one row (a one-row probe)."""
    from weevr.delta import is_table_alias

    reader = spark.read.format("delta")
    table = reader.table(target_path) if is_table_alias(target_path) else reader.load(target_path)
    return table.limit(1).count() > 0


def _writer_managed_columns(thread: Thread, dim_builder: Any) -> list[str]:
    """Columns the writer adds or consumes itself, which are never drift.

    The soft-delete column (from ``write`` or the dimension's overrides)
    exists in the table but not in the working DataFrame; CDC operation and
    change-feed metadata columns exist in the DataFrame but not the table.

    Args:
        thread: Thread configuration.
        dim_builder: The dimension merge builder, or None.

    Returns:
        Column names to exclude from drift detection.
    """
    from weevr.operations.writers import resolve_cdc_columns

    managed: list[str] = []
    if thread.write is not None and thread.write.soft_delete_column:
        managed.append(thread.write.soft_delete_column)
    if dim_builder is not None:
        dim_soft_delete = dim_builder.build_write_config().soft_delete_column
        if dim_soft_delete:
            managed.append(dim_soft_delete)
    if thread.load is not None and thread.load.mode == "cdc" and thread.load.cdc is not None:
        managed += ["_change_type", "_commit_version", "_commit_timestamp"]
        op_col = resolve_cdc_columns(thread.load.cdc)["operation_column"]
        if op_col:
            managed.append(op_col)
    return managed


def _shape_for_target(
    df: Any,
    thread: Thread,
    spark: SparkSession,
    target_handle: TargetHandle,
    target_path: str,
    resolved_warp: Any,
    audit_columns: dict[str, str] | None,
    audit_ctx: Any,
    managed_columns: list[str],
) -> tuple[Any, list[dict[str, str]], Any, list[str]]:
    """Map, audit, and drift-check the working DataFrame for a mapped write.

    Auto mapping's narrowing to the existing table's columns is deferred
    until after drift handling, so drift sees new output columns on an
    existing table. Columns added after mapping (audit, warp-only) are kept.

    Args:
        df: Working DataFrame.
        thread: Thread configuration.
        spark: Active SparkSession.
        target_handle: Pre-resolved target handle.
        target_path: Resolved target path or alias.
        resolved_warp: Resolved WarpConfig, or None.
        audit_columns: Audit column expressions, if configured.
        audit_ctx: Audit context for expression resolution.
        managed_columns: Writer-managed columns excluded from drift.

    Returns:
        ``(df, warp findings, drift report, audit columns applied)``.
    """
    # One read of the existing table's columns serves drift and narrowing
    table_columns = existing_target_columns(spark, target_path, target_handle)
    df = apply_target_mapping(df, thread.target, spark, handle=target_handle, narrow=False)
    mapped_cols = set(df.columns)
    audit_applied: list[str] = []
    if audit_columns and audit_ctx is not None:
        df = inject_audit_columns(df, audit_columns, audit_ctx)
        audit_applied = list(audit_columns)

    # Warp enforcement, drift detection, warp-only append
    df, warp_findings, drift_report = _apply_warp_and_drift(
        df,
        resolved_warp,
        thread,
        audit_applied + managed_columns,
        table_columns=table_columns,
    )
    keep = [c for c in df.columns if c not in mapped_cols]
    if thread.target.schema_drift == "adaptive" and drift_report is not None:
        # adaptive keeps new columns; the write adds them to the table
        keep += drift_report.extra_columns
    df = narrow_to_target(df, thread.target, table_columns, keep=keep)
    return df, warp_findings, drift_report, audit_applied


def _auto_warp_destination(thread: Thread, target_path: str | None) -> tuple[str, str] | None:
    """Return the ``(directory, file stem)`` an auto-generated warp is written to.

    A loaded thread updates the warp it loaded, else writes
    ``<target name>.warp`` beside its config file, where the next load
    discovers it. A thread loaded from a non-local project is skipped with a
    warning, since config locations are read-only there. A thread built
    without the loader writes beside a local path target and is skipped with
    a warning for an alias or remote target.

    Args:
        thread: Thread configuration, with the loader's warp binding if any.
        target_path: Resolved target path or alias.

    Returns:
        ``(directory, stem)``, or None when nothing should be written.
    """
    from weevr.config.warp import target_warp_name

    binding = thread.warp_binding
    if binding is None:
        # Only a local path target has a meaningful directory to write beside;
        # an alias or a remote URI would land in the working directory or a
        # mangled path.
        if thread.target.alias or not target_path or "://" in target_path:
            logger.warning(
                "Thread '%s': warp_mode 'auto' skipped — load the config through "
                "Context or load_config to write the warp beside the thread file",
                thread.name,
            )
            return None
        return (str(Path(target_path).parent), thread.name)
    if not binding.writable:
        logger.warning(
            "Thread '%s': warp_mode 'auto' skipped — the project location %s is not "
            "writable from the engine",
            thread.name,
            binding.thread_dir,
        )
        return None
    if binding.warp_file is not None:
        warp_file = Path(binding.warp_file)
        return (str(warp_file.parent), warp_file.stem)
    return (binding.thread_dir, target_warp_name(thread.target) or thread.name)


def _apply_warp_and_drift(
    df: Any,
    resolved_warp: Any,
    thread: Thread,
    engine_col_names: list[str],
    table_columns: list[str] | None = None,
) -> tuple[Any, list[dict[str, str]], Any]:
    """Apply warp enforcement, drift detection, and warp-only append.

    Called after audit column injection, before writing. Handles drift
    detection, warp enforcement, and warp-only column append in sequence.

    Args:
        df: Working DataFrame.
        resolved_warp: Resolved WarpConfig, or None.
        thread: Thread configuration.
        engine_col_names: Engine-managed column names (audit, SCD, etc.).
        table_columns: The existing target table's columns, the drift
            baseline when there is no warp; None when the table doesn't exist.

    Returns:
        Tuple of (modified df, warp findings, drift report).
    """
    from weevr.config.warp import build_effective_warp

    warp_findings: list[dict[str, str]] = []

    # Include _weevr_-prefixed engine columns (SCD, soft-delete)
    # consistently across all write paths
    weevr_cols = [c for c in df.schema.names if c.startswith("_weevr_")]
    all_engine = list(set(engine_col_names + weevr_cols))

    # Drift detection (works with or without warp)
    baseline = (
        [col.name for col in resolved_warp.columns] if resolved_warp is not None else table_columns
    )
    baseline_source = (
        "warp" if resolved_warp is not None else ("table" if baseline is not None else "none")
    )
    df, drift_report = handle_drift(
        df,
        baseline,
        thread.target.schema_drift,
        thread.target.on_drift,
        engine_columns=all_engine,
        baseline_source=baseline_source,
    )

    # Warp enforcement (only with warp)
    if resolved_warp is not None:
        # Compute effective warp to identify warp-only columns
        pipeline_cols = [f.name for f in df.schema.fields]
        effective = build_effective_warp(resolved_warp.columns, pipeline_cols, all_engine)
        warp_only_names = [c.name for c in effective.warp_only]

        warp_findings = enforce_warp(
            df,
            resolved_warp,
            thread.target.warp_enforcement,
            engine_columns=all_engine,
            warp_only_columns=warp_only_names,
        )

        # Warp-only column append
        if effective.warp_only:
            df = append_warp_only_columns(df, effective.warp_only)

    return df, warp_findings, drift_report


def _build_thread_variable_context(thread: Thread) -> VariableContext:
    """Build a VariableContext from thread-level variable definitions.

    If the thread declares variables, those are used. Otherwise an empty
    context is returned so that hook steps have a valid context to work with.

    Args:
        thread: Thread configuration with optional variables.

    Returns:
        VariableContext initialized from thread variables.
    """
    if thread.variables:
        return VariableContext(dict(thread.variables))
    return VariableContext(None)


def _build_audit_context(
    thread: Thread,
    weave_name: str,
    loom_name: str,
    run_timestamp: str,
    run_id: str,
) -> AuditContext:
    """Build an AuditContext for context variable resolution.

    ``weave_name`` and ``loom_name`` are empty when the thread runs outside
    a weave or loom; the commit lineage stamp uses the same values.
    """
    primary_alias = next(iter(thread.sources))
    primary_source = thread.sources[primary_alias]
    return AuditContext(
        thread_name=thread.name,
        thread_qualified_key=thread.qualified_key,
        thread_source=primary_source.alias,
        thread_sources_json=build_sources_json(thread.sources),
        weave_name=weave_name,
        loom_name=loom_name,
        run_timestamp=run_timestamp,
        run_id=run_id,
    )


def _validate_incremental(thread: Thread, load_mode: str) -> None:
    """Run cross-cutting incremental config validation at executor entry."""
    from weevr.config.validation import validate_incremental_config

    load_raw: dict[str, object] = {"mode": load_mode}
    if thread.load is not None:
        load_raw["watermark_inclusive"] = thread.load.watermark_inclusive
        if thread.load.watermark_column is not None:
            load_raw["watermark_column"] = thread.load.watermark_column
        if thread.load.cdc is not None:
            load_raw["cdc"] = thread.load.cdc.model_dump()
    raw: dict[str, object] = {"load": load_raw}
    if thread.write is not None:
        raw["write"] = {"mode": thread.write.mode}
    diagnostics = validate_incremental_config(raw)
    errors = [d for d in diagnostics if d.startswith("ERROR:")]
    if errors:
        raise ExecutionError(
            "; ".join(errors),
            thread_name=thread.name,
        )
    for diag in diagnostics:
        if diag.startswith("WARN:"):
            logger.warning("Thread '%s': %s", thread.name, diag)


def _validate_cdc_write_config(thread: Thread) -> WriteConfig:
    """Validate and return write config for CDC merge, failing fast if misconfigured."""
    if thread.write is None:
        raise ExecutionError(
            "CDC mode requires a 'write' config with mode='merge' and 'match_keys'",
            thread_name=thread.name,
        )
    if not thread.write.match_keys:
        raise ExecutionError(
            "CDC mode requires 'match_keys' on write config",
            thread_name=thread.name,
        )
    return thread.write


def _read_source_via_registry(
    spark: SparkSession,
    alias_name: str,
    source: Any,
    connections: dict[str, OneLakeConnection] | None,
    cache_registry: CacheManager | None,
) -> Any:
    """Serve a source from the weave cache registry, or read it directly.

    Registry consultation is strictly the thread-target read path (the
    lookup lane has its own materialization); a hit hands back the
    producer's post-write snapshot with the source's own dedup applied —
    identical to what a direct read would see absent interleaved writers.
    Plan-time validation rejects unordered same-target writers across
    alias, path, AND connection declaration forms; the one residual is
    FUSE-vs-abfss notation for the same table, which plan time cannot
    unify (no session) — such pairs miss the registry rather than hit it
    wrongly, and their scheduling blindness predates the registry.
    """
    if cache_registry is not None and (source.type == "delta" or source.connection):
        identity = resolve_target_identity(
            spark,
            alias=source.alias,
            path=source.path,
            connection=source.connection,
            table=source.table,
            schema_override=source.schema_override,
            connections=connections,
        )
        hit = cache_registry.get(identity)
        if hit is not None:
            logger.debug("Source '%s' served from cache registry", alias_name)
            return apply_source_dedup(hit, source)
    return read_source(spark, alias_name, source, connections=connections)


def _build_telemetry(
    span_builder: SpanBuilder | None,
    validation_results: list,
    assertion_results: list,
    rows_read: int,
    rows_written: int | None,
    rows_quarantined: int,
    status: SpanStatus = SpanStatus.OK,
    collector: SpanCollector | None = None,
    load_mode: str | None = None,
    watermark_column: str | None = None,
    watermark_previous_value: str | None = None,
    watermark_new_value: str | None = None,
    watermark_persisted: bool = False,
    watermark_first_run: bool = False,
    cdc_counts: dict[str, int] | None = None,
    rows_after_transforms: int = 0,
    resolved_params: dict[str, Any] | None = None,
    audit_columns_applied: list[str] | None = None,
    export_results: list[ExportResult] | None = None,
    warp_name: str | None = None,
    warp_source: str | None = None,
    warp_enforcement_mode: str | None = None,
    drift_detected: bool = False,
    drift_columns: list[str] | None = None,
    drift_missing_columns: list[str] | None = None,
    drift_mode: str | None = None,
    drift_action_taken: str | None = None,
    step_stats: dict[str, dict[str, Any]] | None = None,
) -> ThreadTelemetry:
    """Build ThreadTelemetry and finalize the span if a builder is active."""
    if span_builder is not None:
        span = span_builder.finish(status=status)
        if collector is not None:
            collector.add_span(span)
    else:
        span = ExecutionSpan(
            trace_id=generate_trace_id(),
            span_id=generate_span_id(),
            name="thread",
            status=status,
            start_time=datetime.now(UTC),
            end_time=datetime.now(UTC),
        )

    return ThreadTelemetry(
        span=span,
        validation_results=validation_results,
        assertion_results=assertion_results,
        rows_read=rows_read,
        rows_written=rows_written,
        rows_quarantined=rows_quarantined,
        rows_after_transforms=rows_after_transforms,
        load_mode=load_mode,
        watermark_column=watermark_column,
        watermark_previous_value=watermark_previous_value,
        watermark_new_value=watermark_new_value,
        watermark_persisted=watermark_persisted,
        watermark_first_run=watermark_first_run,
        cdc_inserts=cdc_counts["inserts"] if cdc_counts else None,
        cdc_updates=cdc_counts["updates"] if cdc_counts else None,
        cdc_deletes=cdc_counts["deletes"] if cdc_counts else None,
        resolved_params=resolved_params,
        audit_columns_applied=audit_columns_applied or [],
        export_results=export_results or [],
        warp_name=warp_name,
        warp_source=warp_source,
        warp_enforcement=warp_enforcement_mode,
        drift_detected=drift_detected,
        drift_columns=drift_columns or [],
        drift_missing_columns=drift_missing_columns or [],
        drift_mode=drift_mode,
        drift_action_taken=drift_action_taken,
        step_stats=step_stats,
    )
