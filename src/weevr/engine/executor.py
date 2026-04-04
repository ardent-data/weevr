"""Thread executor — orchestrates the full read → transform → write pipeline."""

from __future__ import annotations

import contextlib
import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from weevr.engine.column_sets import materialize_column_sets
from weevr.engine.hooks import run_hook_steps
from weevr.engine.lookups import cleanup_lookups, materialize_lookups, resolve_thread_lookups
from weevr.engine.resources import merge_resource_dicts
from weevr.engine.result import ThreadResult
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
from weevr.model.lookup import Lookup
from weevr.model.seed import SeedConfig
from weevr.model.thread import Thread
from weevr.model.write import WriteConfig
from weevr.operations.assertions import evaluate_assertions
from weevr.operations.audit import AuditContext, build_sources_json, inject_audit_columns
from weevr.operations.dimension import DimensionMergeBuilder, execute_dimension_merge
from weevr.operations.drift import handle_drift
from weevr.operations.exports import resolve_exports, write_export
from weevr.operations.fact import validate_fact_target
from weevr.operations.hashing import compute_dimension_keys, compute_keys
from weevr.operations.naming import normalize_columns
from weevr.operations.pipeline import run_pipeline
from weevr.operations.quarantine import write_quarantine
from weevr.operations.readers import (
    read_cdc_source,
    read_source,
    read_source_incremental,
    read_sources,
)
from weevr.operations.seeding import build_system_member_rows, execute_seeds
from weevr.operations.validation import validate_dataframe
from weevr.operations.warp import (
    append_warp_only_columns,
    auto_generate_warp,
    enforce_warp,
    pre_initialize_table,
)
from weevr.operations.writers import apply_target_mapping, execute_cdc_merge, write_target
from weevr.state import WatermarkState, WatermarkStore, resolve_store
from weevr.telemetry.collector import SpanBuilder, SpanCollector
from weevr.telemetry.results import ExportResult, ThreadTelemetry
from weevr.telemetry.span import ExecutionSpan, SpanStatus, generate_span_id, generate_trace_id

logger = logging.getLogger(__name__)


def execute_thread(  # type: ignore[reportGeneralTypeIssues]
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
) -> ThreadResult:
    """Execute a single thread from sources through transforms to a Delta target.

    Execution order:
    1. Initialize thread-level resources (variables, lookups, column_sets).
    2. Run thread-level pre_steps.
    3. Resolve lookup-based sources from cached or on-demand DataFrames.
    4. Read all remaining declared sources into DataFrames.
       - For ``incremental_watermark``: load prior HWM, apply filter, capture new HWM.
       - For ``cdc``: read via CDF or generic CDC source.
    5. Set the primary (first) source as the working DataFrame.
    6. Run pipeline steps against the working DataFrame.
    7. Run validation rules (if configured) — quarantine or abort on failures.
    8. Apply naming normalization (if configured).
    9. Compute business keys and hashes if configured.
    10. Resolve the target write path.
    11. Apply target column mapping.
    12. Inject audit columns (if configured; bypasses mapping mode).
    13. Write to the Delta target.
        - For ``cdc``: use CDC merge routing instead of standard write.
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
        span_label = thread.qualified_key or thread.name
        span_builder = collector.start_span(f"thread:{span_label}", parent_span_id=parent_span_id)
        if thread.template_ref:
            span_builder.set_attribute("thread.template_ref", thread.template_ref)

    validation_results: list = []
    assertion_results: list = []
    rows_read = 0
    rows_written = 0
    rows_quarantined = 0
    rows_after_transforms = 0
    output_schema: list[tuple[str, str]] | None = None
    output_sample: list[dict[str, Any]] | None = None
    quarantine_sample: list[dict[str, Any]] | None = None

    # Incremental processing state
    load_mode = thread.load.mode if thread.load else "full"
    watermark_store: WatermarkStore | None = None
    prior_state: WatermarkState | None = None
    new_hwm: str | None = None
    watermark_first_run = False
    watermark_persisted = False
    cdc_counts: dict[str, int] | None = None

    # Fail-fast: validate incremental config cross-cutting constraints
    if load_mode != "full":
        _validate_incremental(thread, load_mode)

    # --- Thread-level resource lifecycle ---
    thread_variable_ctx = _build_thread_variable_context(thread)
    thread_cached_lookups: dict[str, Any] = {}

    # Compute effective resources by merging weave-level with thread-level
    effective_cached_lookups = cached_lookups
    effective_weave_lookups = weave_lookups
    effective_column_sets = column_sets
    effective_column_set_defs = column_set_defs

    try:  # noqa: PLR0912 — outer try/finally for thread-level lookup cleanup
        # Materialize thread-level lookups and merge with weave-level
        if thread.lookups:
            thread_cached_lookups, _ = materialize_lookups(
                spark, thread.lookups, connections=connections
            )
            all_cached: dict[str, Any] = {}
            if cached_lookups:
                all_cached.update(cached_lookups)
            all_cached.update(thread_cached_lookups)  # thread wins
            effective_cached_lookups = all_cached or None
            # Merge lookup definitions so resolve_thread_lookups sees both
            effective_weave_lookups = merge_resource_dicts(
                weave_lookups, thread.lookups, "lookup", "weave", "thread"
            )

        # Merge thread-level column_sets with weave-level (thread wins)
        if thread.column_sets:
            merged_cs_defs = merge_resource_dicts(
                column_set_defs, thread.column_sets, "column_set", "weave", "thread"
            )
            if merged_cs_defs:
                effective_column_set_defs = merged_cs_defs
                effective_column_sets, _ = materialize_column_sets(
                    spark, merged_cs_defs, resolved_params or {}
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

        # --- Core execution pipeline ---
        try:
            # Resolve target path early (needed for watermark store resolution)
            target_path = thread.target.alias or thread.target.path
            if target_path is None and thread.target.connection:
                conn_name = thread.target.connection
                if not connections or conn_name not in connections:
                    raise ExecutionError(
                        f"Target references undefined connection '{conn_name}'",
                        thread_name=thread.name,
                    )
                from weevr.config.paths import build_onelake_path

                conn = connections[conn_name]
                if thread.target.table is None:
                    raise ExecutionError(
                        "Target connection requires 'table'",
                        thread_name=thread.name,
                    )
                target_path = build_onelake_path(
                    conn,
                    thread.target.schema_override,
                    thread.target.table,
                )
                logger.debug(
                    "Thread '%s': resolved target connection '%s' → %s",
                    thread.name,
                    conn_name,
                    target_path,
                )
            if target_path is None:
                raise ExecutionError(
                    "Target has no 'alias', 'path', or 'connection' "
                    "— cannot resolve write location",
                    thread_name=thread.name,
                )

            # --- Warp resolution ---
            resolved_warp = None
            warp_findings: list[dict[str, str]] = []
            drift_report = None

            # Only resolve warp when explicitly referenced (string).
            # Auto-discovery from target_path is unreliable because
            # alias-based targets are not filesystem paths.
            if isinstance(thread.target.warp, str):
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

            # --- Watermark/CDC state loading ---
            if load_mode in ("incremental_watermark", "cdc") and thread.load is not None:
                watermark_store = resolve_store(thread.load, target_path)
                prior_state = watermark_store.read(spark, thread.name)
                watermark_first_run = prior_state is None
                logger.debug(
                    "Thread '%s': load_mode=%s, first_run=%s, prior_value=%s",
                    thread.name,
                    load_mode,
                    watermark_first_run,
                    prior_state.last_value if prior_state else None,
                )

            # --- Step 3: Resolve lookup-based sources ---
            lookup_dfs: dict[str, Any] = {}
            if effective_cached_lookups is not None or effective_weave_lookups is not None:
                lookup_dfs = resolve_thread_lookups(
                    thread.sources,
                    effective_weave_lookups or {},
                    effective_cached_lookups or {},
                    spark,
                    connections=connections,
                )

            # --- Step 4: Read sources ---
            if load_mode == "incremental_watermark" and thread.load is not None:
                # Read primary source with watermark filter
                primary_alias = next(iter(thread.sources))
                primary_source = thread.sources[primary_alias]
                primary_df, new_hwm = read_source_incremental(
                    spark,
                    primary_alias,
                    primary_source,
                    thread.load,
                    prior_state,
                    connections=connections,
                )

                # Read secondary sources normally (skip lookup-resolved ones)
                sources_map = {primary_alias: primary_df}
                for alias, source in thread.sources.items():
                    if alias != primary_alias and alias not in lookup_dfs:
                        sources_map[alias] = read_source(
                            spark, alias, source, connections=connections
                        )
                sources_map.update(lookup_dfs)

            elif load_mode == "cdc" and thread.load is not None and thread.load.cdc is not None:
                # Read CDC source
                primary_alias = next(iter(thread.sources))
                primary_source = thread.sources[primary_alias]

                last_version: int | None = None
                if prior_state is not None:
                    try:
                        last_version = int(prior_state.last_value)
                    except (ValueError, TypeError):
                        last_version = None

                primary_df = read_cdc_source(
                    spark, primary_source, thread.load.cdc, last_version, connections=connections
                )

                # Capture new CDF version if Delta CDF
                if (
                    thread.load.cdc.preset == "delta_cdf"
                    and "_commit_version" in primary_df.columns
                ):
                    from pyspark.sql import functions as F

                    max_row = primary_df.agg(F.max("_commit_version").alias("mv")).collect()
                    if max_row and max_row[0]["mv"] is not None:
                        new_hwm = str(max_row[0]["mv"])

                # Read secondary sources normally (skip lookup-resolved ones)
                sources_map = {primary_alias: primary_df}
                for alias, source in thread.sources.items():
                    if alias != primary_alias and alias not in lookup_dfs:
                        sources_map[alias] = read_source(
                            spark, alias, source, connections=connections
                        )
                sources_map.update(lookup_dfs)
            else:
                # Full mode or incremental_parameter — read non-lookup sources normally
                normal_sources = {k: v for k, v in thread.sources.items() if k not in lookup_dfs}
                sources_map = read_sources(spark, normal_sources, connections=connections)
                sources_map.update(lookup_dfs)

            # Resolve with: block CTEs before the primary DataFrame is set
            if thread.with_:
                sources_map = _resolve_with_block(
                    thread,
                    sources_map,
                    collector=collector if span_builder else None,
                    parent_span_id=span_builder.span_id if span_builder else None,
                )

            # Step 5 — primary DataFrame is the first declared source
            df = next(iter(sources_map.values()))
            rows_read = df.count()

            # Step 6 — run pipeline steps
            df = run_pipeline(
                df,
                thread.steps,
                sources_map,
                effective_column_sets,
                effective_column_set_defs,
                lookups=lookup_dfs or None,
            )

            # Capture intermediate row count for waterfall visualization
            rows_after_transforms = df.count()

            # Step 7 — run validation rules
            if thread.validations:
                outcome = validate_dataframe(df, thread.validations)
                validation_results = outcome.validation_results

                if outcome.has_fatal:
                    raise DataValidationError(
                        f"Fatal validation failure in thread '{thread.name}'",
                    )

                # Write quarantine rows if present
                if outcome.quarantine_df is not None:
                    rows_quarantined = write_quarantine(spark, outcome.quarantine_df, target_path)
                    with contextlib.suppress(Exception):
                        quarantine_sample = (
                            outcome.quarantine_df.limit(10).toPandas().to_dict("records")
                        )

                # Continue with clean_df
                df = outcome.clean_df

            # Step 8 — apply naming normalization (before target mapping)
            if thread.target.naming is not None:
                df = normalize_columns(df, thread.target.naming)

            # Step 9 — compute keys and hashes
            dim_config = thread.target.dimension
            fact_config = thread.target.fact
            seed_config = thread.target.seed

            if dim_config is not None:
                # Dimension mode: use dimension-aware key computation.
                # Pass audit column names so auto hash excludes them.
                audit_col_names = (
                    set(thread.target.audit_columns.keys()) if thread.target.audit_columns else None
                )
                df = compute_dimension_keys(df, dim_config, audit_columns=audit_col_names)
            elif thread.keys is not None:
                df = compute_keys(df, thread.keys)

            # Step 9b — inject SCD columns for versioned dimensions
            dim_builder: DimensionMergeBuilder | None = None
            if dim_config is not None:
                dim_builder = DimensionMergeBuilder(dim_config, thread.write)
                df = dim_builder.inject_scd_columns(df, run_timestamp)

            # Step 10 — resolve and prepare audit columns
            audit_columns = thread.target.audit_columns or {}
            audit_columns_applied: list[str] = []
            audit_ctx: AuditContext | None = None
            if audit_columns:
                audit_ctx = _build_audit_context(
                    thread, weave_name, loom_name, run_timestamp, run_id
                )

            # Step 10b — validate fact target (if fact mode)
            if fact_config is not None:
                fact_diags = validate_fact_target(df, fact_config)
                for diag in fact_diags:
                    if diag.startswith("ERROR:"):
                        raise ExecutionError(diag)
                    logger.warning(diag)

            # Step 11/12/13 — write to Delta
            write_mode = thread.write.mode if thread.write else "overwrite"

            if dim_config is not None and dim_builder is not None:
                # Dimension merge routing
                df = apply_target_mapping(df, thread.target, spark)
                if audit_columns and audit_ctx is not None:
                    df = inject_audit_columns(df, audit_columns, audit_ctx)
                    audit_columns_applied = list(audit_columns)

                # Warp enforcement, drift detection, warp-only append
                engine_col_names = audit_columns_applied[:]
                df, warp_findings, drift_report = _apply_warp_and_drift(
                    df,
                    resolved_warp,
                    thread,
                    engine_col_names,
                    spark=spark,
                    target_path=target_path,
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
                        execute_seeds(spark, combined_seed, target_path, target_schema)

                output_schema = [(name, str(dtype)) for name, dtype in df.dtypes]
                with contextlib.suppress(Exception):
                    output_sample = df.limit(10).toPandas().to_dict("records")
                execute_dimension_merge(spark, df, dim_builder, target_path, run_timestamp)
                rows_written = df.count()

            elif load_mode == "cdc" and thread.load is not None and thread.load.cdc is not None:
                # CDC merge routing (no dimension) — skip target column
                # mapping because execute_cdc_merge handles column
                # selection internally (retains operation column).
                if audit_columns and audit_ctx is not None:
                    df = inject_audit_columns(df, audit_columns, audit_ctx)
                    audit_columns_applied = list(audit_columns)

                # Warp enforcement, drift detection, warp-only append
                engine_col_names = audit_columns_applied[:]
                df, warp_findings, drift_report = _apply_warp_and_drift(
                    df,
                    resolved_warp,
                    thread,
                    engine_col_names,
                    spark=spark,
                    target_path=target_path,
                )

                output_schema = [(name, str(dtype)) for name, dtype in df.dtypes]
                with contextlib.suppress(Exception):
                    output_sample = df.limit(10).toPandas().to_dict("records")
                cdc_write = _validate_cdc_write_config(thread)
                cdc_counts = execute_cdc_merge(spark, df, target_path, cdc_write, thread.load.cdc)
                rows_written = sum(cdc_counts.values())

            else:
                # Standard write path
                df = apply_target_mapping(df, thread.target, spark)
                if audit_columns and audit_ctx is not None:
                    df = inject_audit_columns(df, audit_columns, audit_ctx)
                    audit_columns_applied = list(audit_columns)

                # Warp enforcement, drift detection, warp-only append
                engine_col_names = audit_columns_applied[:]
                df, warp_findings, drift_report = _apply_warp_and_drift(
                    df,
                    resolved_warp,
                    thread,
                    engine_col_names,
                    spark=spark,
                    target_path=target_path,
                )

                # General seed phase (non-dimension)
                if seed_config is not None:
                    execute_seeds(spark, seed_config, target_path, df.schema)

                output_schema = [(name, str(dtype)) for name, dtype in df.dtypes]
                with contextlib.suppress(Exception):
                    output_sample = df.limit(10).toPandas().to_dict("records")
                rows_written = write_target(spark, df, thread.target, thread.write, target_path)

            # Auto-generate warp after successful write (all paths)
            if thread.target.warp_mode == "auto":
                auto_generate_warp(
                    df,
                    resolved_warp,
                    drift_report,
                    thread.name,
                    str(Path(target_path).parent) if target_path else ".",
                    adaptive=thread.target.schema_drift == "adaptive",
                )

            # Step 14 — persist watermark state
            if (
                watermark_store is not None
                and load_mode in ("incremental_watermark", "cdc")
                and thread.load is not None
                and new_hwm is not None
            ):
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
                    watermark_persisted = True
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

            # Step 15 — run post-write assertions
            if thread.assertions:
                assertion_results = evaluate_assertions(spark, thread.assertions, target_path)

            # Step 16 — write exports (secondary outputs)
            export_results: list[ExportResult] = []
            if thread.exports:
                # Build AuditContext for export path resolution if not already built
                if audit_ctx is None:
                    audit_ctx = _build_audit_context(
                        thread, weave_name, loom_name, run_timestamp, run_id
                    )

                resolved = resolve_exports(thread.exports, audit_ctx)
                for export in resolved:
                    exp = export
                    if export.connection:
                        conn_name = export.connection
                        if not connections or conn_name not in connections:
                            raise ExportError(
                                f"Export '{export.name}' references undefined "
                                f"connection '{conn_name}'",
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
                    result = write_export(spark, df, exp, row_count=rows_written)
                    export_results.append(result)
                    if result.status == "aborted":
                        raise ExportError(
                            f"Export '{export.name}' failed: {result.error}",
                            thread_name=thread.name,
                            export_name=export.name,
                            export_type=export.type,
                        )

            # Run thread-level post_steps (after core execution, before result)
            if thread.post_steps:
                run_hook_steps(
                    spark,
                    thread.post_steps,
                    "post",
                    thread_variable_ctx,
                    params=resolved_params,
                )

            # Step 18 — build result
            samples: dict[str, list[dict[str, Any]]] | None = None
            if output_sample:
                samples = {"output": output_sample}
                if quarantine_sample:
                    samples["quarantine"] = quarantine_sample

            telemetry = _build_telemetry(
                span_builder,
                validation_results,
                assertion_results,
                rows_read,
                rows_written,
                rows_quarantined,
                collector=collector,
                load_mode=load_mode,
                watermark_column=thread.load.watermark_column if thread.load else None,
                watermark_previous_value=prior_state.last_value if prior_state else None,
                watermark_new_value=new_hwm,
                watermark_persisted=watermark_persisted,
                watermark_first_run=watermark_first_run,
                cdc_counts=cdc_counts,
                rows_after_transforms=rows_after_transforms,
                resolved_params=resolved_params,
                audit_columns_applied=audit_columns_applied,
                export_results=export_results,
                warp_name=resolved_warp.description if resolved_warp else None,
                warp_source="explicit"
                if isinstance(thread.target.warp, str)
                else ("auto" if resolved_warp is not None else None),
                warp_enforcement_mode=thread.target.warp_enforcement if resolved_warp else None,
                drift_detected=drift_report.has_drift if drift_report else False,
                drift_columns=drift_report.extra_columns if drift_report else None,
                drift_mode=drift_report.drift_mode if drift_report else None,
                drift_action_taken=drift_report.action_taken if drift_report else None,
            )

            # Build drift report dict for ThreadResult
            drift_report_dict = None
            if drift_report is not None and drift_report.has_drift:
                drift_report_dict = {
                    "extra_columns": drift_report.extra_columns,
                    "action_taken": drift_report.action_taken,
                    "drift_mode": drift_report.drift_mode,
                    "baseline_source": drift_report.baseline_source,
                }

            return ThreadResult(
                status="success",
                thread_name=thread.name,
                rows_written=rows_written,
                write_mode=write_mode,
                target_path=target_path,
                telemetry=telemetry,
                output_schema=output_schema,
                samples=samples,
                drift_report=drift_report_dict,
                warp_findings=warp_findings if warp_findings else None,
            )

        except DataValidationError:
            _build_telemetry(
                span_builder,
                validation_results,
                assertion_results,
                rows_read,
                rows_written,
                rows_quarantined,
                status=SpanStatus.ERROR,
                collector=collector,
            )
            raise

        except WarpEnforcementError as exc:
            if span_builder is not None:
                span = span_builder.finish(status=SpanStatus.ERROR)
                if collector is not None:
                    collector.add_span(span)
            if exc.thread_name is None:
                exc.thread_name = thread.name
            raise

        except SchemaDriftError as exc:
            if span_builder is not None:
                span = span_builder.finish(status=SpanStatus.ERROR)
                if collector is not None:
                    collector.add_span(span)
            if exc.thread_name is None:
                exc.thread_name = thread.name
            raise

        except ExecutionError as exc:
            if span_builder is not None:
                span = span_builder.finish(status=SpanStatus.ERROR)
                if collector is not None:
                    collector.add_span(span)
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
            if span_builder is not None:
                span = span_builder.finish(status=SpanStatus.ERROR)
                if collector is not None:
                    collector.add_span(span)
            raise ExecutionError(
                f"Thread '{thread.name}' failed unexpectedly",
                cause=exc,
                thread_name=thread.name,
            ) from exc

    finally:
        # Cleanup thread-level lookups only; weave-level cleanup is the caller's job
        if thread_cached_lookups:
            cleanup_lookups(thread_cached_lookups)


def _resolve_with_block(
    thread: Thread,
    sources_map: dict[str, Any],
    collector: SpanCollector | None = None,
    parent_span_id: str | None = None,
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
        cte_df = run_pipeline(cte_df, cte.steps, enriched)
        row_count = cte_df.count()
        enriched[cte_name] = cte_df

        if cte_builder is not None:
            cte_builder.set_attribute("row_count", row_count)
            cte_builder.set_attribute("from", cte.from_)
            cte_builder.set_attribute("step_count", len(cte.steps))
            span = cte_builder.finish()
            collector.add_span(span)  # type: ignore[union-attr]

    return enriched


def _apply_warp_and_drift(
    df: Any,
    resolved_warp: Any,
    thread: Thread,
    engine_col_names: list[str],
    spark: SparkSession | None = None,
    target_path: str | None = None,
) -> tuple[Any, list[dict[str, str]], Any]:
    """Apply warp enforcement, drift detection, and warp-only append.

    Called after audit column injection, before writing. Handles drift
    detection, warp enforcement, and warp-only column append in sequence.

    Args:
        df: Working DataFrame.
        resolved_warp: Resolved WarpConfig, or None.
        thread: Thread configuration.
        engine_col_names: Engine-managed column names (audit, SCD, etc.).
        spark: SparkSession for table-based drift baseline fallback.
        target_path: Target path for table-based drift baseline fallback.

    Returns:
        Tuple of (modified df, warp findings, drift report).
    """
    from weevr.config.warp import build_effective_warp, get_drift_baseline

    warp_findings: list[dict[str, str]] = []

    # Include _weevr_-prefixed engine columns (SCD, soft-delete)
    # consistently across all write paths
    weevr_cols = [c for c in df.schema.names if c.startswith("_weevr_")]
    all_engine = list(set(engine_col_names + weevr_cols))

    # Drift detection (works with or without warp)
    baseline = get_drift_baseline(warp=resolved_warp, spark=spark, target_path=target_path)
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
    """Build an AuditContext for context variable resolution."""
    effective_weave = weave_name or (
        thread.qualified_key.rsplit(".", 1)[0] if "." in thread.qualified_key else ""
    )
    primary_alias = next(iter(thread.sources))
    primary_source = thread.sources[primary_alias]
    return AuditContext(
        thread_name=thread.name,
        thread_qualified_key=thread.qualified_key,
        thread_source=primary_source.alias,
        thread_sources_json=build_sources_json(thread.sources),
        weave_name=effective_weave,
        loom_name=loom_name,
        run_timestamp=run_timestamp,
        run_id=run_id,
    )


def _validate_incremental(thread: Thread, load_mode: str) -> None:
    """Run cross-cutting incremental config validation at executor entry."""
    from weevr.config.validation import validate_incremental_config

    raw = {"load": {"mode": load_mode}}
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


def _build_telemetry(
    span_builder: SpanBuilder | None,
    validation_results: list,
    assertion_results: list,
    rows_read: int,
    rows_written: int,
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
    drift_mode: str | None = None,
    drift_action_taken: str | None = None,
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
        drift_mode=drift_mode,
        drift_action_taken=drift_action_taken,
    )
