"""Thread executor — orchestrates the full read → transform → write pipeline."""

from __future__ import annotations

import contextlib
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import SparkSession

from weevr.engine.lookups import resolve_thread_lookups
from weevr.engine.result import ThreadResult
from weevr.errors.exceptions import DataValidationError, ExecutionError, ExportError, StateError
from weevr.model.lookup import Lookup
from weevr.model.thread import Thread
from weevr.model.write import WriteConfig
from weevr.operations.assertions import evaluate_assertions
from weevr.operations.audit import AuditContext, build_sources_json, inject_audit_columns
from weevr.operations.exports import resolve_exports, write_export
from weevr.operations.hashing import compute_keys
from weevr.operations.naming import normalize_columns
from weevr.operations.pipeline import run_pipeline
from weevr.operations.quarantine import write_quarantine
from weevr.operations.readers import (
    read_cdc_source,
    read_source,
    read_source_incremental,
    read_sources,
)
from weevr.operations.validation import validate_dataframe
from weevr.operations.writers import apply_target_mapping, execute_cdc_merge, write_target
from weevr.state import WatermarkState, WatermarkStore, resolve_store
from weevr.telemetry.collector import SpanBuilder, SpanCollector
from weevr.telemetry.results import ExportResult, ThreadTelemetry
from weevr.telemetry.span import ExecutionSpan, SpanStatus, generate_span_id, generate_trace_id

logger = logging.getLogger(__name__)


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
) -> ThreadResult:
    """Execute a single thread from sources through transforms to a Delta target.

    Execution order:
    1. Resolve lookup-based sources from cached or on-demand DataFrames.
    2. Read all remaining declared sources into DataFrames.
       - For ``incremental_watermark``: load prior HWM, apply filter, capture new HWM.
       - For ``cdc``: read via CDF or generic CDC source.
    3. Set the primary (first) source as the working DataFrame.
    4. Run pipeline steps against the working DataFrame.
    5. Run validation rules (if configured) — quarantine or abort on failures.
    6. Apply naming normalization (if configured).
    7. Compute business keys and hashes if configured.
    8. Resolve the target write path.
    9. Apply target column mapping.
    10. Inject audit columns (if configured; bypasses mapping mode).
    11. Write to the Delta target.
        - For ``cdc``: use CDC merge routing instead of standard write.
    12. Persist watermark state (if applicable).
    13. Run post-write assertions (if configured).
    14. Build telemetry and return ThreadResult.

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

    Returns:
        :class:`ThreadResult` with status, row count, write mode, target path,
        and optional telemetry.

    Raises:
        ExecutionError: If any step fails, with ``thread_name`` set on the error.
        DataValidationError: If a fatal-severity validation rule fails.
    """
    # Generate per-execution run context (shared across audit columns and exports)
    run_timestamp = datetime.now(UTC).isoformat()
    run_id = str(uuid.uuid4())

    span_builder = None
    if collector is not None:
        span_label = thread.qualified_key or thread.name
        span_builder = collector.start_span(f"thread:{span_label}", parent_span_id=parent_span_id)

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

    try:
        # Resolve target path early (needed for watermark store resolution)
        target_path = thread.target.alias or thread.target.path
        if target_path is None:
            raise ExecutionError(
                "Target has no 'alias' or 'path' — cannot resolve write location",
                thread_name=thread.name,
            )

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

        # --- Step 0: Resolve lookup-based sources ---
        lookup_dfs: dict[str, Any] = {}
        if cached_lookups is not None or weave_lookups is not None:
            lookup_dfs = resolve_thread_lookups(
                thread.sources,
                weave_lookups or {},
                cached_lookups or {},
                spark,
            )

        # --- Step 1: Read sources ---
        if load_mode == "incremental_watermark" and thread.load is not None:
            # Read primary source with watermark filter
            primary_alias = next(iter(thread.sources))
            primary_source = thread.sources[primary_alias]
            primary_df, new_hwm = read_source_incremental(
                spark, primary_alias, primary_source, thread.load, prior_state
            )

            # Read secondary sources normally (skip lookup-resolved ones)
            sources_map = {primary_alias: primary_df}
            for alias, source in thread.sources.items():
                if alias != primary_alias and alias not in lookup_dfs:
                    sources_map[alias] = read_source(spark, alias, source)
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

            primary_df = read_cdc_source(spark, primary_source, thread.load.cdc, last_version)

            # Capture new CDF version if Delta CDF
            if thread.load.cdc.preset == "delta_cdf" and "_commit_version" in primary_df.columns:
                from pyspark.sql import functions as F

                max_row = primary_df.agg(F.max("_commit_version").alias("mv")).collect()
                if max_row and max_row[0]["mv"] is not None:
                    new_hwm = str(max_row[0]["mv"])

            # Read secondary sources normally (skip lookup-resolved ones)
            sources_map = {primary_alias: primary_df}
            for alias, source in thread.sources.items():
                if alias != primary_alias and alias not in lookup_dfs:
                    sources_map[alias] = read_source(spark, alias, source)
            sources_map.update(lookup_dfs)
        else:
            # Full mode or incremental_parameter — read non-lookup sources normally
            normal_sources = {k: v for k, v in thread.sources.items() if k not in lookup_dfs}
            sources_map = read_sources(spark, normal_sources)
            sources_map.update(lookup_dfs)

        # Step 2 — primary DataFrame is the first declared source
        df = next(iter(sources_map.values()))
        rows_read = df.count()

        # Step 3 — run pipeline steps
        df = run_pipeline(df, thread.steps, sources_map)

        # Capture intermediate row count for waterfall visualization
        rows_after_transforms = df.count()

        # Step 4 — run validation rules
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

        # Step 4b — apply naming normalization (before target mapping)
        if thread.target.naming is not None:
            df = normalize_columns(df, thread.target.naming)

        # Step 5 — compute keys and hashes
        if thread.keys is not None:
            df = compute_keys(df, thread.keys)

        # Step 6 — resolve and prepare audit columns
        audit_columns = thread.target.audit_columns or {}
        audit_columns_applied: list[str] = []
        audit_ctx: AuditContext | None = None
        if audit_columns:
            audit_ctx = _build_audit_context(thread, weave_name, loom_name, run_timestamp, run_id)

        # Step 7/8 — write to Delta
        write_mode = thread.write.mode if thread.write else "overwrite"

        if load_mode == "cdc" and thread.load is not None and thread.load.cdc is not None:
            # CDC merge routing — skip target column mapping because
            # execute_cdc_merge handles column selection internally
            # (it must retain the operation column for row routing).
            if audit_columns and audit_ctx is not None:
                df = inject_audit_columns(df, audit_columns, audit_ctx)
                audit_columns_applied = list(audit_columns)
            output_schema = [(name, str(dtype)) for name, dtype in df.dtypes]
            with contextlib.suppress(Exception):
                output_sample = df.limit(10).toPandas().to_dict("records")
            cdc_write = _validate_cdc_write_config(thread)
            cdc_counts = execute_cdc_merge(spark, df, target_path, cdc_write, thread.load.cdc)
            rows_written = sum(cdc_counts.values())
        else:
            # Apply target column mapping for non-CDC writes
            df = apply_target_mapping(df, thread.target, spark)
            if audit_columns and audit_ctx is not None:
                df = inject_audit_columns(df, audit_columns, audit_ctx)
                audit_columns_applied = list(audit_columns)
            output_schema = [(name, str(dtype)) for name, dtype in df.dtypes]
            with contextlib.suppress(Exception):
                output_sample = df.limit(10).toPandas().to_dict("records")
            rows_written = write_target(spark, df, thread.target, thread.write, target_path)

        # Step 9 — persist watermark state
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

        # Step 10 — run post-write assertions
        if thread.assertions:
            assertion_results = evaluate_assertions(spark, thread.assertions, target_path)

        # Step 11 — write exports (secondary outputs)
        export_results: list[ExportResult] = []
        if thread.exports:
            # Build AuditContext for export path resolution if not already built
            if audit_ctx is None:
                audit_ctx = _build_audit_context(
                    thread, weave_name, loom_name, run_timestamp, run_id
                )

            resolved = resolve_exports(thread.exports, audit_ctx)
            for export in resolved:
                result = write_export(spark, df, export, row_count=rows_written)
                export_results.append(result)
                if result.status == "aborted":
                    raise ExportError(
                        f"Export '{export.name}' failed: {result.error}",
                        thread_name=thread.name,
                        export_name=export.name,
                        export_type=export.type,
                    )

        # Step 12 — build result
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
        )

        return ThreadResult(
            status="success",
            thread_name=thread.name,
            rows_written=rows_written,
            write_mode=write_mode,
            target_path=target_path,
            telemetry=telemetry,
            output_schema=output_schema,
            samples=samples,
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
    )
