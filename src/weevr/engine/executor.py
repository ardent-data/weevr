"""Thread executor — orchestrates the full read → transform → write pipeline."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from weevr.engine.result import ThreadResult
from weevr.errors.exceptions import DataValidationError, ExecutionError, StateError
from weevr.model.thread import Thread
from weevr.model.write import WriteConfig
from weevr.operations.assertions import evaluate_assertions
from weevr.operations.hashing import compute_keys
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
from weevr.state.watermark import WatermarkState, WatermarkStore, resolve_store
from weevr.telemetry.collector import SpanBuilder, SpanCollector
from weevr.telemetry.results import ThreadTelemetry
from weevr.telemetry.span import ExecutionSpan, SpanStatus, generate_span_id, generate_trace_id

logger = logging.getLogger(__name__)


def execute_thread(
    spark: SparkSession,
    thread: Thread,
    collector: SpanCollector | None = None,
    parent_span_id: str | None = None,
) -> ThreadResult:
    """Execute a single thread from sources through transforms to a Delta target.

    Execution order:
    1. Read all declared sources into DataFrames.
       - For ``incremental_watermark``: load prior HWM, apply filter, capture new HWM.
       - For ``cdc``: read via CDF or generic CDC source.
    2. Set the primary (first) source as the working DataFrame.
    3. Run pipeline steps against the working DataFrame.
    4. Run validation rules (if configured) — quarantine or abort on failures.
    5. Compute business keys and hashes if configured.
    6. Resolve the target write path.
    7. Apply target column mapping.
    8. Write to the Delta target.
       - For ``cdc``: use CDC merge routing instead of standard write.
    9. Persist watermark state (if applicable).
    10. Run post-write assertions (if configured).
    11. Build telemetry and return ThreadResult.

    Args:
        spark: Active SparkSession.
        thread: Thread configuration to execute.
        collector: Optional span collector for telemetry. When provided,
            a thread span is created and finalized.
        parent_span_id: Optional parent span ID for trace tree linkage.

    Returns:
        :class:`ThreadResult` with status, row count, write mode, target path,
        and optional telemetry.

    Raises:
        ExecutionError: If any step fails, with ``thread_name`` set on the error.
        DataValidationError: If a fatal-severity validation rule fails.
    """
    span_builder = None
    if collector is not None:
        span_builder = collector.start_span(f"thread:{thread.name}", parent_span_id=parent_span_id)

    validation_results: list = []
    assertion_results: list = []
    rows_read = 0
    rows_written = 0
    rows_quarantined = 0

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

        # --- Step 1: Read sources ---
        if load_mode == "incremental_watermark" and thread.load is not None:
            # Read primary source with watermark filter
            primary_alias = next(iter(thread.sources))
            primary_source = thread.sources[primary_alias]
            primary_df, new_hwm = read_source_incremental(
                spark, primary_alias, primary_source, thread.load, prior_state
            )

            # Read secondary sources normally
            sources_map = {primary_alias: primary_df}
            for alias, source in thread.sources.items():
                if alias != primary_alias:
                    sources_map[alias] = read_source(spark, alias, source)

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

            sources_map = {primary_alias: primary_df}
            for alias, source in thread.sources.items():
                if alias != primary_alias:
                    sources_map[alias] = read_source(spark, alias, source)
        else:
            # Full mode or incremental_parameter — read all sources normally
            sources_map = read_sources(spark, thread.sources)

        # Step 2 — primary DataFrame is the first declared source
        df = next(iter(sources_map.values()))
        rows_read = df.count()

        # Step 3 — run pipeline steps
        df = run_pipeline(df, thread.steps, sources_map)

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

            # Continue with clean_df
            df = outcome.clean_df

        # Step 5 — compute keys and hashes
        if thread.keys is not None:
            df = compute_keys(df, thread.keys)

        # Step 7/8 — write to Delta
        write_mode = thread.write.mode if thread.write else "overwrite"

        if load_mode == "cdc" and thread.load is not None and thread.load.cdc is not None:
            # CDC merge routing — skip target column mapping because
            # execute_cdc_merge handles column selection internally
            # (it must retain the operation column for row routing).
            cdc_write = _validate_cdc_write_config(thread)
            cdc_counts = execute_cdc_merge(spark, df, target_path, cdc_write, thread.load.cdc)
            rows_written = sum(cdc_counts.values())
        else:
            # Apply target column mapping for non-CDC writes
            df = apply_target_mapping(df, thread.target, spark)
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

        # Step 11 — build result
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
        )

        return ThreadResult(
            status="success",
            thread_name=thread.name,
            rows_written=rows_written,
            write_mode=write_mode,
            target_path=target_path,
            telemetry=telemetry,
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
        load_mode=load_mode,
        watermark_column=watermark_column,
        watermark_previous_value=watermark_previous_value,
        watermark_new_value=watermark_new_value,
        watermark_persisted=watermark_persisted,
        watermark_first_run=watermark_first_run,
        cdc_inserts=cdc_counts["inserts"] if cdc_counts else None,
        cdc_updates=cdc_counts["updates"] if cdc_counts else None,
        cdc_deletes=cdc_counts["deletes"] if cdc_counts else None,
    )
