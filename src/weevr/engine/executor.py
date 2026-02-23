"""Thread executor — orchestrates the full read → transform → write pipeline."""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import SparkSession

from weevr.engine.result import ThreadResult
from weevr.errors.exceptions import DataValidationError, ExecutionError
from weevr.model.thread import Thread
from weevr.operations.assertions import evaluate_assertions
from weevr.operations.hashing import compute_keys
from weevr.operations.pipeline import run_pipeline
from weevr.operations.quarantine import write_quarantine
from weevr.operations.readers import read_sources
from weevr.operations.validation import validate_dataframe
from weevr.operations.writers import apply_target_mapping, write_target
from weevr.telemetry.collector import SpanBuilder, SpanCollector
from weevr.telemetry.results import ThreadTelemetry
from weevr.telemetry.span import ExecutionSpan, SpanStatus, generate_span_id, generate_trace_id


def execute_thread(
    spark: SparkSession,
    thread: Thread,
    collector: SpanCollector | None = None,
    parent_span_id: str | None = None,
) -> ThreadResult:
    """Execute a single thread from sources through transforms to a Delta target.

    Execution order:
    1. Read all declared sources into DataFrames.
    2. Set the primary (first) source as the working DataFrame.
    3. Run pipeline steps against the working DataFrame.
    4. Run validation rules (if configured) — quarantine or abort on failures.
    5. Compute business keys and hashes if configured.
    6. Resolve the target write path.
    7. Apply target column mapping.
    8. Write to the Delta target.
    9. Run post-write assertions (if configured).
    10. Build telemetry and return ThreadResult.

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

    validation_results = []
    assertion_results = []
    rows_read = 0
    rows_written = 0
    rows_quarantined = 0
    try:
        # Step 1 — read all sources
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
            target_path_for_q = thread.target.alias or thread.target.path or ""
            if outcome.quarantine_df is not None:
                rows_quarantined = write_quarantine(spark, outcome.quarantine_df, target_path_for_q)

            # Continue with clean_df
            df = outcome.clean_df

        # Step 5 — compute keys and hashes
        if thread.keys is not None:
            df = compute_keys(df, thread.keys)

        # Step 6 — resolve target write path
        target_path = thread.target.alias or thread.target.path
        if target_path is None:
            raise ExecutionError(
                "Target has no 'alias' or 'path' — cannot resolve write location",
                thread_name=thread.name,
            )

        # Step 7 — apply target column mapping
        df = apply_target_mapping(df, thread.target, spark)

        # Step 8 — write to Delta
        write_mode = thread.write.mode if thread.write else "overwrite"
        rows_written = write_target(spark, df, thread.target, thread.write, target_path)

        # Step 9 — run post-write assertions
        if thread.assertions:
            assertion_results = evaluate_assertions(spark, thread.assertions, target_path)

        # Step 10 — build result
        telemetry = _build_telemetry(
            span_builder,
            validation_results,
            assertion_results,
            rows_read,
            rows_written,
            rows_quarantined,
            collector=collector,
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
            span_builder.finish(status=SpanStatus.ERROR)
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
            span_builder.finish(status=SpanStatus.ERROR)
        raise ExecutionError(
            f"Thread '{thread.name}' failed unexpectedly",
            cause=exc,
            thread_name=thread.name,
        ) from exc


def _build_telemetry(
    span_builder: SpanBuilder | None,
    validation_results: list,
    assertion_results: list,
    rows_read: int,
    rows_written: int,
    rows_quarantined: int,
    status: SpanStatus = SpanStatus.OK,
    collector: SpanCollector | None = None,
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
    )
