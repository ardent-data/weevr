"""Thread executor — orchestrates the full read → transform → write pipeline."""

from pyspark.sql import SparkSession

from weevr.engine.result import ThreadResult
from weevr.errors.exceptions import ExecutionError
from weevr.model.thread import Thread
from weevr.operations.hashing import compute_keys
from weevr.operations.pipeline import run_pipeline
from weevr.operations.readers import read_sources
from weevr.operations.writers import apply_target_mapping, write_target


def execute_thread(spark: SparkSession, thread: Thread) -> ThreadResult:
    """Execute a single thread from sources through transforms to a Delta target.

    Execution order:
    1. Read all declared sources into DataFrames.
    2. Set the primary (first) source as the working DataFrame.
    3. Run pipeline steps against the working DataFrame.
    4. Compute business keys and hashes if configured.
    5. Resolve the target write path.
    6. Apply target column mapping.
    7. Write to the Delta target.
    8. Return a :class:`ThreadResult` describing the outcome.

    Args:
        spark: Active SparkSession.
        thread: Thread configuration to execute.

    Returns:
        :class:`ThreadResult` with status, row count, write mode, and target path.

    Raises:
        ExecutionError: If any step fails, with ``thread_name`` set on the error.
    """
    try:
        # Step 1 — read all sources
        sources_map = read_sources(spark, thread.sources)

        # Step 2 — primary DataFrame is the first declared source
        df = next(iter(sources_map.values()))

        # Step 3 — run pipeline steps
        df = run_pipeline(df, thread.steps, sources_map)

        # Step 4 — compute keys and hashes
        if thread.keys is not None:
            df = compute_keys(df, thread.keys)

        # Step 5 — resolve target write path
        target_path = thread.target.alias or thread.target.path
        if target_path is None:
            raise ExecutionError(
                "Target has no 'alias' or 'path' — cannot resolve write location",
                thread_name=thread.name,
            )

        # Step 6 — apply target column mapping
        df = apply_target_mapping(df, thread.target, spark)

        # Step 7 — write to Delta
        write_mode = thread.write.mode if thread.write else "overwrite"
        rows = write_target(spark, df, thread.target, thread.write, target_path)

        # Step 8 — return result
        return ThreadResult(
            status="success",
            thread_name=thread.name,
            rows_written=rows,
            write_mode=write_mode,
            target_path=target_path,
        )

    except ExecutionError as exc:
        # Re-raise, enriching with thread_name if not already set
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
        raise ExecutionError(
            f"Thread '{thread.name}' failed unexpectedly",
            cause=exc,
            thread_name=thread.name,
        ) from exc
