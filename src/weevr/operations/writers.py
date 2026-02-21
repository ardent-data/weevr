"""Target mapping and Delta write operations."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from weevr.errors.exceptions import ExecutionError
from weevr.model.target import Target
from weevr.model.write import WriteConfig


def apply_target_mapping(df: DataFrame, target: Target, spark: SparkSession) -> DataFrame:
    """Apply target column mapping to shape the DataFrame for writing.

    Column-level transformations (expr, type, default, drop) are applied first.
    Then the column set is narrowed according to the mapping mode:

    - **auto**: if the target table already exists, keep only columns present in
      its schema. If it does not yet exist, all columns pass through.
    - **explicit**: keep only columns explicitly declared in ``target.columns``
      (drop columns excluded).

    Args:
        df: Input DataFrame.
        target: Target configuration including mapping mode and column specs.
        spark: Active SparkSession (used to probe existing target schema).

    Returns:
        DataFrame shaped for writing to the target.
    """
    result = df
    cols_to_drop: set[str] = set()

    # Apply column-level transformations and collect drop markers.
    if target.columns:
        for col_name, mapping in target.columns.items():
            if mapping.drop:
                cols_to_drop.add(col_name)
                continue

            # Build a single column expression that chains expr → type → default.
            col_expr = F.expr(mapping.expr) if mapping.expr is not None else F.col(col_name)
            if mapping.type is not None:
                col_expr = col_expr.cast(mapping.type)
            if mapping.default is not None:
                col_expr = F.coalesce(col_expr, F.lit(mapping.default))

            result = result.withColumn(col_name, col_expr)

    # Narrow column set according to mapping mode.
    if target.mapping_mode == "auto":
        target_path = target.alias or target.path
        if target_path and _delta_table_exists(spark, target_path):
            existing_cols = spark.read.format("delta").load(target_path).columns
            keep = [c for c in existing_cols if c in result.columns and c not in cols_to_drop]
            return result.select(keep)
        # New target — pass all columns through, honoring drop markers.
        if cols_to_drop:
            result = result.drop(*cols_to_drop)
        return result

    # explicit mode — keep only declared non-dropped columns.
    if target.columns:
        keep = [c for c in target.columns if c not in cols_to_drop]
        return result.select(keep)

    # explicit with no columns declared — nothing to select, return as-is.
    return result


def write_target(
    spark: SparkSession,
    df: DataFrame,
    target: Target,
    write_config: WriteConfig | None,
    target_path: str,
) -> int:
    """Write a DataFrame to a Delta table.

    Args:
        spark: Active SparkSession.
        df: DataFrame to write.
        target: Target configuration (partition_by).
        write_config: Write mode and merge parameters. Defaults to overwrite when None.
        target_path: Physical path for the Delta table.

    Returns:
        Number of rows in ``df`` (rows written for overwrite/append; input rows for merge).

    Raises:
        ExecutionError: If the write operation fails.
    """
    mode = write_config.mode if write_config else "overwrite"
    partition_cols = target.partition_by or []
    target_exists = _delta_table_exists(spark, target_path)

    try:
        row_count = df.count()

        if mode == "overwrite" or not target_exists:
            # First write (any mode) and overwrite: write as overwrite to create/replace table.
            writer = df.write.format("delta").mode("overwrite")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(target_path)

        elif mode == "append":
            writer = df.write.format("delta").mode("append")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(target_path)

        elif mode == "merge":
            if write_config is None:
                raise ExecutionError("merge mode requires a write_config with match_keys")
            if write_config.match_keys is None:
                raise ExecutionError("merge mode requires 'match_keys' to be set")
            _execute_merge(spark, df, write_config, target_path)

        return row_count

    except ExecutionError:
        raise
    except Exception as exc:
        raise ExecutionError(
            f"Failed to write to '{target_path}' (mode={mode})",
            cause=exc,
        ) from exc


def _execute_merge(
    spark: SparkSession,
    df: DataFrame,
    write_config: WriteConfig,
    target_path: str,
) -> None:
    """Execute a Delta merge operation against an existing table."""
    if write_config.match_keys is None:
        raise ExecutionError("merge mode requires 'match_keys' to be set")
    merge_condition = " AND ".join(f"target.{k} = source.{k}" for k in write_config.match_keys)
    delta_table = DeltaTable.forPath(spark, target_path)
    merger = delta_table.alias("target").merge(df.alias("source"), merge_condition)

    has_when_clause = False

    if write_config.on_match == "update":
        merger = merger.whenMatchedUpdateAll()
        has_when_clause = True
    # on_match == "ignore": no whenMatched clause → matched rows unchanged

    if write_config.on_no_match_target == "insert":
        merger = merger.whenNotMatchedInsertAll()
        has_when_clause = True
    # on_no_match_target == "ignore": no insert clause → new source rows discarded

    if write_config.on_no_match_source == "soft_delete":
        raise ExecutionError(
            "on_no_match_source='soft_delete' is not supported in this version; "
            "use 'delete' or 'ignore'"
        )
    if write_config.on_no_match_source == "delete":
        merger = merger.whenNotMatchedBySourceDelete()
        has_when_clause = True
    # on_no_match_source == "ignore": no delete clause → unmatched target rows kept

    # Delta requires at least one WHEN clause; if all are "ignore", nothing to do.
    if has_when_clause:
        merger.execute()


def _delta_table_exists(spark: SparkSession, path: str) -> bool:
    """Return True if a Delta table exists at the given path."""
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False
