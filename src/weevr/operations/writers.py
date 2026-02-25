"""Target mapping and Delta write operations."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from weevr.errors.exceptions import ExecutionError
from weevr.model.load import CdcConfig
from weevr.model.target import Target
from weevr.model.write import WriteConfig


def _quote_identifier(name: str) -> str:
    """Backtick-escape a SQL identifier to prevent injection.

    Escapes any existing backticks by doubling them, then wraps the name
    in backticks so it is treated as a single identifier token.
    """
    return f"`{name.replace('`', '``')}`"


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
    merge_condition = " AND ".join(
        f"target.{_quote_identifier(k)} = source.{_quote_identifier(k)}"
        for k in write_config.match_keys
    )
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
        if not write_config.soft_delete_column:
            raise ExecutionError(
                "on_no_match_source='soft_delete' requires 'soft_delete_column' on WriteConfig"
            )
        merger = merger.whenNotMatchedBySourceUpdate(
            set={write_config.soft_delete_column: F.lit(write_config.soft_delete_value)}
        )
        has_when_clause = True
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


def resolve_cdc_columns(cdc_config: CdcConfig) -> dict[str, str | None]:
    """Resolve CDC column mapping, expanding presets if needed.

    Returns a dict with keys: ``operation_column``, ``insert_value``,
    ``update_value``, ``delete_value``.
    """
    if cdc_config.preset == "delta_cdf":
        return {
            "operation_column": "_change_type",
            "insert_value": "insert",
            "update_value": "update_postimage",
            "delete_value": "delete",
        }
    return {
        "operation_column": cdc_config.operation_column,
        "insert_value": cdc_config.insert_value,
        "update_value": cdc_config.update_value,
        "delete_value": cdc_config.delete_value,
    }


def execute_cdc_merge(
    spark: SparkSession,
    df: DataFrame,
    target_path: str,
    write_config: WriteConfig,
    cdc_config: CdcConfig,
) -> dict[str, int]:
    """Execute a CDC merge operation, routing rows by operation type.

    Rows are classified by the ``operation_column`` and merged against
    the target table using the ``match_keys`` from ``write_config``.

    Args:
        spark: Active SparkSession.
        df: Source DataFrame containing CDC rows with operation column.
        target_path: Path to the Delta target table.
        write_config: Write configuration (must be merge mode with match_keys).
        cdc_config: CDC configuration (preset or explicit mapping).

    Returns:
        Dict with counts: ``{"inserts": N, "updates": N, "deletes": N}``.

    Raises:
        ExecutionError: If the merge operation fails.
    """
    if not write_config.match_keys:
        raise ExecutionError("CDC merge requires 'match_keys' on write_config")

    cols = resolve_cdc_columns(cdc_config)
    op_col = cols["operation_column"]
    if op_col is None:
        raise ExecutionError("CDC config must resolve an operation_column")

    insert_val = cols["insert_value"]
    update_val = cols["update_value"]
    delete_val = cols["delete_value"]

    merge_condition = " AND ".join(
        f"target.{_quote_identifier(k)} = source.{_quote_identifier(k)}"
        for k in write_config.match_keys
    )

    # Drop CDC metadata columns before writing to target
    cdc_meta_cols: set[str] = {"_change_type", "_commit_version", "_commit_timestamp"}
    if op_col:
        cdc_meta_cols.add(op_col)
    data_cols = [c for c in df.columns if c not in cdc_meta_cols]

    # Filter to recognized operation values only (e.g., exclude CDF update_preimage)
    recognized_ops = [v for v in (insert_val, update_val, delete_val) if v]
    if recognized_ops:
        df = df.filter(F.col(op_col).isin(recognized_ops))

    counts: dict[str, int] = {"inserts": 0, "updates": 0, "deletes": 0}
    target_exists = _delta_table_exists(spark, target_path)

    try:
        # Count operations
        if insert_val:
            counts["inserts"] = df.filter(F.col(op_col) == insert_val).count()
        if update_val:
            counts["updates"] = df.filter(F.col(op_col) == update_val).count()
        if delete_val:
            counts["deletes"] = df.filter(F.col(op_col) == delete_val).count()

        if not target_exists:
            # First CDC write — insert all non-delete rows
            insert_df = df
            if delete_val:
                insert_df = df.filter(F.col(op_col) != delete_val)
            insert_df.select(data_cols).write.format("delta").mode("overwrite").save(target_path)
            return counts

        delta_table = DeltaTable.forPath(spark, target_path)
        merger = delta_table.alias("target").merge(
            df.select(data_cols + [op_col]).alias("source"),
            merge_condition,
        )

        # Route by operation type
        quoted_op = _quote_identifier(op_col)

        if update_val:
            escaped_val = update_val.replace("'", "''")
            update_set: dict[str, str | Column] = {
                c: F.col(f"source.{_quote_identifier(c)}")
                for c in data_cols
                if c not in write_config.match_keys
            }
            merger = merger.whenMatchedUpdate(
                condition=f"source.{quoted_op} = '{escaped_val}'",
                set=update_set,
            )

        if delete_val:
            escaped_del = delete_val.replace("'", "''")
            if cdc_config.on_delete == "hard_delete":
                merger = merger.whenMatchedDelete(condition=f"source.{quoted_op} = '{escaped_del}'")
            elif cdc_config.on_delete == "soft_delete":
                if not write_config.soft_delete_column:
                    raise ExecutionError(
                        "CDC soft_delete requires 'soft_delete_column' on write_config"
                    )
                merger = merger.whenMatchedUpdate(
                    condition=f"source.{quoted_op} = '{escaped_del}'",
                    set={write_config.soft_delete_column: F.lit(write_config.soft_delete_value)},
                )

        if insert_val:
            escaped_ins = insert_val.replace("'", "''")
            insert_set: dict[str, str | Column] = {
                c: F.col(f"source.{_quote_identifier(c)}") for c in data_cols
            }
            merger = merger.whenNotMatchedInsert(
                condition=f"source.{quoted_op} = '{escaped_ins}'",
                values=insert_set,
            )

        merger.execute()
        return counts

    except ExecutionError:
        raise
    except Exception as exc:
        raise ExecutionError(
            f"CDC merge failed on target '{target_path}'",
            cause=exc,
        ) from exc
