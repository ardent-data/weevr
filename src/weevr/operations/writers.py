"""Target mapping and Delta write operations."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from weevr.model.target import Target


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


def _delta_table_exists(spark: SparkSession, path: str) -> bool:
    """Return True if a Delta table exists at the given path."""
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False
