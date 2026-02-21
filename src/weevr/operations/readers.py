"""Source readers — read one or more sources into Spark DataFrames."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from weevr.errors.exceptions import ExecutionError
from weevr.model.source import DedupConfig, Source


def read_source(spark: SparkSession, alias: str, source: Source) -> DataFrame:
    """Read a single source into a DataFrame.

    Args:
        spark: Active SparkSession.
        alias: Logical name for this source (used in error messages).
        source: Source configuration.

    Returns:
        DataFrame with source data, with dedup applied if configured.

    Raises:
        ExecutionError: If the source cannot be read or the type is unsupported.
    """
    try:
        df = _read_raw(spark, source)
        if source.dedup is not None:
            df = _apply_dedup(df, source.dedup)
        return df
    except ExecutionError:
        raise
    except Exception as exc:
        raise ExecutionError(
            f"Failed to read source '{alias}'",
            cause=exc,
            source_name=alias,
        ) from exc


def read_sources(spark: SparkSession, sources: dict[str, Source]) -> dict[str, DataFrame]:
    """Read all sources into a mapping of alias -> DataFrame.

    Args:
        spark: Active SparkSession.
        sources: Mapping of alias -> Source config.

    Returns:
        Mapping of alias -> DataFrame, in the same key order as ``sources``.
    """
    return {alias: read_source(spark, alias, source) for alias, source in sources.items()}


def _read_raw(spark: SparkSession, source: Source) -> DataFrame:
    """Read source data without applying deduplication."""
    if source.type == "delta":
        if source.alias is None:
            raise ExecutionError("Delta source requires 'alias' to be set")
        return spark.read.format("delta").load(source.alias)

    if source.type in {"csv", "json", "parquet"}:
        if source.path is None:
            raise ExecutionError(f"'{source.type}' source requires 'path' to be set")
        return spark.read.format(source.type).options(**source.options).load(source.path)

    raise ExecutionError(f"Unsupported source type: '{source.type}'")


def _apply_dedup(df: DataFrame, dedup: DedupConfig) -> DataFrame:
    """Remove duplicate rows, keeping one row per unique key combination.

    When ``dedup.order_by`` is set, rows are ranked by that expression and only
    rank-1 rows are kept. When omitted, ``dropDuplicates`` is used which retains
    an arbitrary representative row for each key group.
    """
    if dedup.order_by is None:
        return df.dropDuplicates(dedup.keys)

    order_col = _parse_order_col(dedup.order_by)
    window = Window.partitionBy(dedup.keys).orderBy(order_col)
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def _parse_order_col(order_by: str):  # type: ignore[return]
    """Parse an order expression like 'ts DESC' into a Spark sort Column.

    Supports an optional trailing ASC or DESC keyword. The leading expression
    is evaluated via ``F.expr`` so arbitrary SQL expressions are accepted.
    """
    stripped = order_by.strip()
    parts = stripped.rsplit(None, 1)
    if len(parts) == 2 and parts[1].upper() in ("ASC", "DESC"):
        expr_str, direction = parts[0], parts[1].upper()
    else:
        expr_str, direction = stripped, "ASC"

    col = F.expr(expr_str)
    return col.desc() if direction == "DESC" else col.asc()
