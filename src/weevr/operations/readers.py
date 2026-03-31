"""Source readers — read one or more sources into Spark DataFrames."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from weevr.delta import is_table_alias
from weevr.errors.exceptions import ExecutionError
from weevr.model.connection import OneLakeConnection
from weevr.model.load import CdcConfig, LoadConfig
from weevr.model.source import DedupConfig, Source
from weevr.state.watermark import WatermarkState


def read_source(
    spark: SparkSession,
    alias: str,
    source: Source,
    connections: dict[str, OneLakeConnection] | None = None,
) -> DataFrame:
    """Read a single source into a DataFrame.

    Args:
        spark: Active SparkSession.
        alias: Logical name for this source (used in error messages).
        source: Source configuration.
        connections: Named connection declarations keyed by connection name.
            Required when ``source.connection`` is set.

    Returns:
        DataFrame with source data, with dedup applied if configured.

    Raises:
        ExecutionError: If the source cannot be read or the type is unsupported.
    """
    try:
        df = _read_raw(spark, source, connections)
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


def read_sources(
    spark: SparkSession,
    sources: dict[str, Source],
    connections: dict[str, OneLakeConnection] | None = None,
) -> dict[str, DataFrame]:
    """Read all sources into a mapping of alias -> DataFrame.

    Args:
        spark: Active SparkSession.
        sources: Mapping of alias -> Source config.
        connections: Named connection declarations forwarded to each read.

    Returns:
        Mapping of alias -> DataFrame, in the same key order as ``sources``.
    """
    return {
        alias: read_source(spark, alias, source, connections) for alias, source in sources.items()
    }


def _read_raw(
    spark: SparkSession,
    source: Source,
    connections: dict[str, OneLakeConnection] | None = None,
) -> DataFrame:
    """Read source data without applying deduplication."""
    # Connection-based read: resolve to an abfss:// path via the named connection.
    if source.connection:
        if not connections or source.connection not in connections:
            raise ExecutionError(
                f"Source references undefined connection '{source.connection}'",
                source_name=source.connection,
            )
        from weevr.config.paths import build_onelake_path

        conn = connections[source.connection]
        assert source.table is not None  # guaranteed by Source validator
        path = build_onelake_path(conn, source.schema_override, source.table)
        return spark.read.format("delta").load(path)

    if source.type == "delta":
        if source.alias is None:
            raise ExecutionError("Delta source requires 'alias' to be set")
        # Table aliases (schema.table) use the metastore; file paths use load().
        if is_table_alias(source.alias):
            return spark.read.format("delta").table(source.alias)
        from weevr.config.paths import resolve_fuse_path

        return spark.read.format("delta").load(resolve_fuse_path(source.alias, spark))

    if source.type in {"csv", "json", "parquet"}:
        if source.path is None:
            raise ExecutionError(f"'{source.type}' source requires 'path' to be set")
        from weevr.config.paths import resolve_fuse_path

        resolved_path = resolve_fuse_path(source.path, spark)
        return spark.read.format(source.type).options(**source.options).load(resolved_path)

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
    return (
        df.withColumn("__dedup_rn__", F.row_number().over(window))
        .filter(F.col("__dedup_rn__") == 1)
        .drop("__dedup_rn__")
    )


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


def build_watermark_filter(
    watermark_column: str,
    watermark_type: str,
    last_value: str,
    inclusive: bool = False,
) -> Column:
    """Build a Spark Column filter expression for watermark-based incremental reads.

    Args:
        watermark_column: Column name to filter on.
        watermark_type: One of ``timestamp``, ``date``, ``int``, ``long``.
        last_value: Serialized high-water mark value.
        inclusive: If ``True``, use ``>=`` (re-read boundary row).
            Defaults to ``False`` (strict ``>``).

    Returns:
        A Spark Column expression suitable for ``df.filter()``.
    """
    col = F.col(watermark_column)
    if watermark_type in ("timestamp", "date"):
        lit_val = F.lit(last_value).cast(watermark_type)
    elif watermark_type == "long":
        lit_val = F.lit(int(last_value)).cast("long")
    else:
        lit_val = F.lit(int(last_value))
    return col >= lit_val if inclusive else col > lit_val


def read_source_incremental(
    spark: SparkSession,
    alias: str,
    source: Source,
    load_config: LoadConfig,
    prior_state: WatermarkState | None,
    connections: dict[str, OneLakeConnection] | None = None,
) -> tuple[DataFrame, str | None]:
    """Read source with watermark filter and capture new HWM.

    If ``prior_state`` is ``None`` (first run), reads all data.
    Otherwise applies a watermark filter for predicate pushdown.
    HWM is captured as ``MAX(watermark_column)`` from the source read
    (before transforms).

    Args:
        spark: Active SparkSession.
        alias: Logical source name.
        source: Source configuration.
        load_config: Thread-level load configuration.
        prior_state: Previously persisted watermark state, or ``None``.
        connections: Named connection declarations forwarded to the read.

    Returns:
        Tuple of ``(DataFrame, new_hwm_value)``. ``new_hwm_value`` is
        ``None`` if the source returned zero rows.
    """
    df = _read_raw(spark, source, connections)

    # Apply watermark filter if prior state exists
    if (
        prior_state is not None
        and load_config.watermark_column is not None
        and load_config.watermark_type is not None
    ):
        filter_expr = build_watermark_filter(
            watermark_column=load_config.watermark_column,
            watermark_type=load_config.watermark_type,
            last_value=prior_state.last_value,
            inclusive=load_config.watermark_inclusive,
        )
        df = df.filter(filter_expr)

    # Capture HWM before dedup (from filtered source)
    new_hwm: str | None = None
    if load_config.watermark_column is not None:
        hwm_row = df.agg(F.max(F.col(load_config.watermark_column)).alias("hwm")).collect()
        if hwm_row and hwm_row[0]["hwm"] is not None:
            new_hwm = str(hwm_row[0]["hwm"])

    # Apply dedup after HWM capture
    if source.dedup is not None:
        df = _apply_dedup(df, source.dedup)

    return df, new_hwm


def read_cdc_source(
    spark: SparkSession,
    source: Source,
    cdc_config: CdcConfig,
    last_version: int | None = None,
    connections: dict[str, OneLakeConnection] | None = None,
) -> DataFrame:
    """Read a CDC source, optionally starting from a specific version.

    For ``preset=delta_cdf``, reads with Delta Change Data Feed options.
    For generic CDC (explicit column mapping), reads the source normally
    since change flags are already in the data.

    Args:
        spark: Active SparkSession.
        source: Source configuration (must be Delta for CDF preset).
        cdc_config: CDC configuration with preset or explicit mapping.
        last_version: Last processed CDF commit version. If provided,
            reads changes starting from ``last_version + 1``.
        connections: Named connection declarations forwarded to the read.

    Returns:
        DataFrame with CDC data (includes change type column for CDF).
    """
    if cdc_config.preset == "delta_cdf":
        if source.type != "delta" or source.alias is None:
            raise ExecutionError("Delta CDF preset requires a Delta source with 'alias' set")

        reader = spark.read.format("delta").option("readChangeFeed", "true")
        if last_version is not None:
            reader = reader.option("startingVersion", last_version + 1)
        if is_table_alias(source.alias):
            return reader.table(source.alias)
        return reader.load(source.alias)

    # Generic CDC: read source normally; change flags are in the data
    return _read_raw(spark, source, connections)
