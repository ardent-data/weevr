"""Warp enforcement, warp-only column append, pre-initialization, and auto-generation."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from weevr.errors import WarpEnforcementError
from weevr.model.warp import WarpColumn, WarpConfig

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from weevr.model.warp import DriftReport, EffectiveWarp

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Warp Enforcement (Task 10)
# ---------------------------------------------------------------------------


def enforce_warp(
    df: DataFrame,
    warp: WarpConfig,
    mode: str,
    engine_columns: list[str] | None = None,
) -> list[dict[str, str]]:
    """Validate a DataFrame against a warp contract.

    Checks for missing columns, type mismatches, and nullable violations
    among warp-declared columns. Engine-managed and warp-only columns
    are excluded from checks.

    Args:
        df: DataFrame to validate.
        warp: Warp config with column declarations.
        mode: Enforcement mode ('enforce', 'warn', 'off').
        engine_columns: Engine-managed column names to exclude from checks.

    Returns:
        List of finding dicts. Empty if no issues found.

    Raises:
        WarpEnforcementError: If mode is 'enforce' and violations exist.
    """
    if mode == "off":
        return []

    engine_set = set(engine_columns or [])
    df_fields = {field.name: field for field in df.schema.fields}
    findings: list[dict[str, str]] = []

    for col in warp.columns:
        # Skip engine-managed columns
        if col.name in engine_set:
            continue

        # Skip warp-only columns (not in pipeline output)
        if col.name not in df_fields:
            # This is a warp-only column if it's expected to be appended later
            # But if the column is expected to come from the pipeline and is
            # missing, that's a finding
            findings.append(
                {
                    "type": "missing_column",
                    "column": col.name,
                    "expected": col.type,
                    "actual": "absent",
                }
            )
            continue

        df_field = df_fields[col.name]

        # Type comparison: normalize Spark type strings
        expected_type = col.type.lower()
        actual_type = df_field.dataType.simpleString().lower()
        if expected_type != actual_type:
            findings.append(
                {
                    "type": "type_mismatch",
                    "column": col.name,
                    "expected": col.type,
                    "actual": df_field.dataType.simpleString(),
                }
            )

        # Nullable check: if warp says non-nullable, verify no nulls
        if not col.nullable:
            null_count = df.where(F.col(col.name).isNull()).limit(1).count()
            if null_count > 0:
                findings.append(
                    {
                        "type": "nullable_violation",
                        "column": col.name,
                        "expected": "non-nullable",
                        "actual": "contains nulls",
                    }
                )

    if findings and mode == "enforce":
        raise WarpEnforcementError(
            "Warp contract violation: output does not match declared schema",
            findings=findings,
        )

    if findings and mode == "warn":
        for f in findings:
            logger.warning(
                "Warp finding [%s]: column '%s' expected %s, got %s",
                f["type"],
                f["column"],
                f["expected"],
                f["actual"],
            )

    return findings


# ---------------------------------------------------------------------------
# Warp-only column append (Task 12)
# ---------------------------------------------------------------------------


def append_warp_only_columns(
    df: DataFrame,
    warp_only: list[WarpColumn],
) -> DataFrame:
    """Append warp-declared columns absent from the DataFrame.

    For each warp-only column, adds a column with the declared default
    value (or null) cast to the declared type.

    Args:
        df: Input DataFrame.
        warp_only: WarpColumn instances to append.

    Returns:
        DataFrame with warp-only columns appended.
    """
    if not warp_only:
        return df

    existing = {field.name for field in df.schema.fields}
    for col in warp_only:
        if col.name in existing:
            continue
        spark_type = _parse_spark_type(col.type)
        if col.default is not None:
            df = df.withColumn(col.name, F.lit(col.default).cast(spark_type))
        else:
            df = df.withColumn(col.name, F.lit(None).cast(spark_type))
    return df


# ---------------------------------------------------------------------------
# Pre-initialization (Task 13)
# ---------------------------------------------------------------------------


def pre_initialize_table(
    spark: SparkSession,
    target_path: str,
    effective_warp: EffectiveWarp,
) -> bool:
    """Create an empty Delta table from the effective warp schema.

    No-op if the table already exists at the target path.

    Args:
        spark: Active SparkSession.
        target_path: Path where the Delta table should be created.
        effective_warp: Effective warp with all column declarations.

    Returns:
        True if a table was created, False if it already existed.
    """
    try:
        spark.read.format("delta").load(target_path)
        logger.debug("Table already exists at %s, skipping pre-init", target_path)
        return False
    except Exception:
        pass

    schema = _build_spark_schema(effective_warp)
    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta").mode("overwrite").save(target_path)
    logger.info("Pre-initialized Delta table at %s from warp", target_path)
    return True


def _build_spark_schema(effective_warp: EffectiveWarp) -> T.StructType:
    """Build a Spark StructType from the effective warp.

    Args:
        effective_warp: Effective warp with declared, warp-only, and engine columns.

    Returns:
        Spark StructType for the table schema.
    """
    fields: list[T.StructField] = []

    # Declared + warp-only columns (from warp definitions)
    all_warp_cols = effective_warp.declared + effective_warp.warp_only
    seen: set[str] = set()
    for col in all_warp_cols:
        if col.name in seen:
            continue
        seen.add(col.name)
        spark_type = _parse_spark_type(col.type)
        fields.append(T.StructField(col.name, spark_type, nullable=col.nullable))

    # Engine columns (default to string, nullable)
    for col_name in effective_warp.engine:
        if col_name not in seen:
            seen.add(col_name)
            fields.append(T.StructField(col_name, T.StringType(), nullable=True))

    return T.StructType(fields)


# ---------------------------------------------------------------------------
# Auto-generation (Task 14)
# ---------------------------------------------------------------------------


def auto_generate_warp(
    df: DataFrame,
    warp: WarpConfig | None,
    drift_report: DriftReport | None,
    target_name: str,
    output_dir: str | Path,
    adaptive: bool = False,
) -> bool:
    """Write or update a .warp YAML file from pipeline output.

    Generates a warp with auto_generated: true. When adaptive drift
    is active and a warp already exists, new columns are marked with
    discovered: true.

    Args:
        df: DataFrame whose schema to capture.
        warp: Existing warp (if updating), or None (if generating).
        drift_report: Drift report with extra column info.
        target_name: Target table name for the output file.
        output_dir: Directory to write the .warp file to.
        adaptive: Whether adaptive mode is active.

    Returns:
        True if the file was written, False on failure.
    """
    try:
        columns = _columns_from_schema(df.schema)

        # Mark drift-discovered columns if adaptive + existing warp
        if adaptive and warp is not None and drift_report is not None:
            existing_names = {col.name for col in warp.columns}
            for col_data in columns:
                if col_data["name"] not in existing_names:
                    col_data["discovered"] = True

        output_path = Path(output_dir) / f"{target_name}.warp"
        _write_warp_yaml(output_path, columns)
        logger.info("Auto-generated warp: %s", output_path)
        return True
    except Exception:
        logger.warning("Failed to auto-generate warp for %s", target_name, exc_info=True)
        return False


def _columns_from_schema(schema: T.StructType) -> list[dict[str, Any]]:
    """Extract column definitions from a Spark schema.

    Args:
        schema: Spark StructType to extract from.

    Returns:
        List of column definition dicts.
    """
    columns: list[dict[str, Any]] = []
    for field in schema.fields:
        col: dict[str, Any] = {
            "name": field.name,
            "type": field.dataType.simpleString(),
            "nullable": field.nullable,
        }
        columns.append(col)
    return columns


def _write_warp_yaml(path: Path, columns: list[dict[str, Any]]) -> None:
    """Write a .warp YAML file.

    Args:
        path: Output file path.
        columns: Column definition dicts.
    """
    import yaml

    data: dict[str, Any] = {
        "config_version": "1.0",
        "auto_generated": True,
        "columns": columns,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Mapping of simple Spark type strings to their DataType classes.
_SIMPLE_TYPES: dict[str, T.DataType] = {
    "string": T.StringType(),
    "bigint": T.LongType(),
    "long": T.LongType(),
    "int": T.IntegerType(),
    "integer": T.IntegerType(),
    "short": T.ShortType(),
    "smallint": T.ShortType(),
    "byte": T.ByteType(),
    "tinyint": T.ByteType(),
    "float": T.FloatType(),
    "double": T.DoubleType(),
    "boolean": T.BooleanType(),
    "date": T.DateType(),
    "timestamp": T.TimestampType(),
    "binary": T.BinaryType(),
}


def _parse_spark_type(type_str: str) -> T.DataType:
    """Parse a Spark SQL type string into a DataType.

    Handles simple types (string, bigint, etc.) and parameterized types
    (decimal(18,2)).

    Args:
        type_str: Spark SQL type string.

    Returns:
        Corresponding PySpark DataType.
    """
    normalized = type_str.strip().lower()

    if normalized in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[normalized]

    # Handle decimal(precision, scale)
    if normalized.startswith("decimal"):
        import re

        match = re.match(r"decimal\((\d+),\s*(\d+)\)", normalized)
        if match:
            return T.DecimalType(int(match.group(1)), int(match.group(2)))
        return T.DecimalType()

    # Fallback: let Spark parse it
    # This handles array, map, struct types
    from pyspark.sql.types import _parse_datatype_string

    return _parse_datatype_string(type_str)
