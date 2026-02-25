"""Pipeline column-ops step handlers — string_ops, date_ops with type selector resolution."""

import logging
from fnmatch import fnmatch

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from weevr.errors.exceptions import ConfigError
from weevr.model.pipeline import DateOpsParams, StringOpsParams

logger = logging.getLogger(__name__)

# Maps type selector names to PySpark type classes
_TYPE_MAP: dict[str, tuple[type[T.DataType], ...]] = {
    "string": (T.StringType,),
    "numeric": (
        T.IntegerType,
        T.LongType,
        T.FloatType,
        T.DoubleType,
        T.DecimalType,
        T.ShortType,
        T.ByteType,
    ),
    "date": (T.DateType,),
    "timestamp": (T.TimestampType, T.TimestampNTZType),
    "boolean": (T.BooleanType,),
}


def resolve_type_selectors(df: DataFrame, columns: list[str]) -> list[str]:
    """Resolve column selectors, expanding ``*:type`` patterns against the DataFrame schema.

    Each entry in ``columns`` is either:
    - An explicit column name (no ``:``) passed through as-is.
    - A ``name_pattern:type_selector`` where the name pattern supports ``*`` glob
      matching and the type selector matches Spark type categories.

    Args:
        df: DataFrame whose schema is inspected for type matching.
        columns: List of column selectors.

    Returns:
        Deduplicated list of resolved column names preserving insertion order.
    """
    seen: set[str] = set()
    result: list[str] = []

    for selector in columns:
        if ":" not in selector:
            if selector not in seen:
                seen.add(selector)
                result.append(selector)
            continue

        name_pattern, type_name = selector.rsplit(":", 1)
        type_classes = _TYPE_MAP.get(type_name.lower())
        if type_classes is None:
            raise ConfigError(
                f"Unknown type selector '{type_name}' in '{selector}'. "
                f"Valid types: {', '.join(sorted(_TYPE_MAP))}"
            )

        for field in df.schema.fields:
            name_matches = fnmatch(field.name, name_pattern) if name_pattern != "*" else True
            type_matches = isinstance(field.dataType, type_classes)
            if name_matches and type_matches and field.name not in seen:
                seen.add(field.name)
                result.append(field.name)

    return result


def _apply_expr_template(
    df: DataFrame,
    columns: list[str],
    expr: str,
    on_empty: str,
    step_name: str,
) -> DataFrame:
    """Apply a Spark SQL expression template to resolved columns.

    Args:
        df: Input DataFrame.
        columns: Raw column selectors (may include ``*:type`` patterns).
        expr: Expression template containing ``{col}`` placeholder.
        on_empty: Behavior when no columns match (``"warn"`` or ``"error"``).
        step_name: Step type name for error messages.

    Returns:
        DataFrame with the expression applied to each matched column.
    """
    resolved = resolve_type_selectors(df, columns)

    if not resolved:
        if on_empty == "error":
            raise ConfigError(
                f"{step_name}: no columns matched selectors {columns}"
            )
        logger.warning("%s: no columns matched selectors %s — skipping", step_name, columns)
        return df

    result = df
    for col_name in resolved:
        result = result.withColumn(col_name, F.expr(expr.replace("{col}", col_name)))
    return result


def apply_string_ops(df: DataFrame, params: StringOpsParams) -> DataFrame:
    """Apply a Spark SQL expression template across string columns.

    Args:
        df: Input DataFrame.
        params: String-ops parameters — columns, expr template, on_empty behavior.

    Returns:
        DataFrame with the expression applied to matched columns.
    """
    return _apply_expr_template(df, params.columns, params.expr, params.on_empty, "string_ops")


def apply_date_ops(df: DataFrame, params: DateOpsParams) -> DataFrame:
    """Apply a Spark SQL expression template across date/timestamp columns.

    Args:
        df: Input DataFrame.
        params: Date-ops parameters — columns, expr template, on_empty behavior.

    Returns:
        DataFrame with the expression applied to matched columns.
    """
    return _apply_expr_template(df, params.columns, params.expr, params.on_empty, "date_ops")
