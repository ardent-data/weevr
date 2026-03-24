"""Column set resolution engine — resolves named column mappings from various sources."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from pyspark.sql import functions as F

from weevr.errors.exceptions import ConfigError, ExecutionError
from weevr.model.column_set import ColumnSet
from weevr.model.source import Source
from weevr.operations.readers import read_source
from weevr.telemetry.results import ColumnSetResult
from weevr.telemetry.span import SpanStatus

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from weevr.telemetry.collector import SpanCollector

logger = logging.getLogger("weevr.engine.column_sets")


def resolve_column_set(
    spark: SparkSession,
    name: str,
    column_set: ColumnSet,
    resolved_params: dict[str, Any],
    collector: SpanCollector | None = None,
    parent_span_id: str | None = None,
) -> dict[str, str] | None:
    """Resolve a single column set to a from→to mapping dict.

    Supports three resolution paths:

    - **param**: read the mapping dict directly from ``resolved_params``.
    - **delta / yaml source**: call ``read_source()``, apply an optional filter,
      select the from/to columns, and collect rows into a dict.

    Args:
        spark: Active SparkSession (used for source-backed column sets).
        name: Column set name, used in log and error messages.
        column_set: Column set definition with source or param reference.
        resolved_params: Runtime notebook parameters already resolved.
        collector: Optional span collector for telemetry.
        parent_span_id: Optional parent span ID for span hierarchy.

    Returns:
        Mapping of from-column name to to-column name, or ``None`` when
        ``on_failure="skip"`` and the source is empty or unreadable.

    Raises:
        ConfigError: If a param value is not a dict, a param key is missing, or
            duplicate from-column values are found in the source.
        ExecutionError: If the source cannot be read and ``on_failure="abort"``,
            or if the source is empty and ``on_failure="abort"``.
    """
    span = None
    if collector:
        span = collector.start_span(f"column_set:resolve:{name}", parent_span_id=parent_span_id)
        span.set_attribute("column_set.name", name)

    try:
        result = _resolve(spark, name, column_set, resolved_params)

        if span:
            mappings_loaded = len(result) if result is not None else 0
            source_type = (
                "param"
                if column_set.param is not None
                else (column_set.source.type if column_set.source is not None else "unknown")
            )
            span.set_attribute("column_set.source_type", source_type)
            span.set_attribute("column_set.mappings_loaded", mappings_loaded)
            span.set_attribute("column_set.skipped", result is None)
            collector.add_span(span.finish(SpanStatus.OK))  # type: ignore[union-attr]

        return result

    except Exception as exc:
        if span:
            span.add_event("error", {"message": str(exc)})
            collector.add_span(span.finish(SpanStatus.ERROR))  # type: ignore[union-attr]
        raise


def _resolve(
    spark: SparkSession,
    name: str,
    column_set: ColumnSet,
    resolved_params: dict[str, Any],
) -> dict[str, str] | None:
    """Internal resolution logic, separate from telemetry concerns."""
    if column_set.param is not None:
        return _resolve_from_param(name, column_set, resolved_params)

    # Source-backed resolution
    assert column_set.source is not None  # guaranteed by ColumnSet model validator
    return _resolve_from_source(spark, name, column_set)


def _resolve_from_param(
    name: str,
    column_set: ColumnSet,
    resolved_params: dict[str, Any],
) -> dict[str, str]:
    """Resolve a column set from a runtime notebook parameter.

    Args:
        name: Column set name for error messages.
        column_set: Column set with ``param`` set.
        resolved_params: Runtime parameter values keyed by name.

    Returns:
        The mapping dict from the resolved parameter.

    Raises:
        ConfigError: If the param key is missing or the value is not a dict.
    """
    param_name = column_set.param
    assert param_name is not None

    if param_name not in resolved_params:
        raise ConfigError(
            f"Column set '{name}': param '{param_name}' not found in resolved_params",
            config_key=param_name,
        )

    value = resolved_params[param_name]
    if not isinstance(value, dict):
        raise ConfigError(
            f"Column set '{name}': param '{param_name}' must be a dict, got {type(value).__name__}",
            config_key=param_name,
        )

    return dict(value)


def _resolve_from_source(
    spark: SparkSession,
    name: str,
    column_set: ColumnSet,
) -> dict[str, str] | None:
    """Resolve a column set by reading a Delta or YAML source.

    Args:
        spark: Active SparkSession.
        name: Column set name for error messages.
        column_set: Column set with ``source`` set.

    Returns:
        Resolved mapping dict, an empty dict (on_failure=warn), or
        ``None`` (on_failure=skip).

    Raises:
        ConfigError: If duplicate from-column values are detected.
        ExecutionError: If reading fails and on_failure=abort, or if the
            source is empty and on_failure=abort.
    """
    cs = column_set.source
    assert cs is not None

    on_failure = column_set.on_failure

    try:
        source = Source(type=cs.type, alias=cs.alias, path=cs.path)
        df = read_source(spark, name, source)

        if cs.filter:
            df = df.filter(F.expr(cs.filter))

        df = df.select(cs.from_column, cs.to_column)
        rows = df.collect()

    except (ConfigError, ExecutionError):
        raise
    except Exception as exc:
        msg = f"Failed to resolve column set '{name}': {exc}"
        if on_failure == "abort":
            raise ExecutionError(msg, cause=exc, source_name=name) from exc
        logger.warning(msg)
        return {} if on_failure == "warn" else None

    if not rows:
        msg = f"Column set '{name}': source returned 0 rows (empty)"
        if on_failure == "abort":
            raise ExecutionError(msg, source_name=name)
        logger.warning(msg)
        return {} if on_failure == "warn" else None

    # Build the mapping and check for duplicate from-column values
    mapping: dict[str, str] = {}
    for row in rows:
        from_val = row[cs.from_column]
        to_val = row[cs.to_column]
        if from_val in mapping:
            raise ConfigError(
                f"Column set '{name}': duplicate from-column value '{from_val}' — "
                "each source name must appear at most once (one-to-one invariant)"
            )
        mapping[from_val] = to_val

    return mapping


def materialize_column_sets(
    spark: SparkSession,
    column_sets: dict[str, ColumnSet],
    resolved_params: dict[str, Any],
    collector: SpanCollector | None = None,
    parent_span_id: str | None = None,
) -> tuple[dict[str, dict[str, str]], list[ColumnSetResult]]:
    """Resolve all column sets and return them as a name→mapping dict.

    Column sets where resolution returns ``None`` (``on_failure="skip"``) are
    excluded from the resolved mappings but still appear in the results list
    with ``skipped=True``.

    Args:
        spark: Active SparkSession.
        column_sets: Mapping of column set name to definition.
        resolved_params: Runtime notebook parameters already resolved.
        collector: Optional span collector for telemetry.
        parent_span_id: Optional parent span ID for span hierarchy.

    Returns:
        Tuple of (resolved mappings dict, list of ColumnSetResult).
    """
    resolved_mappings: dict[str, dict[str, str]] = {}
    results: list[ColumnSetResult] = []

    for name, column_set in column_sets.items():
        source_type = (
            "param"
            if column_set.param is not None
            else (column_set.source.type if column_set.source is not None else "unknown")
        )
        resolved = resolve_column_set(
            spark,
            name,
            column_set,
            resolved_params,
            collector=collector,
            parent_span_id=parent_span_id,
        )
        skipped = resolved is None
        if resolved is not None:
            resolved_mappings[name] = resolved
        results.append(
            ColumnSetResult(
                name=name,
                source_type=source_type,
                mappings_loaded=len(resolved) if resolved else 0,
                skipped=skipped,
            )
        )

    return resolved_mappings, results
