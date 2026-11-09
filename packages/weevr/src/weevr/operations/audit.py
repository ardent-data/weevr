"""Audit column injection.

The Spark-bound audit primitives: ``inject_audit_columns`` and
``build_sources_json``. Audit-template resolution and merging (presets,
``AuditContext``, context variables) live in :mod:`weevr.config.audit_templates`.
"""

from __future__ import annotations

import json
from collections.abc import Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from weevr.config.audit_templates import AuditContext, resolve_context_variables
from weevr.errors.exceptions import ExecutionError

__all__ = ["build_sources_json", "inject_audit_columns"]


def inject_audit_columns(
    df: DataFrame,
    audit_columns: dict[str, str],
    context: AuditContext,
) -> DataFrame:
    """Resolve context variables, validate, and inject audit columns.

    Each audit column expression is first processed for context variable
    substitution, then evaluated as a Spark SQL expression via ``F.expr()``.

    Args:
        df: Input DataFrame.
        audit_columns: Audit column definitions as ``{name: expression}``.
        context: Execution context for variable resolution.

    Returns:
        DataFrame with audit columns appended.

    Raises:
        ExecutionError: If an audit column name conflicts with an existing
            DataFrame column.
    """
    if not audit_columns:
        return df

    existing = set(df.columns)
    conflicts = existing & set(audit_columns)
    if conflicts:
        names = ", ".join(sorted(conflicts))
        raise ExecutionError(f"Audit column name conflict with existing DataFrame columns: {names}")

    result = df
    for col_name, expression in audit_columns.items():
        resolved = resolve_context_variables(expression, context)
        result = result.withColumn(col_name, F.expr(resolved))

    return result


def build_sources_json(
    sources: Mapping[str, object],
) -> str:
    """Build the ${thread.sources} JSON array from thread source definitions.

    Each source entry includes name, alias (if available), and type
    classification (primary for the first source, lookup for lookup
    references, secondary for all others).

    Args:
        sources: Thread source definitions keyed by alias.

    Returns:
        JSON string array of source metadata.
    """
    entries: list[dict[str, str | None]] = []
    for i, (alias, source) in enumerate(sources.items()):
        source_type = "primary" if i == 0 else "secondary"
        source_alias: str | None = None

        # Detect lookup sources and extract alias from direct sources
        if hasattr(source, "lookup") and getattr(source, "lookup", None) is not None:
            source_type = "lookup"
        if hasattr(source, "alias"):
            source_alias = getattr(source, "alias", None)

        entries.append(
            {
                "name": alias,
                "alias": source_alias,
                "type": source_type,
            }
        )

    return json.dumps(entries)
