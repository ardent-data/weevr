"""Audit column resolution and injection."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from weevr.errors.exceptions import ExecutionError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuditContext:
    """Execution context for resolving audit column variables.

    Attributes:
        thread_name: Thread name (e.g. ``stg_orders``).
        thread_qualified_key: Fully qualified key (e.g. ``staging.stg_orders``).
        thread_source: Primary source alias (first declared source).
        thread_sources_json: JSON array of all sources with type metadata.
        weave_name: Weave name (e.g. ``staging``).
        loom_name: Loom name (e.g. ``my-project``).
    """

    thread_name: str
    thread_qualified_key: str
    thread_source: str | None
    thread_sources_json: str
    weave_name: str
    loom_name: str


# Pattern matching ${namespace.property} context variables.
_CONTEXT_VAR_PATTERN = re.compile(r"\$\{(thread|weave|loom)\.([a-z_]+)\}")


def resolve_audit_columns(
    loom_audit: dict[str, str] | None,
    weave_audit: dict[str, str] | None,
    thread_audit: dict[str, str] | None,
) -> dict[str, str]:
    """Merge audit columns additively: loom -> weave -> thread.

    Lower levels extend the column set. Same-named columns at a lower
    level override the expression from a higher level.

    Args:
        loom_audit: Audit columns from loom defaults.
        weave_audit: Audit columns from weave defaults.
        thread_audit: Audit columns from thread target.

    Returns:
        Merged audit column dict. Empty dict if none defined.
    """
    merged: dict[str, str] = {}
    if loom_audit:
        merged.update(loom_audit)
    if weave_audit:
        merged.update(weave_audit)
    if thread_audit:
        merged.update(thread_audit)
    return merged


def _resolve_context_variables(expression: str, context: AuditContext) -> str:
    """Substitute ${namespace.property} variables in an expression string.

    Args:
        expression: Spark SQL expression with context variable placeholders.
        context: Audit context providing variable values.

    Returns:
        Expression with context variables replaced by their values.
    """
    lookup: dict[str, dict[str, str | None]] = {
        "thread": {
            "name": context.thread_name,
            "qualified_key": context.thread_qualified_key,
            "source": context.thread_source,
            "sources": context.thread_sources_json,
        },
        "weave": {
            "name": context.weave_name,
        },
        "loom": {
            "name": context.loom_name,
        },
    }

    def _replace(match: re.Match[str]) -> str:
        namespace = match.group(1)
        prop = match.group(2)
        ns_lookup = lookup.get(namespace, {})
        if prop not in ns_lookup:
            return match.group(0)  # Unknown property — leave unresolved
        value = ns_lookup[prop]
        if value is None:
            logger.warning(
                "Audit column context variable ${%s.%s} resolved to None, "
                "substituting empty string",
                namespace,
                prop,
            )
            return ""
        return value

    return _CONTEXT_VAR_PATTERN.sub(_replace, expression)


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
        resolved = _resolve_context_variables(expression, context)
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
