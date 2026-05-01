"""Audit column resolution and injection.

The pure-Python audit-template merge primitives (``BUILTIN_AUDIT_PRESETS``,
``AuditContext``, ``CONTEXT_VAR_PATTERN``, ``resolve_audit_columns``,
``apply_audit_exclusions``, ``resolve_template_columns``,
``merge_audit_columns_with_templates``, ``resolve_context_variables``) live in
:mod:`weevr.config.audit_templates` and are re-exported below for back-compat.
This module retains the Spark-bound primitives ``inject_audit_columns`` and
``build_sources_json``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# The star import re-exports the canonical audit-template surface so
# back-compat consumers of ``weevr.operations.audit`` keep working.
# The explicit re-import below names the symbols this module itself
# uses (in ``inject_audit_columns``) so static analyzers resolve them
# directly rather than through the wildcard.
from weevr.config.audit_templates import *  # noqa: F401,F403
from weevr.config.audit_templates import (
    AuditContext,
    resolve_context_variables,
)
from weevr.errors.exceptions import ExecutionError

logger = logging.getLogger(__name__)


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
