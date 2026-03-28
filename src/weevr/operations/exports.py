"""Export write operations — secondary output destinations."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from weevr.errors.exceptions import ExportError
from weevr.model.export import Export
from weevr.operations.audit import CONTEXT_VAR_PATTERN, resolve_context_variables
from weevr.telemetry.results import ExportResult

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from weevr.operations.audit import AuditContext

logger = logging.getLogger(__name__)


def resolve_export_path(path: str, context: AuditContext) -> str:
    """Substitute context variables in an export path string.

    Delegates to the shared ``resolve_context_variables()`` in the audit
    module, which handles ``${thread.*}``, ``${weave.*}``, ``${loom.*}``,
    and ``${run.*}`` namespaces.

    Args:
        path: Export path with variable placeholders.
        context: Execution context providing variable values.

    Returns:
        Path with all context variables resolved. Unknown variables
        are left as-is.
    """
    return resolve_context_variables(path, context)


def resolve_exports(
    exports: list[Export],
    context: AuditContext,
) -> list[Export]:
    """Resolve context variables in export path fields.

    Creates new Export instances with resolved paths. Exports using
    ``alias`` (no path) are returned unchanged.

    Args:
        exports: Export definitions with potential variable placeholders.
        context: Execution context for variable resolution.

    Returns:
        List of exports with resolved paths.
    """
    resolved: list[Export] = []
    for export in exports:
        if export.path is not None and CONTEXT_VAR_PATTERN.search(export.path):
            resolved_path = resolve_export_path(export.path, context)
            resolved.append(export.model_copy(update={"path": resolved_path}))
        else:
            resolved.append(export)
    return resolved


def write_export(
    spark: SparkSession,
    df: DataFrame,
    export: Export,
    *,
    row_count: int | None = None,
) -> ExportResult:
    """Write a DataFrame to an export target.

    Dispatches to the appropriate Spark format writer based on the
    export type. Captures timing, row count, and any errors.

    Args:
        spark: Active SparkSession.
        df: DataFrame to write (post-mapping, audit-injected).
        export: Export configuration with resolved path.
        row_count: Pre-computed row count to avoid an extra ``df.count()``
            action. When ``None``, the count is computed before writing.

    Returns:
        ExportResult with write metrics and status.
    """
    target = export.alias or export.path or ""
    if not target:
        raise ExportError(
            f"Export '{export.name}' has no path or alias",
            export_name=export.name,
            export_type=export.type,
        )

    start = time.monotonic()
    try:
        count = row_count if row_count is not None else df.count()

        writer = df.write.format(export.type).mode(export.mode)

        if export.partition_by:
            writer = writer.partitionBy(*export.partition_by)

        if export.options:
            writer = writer.options(**export.options)

        if export.alias and export.type == "delta":
            writer.saveAsTable(export.alias)
        else:
            writer.save(export.path)

        duration = (time.monotonic() - start) * 1000
        logger.info(
            "Export '%s' (%s) wrote %d rows to %s in %.0fms",
            export.name,
            export.type,
            count,
            target,
            duration,
        )
        return ExportResult(
            name=export.name,
            type=export.type,
            target=target,
            rows_written=count,
            duration_ms=duration,
            status="success",
        )

    except ExportError:
        raise

    except Exception as exc:
        duration = (time.monotonic() - start) * 1000
        error_msg = str(exc)
        logger.warning(
            "Export '%s' (%s) failed after %.0fms: %s",
            export.name,
            export.type,
            duration,
            error_msg,
        )
        status = "aborted" if export.on_failure == "abort" else "warned"
        return ExportResult(
            name=export.name,
            type=export.type,
            target=target,
            rows_written=0,
            duration_ms=duration,
            status=status,
            error=error_msg,
        )
