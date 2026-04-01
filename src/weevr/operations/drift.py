"""Schema drift detection and handling."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame

from weevr.errors import SchemaDriftError
from weevr.model.warp import DriftFinding, DriftReport

logger = logging.getLogger(__name__)


def handle_drift(
    df: DataFrame,
    baseline: list[str] | None,
    schema_drift: str,
    on_drift: str,
    engine_columns: list[str] | None = None,
    baseline_source: str = "unknown",
) -> tuple[DataFrame, DriftReport]:
    """Detect and handle schema drift (extra columns) at the target boundary.

    Compares the DataFrame columns against the baseline. Extra columns
    are handled per the schema_drift mode and on_drift severity.

    Args:
        df: DataFrame at the target boundary.
        baseline: Expected column names (from warp or table schema).
            None means no baseline is available.
        schema_drift: Drift mode ('lenient', 'strict', 'adaptive').
        on_drift: Severity for strict mode ('error', 'warn', 'ignore').
        engine_columns: Engine-managed column names to exclude from
            drift detection.
        baseline_source: Provenance of the baseline ('warp' or 'table').

    Returns:
        Tuple of (possibly modified DataFrame, DriftReport).

    Raises:
        SchemaDriftError: If schema_drift='strict' and on_drift='error'.
    """
    if baseline is None:
        return df, DriftReport.empty()

    engine_set = set(engine_columns or [])
    baseline_set = set(baseline)
    df_columns = [f.name for f in df.schema.fields]

    # Detect extra columns (in df but not in baseline, excluding engine columns)
    extra = [c for c in df_columns if c not in baseline_set and c not in engine_set]

    if not extra:
        return df, DriftReport.empty()

    # Build report
    report = DriftReport(
        extra_columns=extra,
        drift_mode=schema_drift,
        baseline_source=baseline_source,
    )

    if schema_drift == "lenient":
        report.action_taken = "pass_through"
        report.findings = [DriftFinding(column=c, action="pass_through") for c in extra]
        logger.info(
            "Schema drift (lenient): %d extra column(s) passed through: %s",
            len(extra),
            extra,
        )
        return df, report

    if schema_drift == "adaptive":
        report.action_taken = "pass_through_evolve"
        report.findings = [DriftFinding(column=c, action="pass_through_evolve") for c in extra]
        logger.info(
            "Schema drift (adaptive): %d extra column(s) passed through for evolution: %s",
            len(extra),
            extra,
        )
        return df, report

    # strict mode
    if on_drift == "error":
        report.action_taken = "error"
        report.findings = [DriftFinding(column=c, action="error") for c in extra]
        raise SchemaDriftError(
            f"Schema drift detected: {len(extra)} extra column(s): {extra}",
            drift_report=report,
        )

    if on_drift == "warn":
        report.action_taken = "drop_warn"
        report.findings = [DriftFinding(column=c, action="drop_warn") for c in extra]
        logger.warning(
            "Schema drift (strict/warn): dropping %d extra column(s): %s",
            len(extra),
            extra,
        )
        df = df.drop(*extra)
        return df, report

    # on_drift == "ignore"
    report.action_taken = "drop_silent"
    report.findings = [DriftFinding(column=c, action="drop_silent") for c in extra]
    df = df.drop(*extra)
    return df, report
