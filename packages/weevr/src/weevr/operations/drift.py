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
    """Detect and handle schema drift at the target boundary.

    Compares the DataFrame columns against the baseline. Extra columns
    are handled per the schema_drift mode and on_drift severity. Baseline
    columns missing from the output are drift too when the baseline is the
    existing table; against a warp, absent columns are warp-only columns
    the engine appends, and warp enforcement reports them.

    Args:
        df: DataFrame at the target boundary.
        baseline: Expected column names (from warp or table schema).
            None means no baseline is available.
        schema_drift: Drift mode ('lenient', 'strict', 'adaptive').
        on_drift: Severity for strict mode ('error', 'warn', 'ignore').
        engine_columns: Engine-managed column names to exclude from
            drift detection, including columns a writer adds itself.
        baseline_source: Provenance of the baseline ('warp' or 'table').

    Returns:
        Tuple of (possibly modified DataFrame, DriftReport). Extra columns
        may be dropped; missing columns are left to the writer, which
        writes them as NULL.

    Raises:
        SchemaDriftError: If schema_drift='strict' and on_drift='error'.
    """
    if baseline is None:
        return df, DriftReport.empty()

    engine_set = set(engine_columns or [])
    baseline_set = set(baseline)
    df_columns = [f.name for f in df.schema.fields]
    df_set = set(df_columns)

    # Detect extra columns (in df but not in baseline, excluding engine columns)
    extra = [c for c in df_columns if c not in baseline_set and c not in engine_set]
    missing = (
        [c for c in baseline if c not in df_set and c not in engine_set]
        if baseline_source == "table"
        else []
    )

    if not extra and not missing:
        return df, DriftReport.empty()

    # Build report
    report = DriftReport(
        extra_columns=extra,
        missing_columns=missing,
        drift_mode=schema_drift,
        baseline_source=baseline_source,
    )

    if schema_drift == "strict" and on_drift == "error":
        report.action_taken = "error"
        report.findings = [DriftFinding(column=c, action="error") for c in extra + missing]
        parts = []
        if extra:
            parts.append(f"{len(extra)} extra column(s): {extra}")
        if missing:
            parts.append(f"{len(missing)} missing column(s): {missing}")
        raise SchemaDriftError(
            f"Schema drift detected: {'; '.join(parts)}",
            drift_report=report,
        )

    df, extra_action = _handle_extra(df, extra, schema_drift, on_drift, report)
    missing_action = _handle_missing(missing, schema_drift, on_drift, report)
    report.action_taken = extra_action or missing_action
    return df, report


def _handle_extra(
    df: DataFrame, extra: list[str], schema_drift: str, on_drift: str, report: DriftReport
) -> tuple[DataFrame, str]:
    """Apply the drift mode to extra columns; return the frame and the action."""
    if not extra:
        return df, ""

    if schema_drift == "lenient":
        report.findings += [DriftFinding(column=c, action="pass_through") for c in extra]
        logger.info(
            "Schema drift (lenient): %d extra column(s) passed through: %s",
            len(extra),
            extra,
        )
        return df, "pass_through"

    if schema_drift == "adaptive":
        report.findings += [DriftFinding(column=c, action="pass_through_evolve") for c in extra]
        logger.info(
            "Schema drift (adaptive): %d extra column(s) passed through for evolution: %s",
            len(extra),
            extra,
        )
        return df, "pass_through_evolve"

    # strict mode (error already raised)
    if on_drift == "warn":
        report.findings += [DriftFinding(column=c, action="drop_warn") for c in extra]
        logger.warning(
            "Schema drift (strict/warn): dropping %d extra column(s): %s",
            len(extra),
            extra,
        )
        return df.drop(*extra), "drop_warn"

    # on_drift == "ignore"
    report.findings += [DriftFinding(column=c, action="drop_silent") for c in extra]
    return df.drop(*extra), "drop_silent"


def _handle_missing(
    missing: list[str], schema_drift: str, on_drift: str, report: DriftReport
) -> str:
    """Record missing columns per the drift mode; return the action."""
    if not missing:
        return ""

    if schema_drift == "strict" and on_drift == "ignore":
        action = "null_fill_silent"
    elif schema_drift == "strict":
        action = "null_fill_warn"
        logger.warning(
            "Schema drift (strict/warn): %d column(s) missing from the output, written as NULL: %s",
            len(missing),
            missing,
        )
    else:
        action = "null_fill"
        logger.info(
            "Schema drift (%s): %d column(s) missing from the output, written as NULL: %s",
            schema_drift,
            len(missing),
            missing,
        )
    report.findings += [DriftFinding(column=c, action=action) for c in missing]
    return action
