"""Single-pass DataFrame validation with severity-based routing."""

from __future__ import annotations

from datetime import UTC, datetime
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from weevr.model.validation import ValidationRule
from weevr.telemetry.results import ValidationResult


class ValidationOutcome:
    """Result of validating a DataFrame against a set of rules.

    Attributes:
        clean_df: Rows that passed all error-severity rules.
        quarantine_df: Rows that failed at least one error-severity rule,
            exploded by failed rule with metadata columns. None when no
            error-severity rules exist or when a fatal rule triggered.
        validation_results: Per-rule pass/fail counts.
        has_fatal: True if any fatal-severity rule had failures.
    """

    __slots__ = ("clean_df", "quarantine_df", "validation_results", "has_fatal")

    def __init__(
        self,
        clean_df: DataFrame,
        quarantine_df: DataFrame | None,
        validation_results: list[ValidationResult],
        has_fatal: bool,
    ) -> None:
        """Initialize with clean/quarantine splits and validation results."""
        self.clean_df = clean_df
        self.quarantine_df = quarantine_df
        self.validation_results = validation_results
        self.has_fatal = has_fatal


def validate_dataframe(
    df: DataFrame,
    rules: list[ValidationRule],
    compute_results: bool = True,
) -> ValidationOutcome:
    """Evaluate all validation rules in a single pass with severity routing.

    Rules are evaluated as boolean Spark SQL expressions. Each row is tagged
    with pass/fail per rule, then routed based on severity:
    - info/warn: logged only, rows stay in clean_df
    - error: rows moved to quarantine_df (one row per failed rule)
    - fatal: has_fatal=True, no split performed (caller should abort)

    Args:
        df: Input DataFrame to validate.
        rules: Validation rules to evaluate.
        compute_results: When False, skip the per-rule results aggregation
            (a collect job) for callers that discard it — preview mode.
            ``validation_results`` comes back empty and, with no computed
            counts, there is no fatal detection: the error-rule split
            always runs. Execute-mode callers must keep the default.

    Returns:
        ValidationOutcome with clean/quarantine split and per-rule results.
    """
    if not rules:
        return ValidationOutcome(
            clean_df=df,
            quarantine_df=None,
            validation_results=[],
            has_fatal=False,
        )

    # Step 1: Tag each row with pass/fail per rule
    tagged = df
    applied: list[tuple[int, str, ValidationRule]] = []
    unapplied: list[tuple[int, ValidationRule]] = []

    # Probe each rule's expression individually (driver-side analysis via a
    # no-action select — same construction the application uses), then apply
    # every valid rule in ONE plan mutation. Preserves per-rule
    # applied=False granularity without a per-rule analyzer pass on the
    # growing plan.
    tag_exprs: dict[str, Column] = {}
    for i, rule in enumerate(rules):
        col_name = f"__vr_{i}"
        expr = F.coalesce(F.expr(str(rule.rule)).cast("boolean"), F.lit(False))
        try:
            tagged.select(expr)  # analysis only — no job
            tag_exprs[col_name] = expr
            applied.append((i, col_name, rule))
        except Exception:
            unapplied.append((i, rule))
    if tag_exprs:
        # Unlike the other batched sites, this ADDS columns rather than
        # replacing in place. Safe for a different reason: the __vr_* tags
        # are always dropped (or excluded from the quarantine projection)
        # before any external exposure, so their insertion order can never
        # be observed.
        tagged = tagged.withColumns(tag_exprs)

    # Step 2: Compute validation results in a single aggregation
    validation_results: list[ValidationResult] = []
    if compute_results:
        validation_results = _compute_results(tagged, applied, unapplied)

        # Step 3: Check for fatal failures
        has_fatal = any(vr.severity == "fatal" and vr.rows_failed > 0 for vr in validation_results)
        if has_fatal:
            return ValidationOutcome(
                clean_df=df,
                quarantine_df=None,
                validation_results=validation_results,
                has_fatal=True,
            )

    # Step 4: Identify error-severity rules for split
    error_rules = [(i, col, rule) for i, col, rule in applied if rule.severity == "error"]

    if not error_rules:
        clean_df = _drop_tag_columns(tagged, applied)
        return ValidationOutcome(
            clean_df=clean_df,
            quarantine_df=None,
            validation_results=validation_results,
            has_fatal=False,
        )

    # Step 5: Split clean/quarantine on error-severity rules
    original_cols = df.columns
    all_error_pass = reduce(lambda a, b: a & b, [F.col(c) for _, c, _ in error_rules])
    clean_df = tagged.filter(all_error_pass).select(*original_cols)
    quarantine_base = tagged.filter(~all_error_pass)

    # Step 6: Explode quarantine rows — one row per failed error rule
    ts = datetime.now(UTC).isoformat()
    parts: list[DataFrame] = []
    for _, col_name, rule in error_rules:
        part = quarantine_base.filter(~F.col(col_name)).select(
            *original_cols,
            F.lit(rule.name).alias("__rule_name"),
            F.lit(str(rule.rule)).alias("__rule_expression"),
            F.lit(rule.severity).alias("__severity"),
            F.lit(ts).alias("__quarantine_ts"),
        )
        parts.append(part)

    quarantine_df = reduce(DataFrame.unionAll, parts) if parts else None

    return ValidationOutcome(
        clean_df=clean_df,
        quarantine_df=quarantine_df,
        validation_results=validation_results,
        has_fatal=False,
    )


def _compute_results(
    tagged: DataFrame,
    applied: list[tuple[int, str, ValidationRule]],
    unapplied: list[tuple[int, ValidationRule]],
) -> list[ValidationResult]:
    """Aggregate pass/fail counts per rule in a single Spark action."""
    results: list[ValidationResult] = []

    if applied:
        agg_exprs = []
        for _, col_name, _ in applied:
            agg_exprs.append(F.sum(F.when(F.col(col_name), 1).otherwise(0)).alias(f"{col_name}_p"))
            agg_exprs.append(F.sum(F.when(~F.col(col_name), 1).otherwise(0)).alias(f"{col_name}_f"))

        counts_row = tagged.agg(*agg_exprs).collect()[0]

        for _, col_name, rule in applied:
            results.append(
                ValidationResult(
                    rule_name=rule.name,
                    expression=str(rule.rule),
                    severity=rule.severity,
                    rows_passed=int(counts_row[f"{col_name}_p"] or 0),
                    rows_failed=int(counts_row[f"{col_name}_f"] or 0),
                )
            )

    for _, rule in unapplied:
        results.append(
            ValidationResult(
                rule_name=rule.name,
                expression=str(rule.rule),
                severity=rule.severity,
                rows_passed=0,
                rows_failed=0,
                applied=False,
            )
        )

    return results


def _drop_tag_columns(
    tagged: DataFrame,
    applied: list[tuple[int, str, ValidationRule]],
) -> DataFrame:
    """Remove temporary validation tag columns from a DataFrame."""
    tags = [col_name for _, col_name, _ in applied]
    return tagged.drop(*tags) if tags else tagged
