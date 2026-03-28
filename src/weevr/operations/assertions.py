"""Post-execution assertion evaluator."""

from __future__ import annotations

import uuid

from pyspark.sql import SparkSession

from weevr.delta import read_delta
from weevr.model.validation import Assertion
from weevr.telemetry.results import AssertionResult


def evaluate_assertions(
    spark: SparkSession,
    assertions: list[Assertion],
    target_path: str,
) -> list[AssertionResult]:
    """Evaluate post-write assertions against a Delta target table.

    Supports four assertion types: row_count, column_not_null, unique,
    and expression. Each assertion produces an AssertionResult with
    pass/fail status, details, and the configured severity.

    Args:
        spark: Active SparkSession.
        assertions: List of assertion definitions to evaluate.
        target_path: Path to the Delta table to assert against.

    Returns:
        List of AssertionResult, one per input assertion.
    """
    results: list[AssertionResult] = []

    for assertion in assertions:
        try:
            result = _evaluate_one(spark, assertion, target_path)
        except Exception as exc:
            result = AssertionResult(
                assertion_type=assertion.type,
                severity=assertion.severity,
                passed=False,
                details=f"Evaluation error: {exc}",
                columns=assertion.columns,
            )
        results.append(result)

    return results


def _evaluate_one(
    spark: SparkSession,
    assertion: Assertion,
    target_path: str,
) -> AssertionResult:
    """Evaluate a single assertion against the target."""
    if assertion.type == "row_count":
        return _eval_row_count(spark, assertion, target_path)
    if assertion.type == "column_not_null":
        return _eval_column_not_null(spark, assertion, target_path)
    if assertion.type == "unique":
        return _eval_unique(spark, assertion, target_path)
    if assertion.type == "expression":
        return _eval_expression(spark, assertion, target_path)
    if assertion.type == "fk_sentinel_rate":
        return _eval_fk_sentinel_rate(spark, assertion, target_path)
    return AssertionResult(
        assertion_type=assertion.type,
        severity=assertion.severity,
        passed=False,
        details=f"Unknown assertion type: {assertion.type}",
    )


def _eval_row_count(
    spark: SparkSession,
    assertion: Assertion,
    target_path: str,
) -> AssertionResult:
    """Check row count is within min/max bounds."""
    df = read_delta(spark, target_path)
    count = df.count()

    if assertion.min is not None and count < assertion.min:
        return AssertionResult(
            assertion_type="row_count",
            severity=assertion.severity,
            passed=False,
            details=f"Row count {count} below minimum {assertion.min}",
        )
    if assertion.max is not None and count > assertion.max:
        return AssertionResult(
            assertion_type="row_count",
            severity=assertion.severity,
            passed=False,
            details=f"Row count {count} above maximum {assertion.max}",
        )
    return AssertionResult(
        assertion_type="row_count",
        severity=assertion.severity,
        passed=True,
        details=f"Row count {count} within bounds",
    )


def _eval_column_not_null(
    spark: SparkSession,
    assertion: Assertion,
    target_path: str,
) -> AssertionResult:
    """Check that specified columns have no null values."""
    columns = assertion.columns or []
    if not columns:
        return AssertionResult(
            assertion_type="column_not_null",
            severity=assertion.severity,
            passed=False,
            details="No columns specified for column_not_null assertion",
        )

    df = read_delta(spark, target_path)

    null_counts: dict[str, int] = {}
    for col in columns:
        null_count = df.filter(df[col].isNull()).count()
        if null_count > 0:
            null_counts[col] = null_count

    if null_counts:
        details_parts = [f"'{c}' has {n} nulls" for c, n in null_counts.items()]
        return AssertionResult(
            assertion_type="column_not_null",
            severity=assertion.severity,
            passed=False,
            details=f"Null values found: {', '.join(details_parts)}",
            columns=columns,
        )
    return AssertionResult(
        assertion_type="column_not_null",
        severity=assertion.severity,
        passed=True,
        details=f"No nulls in columns: {', '.join(columns)}",
        columns=columns,
    )


def _eval_unique(
    spark: SparkSession,
    assertion: Assertion,
    target_path: str,
) -> AssertionResult:
    """Check that specified columns form a unique key."""
    columns = assertion.columns or []
    if not columns:
        return AssertionResult(
            assertion_type="unique",
            severity=assertion.severity,
            passed=False,
            details="No columns specified for unique assertion",
        )

    df = read_delta(spark, target_path)

    total = df.count()
    distinct = df.select(*columns).distinct().count()
    duplicates = total - distinct

    if duplicates > 0:
        return AssertionResult(
            assertion_type="unique",
            severity=assertion.severity,
            passed=False,
            details=f"{duplicates} duplicate rows on columns: {', '.join(columns)}",
            columns=columns,
        )
    return AssertionResult(
        assertion_type="unique",
        severity=assertion.severity,
        passed=True,
        details=f"All rows unique on columns: {', '.join(columns)}",
        columns=columns,
    )


def _eval_expression(
    spark: SparkSession,
    assertion: Assertion,
    target_path: str,
) -> AssertionResult:
    """Evaluate a Spark SQL expression against the target table."""
    df = read_delta(spark, target_path)
    expr = assertion.expression

    if not expr:
        return AssertionResult(
            assertion_type="expression",
            severity=assertion.severity,
            passed=False,
            details="No expression provided for expression assertion",
        )

    # Create a unique temp view to avoid collisions during concurrent execution
    view_name = f"__weevr_assertion_{uuid.uuid4().hex[:8]}"
    df.createOrReplaceTempView(view_name)
    try:
        result_df = spark.sql(f"SELECT ({expr}) AS __result FROM {view_name} LIMIT 1")
        rows = result_df.collect()
        if not rows or rows[0]["__result"] is None:
            passed = False
            details = f"Expression returned no result: {expr}"
        else:
            passed = bool(rows[0]["__result"])
            details = f"Expression '{expr}' evaluated to {passed}"
    finally:
        spark.catalog.dropTempView(view_name)

    return AssertionResult(
        assertion_type="expression",
        severity=assertion.severity,
        passed=passed,
        details=details,
    )


def _eval_fk_sentinel_rate(
    spark: SparkSession,
    assertion: Assertion,
    target_path: str,
) -> AssertionResult:
    """Check FK sentinel value rates against thresholds.

    Supports single sentinel, named sentinel groups with shared or
    per-group max_rate, and column/columns specification.
    """
    from weevr.model.validation import SentinelGroup

    df = read_delta(spark, target_path)
    total = df.count()

    if total == 0:
        return AssertionResult(
            assertion_type="fk_sentinel_rate",
            severity=assertion.severity,
            passed=True,
            details="Empty table — 0% sentinel rate",
            columns=assertion.columns or ([assertion.column] if assertion.column else None),
        )

    # Determine columns to check
    check_cols: list[str] = []
    if assertion.column is not None:
        check_cols = [assertion.column]
    elif assertion.columns is not None:
        check_cols = list(assertion.columns)

    if not check_cols:
        return AssertionResult(
            assertion_type="fk_sentinel_rate",
            severity=assertion.severity,
            passed=False,
            details="fk_sentinel_rate requires column or columns to be set",
        )

    # Build sentinel groups to evaluate
    groups: dict[str, SentinelGroup] = {}
    if assertion.sentinels is not None:
        groups = dict(assertion.sentinels)
    elif assertion.sentinel is not None:
        groups = {"default": SentinelGroup(value=assertion.sentinel)}

    shared_max_rate = assertion.max_rate
    failures: list[str] = []

    for col_name in check_cols:
        for group_name, group in groups.items():
            sentinel_val = group.value
            threshold = group.max_rate if group.max_rate is not None else shared_max_rate
            if threshold is None:
                continue

            sentinel_count = df.filter(df[col_name] == sentinel_val).count()
            rate = sentinel_count / total

            if rate > threshold:
                failures.append(
                    f"column '{col_name}' sentinel group '{group_name}': "
                    f"rate {rate:.2%} exceeds threshold {threshold:.2%} "
                    f"(sentinel={sentinel_val}, count={sentinel_count}/{total})"
                )

    if failures:
        msg = assertion.message or "FK sentinel rate exceeded"
        return AssertionResult(
            assertion_type="fk_sentinel_rate",
            severity=assertion.severity,
            passed=False,
            details=f"{msg}: {'; '.join(failures)}",
            columns=check_cols,
        )

    return AssertionResult(
        assertion_type="fk_sentinel_rate",
        severity=assertion.severity,
        passed=True,
        details=f"FK sentinel rates within thresholds for {', '.join(check_cols)}",
        columns=check_cols,
    )
