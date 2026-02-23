"""Post-execution assertion evaluator."""

from __future__ import annotations

from pyspark.sql import SparkSession

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
    df = spark.read.format("delta").load(target_path)
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
    df = spark.read.format("delta").load(target_path)
    columns = assertion.columns or []

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
    df = spark.read.format("delta").load(target_path)
    columns = assertion.columns or []

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
    df = spark.read.format("delta").load(target_path)
    expr = assertion.expression

    if not expr:
        return AssertionResult(
            assertion_type="expression",
            severity=assertion.severity,
            passed=False,
            details="No expression provided for expression assertion",
        )

    # Create a temp view and evaluate the expression
    view_name = "__weevr_assertion_check"
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
