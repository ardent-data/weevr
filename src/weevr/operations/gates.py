"""Quality gate check implementations for hook steps."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from weevr.duration import parse_duration
from weevr.model.base import FrozenBase

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class GateResult(FrozenBase):
    """Result of a quality gate check.

    Attributes:
        check: Gate check type name.
        passed: Whether the check passed.
        measured_value: Measured value for diagnostics (e.g. actual age, count).
        threshold: Threshold that was checked against.
        message: Human-readable result message.
    """

    check: str
    passed: bool
    measured_value: Any = None
    threshold: Any = None
    message: str = ""


def check_source_freshness(spark: SparkSession, source_alias: str, max_age_str: str) -> GateResult:
    """Check that a Delta table was updated within the allowed time window.

    Uses ``DESCRIBE HISTORY`` to find the most recent commit timestamp
    and compares it to ``now - max_age``.

    Args:
        spark: Active SparkSession.
        source_alias: Fully qualified table name or alias.
        max_age_str: Duration string (e.g. ``"24h"``).

    Returns:
        GateResult with pass/fail and measured age.
    """
    max_age = parse_duration(max_age_str)
    history = spark.sql(f"DESCRIBE HISTORY {source_alias} LIMIT 1")  # noqa: S608
    rows = history.collect()
    if not rows:
        return GateResult(
            check="source_freshness",
            passed=False,
            measured_value=None,
            threshold=max_age_str,
            message=f"No history found for '{source_alias}'",
        )
    last_commit: datetime = rows[0]["timestamp"]
    now = datetime.now(tz=UTC)
    if last_commit.tzinfo is None:
        last_commit = last_commit.replace(tzinfo=UTC)
    age = now - last_commit
    passed = age <= max_age
    return GateResult(
        check="source_freshness",
        passed=passed,
        measured_value=str(age),
        threshold=max_age_str,
        message=f"Source '{source_alias}' age {age} {'<=' if passed else '>'} {max_age_str}",
    )


def check_row_count_delta(
    spark: SparkSession,
    target_alias: str,
    before_count: int,
    *,
    max_decrease_pct: float | None = None,
    max_increase_pct: float | None = None,
    min_delta: int | None = None,
    max_delta: int | None = None,
) -> GateResult:
    """Check that row count change is within thresholds.

    Compares ``before_count`` (pre-snapshot) against current count.

    Args:
        spark: Active SparkSession.
        target_alias: Fully qualified table name.
        before_count: Row count before thread execution.
        max_decrease_pct: Maximum allowed decrease as a percentage.
        max_increase_pct: Maximum allowed increase as a percentage.
        min_delta: Minimum absolute row change (can be negative for expected deletes).
        max_delta: Maximum absolute row change.

    Returns:
        GateResult with pass/fail and delta details.
    """
    after_row = spark.sql(f"SELECT COUNT(*) AS cnt FROM {target_alias}").collect()  # noqa: S608
    after_count = after_row[0]["cnt"] if after_row else 0
    delta = after_count - before_count
    if before_count != 0:
        pct_change = delta / before_count * 100
    else:
        pct_change = 0.0 if delta == 0 else float("inf")
    failures: list[str] = []

    if max_decrease_pct is not None and pct_change < -max_decrease_pct:
        failures.append(f"decrease {abs(pct_change):.1f}% > max {max_decrease_pct}%")
    if max_increase_pct is not None and pct_change > max_increase_pct:
        failures.append(f"increase {pct_change:.1f}% > max {max_increase_pct}%")
    if min_delta is not None and delta < min_delta:
        failures.append(f"delta {delta} < min {min_delta}")
    if max_delta is not None and delta > max_delta:
        failures.append(f"delta {delta} > max {max_delta}")

    passed = len(failures) == 0
    msg = f"Row count delta: {before_count} -> {after_count} (delta={delta})" + (
        f"; FAILED: {'; '.join(failures)}" if failures else ""
    )
    return GateResult(
        check="row_count_delta",
        passed=passed,
        measured_value={"before": before_count, "after": after_count, "delta": delta},
        threshold={
            "max_decrease_pct": max_decrease_pct,
            "max_increase_pct": max_increase_pct,
            "min_delta": min_delta,
            "max_delta": max_delta,
        },
        message=msg,
    )


def check_row_count(
    spark: SparkSession,
    target_alias: str,
    *,
    min_count: int | None = None,
    max_count: int | None = None,
) -> GateResult:
    """Check that a table's row count is within absolute bounds.

    Args:
        spark: Active SparkSession.
        target_alias: Fully qualified table name.
        min_count: Minimum required row count.
        max_count: Maximum allowed row count.

    Returns:
        GateResult with pass/fail and count.
    """
    row = spark.sql(f"SELECT COUNT(*) AS cnt FROM {target_alias}").collect()  # noqa: S608
    count = row[0]["cnt"] if row else 0
    failures: list[str] = []

    if min_count is not None and count < min_count:
        failures.append(f"count {count} < min {min_count}")
    if max_count is not None and count > max_count:
        failures.append(f"count {count} > max {max_count}")

    passed = len(failures) == 0
    return GateResult(
        check="row_count",
        passed=passed,
        measured_value=count,
        threshold={"min_count": min_count, "max_count": max_count},
        message=f"Row count {count}" + (f"; FAILED: {'; '.join(failures)}" if failures else ""),
    )


def check_table_exists(spark: SparkSession, source_alias: str) -> GateResult:
    """Check that a table exists in the catalog.

    Args:
        spark: Active SparkSession.
        source_alias: Fully qualified table name.

    Returns:
        GateResult with pass/fail.
    """
    exists = spark.catalog.tableExists(source_alias)
    return GateResult(
        check="table_exists",
        passed=exists,
        measured_value=exists,
        threshold=True,
        message=f"Table '{source_alias}' {'exists' if exists else 'does not exist'}",
    )


def check_expression(spark: SparkSession, sql_expr: str, message: str | None = None) -> GateResult:
    """Evaluate a Spark SQL boolean expression.

    Runs ``SELECT (<expression>) AS result`` and checks the boolean value.

    Args:
        spark: Active SparkSession.
        sql_expr: Spark SQL expression that should evaluate to a boolean.
        message: Optional failure message for diagnostics.

    Returns:
        GateResult with pass/fail.
    """
    result_row = spark.sql(f"SELECT ({sql_expr}) AS result").collect()  # noqa: S608
    result_val = result_row[0]["result"] if result_row else False
    passed = bool(result_val)
    msg = message or f"Expression '{sql_expr}' evaluated to {result_val}"
    if not passed and message:
        msg = f"{message} (expression evaluated to {result_val})"
    return GateResult(
        check="expression",
        passed=passed,
        measured_value=result_val,
        threshold=True,
        message=msg,
    )
