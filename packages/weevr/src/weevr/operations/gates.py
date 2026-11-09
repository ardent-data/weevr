"""Quality gate check implementations for hook steps."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from weevr.delta import probe_delta_table_exists
from weevr.duration import parse_duration
from weevr.model.base import FrozenBase

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _split_table_ref(alias: str) -> list[str] | None:
    """Split a table reference into its dot-separated name parts.

    A part wrapped in backticks is taken as already quoted: dots inside it
    belong to the part and doubled backticks unescape to one. Returns
    ``None`` when the reference can't be split cleanly (an unterminated
    quote, text after a closing quote, or an empty part).
    """
    parts: list[str] = []
    i, n = 0, len(alias)
    while True:
        if i < n and alias[i] == "`":
            chars: list[str] = []
            i += 1
            while True:
                if i >= n:
                    return None
                if alias[i] == "`":
                    if i + 1 < n and alias[i + 1] == "`":
                        chars.append("`")
                        i += 2
                        continue
                    i += 1
                    break
                chars.append(alias[i])
                i += 1
            part = "".join(chars)
        else:
            end = alias.find(".", i)
            end = n if end == -1 else end
            part = alias[i:end]
            i = end
        if not part:
            return None
        parts.append(part)
        if i == n:
            return parts
        if alias[i] != ".":
            return None
        i += 1


def _quote_table_ref(alias: str) -> str:
    """Backtick-quote each part of a table reference for SQL injection safety.

    ``silver.orders`` becomes `` `silver`.`orders` `` so Spark resolves the
    schema and table separately; quoting the whole name would make it one
    identifier. A reference that can't be split cleanly is quoted whole, so
    it fails as an unknown table rather than reaching SQL unescaped.
    """
    parts = _split_table_ref(alias) or [alias]
    return ".".join(f"`{part.replace('`', '``')}`" for part in parts)


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
    history = spark.sql(f"DESCRIBE HISTORY {_quote_table_ref(source_alias)} LIMIT 1")  # noqa: S608
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


def _count_rows(spark: SparkSession, target_alias: str) -> int:
    """Count a table's rows the way every row-count gate does."""
    row = spark.sql(f"SELECT COUNT(*) AS cnt FROM {_quote_table_ref(target_alias)}").collect()  # noqa: S608
    return row[0]["cnt"] if row else 0


def snapshot_row_count(spark: SparkSession, target_alias: str) -> int:
    """Take the baseline row count for a ``row_count_delta`` gate.

    A table that doesn't exist yet (a first load) counts as 0. A failed
    existence probe or count raises, so callers can tell "no rows" apart
    from "couldn't ask".

    Args:
        spark: Active SparkSession.
        target_alias: Table name the gate checks.

    Returns:
        The table's current row count, or 0 if it doesn't exist.
    """
    if not probe_delta_table_exists(spark, target_alias):
        return 0
    return _count_rows(spark, target_alias)


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

    Compares ``before_count`` (pre-snapshot) against current count. When
    ``before_count`` is 0 there is no baseline: ``max_decrease_pct`` and
    ``max_increase_pct`` are skipped (the message says so), while
    ``min_delta`` and ``max_delta`` still apply.

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
    after_count = _count_rows(spark, target_alias)
    delta = after_count - before_count
    # A zero baseline (missing or empty target, e.g. a first load) has no
    # percentage change, so the percentage limits are skipped rather than
    # judged against an infinite value. Absolute limits still apply.
    has_baseline = before_count != 0
    failures: list[str] = []

    if has_baseline:
        pct_change = delta / before_count * 100
        if max_decrease_pct is not None and pct_change < -max_decrease_pct:
            failures.append(f"decrease {abs(pct_change):.1f}% > max {max_decrease_pct}%")
        if max_increase_pct is not None and pct_change > max_increase_pct:
            failures.append(f"increase {pct_change:.1f}% > max {max_increase_pct}%")
    if min_delta is not None and delta < min_delta:
        failures.append(f"delta {delta} < min {min_delta}")
    if max_delta is not None and delta > max_delta:
        failures.append(f"delta {delta} > max {max_delta}")

    passed = len(failures) == 0
    pct_skipped = not has_baseline and (
        max_decrease_pct is not None or max_increase_pct is not None
    )
    msg = (
        f"Row count delta: {before_count} -> {after_count} (delta={delta})"
        + ("; no baseline, percentage limits skipped" if pct_skipped else "")
        + (f"; FAILED: {'; '.join(failures)}" if failures else "")
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
    row = spark.sql(f"SELECT COUNT(*) AS cnt FROM {_quote_table_ref(target_alias)}").collect()  # noqa: S608
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
