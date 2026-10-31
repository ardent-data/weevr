"""High-water mark text: the one serializer and parser every watermark uses.

Timestamp HWMs are stored as UTC ISO-8601 instants with microseconds and a
``Z`` suffix (``2024-01-01T17:00:00.000000Z``). The instant is taken inside
Spark (``unix_micros``) and read back with ``timestamp_micros``, so neither
the driver's nor the session's time zone takes part. ``TimestampNTZ``
columns have no zone and keep their wall-clock text; dates and numbers are
stored as before.

A stored timestamp without ``Z`` predates this format: it is ``str()`` of a
datetime PySpark collected in the driver's zone, so it is read in that zone
and rewritten canonically at the next save.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, tzinfo

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, TimestampNTZType, TimestampType

from weevr.errors.exceptions import StateError

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def format_utc_micros(micros: int) -> str:
    """Format microseconds since the epoch as a canonical UTC HWM.

    Args:
        micros: Microseconds since 1970-01-01T00:00:00Z.

    Returns:
        ``YYYY-MM-DDTHH:MM:SS.ffffffZ``.
    """
    return (_EPOCH + timedelta(microseconds=micros)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_timestamp_hwm(text: str) -> int:
    """Parse a stored timestamp HWM to microseconds since the epoch.

    Canonical values (``Z`` suffix) and values with an explicit offset are
    absolute. A naive value is a legacy HWM in the driver's zone.

    Args:
        text: The stored HWM.

    Returns:
        Microseconds since 1970-01-01T00:00:00Z.

    Raises:
        ValueError: If the text is not a timestamp.
    """
    value = datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=_driver_tz())
    delta = value - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _driver_tz() -> tzinfo:
    """The driver's local time zone (the zone legacy HWMs were written in)."""
    local = datetime.now().astimezone().tzinfo
    return local if local is not None else UTC


def max_hwm(df: DataFrame, typed: Column) -> str | None:
    """Return the serialized maximum of a watermark column, or None when empty.

    Args:
        df: The (filtered) watermark window.
        typed: The typed watermark column expression.

    Returns:
        The HWM text for the column's type, or None if every value is null.
    """
    if isinstance(_column_type(df, typed), TimestampType):
        row = (
            df.select(typed.alias("__hwm"))
            .agg(F.expr("unix_micros(max(__hwm))").alias("hwm"))
            .collect()
        )
        if row and row[0]["hwm"] is not None:
            return format_utc_micros(int(row[0]["hwm"]))
        return None
    row = df.agg(F.max(typed).alias("hwm")).collect()
    if row and row[0]["hwm"] is not None:
        return str(row[0]["hwm"])
    return None


def hwm_literal(
    df: DataFrame,
    typed: Column,
    watermark_type: str,
    last_value: str,
    thread_name: str | None = None,
) -> Column:
    """Return the stored HWM as a literal comparable with the watermark column.

    Args:
        df: The source DataFrame (used only to resolve the column's type).
        typed: The typed watermark column expression.
        watermark_type: One of ``timestamp``, ``date``, ``int``, ``long``.
        last_value: The stored HWM text.
        thread_name: The thread the HWM belongs to, for error context.

    Returns:
        A literal Column of the column's type.

    Raises:
        StateError: If the stored text can't be parsed for the watermark type.
    """
    try:
        return _parsed_literal(df, typed, watermark_type, last_value)
    except ValueError as exc:
        remedy = "seed or reset the watermark"
        if watermark_type in ("timestamp", "date"):
            remedy += ", or set watermark_format for a string column in another format"
        raise StateError(
            f"Stored watermark {last_value!r} is not a valid {watermark_type} value; {remedy}",
            cause=exc,
            thread_name=thread_name,
        ) from exc


def _parsed_literal(df: DataFrame, typed: Column, watermark_type: str, last_value: str) -> Column:
    """Build the literal for ``hwm_literal``; a ValueError means unparseable text."""
    if watermark_type == "timestamp":
        column_type = _column_type(df, typed)
        if isinstance(column_type, TimestampType):
            return F.expr(f"timestamp_micros({parse_timestamp_hwm(last_value)})")
        if isinstance(column_type, TimestampNTZType):
            return _checked_cast(df, last_value, "timestamp_ntz")
        return _checked_cast(df, last_value, "timestamp")
    if watermark_type == "date":
        return _checked_cast(df, last_value, "date")
    if watermark_type == "long":
        return F.lit(int(last_value)).cast("long")
    return F.lit(int(last_value))


def _checked_cast(df: DataFrame, last_value: str, type_name: str) -> Column:
    """Cast stored text by Spark's rules, rejecting text Spark reads as NULL.

    Comparing with a NULL literal matches no rows, so the thread would report
    success and never read new data. Asking Spark keeps every value it can
    cast working, including forms Python's parsers reject. ``try_cast`` gives
    NULL under ANSI mode too, where a plain cast raises instead.

    Raises:
        ValueError: If Spark can't cast the text.
    """
    row = df.sparkSession.sql(
        f"SELECT try_cast(:value AS {type_name}) IS NULL AS bad", args={"value": last_value}
    ).first()
    if row is None or row["bad"]:
        raise ValueError(f"not castable to {type_name}: {last_value!r}")
    return F.lit(last_value).cast(type_name)


def _column_type(df: DataFrame, typed: Column) -> DataType:
    """Resolve the data type of a column expression without running a job."""
    return df.select(typed.alias("__hwm")).schema["__hwm"].dataType


def hwm_is_newer(candidate: str, current: str, watermark_type: str) -> bool:
    """Whether HWM *candidate* is past *current* for the watermark type.

    Args:
        candidate: A stored or stamped HWM.
        current: The HWM to compare against.
        watermark_type: One of ``timestamp``, ``date``, ``int``, ``long``.

    Returns:
        True when *candidate* is strictly later; False when it isn't or
        either value can't be parsed.
    """
    try:
        if watermark_type == "timestamp":
            return parse_timestamp_hwm(candidate) > parse_timestamp_hwm(current)
        if watermark_type == "date":
            return candidate.strip() > current.strip()
        return int(candidate) > int(current)
    except ValueError:
        return False


def as_utc(value: datetime) -> datetime:
    """Treat a naive datetime as driver-local (how PySpark collects them) and convert to UTC."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=_driver_tz())
    return value.astimezone(UTC)
