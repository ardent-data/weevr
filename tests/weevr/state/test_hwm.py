"""High-water mark text format: canonical UTC instants and legacy values."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from weevr.state.hwm import format_utc_micros, parse_timestamp_hwm


def _micros(dt: datetime) -> int:
    delta = dt - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


class TestCanonicalFormat:
    def test_formats_utc_with_microseconds_and_z(self):
        instant = datetime(2024, 1, 1, 17, 0, tzinfo=UTC)
        assert format_utc_micros(_micros(instant)) == "2024-01-01T17:00:00.000000Z"

    def test_keeps_microseconds(self):
        instant = datetime(2024, 1, 1, 17, 0, 0, 123456, tzinfo=UTC)
        assert format_utc_micros(_micros(instant)) == "2024-01-01T17:00:00.123456Z"

    def test_pre_epoch(self):
        instant = datetime(1969, 12, 31, 23, 59, 59, 500000, tzinfo=UTC)
        assert format_utc_micros(_micros(instant)) == "1969-12-31T23:59:59.500000Z"

    def test_round_trip(self):
        instant = datetime(2024, 3, 10, 7, 30, 1, 42, tzinfo=UTC)
        text = format_utc_micros(_micros(instant))
        assert parse_timestamp_hwm(text) == _micros(instant)


class TestLegacyValues:
    """A value without Z encodes the driver's wall clock (str() of a collected datetime)."""

    def test_naive_value_read_in_driver_zone(self):
        new_york = timezone(timedelta(hours=-5))
        with patch("weevr.state.hwm._driver_tz", return_value=new_york):
            micros = parse_timestamp_hwm("2024-01-01 12:00:00")
        assert micros == _micros(datetime(2024, 1, 1, 17, 0, tzinfo=UTC))

    def test_naive_value_with_fraction(self):
        with patch("weevr.state.hwm._driver_tz", return_value=UTC):
            micros = parse_timestamp_hwm("2024-01-01 12:00:00.250000")
        assert micros == _micros(datetime(2024, 1, 1, 12, 0, 0, 250000, tzinfo=UTC))

    def test_explicit_offset_respected(self):
        assert parse_timestamp_hwm("2024-01-01T12:00:00+02:00") == _micros(
            datetime(2024, 1, 1, 10, 0, tzinfo=UTC)
        )

    def test_garbage_raises(self):
        with pytest.raises(ValueError):
            parse_timestamp_hwm("not a timestamp")
