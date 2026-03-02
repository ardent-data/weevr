"""Tests for duration string parser."""

from datetime import timedelta

import pytest

from weevr.config.duration import parse_duration


class TestParseDuration:
    """Test parse_duration with valid formats."""

    def test_hours(self):
        """Parse hours-only duration."""
        assert parse_duration("24h") == timedelta(hours=24)

    def test_minutes(self):
        """Parse minutes-only duration."""
        assert parse_duration("30m") == timedelta(minutes=30)

    def test_days(self):
        """Parse days-only duration."""
        assert parse_duration("7d") == timedelta(days=7)

    def test_hours_and_minutes(self):
        """Parse combined hours and minutes."""
        assert parse_duration("2h30m") == timedelta(hours=2, minutes=30)

    def test_days_and_hours(self):
        """Parse combined days and hours."""
        assert parse_duration("1d12h") == timedelta(days=1, hours=12)

    def test_days_hours_minutes(self):
        """Parse combined days, hours, and minutes."""
        assert parse_duration("1d6h30m") == timedelta(days=1, hours=6, minutes=30)

    def test_single_unit_values(self):
        """Parse single-digit unit values."""
        assert parse_duration("1h") == timedelta(hours=1)
        assert parse_duration("1m") == timedelta(minutes=1)
        assert parse_duration("1d") == timedelta(days=1)

    def test_large_values(self):
        """Parse large numeric values."""
        assert parse_duration("365d") == timedelta(days=365)
        assert parse_duration("1000h") == timedelta(hours=1000)


class TestParseDurationInvalid:
    """Test parse_duration rejects invalid formats."""

    def test_empty_string(self):
        """Empty string raises ValueError."""
        with pytest.raises(ValueError, match="must not be empty"):
            parse_duration("")

    def test_whitespace_only(self):
        """Whitespace-only raises ValueError."""
        with pytest.raises(ValueError, match="must not be empty"):
            parse_duration("   ")

    def test_unknown_unit(self):
        """Unknown unit letter raises ValueError."""
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_duration("10s")

    def test_no_number(self):
        """Unit without number raises ValueError."""
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_duration("h")

    def test_negative_value(self):
        """Negative values raise ValueError."""
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_duration("-1h")

    def test_decimal_value(self):
        """Decimal values raise ValueError."""
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_duration("1.5h")

    def test_wrong_order(self):
        """Units in wrong order raise ValueError."""
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_duration("30m2h")

    def test_plain_number(self):
        """Plain number without unit raises ValueError."""
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_duration("30")
