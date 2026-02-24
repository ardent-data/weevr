"""Tests for watermark filter expression construction (pure Python, no Spark)."""

from weevr.operations.readers import build_watermark_filter


class TestBuildWatermarkFilter:
    """Test build_watermark_filter expression generation."""

    def test_timestamp_exclusive(self):
        """Timestamp type produces quoted value with >."""
        result = build_watermark_filter("ts", "timestamp", "2024-01-15T10:30:00")
        assert result == "ts > '2024-01-15T10:30:00'"

    def test_timestamp_inclusive(self):
        """Timestamp type with inclusive produces >=."""
        result = build_watermark_filter("ts", "timestamp", "2024-01-15T10:30:00", inclusive=True)
        assert result == "ts >= '2024-01-15T10:30:00'"

    def test_date_exclusive(self):
        """Date type produces quoted value with >."""
        result = build_watermark_filter("event_date", "date", "2024-01-15")
        assert result == "event_date > '2024-01-15'"

    def test_date_inclusive(self):
        """Date type with inclusive produces >=."""
        result = build_watermark_filter("event_date", "date", "2024-01-15", inclusive=True)
        assert result == "event_date >= '2024-01-15'"

    def test_int_exclusive(self):
        """Int type produces unquoted value with >."""
        result = build_watermark_filter("row_id", "int", "42")
        assert result == "row_id > 42"

    def test_int_inclusive(self):
        """Int type with inclusive produces >=."""
        result = build_watermark_filter("row_id", "int", "42", inclusive=True)
        assert result == "row_id >= 42"

    def test_long_exclusive(self):
        """Long type produces unquoted value with >."""
        result = build_watermark_filter("seq", "long", "1000000")
        assert result == "seq > 1000000"

    def test_long_inclusive(self):
        """Long type with inclusive produces >=."""
        result = build_watermark_filter("seq", "long", "1000000", inclusive=True)
        assert result == "seq >= 1000000"

    def test_default_not_inclusive(self):
        """Default inclusive is False (strict >)."""
        result = build_watermark_filter("ts", "timestamp", "2024-01-01")
        assert ">" in result
        assert ">=" not in result
