"""Tests for quality gate check implementations."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from weevr.operations.gates import (
    GateResult,
    check_expression,
    check_row_count,
    check_row_count_delta,
    check_source_freshness,
    check_table_exists,
)


class TestGateResult:
    """Test GateResult dataclass."""

    def test_construction(self):
        """GateResult is a frozen dataclass."""
        r = GateResult(check="test", passed=True)
        assert r.check == "test"
        assert r.passed is True
        assert r.measured_value is None
        assert r.message == ""


class TestCheckSourceFreshness:
    """Test check_source_freshness."""

    def test_fresh_table(self):
        """Table updated within max_age passes."""
        recent = datetime.now(tz=UTC) - timedelta(hours=1)
        spark = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value=recent)
        spark.sql.return_value.collect.return_value = [mock_row]

        result = check_source_freshness(spark, "db.table", "24h")
        assert result.passed is True
        assert result.check == "source_freshness"

    def test_stale_table(self):
        """Table updated beyond max_age fails."""
        old = datetime.now(tz=UTC) - timedelta(days=3)
        spark = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value=old)
        spark.sql.return_value.collect.return_value = [mock_row]

        result = check_source_freshness(spark, "db.table", "24h")
        assert result.passed is False

    def test_no_history(self):
        """Empty history returns failure."""
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = []

        result = check_source_freshness(spark, "db.table", "24h")
        assert result.passed is False
        assert "No history" in result.message


class TestCheckRowCountDelta:
    """Test check_row_count_delta."""

    def _mock_count(self, count: int) -> MagicMock:
        """Create a mock that returns a specific count."""
        spark = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value=count)
        spark.sql.return_value.collect.return_value = [mock_row]
        return spark

    def test_within_thresholds(self):
        """Delta within thresholds passes."""
        spark = self._mock_count(110)
        result = check_row_count_delta(spark, "db.table", before_count=100, max_increase_pct=20.0)
        assert result.passed is True

    def test_decrease_exceeded(self):
        """Decrease beyond threshold fails."""
        spark = self._mock_count(80)
        result = check_row_count_delta(spark, "db.table", before_count=100, max_decrease_pct=10.0)
        assert result.passed is False
        assert "decrease" in result.message

    def test_increase_exceeded(self):
        """Increase beyond threshold fails."""
        spark = self._mock_count(200)
        result = check_row_count_delta(spark, "db.table", before_count=100, max_increase_pct=50.0)
        assert result.passed is False
        assert "increase" in result.message

    def test_absolute_delta_bounds(self):
        """Absolute delta thresholds checked."""
        spark = self._mock_count(150)
        result = check_row_count_delta(spark, "db.table", before_count=100, max_delta=30)
        assert result.passed is False
        assert "delta 50 > max 30" in result.message

    def test_no_thresholds_passes(self):
        """No thresholds set always passes."""
        spark = self._mock_count(999)
        result = check_row_count_delta(spark, "db.table", before_count=1)
        assert result.passed is True

    def test_before_count_zero_no_change(self):
        """Zero before_count with zero after_count passes."""
        spark = self._mock_count(0)
        result = check_row_count_delta(spark, "db.table", before_count=0, max_increase_pct=50.0)
        assert result.passed is True

    def test_before_count_zero_with_increase(self):
        """Zero before_count with increase produces infinite pct, exceeds threshold."""
        spark = self._mock_count(10)
        result = check_row_count_delta(spark, "db.table", before_count=0, max_increase_pct=50.0)
        assert result.passed is False

    def test_min_delta_failing(self):
        """Delta below min_delta threshold fails."""
        spark = self._mock_count(102)
        result = check_row_count_delta(spark, "db.table", before_count=100, min_delta=10)
        assert result.passed is False
        assert "delta 2 < min 10" in result.message

    def test_min_delta_passing(self):
        """Delta above min_delta threshold passes."""
        spark = self._mock_count(115)
        result = check_row_count_delta(spark, "db.table", before_count=100, min_delta=10)
        assert result.passed is True


class TestCheckRowCount:
    """Test check_row_count."""

    def _mock_count(self, count: int) -> MagicMock:
        spark = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value=count)
        spark.sql.return_value.collect.return_value = [mock_row]
        return spark

    def test_within_bounds(self):
        """Count within min/max passes."""
        spark = self._mock_count(50)
        result = check_row_count(spark, "db.table", min_count=10, max_count=100)
        assert result.passed is True

    def test_below_min(self):
        """Count below min fails."""
        spark = self._mock_count(5)
        result = check_row_count(spark, "db.table", min_count=10)
        assert result.passed is False
        assert "count 5 < min 10" in result.message

    def test_above_max(self):
        """Count above max fails."""
        spark = self._mock_count(200)
        result = check_row_count(spark, "db.table", max_count=100)
        assert result.passed is False
        assert "count 200 > max 100" in result.message

    def test_min_only(self):
        """Only min_count checked."""
        spark = self._mock_count(50)
        result = check_row_count(spark, "db.table", min_count=1)
        assert result.passed is True


class TestCheckTableExists:
    """Test check_table_exists."""

    def test_table_exists(self):
        """Existing table passes."""
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        result = check_table_exists(spark, "db.table")
        assert result.passed is True
        assert "exists" in result.message

    def test_table_not_exists(self):
        """Missing table fails."""
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        result = check_table_exists(spark, "db.missing")
        assert result.passed is False
        assert "does not exist" in result.message


class TestCheckExpression:
    """Test check_expression."""

    def test_true_expression(self):
        """Expression evaluating to True passes."""
        spark = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value=True)
        spark.sql.return_value.collect.return_value = [mock_row]

        result = check_expression(spark, "1 = 1")
        assert result.passed is True

    def test_false_expression(self):
        """Expression evaluating to False fails."""
        spark = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value=False)
        spark.sql.return_value.collect.return_value = [mock_row]

        result = check_expression(spark, "1 = 0")
        assert result.passed is False

    def test_custom_message(self):
        """Custom failure message used when check fails."""
        spark = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value=False)
        spark.sql.return_value.collect.return_value = [mock_row]

        result = check_expression(spark, "false", message="Custom fail")
        assert result.passed is False
        assert "Custom fail" in result.message

    def test_empty_result(self):
        """Empty result set treated as False."""
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = []
        result = check_expression(spark, "SELECT false")
        assert result.passed is False
