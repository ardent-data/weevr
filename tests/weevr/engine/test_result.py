"""Tests for ThreadResult model."""

import pytest
from pydantic import ValidationError

from weevr.engine.result import ThreadResult


class TestThreadResult:
    """ThreadResult construction and field validation."""

    def test_success_result_construction(self) -> None:
        result = ThreadResult(
            status="success",
            thread_name="dim_customer",
            rows_written=100,
            write_mode="overwrite",
            target_path="/data/dim_customer",
        )
        assert result.status == "success"
        assert result.thread_name == "dim_customer"
        assert result.rows_written == 100
        assert result.write_mode == "overwrite"
        assert result.target_path == "/data/dim_customer"

    def test_failure_result_construction(self) -> None:
        result = ThreadResult(
            status="failure",
            thread_name="fact_orders",
            rows_written=0,
            write_mode="merge",
            target_path="/data/fact_orders",
        )
        assert result.status == "failure"

    def test_invalid_status_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            ThreadResult(
                status="unknown",  # type: ignore[arg-type]
                thread_name="t",
                rows_written=0,
                write_mode="overwrite",
                target_path="/p",
            )

    def test_result_is_immutable(self) -> None:
        from pydantic import ValidationError as PydanticValidationError

        result = ThreadResult(
            status="success",
            thread_name="t",
            rows_written=1,
            write_mode="overwrite",
            target_path="/p",
        )
        with pytest.raises((PydanticValidationError, TypeError, AttributeError)):
            result.status = "failure"  # type: ignore[misc]

    def test_zero_rows_written_is_valid(self) -> None:
        result = ThreadResult(
            status="success",
            thread_name="empty_thread",
            rows_written=0,
            write_mode="append",
            target_path="/data/empty",
        )
        assert result.rows_written == 0
