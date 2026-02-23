"""Tests for ThreadResult, WeaveResult, and LoomResult models."""

import pytest
from pydantic import ValidationError

from weevr.engine.result import LoomResult, ThreadResult, WeaveResult


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


def _make_thread_result(name: str, status: str = "success") -> ThreadResult:
    return ThreadResult(
        status=status,  # type: ignore[arg-type]
        thread_name=name,
        rows_written=10,
        write_mode="overwrite",
        target_path=f"/data/{name}",
    )


class TestWeaveResult:
    """WeaveResult construction and status semantics."""

    def test_success_when_all_threads_succeed(self) -> None:
        result = WeaveResult(
            status="success",
            weave_name="dims",
            thread_results=[_make_thread_result("a"), _make_thread_result("b")],
            threads_skipped=[],
            duration_ms=100,
        )
        assert result.status == "success"
        assert result.weave_name == "dims"
        assert len(result.thread_results) == 2
        assert result.threads_skipped == []
        assert result.duration_ms == 100

    def test_failure_status(self) -> None:
        result = WeaveResult(
            status="failure",
            weave_name="facts",
            thread_results=[_make_thread_result("a", "failure")],
            threads_skipped=["b", "c"],
            duration_ms=50,
        )
        assert result.status == "failure"
        assert result.threads_skipped == ["b", "c"]

    def test_partial_status(self) -> None:
        result = WeaveResult(
            status="partial",
            weave_name="pipeline",
            thread_results=[_make_thread_result("a"), _make_thread_result("b", "failure")],
            threads_skipped=["c"],
            duration_ms=200,
        )
        assert result.status == "partial"
        assert len(result.thread_results) == 2
        assert result.threads_skipped == ["c"]

    def test_invalid_status_raises(self) -> None:
        with pytest.raises(ValidationError):
            WeaveResult(
                status="unknown",  # type: ignore[arg-type]
                weave_name="w",
                thread_results=[],
                threads_skipped=[],
                duration_ms=0,
            )

    def test_weave_result_is_immutable(self) -> None:
        result = WeaveResult(
            status="success",
            weave_name="w",
            thread_results=[],
            threads_skipped=[],
            duration_ms=0,
        )
        with pytest.raises((ValidationError, TypeError)):
            result.status = "failure"  # type: ignore[misc]

    def test_threads_skipped_list_correctness(self) -> None:
        result = WeaveResult(
            status="partial",
            weave_name="w",
            thread_results=[_make_thread_result("a")],
            threads_skipped=["b", "c", "d"],
            duration_ms=1,
        )
        assert result.threads_skipped == ["b", "c", "d"]


class TestLoomResult:
    """LoomResult construction and status semantics."""

    def test_success_when_all_weaves_succeed(self) -> None:
        wr1 = WeaveResult(
            status="success",
            weave_name="dims",
            thread_results=[_make_thread_result("a")],
            threads_skipped=[],
            duration_ms=100,
        )
        wr2 = WeaveResult(
            status="success",
            weave_name="facts",
            thread_results=[_make_thread_result("b")],
            threads_skipped=[],
            duration_ms=200,
        )
        result = LoomResult(
            status="success",
            loom_name="nightly",
            weave_results=[wr1, wr2],
            duration_ms=300,
        )
        assert result.status == "success"
        assert result.loom_name == "nightly"
        assert len(result.weave_results) == 2
        assert result.duration_ms == 300

    def test_failure_status(self) -> None:
        wr = WeaveResult(
            status="failure",
            weave_name="dims",
            thread_results=[_make_thread_result("a", "failure")],
            threads_skipped=[],
            duration_ms=50,
        )
        result = LoomResult(
            status="failure",
            loom_name="nightly",
            weave_results=[wr],
            duration_ms=50,
        )
        assert result.status == "failure"

    def test_partial_status(self) -> None:
        wr1 = WeaveResult(
            status="success",
            weave_name="dims",
            thread_results=[_make_thread_result("a")],
            threads_skipped=[],
            duration_ms=100,
        )
        wr2 = WeaveResult(
            status="failure",
            weave_name="facts",
            thread_results=[_make_thread_result("b", "failure")],
            threads_skipped=[],
            duration_ms=50,
        )
        result = LoomResult(
            status="partial",
            loom_name="nightly",
            weave_results=[wr1, wr2],
            duration_ms=150,
        )
        assert result.status == "partial"
        assert len(result.weave_results) == 2

    def test_invalid_status_raises(self) -> None:
        with pytest.raises(ValidationError):
            LoomResult(
                status="bad",  # type: ignore[arg-type]
                loom_name="l",
                weave_results=[],
                duration_ms=0,
            )

    def test_loom_result_is_immutable(self) -> None:
        result = LoomResult(
            status="success",
            loom_name="l",
            weave_results=[],
            duration_ms=0,
        )
        with pytest.raises((ValidationError, TypeError)):
            result.status = "failure"  # type: ignore[misc]
