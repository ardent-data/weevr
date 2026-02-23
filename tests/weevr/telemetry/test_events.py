"""Tests for LogEvent model and create_log_event factory."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from weevr.telemetry.events import LogEvent, create_log_event


class TestLogEvent:
    def test_create(self) -> None:
        now = datetime.now(UTC)
        event = LogEvent(
            timestamp=now,
            level="INFO",
            component="engine.executor",
            message="Thread started",
        )
        assert event.timestamp == now
        assert event.level == "INFO"
        assert event.component == "engine.executor"
        assert event.message == "Thread started"

    def test_defaults(self) -> None:
        event = LogEvent(
            timestamp=datetime.now(UTC),
            level="INFO",
            component="test",
            message="msg",
        )
        assert event.thread_name is None
        assert event.weave_name is None
        assert event.loom_name is None
        assert event.trace_id is None
        assert event.span_id is None
        assert event.attributes == {}

    def test_all_fields(self) -> None:
        event = LogEvent(
            timestamp=datetime.now(UTC),
            level="ERROR",
            component="engine.runner",
            thread_name="customer_dim",
            weave_name="dimensions",
            loom_name="nightly",
            trace_id="a" * 32,
            span_id="b" * 16,
            message="Validation failed",
            attributes={"rows_failed": 42},
        )
        assert event.thread_name == "customer_dim"
        assert event.loom_name == "nightly"
        assert event.attributes["rows_failed"] == 42

    def test_immutable(self) -> None:
        event = LogEvent(
            timestamp=datetime.now(UTC),
            level="INFO",
            component="test",
            message="msg",
        )
        with pytest.raises(ValidationError):
            event.message = "changed"  # type: ignore[misc]


class TestCreateLogEvent:
    def test_auto_timestamp(self) -> None:
        before = datetime.now(UTC)
        event = create_log_event("INFO", "test", "hello")
        after = datetime.now(UTC)
        assert before <= event.timestamp <= after

    def test_with_context(self) -> None:
        event = create_log_event(
            "WARNING",
            "operations.validation",
            "Quarantine rows detected",
            thread_name="customer_dim",
            trace_id="a" * 32,
            attributes={"rows_quarantined": 5},
        )
        assert event.level == "WARNING"
        assert event.component == "operations.validation"
        assert event.thread_name == "customer_dim"
        assert event.attributes["rows_quarantined"] == 5

    def test_default_attributes(self) -> None:
        event = create_log_event("INFO", "test", "msg")
        assert event.attributes == {}
