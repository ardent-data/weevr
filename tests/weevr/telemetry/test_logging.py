"""Tests for StructuredJsonFormatter and configure_logging."""

import json
import logging

from weevr.telemetry.logging import LogLevel, StructuredJsonFormatter, configure_logging


class TestLogLevel:
    def test_values(self) -> None:
        assert LogLevel.MINIMAL == "minimal"
        assert LogLevel.STANDARD == "standard"
        assert LogLevel.VERBOSE == "verbose"
        assert LogLevel.DEBUG == "debug"

    def test_is_str_enum(self) -> None:
        assert isinstance(LogLevel.STANDARD, str)


class TestStructuredJsonFormatter:
    def test_produces_valid_json(self) -> None:
        formatter = StructuredJsonFormatter()
        record = logging.LogRecord(
            name="weevr.engine",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Thread completed",
            args=None,
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "INFO"
        assert parsed["logger"] == "weevr.engine"
        assert parsed["message"] == "Thread completed"
        assert "timestamp" in parsed

    def test_includes_weevr_context(self) -> None:
        formatter = StructuredJsonFormatter()
        record = logging.LogRecord(
            name="weevr.engine",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="ok",
            args=None,
            exc_info=None,
        )
        record.thread_name = "customer_dim"  # type: ignore[attr-defined]
        record.trace_id = "a" * 32  # type: ignore[attr-defined]
        record.span_id = "b" * 16  # type: ignore[attr-defined]
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["weevr_thread"] == "customer_dim"
        assert parsed["trace_id"] == "a" * 32

    def test_includes_extra_attributes(self) -> None:
        formatter = StructuredJsonFormatter()
        record = logging.LogRecord(
            name="weevr",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="test",
            args=None,
            exc_info=None,
        )
        record.attributes = {"rows_read": 100}  # type: ignore[attr-defined]
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["attributes"]["rows_read"] == 100

    def test_omits_none_context(self) -> None:
        formatter = StructuredJsonFormatter()
        record = logging.LogRecord(
            name="weevr",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=None,
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert "weevr_thread" not in parsed
        assert "weevr_weave" not in parsed


class TestConfigureLogging:
    def test_standard_level(self) -> None:
        logger = configure_logging(LogLevel.STANDARD, logger_name="weevr.test.std")
        assert logger.level == logging.INFO

    def test_minimal_level(self) -> None:
        logger = configure_logging(LogLevel.MINIMAL, logger_name="weevr.test.min")
        assert logger.level == logging.WARNING

    def test_verbose_level(self) -> None:
        logger = configure_logging(LogLevel.VERBOSE, logger_name="weevr.test.verb")
        assert logger.level == logging.DEBUG

    def test_debug_level(self) -> None:
        logger = configure_logging(LogLevel.DEBUG, logger_name="weevr.test.dbg")
        assert logger.level == logging.DEBUG

    def test_has_json_formatter(self) -> None:
        logger = configure_logging(LogLevel.STANDARD, logger_name="weevr.test.fmt")
        assert any(isinstance(h.formatter, StructuredJsonFormatter) for h in logger.handlers)

    def test_no_duplicate_handlers(self) -> None:
        name = "weevr.test.dup"
        configure_logging(LogLevel.STANDARD, logger_name=name)
        configure_logging(LogLevel.VERBOSE, logger_name=name)
        logger = logging.getLogger(name)
        stream_handlers = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]
        assert len(stream_handlers) == 1
