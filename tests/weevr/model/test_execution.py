"""Tests for ExecutionConfig and LogLevel models."""

import pytest
from pydantic import ValidationError

from weevr.model.execution import ExecutionConfig, LogLevel


class TestLogLevel:
    def test_values(self) -> None:
        assert LogLevel.MINIMAL == "minimal"
        assert LogLevel.STANDARD == "standard"
        assert LogLevel.VERBOSE == "verbose"
        assert LogLevel.DEBUG == "debug"

    def test_is_str_enum(self) -> None:
        assert isinstance(LogLevel.STANDARD, str)

    def test_all_members(self) -> None:
        assert set(LogLevel) == {
            LogLevel.MINIMAL,
            LogLevel.STANDARD,
            LogLevel.VERBOSE,
            LogLevel.DEBUG,
        }


class TestExecutionConfig:
    def test_defaults(self) -> None:
        ec = ExecutionConfig()
        assert ec.log_level == LogLevel.STANDARD
        assert ec.trace is True

    def test_custom_values(self) -> None:
        ec = ExecutionConfig(log_level=LogLevel.DEBUG, trace=False)
        assert ec.log_level == LogLevel.DEBUG
        assert ec.trace is False

    def test_from_string_log_level(self) -> None:
        ec = ExecutionConfig(log_level="verbose")  # type: ignore[arg-type]
        assert ec.log_level == LogLevel.VERBOSE

    def test_invalid_log_level_raises(self) -> None:
        with pytest.raises(ValidationError):
            ExecutionConfig(log_level="extreme")  # type: ignore[arg-type]

    def test_frozen(self) -> None:
        ec = ExecutionConfig()
        with pytest.raises(ValidationError):
            ec.log_level = LogLevel.DEBUG  # type: ignore[misc]

    def test_round_trip(self) -> None:
        ec = ExecutionConfig(log_level=LogLevel.MINIMAL, trace=False)
        assert ExecutionConfig.model_validate(ec.model_dump()) == ec
