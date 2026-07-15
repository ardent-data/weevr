"""Tests for ExecutionConfig, LogLevel, and effective-config resolution."""

import pytest
from pydantic import ValidationError

from weevr.model.execution import ExecutionConfig, LogLevel, resolve_effective_execution


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

    def test_max_parallel_threads_default_none(self) -> None:
        assert ExecutionConfig().max_parallel_threads is None

    def test_max_parallel_threads_valid(self) -> None:
        assert ExecutionConfig(max_parallel_threads=1).max_parallel_threads == 1
        assert ExecutionConfig(max_parallel_threads=8).max_parallel_threads == 8

    def test_max_parallel_threads_zero_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ExecutionConfig(max_parallel_threads=0)

    def test_max_parallel_threads_negative_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ExecutionConfig(max_parallel_threads=-2)


class TestResolveEffectiveExecution:
    """Field-level merge: child-explicitly-set > parent-set > default."""

    def test_both_absent_gives_defaults(self) -> None:
        eff = resolve_effective_execution(None, None)
        assert eff.log_level == LogLevel.STANDARD
        assert eff.trace is True
        assert eff.max_parallel_threads is None

    def test_parent_only(self) -> None:
        parent = ExecutionConfig(max_parallel_threads=3, log_level=LogLevel.VERBOSE)
        eff = resolve_effective_execution(parent, None)
        assert eff.max_parallel_threads == 3
        assert eff.log_level == LogLevel.VERBOSE
        assert eff.trace is True

    def test_child_only(self) -> None:
        child = ExecutionConfig(log_level=LogLevel.DEBUG)
        eff = resolve_effective_execution(None, child)
        assert eff.log_level == LogLevel.DEBUG
        assert eff.max_parallel_threads is None

    def test_child_overrides_parent_field(self) -> None:
        parent = ExecutionConfig(log_level=LogLevel.MINIMAL)
        child = ExecutionConfig(log_level=LogLevel.DEBUG)
        eff = resolve_effective_execution(parent, child)
        assert eff.log_level == LogLevel.DEBUG

    def test_partial_child_block_keeps_parent_cap(self) -> None:
        """A logging-only child block must not drop the parent's cap."""
        parent = ExecutionConfig(max_parallel_threads=3)
        child = ExecutionConfig(log_level=LogLevel.VERBOSE)
        eff = resolve_effective_execution(parent, child)
        assert eff.max_parallel_threads == 3
        assert eff.log_level == LogLevel.VERBOSE

    def test_child_default_valued_field_not_treated_as_explicit(self) -> None:
        """A child block declaring only log_level leaves trace at the
        parent's value even though trace has a non-None default."""
        parent = ExecutionConfig(trace=False)
        child = ExecutionConfig(log_level=LogLevel.VERBOSE)
        eff = resolve_effective_execution(parent, child)
        assert eff.trace is False

    def test_child_explicit_default_value_wins(self) -> None:
        """Explicitly setting a field to its default still counts as set."""
        parent = ExecutionConfig(log_level=LogLevel.MINIMAL)
        child = ExecutionConfig(log_level=LogLevel.STANDARD)
        eff = resolve_effective_execution(parent, child)
        assert eff.log_level == LogLevel.STANDARD

    def test_child_can_re_enable_trace_field_level(self) -> None:
        """Pure field-level merge: child trace: true wins over parent
        false. (The one-directional runtime limitation is enforced at the
        collector layer, not in the merge.)"""
        parent = ExecutionConfig(trace=False)
        child = ExecutionConfig(trace=True)
        eff = resolve_effective_execution(parent, child)
        assert eff.trace is True

    def test_returns_execution_config_instance(self) -> None:
        eff = resolve_effective_execution(ExecutionConfig(), ExecutionConfig())
        assert isinstance(eff, ExecutionConfig)

    def test_hydrated_from_yaml_dict_partial(self) -> None:
        """Model_validate'd partial dicts behave like authored YAML."""
        parent = ExecutionConfig.model_validate({"max_parallel_threads": 2})
        child = ExecutionConfig.model_validate({"log_level": "debug"})
        eff = resolve_effective_execution(parent, child)
        assert eff.max_parallel_threads == 2
        assert eff.log_level == LogLevel.DEBUG
        assert eff.trace is True
