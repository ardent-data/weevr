"""Tests for execution-scope warning collection.

Thread-scoped ``execution:`` blocks (authored or arrived via
``defaults.execution``) are declared but not applied in v1.x; a weave's
explicit ``trace: true`` under an effectively-traceless loom cannot
re-enable tracing. Both must warn, once per distinct origin.
"""

from weevr.config.validation import (
    collect_execution_scope_warnings,
    collect_trace_composition_warnings,
)
from weevr.model.execution import ExecutionConfig


class TestCollectExecutionScopeWarnings:
    def test_clean_configs_produce_no_warnings(self) -> None:
        warnings = collect_execution_scope_warnings(
            defaults_by_scope=[("loom 'nightly'", {"naming": {}}), ("weave 'facts'", None)],
            threads=[{"name": "t1"}, {"name": "t2"}],
        )
        assert warnings == []

    def test_defaults_execution_warns_once_regardless_of_thread_count(self) -> None:
        warnings = collect_execution_scope_warnings(
            defaults_by_scope=[("loom 'nightly'", {"execution": {"log_level": "debug"}})],
            threads=[{"name": f"t{i}"} for i in range(40)],
        )
        assert len(warnings) == 1
        assert "defaults.execution" in warnings[0]
        assert "loom 'nightly'" in warnings[0]
        assert "log_level" in warnings[0]

    def test_authored_thread_block_warns_per_thread(self) -> None:
        warnings = collect_execution_scope_warnings(
            defaults_by_scope=[("loom 'nightly'", None)],
            threads=[
                {"name": "t1", "execution": {"trace": False}},
                {"name": "t2"},
                {"name": "t3", "execution": {"log_level": "minimal"}},
            ],
        )
        assert len(warnings) == 2
        assert any("t1" in w and "trace" in w for w in warnings)
        assert any("t3" in w and "log_level" in w for w in warnings)

    def test_both_origins_warn_independently(self) -> None:
        """Authored blocks get the authored label; the defaults warning
        still fires once."""
        warnings = collect_execution_scope_warnings(
            defaults_by_scope=[("weave 'facts'", {"execution": {"log_level": "verbose"}})],
            threads=[
                {"name": "t1", "execution": {"trace": False}},
                {"name": "t2"},
            ],
        )
        assert len(warnings) == 2
        defaults_warnings = [w for w in warnings if "defaults.execution" in w]
        authored_warnings = [w for w in warnings if "t1" in w]
        assert len(defaults_warnings) == 1
        assert len(authored_warnings) == 1

    def test_warning_points_at_top_level_block(self) -> None:
        warnings = collect_execution_scope_warnings(
            defaults_by_scope=[("loom 'nightly'", {"execution": {"log_level": "debug"}})],
            threads=[],
        )
        assert "top-level" in warnings[0]


class TestCollectTraceCompositionWarnings:
    def test_weave_trace_true_under_traceless_loom_warns(self) -> None:
        warnings = collect_trace_composition_warnings(
            loom_execution=ExecutionConfig(trace=False),
            weave_executions={"facts": ExecutionConfig(trace=True)},
        )
        assert len(warnings) == 1
        assert "facts" in warnings[0]
        assert "trace" in warnings[0]

    def test_implicit_weave_trace_does_not_warn(self) -> None:
        """A weave that never set trace is not warned — only explicit
        trace: true is a no-op worth flagging."""
        warnings = collect_trace_composition_warnings(
            loom_execution=ExecutionConfig(trace=False),
            weave_executions={"facts": ExecutionConfig(log_level="verbose")},  # type: ignore[arg-type]
        )
        assert warnings == []

    def test_tracing_loom_produces_no_warnings(self) -> None:
        warnings = collect_trace_composition_warnings(
            loom_execution=None,
            weave_executions={"facts": ExecutionConfig(trace=True)},
        )
        assert warnings == []

    def test_weave_without_block_does_not_warn(self) -> None:
        warnings = collect_trace_composition_warnings(
            loom_execution=ExecutionConfig(trace=False),
            weave_executions={"facts": None},
        )
        assert warnings == []
