"""Unit tests for the execution planner — DAG construction, cycle detection, and cache analysis.

All tests are fast (no Spark required). Thread objects are constructed directly
from dicts using model_validate with controlled source and target paths.
"""

import pytest

from weevr.engine.planner import build_plan
from weevr.errors.exceptions import ConfigError
from weevr.model.thread import Thread
from weevr.model.weave import ThreadEntry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_thread(
    name: str,
    source_aliases: list[str] | None = None,
    target_alias: str | None = None,
    target_path: str | None = None,
    cache: bool | None = None,
) -> Thread:
    """Create a minimal Thread with controlled source/target paths for DAG testing."""
    sources: dict = {}
    if source_aliases:
        for alias in source_aliases:
            sources[alias] = {"type": "delta", "alias": alias}
    else:
        # Every thread needs at least one source
        sources["_default"] = {"type": "delta", "alias": "_default_src"}

    target: dict = {}
    if target_alias:
        target["alias"] = target_alias
    if target_path:
        target["path"] = target_path

    data: dict = {
        "name": name,
        "config_version": "1.0",
        "sources": sources,
        "target": target,
    }
    if cache is not None:
        data["cache"] = cache
    return Thread.model_validate(data)


def _entries(*names: str) -> list[ThreadEntry]:
    """Create a list of plain ThreadEntry objects (no explicit deps)."""
    return [ThreadEntry(name=n) for n in names]


def _entry_with_deps(name: str, deps: list[str]) -> ThreadEntry:
    return ThreadEntry(name=name, dependencies=deps)


# ---------------------------------------------------------------------------
# DAG construction tests
# ---------------------------------------------------------------------------


class TestDAGConstruction:
    def test_linear_chain_infers_dependency(self):
        """A writes to 'table_x'; B reads from 'table_x' → B depends on A."""
        threads = {
            "A": _make_thread("A", target_alias="table_x"),
            "B": _make_thread("B", source_aliases=["table_x"]),
        }
        plan = build_plan("w", threads, _entries("A", "B"))
        assert "A" in plan.dependencies["B"]
        assert plan.inferred_dependencies["B"] == ["A"]

    def test_diamond_dependency(self):
        """A → {B, C} → D: B and C depend on A, D depends on B and C."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", source_aliases=["out_a"], target_alias="out_b"),
            "C": _make_thread("C", source_aliases=["out_a"], target_alias="out_c"),
            "D": _make_thread("D", source_aliases=["out_b", "out_c"]),
        }
        plan = build_plan("w", threads, _entries("A", "B", "C", "D"))
        assert "A" in plan.dependencies["B"]
        assert "A" in plan.dependencies["C"]
        assert "B" in plan.dependencies["D"]
        assert "C" in plan.dependencies["D"]

    def test_independent_threads_have_no_dependencies(self):
        """Three threads with no shared paths have no inferred dependencies."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", target_alias="out_b"),
            "C": _make_thread("C", target_alias="out_c"),
        }
        plan = build_plan("w", threads, _entries("A", "B", "C"))
        assert plan.dependencies["A"] == []
        assert plan.dependencies["B"] == []
        assert plan.dependencies["C"] == []

    def test_explicit_dependency_only_no_path_overlap(self):
        """Explicit dependency is honoured even when paths don't overlap."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", target_alias="out_b"),
        }
        entries = [ThreadEntry(name="A"), _entry_with_deps("B", ["A"])]
        plan = build_plan("w", threads, entries)
        assert "A" in plan.dependencies["B"]
        assert "A" in plan.explicit_dependencies["B"]

    def test_mixed_inferred_and_explicit_dependencies_merged(self):
        """When inferred and explicit deps exist, they are merged without duplicates."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", target_alias="out_b"),
            "C": _make_thread("C", source_aliases=["out_a"]),
        }
        # C already infers dep on A; also explicitly declares dep on B
        entries = [ThreadEntry(name="A"), ThreadEntry(name="B"), _entry_with_deps("C", ["B"])]
        plan = build_plan("w", threads, entries)
        assert "A" in plan.dependencies["C"]
        assert "B" in plan.dependencies["C"]
        assert "A" in plan.inferred_dependencies["C"]
        assert "B" in plan.explicit_dependencies["C"]
        # No duplicates
        assert len(plan.dependencies["C"]) == len(set(plan.dependencies["C"]))

    def test_explicit_dep_on_nonexistent_thread_raises(self):
        """Referencing a non-existent thread in explicit deps raises ConfigError."""
        threads = {"A": _make_thread("A")}
        entries = [_entry_with_deps("A", ["ghost"])]
        with pytest.raises(ConfigError, match="ghost"):
            build_plan("w", threads, entries)

    def test_single_thread_no_dependencies(self):
        """A weave with one thread produces a trivial plan with no deps."""
        threads = {"A": _make_thread("A", target_alias="out_a")}
        plan = build_plan("w", threads, _entries("A"))
        assert plan.dependencies["A"] == []
        assert plan.execution_order == [["A"]]


# ---------------------------------------------------------------------------
# Cycle detection tests
# ---------------------------------------------------------------------------


class TestCycleDetection:
    def test_direct_cycle_raises_config_error(self):
        """A → B → A raises ConfigError with cycle path in message."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", source_aliases=["out_a"], target_alias="out_b"),
        }
        # Add explicit dep to force B → A as well
        entries = [ThreadEntry(name="A", dependencies=["B"]), ThreadEntry(name="B")]
        with pytest.raises(ConfigError, match="Circular dependency"):
            build_plan("w", threads, entries)

    def test_indirect_cycle_raises_config_error(self):
        """A → B → C → A raises ConfigError."""
        threads = {
            "A": _make_thread("A"),
            "B": _make_thread("B"),
            "C": _make_thread("C"),
        }
        entries = [
            _entry_with_deps("A", ["C"]),
            _entry_with_deps("B", ["A"]),
            _entry_with_deps("C", ["B"]),
        ]
        with pytest.raises(ConfigError, match="Circular dependency"):
            build_plan("w", threads, entries)

    def test_self_dependency_raises_config_error(self):
        """A → A (self-loop) raises ConfigError."""
        threads = {"A": _make_thread("A")}
        entries = [_entry_with_deps("A", ["A"])]
        with pytest.raises(ConfigError, match="Circular dependency"):
            build_plan("w", threads, entries)

    def test_no_cycle_passes(self):
        """A valid DAG without cycles does not raise."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", source_aliases=["out_a"]),
        }
        plan = build_plan("w", threads, _entries("A", "B"))
        assert plan is not None

    def test_cycle_error_message_contains_path(self):
        """Cycle error message includes the cycle path."""
        threads = {
            "X": _make_thread("X"),
            "Y": _make_thread("Y"),
        }
        entries = [_entry_with_deps("X", ["Y"]), _entry_with_deps("Y", ["X"])]
        with pytest.raises(ConfigError) as exc_info:
            build_plan("w", threads, entries)
        assert "→" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Topological sort tests
# ---------------------------------------------------------------------------


class TestTopologicalSort:
    def test_linear_chain_produces_sequential_groups(self):
        """A → B → C produces [[A], [B], [C]]."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", source_aliases=["out_a"], target_alias="out_b"),
            "C": _make_thread("C", source_aliases=["out_b"]),
        }
        plan = build_plan("w", threads, _entries("A", "B", "C"))
        assert plan.execution_order == [["A"], ["B"], ["C"]]

    def test_diamond_produces_correct_groups(self):
        """A → {B, C} → D produces [[A], [B, C], [D]]."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", source_aliases=["out_a"], target_alias="out_b"),
            "C": _make_thread("C", source_aliases=["out_a"], target_alias="out_c"),
            "D": _make_thread("D", source_aliases=["out_b", "out_c"]),
        }
        plan = build_plan("w", threads, _entries("A", "B", "C", "D"))
        assert plan.execution_order[0] == ["A"]
        assert sorted(plan.execution_order[1]) == ["B", "C"]
        assert plan.execution_order[2] == ["D"]

    def test_independent_threads_in_single_group(self):
        """Three independent threads produce a single parallel group."""
        threads = {
            "A": _make_thread("A"),
            "B": _make_thread("B"),
            "C": _make_thread("C"),
        }
        plan = build_plan("w", threads, _entries("A", "B", "C"))
        assert len(plan.execution_order) == 1
        assert sorted(plan.execution_order[0]) == ["A", "B", "C"]

    def test_single_thread_produces_single_group(self):
        """A weave with one thread produces [[thread]]."""
        threads = {"X": _make_thread("X")}
        plan = build_plan("w", threads, _entries("X"))
        assert plan.execution_order == [["X"]]


# ---------------------------------------------------------------------------
# Cache analysis tests
# ---------------------------------------------------------------------------


class TestCacheAnalysis:
    def test_thread_with_two_consumers_is_cache_target(self):
        """A thread whose output feeds 2+ threads is in cache_targets."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", source_aliases=["out_a"]),
            "C": _make_thread("C", source_aliases=["out_a"]),
        }
        plan = build_plan("w", threads, _entries("A", "B", "C"))
        assert "A" in plan.cache_targets

    def test_thread_with_one_consumer_not_in_cache_targets(self):
        """A thread with only one consumer is not cached."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", source_aliases=["out_a"]),
        }
        plan = build_plan("w", threads, _entries("A", "B"))
        assert "A" not in plan.cache_targets

    def test_thread_with_no_consumers_not_in_cache_targets(self):
        """A leaf thread with no consumers is not cached."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
        }
        plan = build_plan("w", threads, _entries("A"))
        assert "A" not in plan.cache_targets

    def test_cache_false_suppresses_auto_caching(self):
        """Thread with cache=False is excluded from cache_targets even with 2+ consumers."""
        threads = {
            "A": _make_thread("A", target_alias="out_a", cache=False),
            "B": _make_thread("B", source_aliases=["out_a"]),
            "C": _make_thread("C", source_aliases=["out_a"]),
        }
        plan = build_plan("w", threads, _entries("A", "B", "C"))
        assert "A" not in plan.cache_targets

    def test_cache_true_with_single_consumer_not_cached(self):
        """Thread with cache=True but only one consumer is not auto-cached (cache=True
        is reserved for future forced-caching; current planner only caches 2+ consumers)."""
        # Note: cache=True prevents suppression but doesn't force with 1 consumer.
        # The planner only suppresses on cache=False; multi-consumer is still the threshold.
        threads = {
            "A": _make_thread("A", target_alias="out_a", cache=True),
            "B": _make_thread("B", source_aliases=["out_a"]),
        }
        plan = build_plan("w", threads, _entries("A", "B"))
        # cache=True does not override the "2+ consumers" threshold
        assert "A" not in plan.cache_targets


# ---------------------------------------------------------------------------
# ExecutionPlan inspectability tests
# ---------------------------------------------------------------------------


class TestExecutionPlanInspectability:
    def test_plan_exposes_dependency_graph(self):
        """ExecutionPlan.dependencies is accessible and correct."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", source_aliases=["out_a"]),
        }
        plan = build_plan("w", threads, _entries("A", "B"))
        assert isinstance(plan.dependencies, dict)
        assert "A" in plan.dependencies["B"]

    def test_plan_exposes_parallel_groups(self):
        """ExecutionPlan.execution_order exposes parallel groups."""
        threads = {
            "A": _make_thread("A"),
            "B": _make_thread("B"),
        }
        plan = build_plan("w", threads, _entries("A", "B"))
        assert isinstance(plan.execution_order, list)
        assert all(isinstance(g, list) for g in plan.execution_order)

    def test_plan_exposes_cache_decisions(self):
        """ExecutionPlan.cache_targets is accessible and contains correct threads."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", source_aliases=["out_a"]),
            "C": _make_thread("C", source_aliases=["out_a"]),
        }
        plan = build_plan("w", threads, _entries("A", "B", "C"))
        assert isinstance(plan.cache_targets, list)
        assert "A" in plan.cache_targets

    def test_plan_exposes_inferred_vs_explicit_split(self):
        """Plan exposes inferred and explicit dependency dicts separately."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", source_aliases=["out_a"]),
            "C": _make_thread("C"),
        }
        entries = [ThreadEntry(name="A"), ThreadEntry(name="B"), _entry_with_deps("C", ["A"])]
        plan = build_plan("w", threads, entries)
        # B's dep on A is inferred
        assert "A" in plan.inferred_dependencies["B"]
        assert "A" not in plan.explicit_dependencies["B"]
        # C's dep on A is explicit
        assert "A" in plan.explicit_dependencies["C"]
        assert "A" not in plan.inferred_dependencies["C"]

    def test_plan_is_immutable(self):
        """ExecutionPlan is a FrozenBase and cannot be mutated."""
        from pydantic import ValidationError

        threads = {"A": _make_thread("A")}
        plan = build_plan("w", threads, _entries("A"))
        with pytest.raises((ValidationError, TypeError)):
            plan.weave_name = "other"  # type: ignore[misc]

    def test_plan_dependents_is_reverse_of_dependencies(self):
        """plan.dependents is the exact reverse of plan.dependencies."""
        threads = {
            "A": _make_thread("A", target_alias="out_a"),
            "B": _make_thread("B", source_aliases=["out_a"]),
        }
        plan = build_plan("w", threads, _entries("A", "B"))
        assert "B" in plan.dependents["A"]
        assert plan.dependents["B"] == []

    def test_path_normalization_strips_trailing_slash(self):
        """Paths with trailing slashes are normalized for matching."""
        threads = {
            "A": _make_thread("A", target_alias="lakehouse/table_x/"),
            "B": _make_thread("B", source_aliases=["lakehouse/table_x"]),
        }
        plan = build_plan("w", threads, _entries("A", "B"))
        assert "A" in plan.dependencies["B"]
