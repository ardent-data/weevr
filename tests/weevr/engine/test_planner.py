"""Unit tests for the execution planner — DAG construction, cycle detection, and cache analysis.

All tests are fast (no Spark required). Thread objects are constructed directly
from dicts using model_validate with controlled source and target paths.
"""

import pytest

from weevr.engine.planner import ExecutionPlan, build_plan
from weevr.errors.exceptions import ConfigError
from weevr.model.lookup import Lookup
from weevr.model.source import Source
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


# ---------------------------------------------------------------------------
# Helpers — lookups
# ---------------------------------------------------------------------------


def _make_lookup(alias: str, materialize: bool = True) -> Lookup:
    """Create a minimal Lookup pointing at the given source alias."""
    return Lookup(source=Source(type="delta", alias=alias), materialize=materialize)


def _make_thread_with_lookup(
    name: str,
    lookup_name: str,
    target_alias: str | None = None,
    source_aliases: list[str] | None = None,
) -> Thread:
    """Create a Thread with a lookup-based source."""
    sources: dict = {}
    if source_aliases:
        for alias in source_aliases:
            sources[alias] = {"type": "delta", "alias": alias}
    sources[f"lk_{lookup_name}"] = {"lookup": lookup_name}

    target: dict = {}
    if target_alias:
        target["alias"] = target_alias

    return Thread.model_validate(
        {
            "name": name,
            "config_version": "1.0",
            "sources": sources,
            "target": target,
        }
    )


# ---------------------------------------------------------------------------
# Lookup dependency inference tests
# ---------------------------------------------------------------------------


class TestLookupDependencyInference:
    def test_lookup_creates_implicit_dependency(self):
        """Thread A produces data → lookup reads it → Thread B consumes lookup → B depends on A."""
        threads = {
            "A": _make_thread("A", target_alias="silver.dim_customer"),
            "B": _make_thread_with_lookup("B", "cust_lookup"),
        }
        lookups = {"cust_lookup": _make_lookup("silver.dim_customer")}
        plan = build_plan("w", threads, _entries("A", "B"), lookups=lookups)
        assert "A" in plan.dependencies["B"]

    def test_consumer_moves_to_later_group(self):
        """Lookup-mediated dep forces consumer into a later group than producer."""
        threads = {
            "A": _make_thread("A", target_alias="silver.dim_customer"),
            "B": _make_thread_with_lookup("B", "cust_lookup"),
        }
        lookups = {"cust_lookup": _make_lookup("silver.dim_customer")}
        plan = build_plan("w", threads, _entries("A", "B"), lookups=lookups)
        assert plan.execution_order == [["A"], ["B"]]

    def test_external_lookup_scheduled_at_group_0(self):
        """Lookup with no producer thread is scheduled at group 0."""
        threads = {"A": _make_thread_with_lookup("A", "ext_lookup")}
        lookups = {"ext_lookup": _make_lookup("external.ref_table")}
        plan = build_plan("w", threads, _entries("A"), lookups=lookups)
        assert plan.lookup_schedule is not None
        assert 0 in plan.lookup_schedule
        assert "ext_lookup" in plan.lookup_schedule[0]

    def test_internal_lookup_scheduled_after_producer(self):
        """Lookup whose source is produced by thread in group 0 → scheduled at group 1."""
        threads = {
            "A": _make_thread("A", target_alias="silver.dim_customer"),
            "B": _make_thread_with_lookup("B", "cust_lookup"),
        }
        lookups = {"cust_lookup": _make_lookup("silver.dim_customer")}
        plan = build_plan("w", threads, _entries("A", "B"), lookups=lookups)
        assert plan.lookup_schedule is not None
        assert 1 in plan.lookup_schedule
        assert "cust_lookup" in plan.lookup_schedule[1]
        # Should not be at group 0
        assert "cust_lookup" not in plan.lookup_schedule.get(0, [])

    def test_mixed_external_and_internal_lookups(self):
        """External lookup at group 0, internal lookup at producer_group + 1."""
        threads = {
            "A": _make_thread("A", target_alias="silver.dim_customer"),
            "B": _make_thread_with_lookup("B", "cust_lookup"),
        }
        lookups = {
            "cust_lookup": _make_lookup("silver.dim_customer"),
            "ext_ref": _make_lookup("external.codes"),
        }
        # B also has a normal source for ext_ref — but the lookup schedule is about lookups
        plan = build_plan("w", threads, _entries("A", "B"), lookups=lookups)
        assert plan.lookup_schedule is not None
        assert "ext_ref" in plan.lookup_schedule.get(0, [])
        assert "cust_lookup" in plan.lookup_schedule.get(1, [])

    def test_cycle_through_lookup_raises(self):
        """Circular dependency through a lookup is caught."""
        # A produces silver.dim → lookup reads silver.dim → B consumes lookup
        # B produces silver.fact → A reads silver.fact
        # This creates: A → B (via lookup) and B → A (via source) = cycle
        threads = {
            "A": _make_thread("A", target_alias="silver.dim", source_aliases=["silver.fact"]),
            "B": _make_thread_with_lookup("B", "dim_lk", target_alias="silver.fact"),
        }
        lookups = {"dim_lk": _make_lookup("silver.dim")}
        with pytest.raises(ConfigError, match="Circular dependency"):
            build_plan("w", threads, _entries("A", "B"), lookups=lookups)

    def test_no_lookups_schedule_is_none(self):
        """Without lookups, lookup_schedule is None."""
        threads = {"A": _make_thread("A")}
        plan = build_plan("w", threads, _entries("A"))
        assert plan.lookup_schedule is None

    def test_non_materialized_lookup_excluded_from_schedule(self):
        """Lookup with materialize=False is not in the schedule."""
        threads = {
            "A": _make_thread("A", target_alias="silver.dim"),
            "B": _make_thread_with_lookup("B", "dim_lk"),
        }
        lookups = {"dim_lk": _make_lookup("silver.dim", materialize=False)}
        plan = build_plan("w", threads, _entries("A", "B"), lookups=lookups)
        assert plan.lookup_schedule is not None
        # Schedule should be empty (no materialized lookups)
        all_scheduled = [lk for lks in plan.lookup_schedule.values() for lk in lks]
        assert "dim_lk" not in all_scheduled

    def test_lookup_dep_still_inferred_when_not_materialized(self):
        """Even non-materialized lookups create implicit dependencies for ordering."""
        threads = {
            "A": _make_thread("A", target_alias="silver.dim"),
            "B": _make_thread_with_lookup("B", "dim_lk"),
        }
        lookups = {"dim_lk": _make_lookup("silver.dim", materialize=False)}
        plan = build_plan("w", threads, _entries("A", "B"), lookups=lookups)
        # B still depends on A via the lookup
        assert "A" in plan.dependencies["B"]
        assert plan.execution_order == [["A"], ["B"]]


class TestExecutionPlanDisplay:
    """Tests for dag() and _repr_html_() on ExecutionPlan."""

    def _simple_plan(self) -> ExecutionPlan:
        return ExecutionPlan(
            weave_name="test_weave",
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            dependents={"a": ["b"], "b": []},
            execution_order=[["a"], ["b"]],
            cache_targets=[],
            inferred_dependencies={"a": [], "b": ["a"]},
            explicit_dependencies={"a": [], "b": []},
        )

    def test_dag_returns_diagram(self) -> None:
        from weevr.engine.display import DAGDiagram

        plan = self._simple_plan()
        diagram = plan.dag()
        assert isinstance(diagram, DAGDiagram)

    def test_dag_valid_svg(self) -> None:
        import xml.etree.ElementTree as ET

        plan = self._simple_plan()
        diagram = plan.dag()
        ET.fromstring(diagram.svg)

    def test_repr_html(self) -> None:
        plan = self._simple_plan()
        html_out = plan._repr_html_()
        assert "<svg" in html_out
        assert "<table>" in html_out

    def test_repr_html_dependency_table(self) -> None:
        plan = self._simple_plan()
        html_out = plan._repr_html_()
        # Thread names appear in the HTML
        assert ">a<" in html_out
        assert ">b<" in html_out
