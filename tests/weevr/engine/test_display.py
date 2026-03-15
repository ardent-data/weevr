"""Unit tests for the plan display module — DAGDiagram, SVG renderer, and HTML builder."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import UTC
from pathlib import Path
from typing import Any

import pytest

from weevr.engine.display import (
    DAGDiagram,
    FlowDiagram,
    render_annotated_dag_svg,
    render_dag_svg,
    render_execution_plan_html,
    render_flow_svg,
    render_loom_dag_svg,
    render_plan_html,
    render_result_html,
    render_timeline_svg,
    render_waterfall_svg,
)
from weevr.engine.planner import ExecutionPlan

_SAMPLE_SVG = '<svg xmlns="http://www.w3.org/2000/svg"><rect/></svg>'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_plan(
    *,
    threads: list[str] | None = None,
    dependencies: dict[str, list[str]] | None = None,
    execution_order: list[list[str]] | None = None,
    cache_targets: list[str] | None = None,
    lookup_schedule: dict[int, list[str]] | None = None,
    lookup_producers: dict[str, str | None] | None = None,
    lookup_consumers: dict[str, list[str]] | None = None,
    weave_name: str = "test_weave",
) -> ExecutionPlan:
    """Build a minimal ExecutionPlan for display testing."""
    t = threads or []
    deps = dependencies or {n: [] for n in t}
    dependents: dict[str, list[str]] = {n: [] for n in t}
    for name, upstream in deps.items():
        for u in upstream:
            if name not in dependents.get(u, []):
                dependents.setdefault(u, []).append(name)
    return ExecutionPlan(
        weave_name=weave_name,
        threads=t,
        dependencies=deps,
        dependents=dependents,
        execution_order=execution_order or [],
        cache_targets=cache_targets or [],
        inferred_dependencies={n: deps.get(n, []) for n in t},
        explicit_dependencies={n: [] for n in t},
        lookup_schedule=lookup_schedule,
        lookup_producers=lookup_producers,
        lookup_consumers=lookup_consumers,
    )


class TestDAGDiagram:
    def test_repr_svg(self) -> None:
        diagram = DAGDiagram(_SAMPLE_SVG)
        assert diagram._repr_svg_() == _SAMPLE_SVG

    def test_str(self) -> None:
        diagram = DAGDiagram(_SAMPLE_SVG)
        assert str(diagram) == _SAMPLE_SVG

    def test_svg_property(self) -> None:
        diagram = DAGDiagram(_SAMPLE_SVG)
        assert diagram.svg == _SAMPLE_SVG

    def test_save(self, tmp_path: pytest.TempPathFactory) -> None:
        diagram = DAGDiagram(_SAMPLE_SVG)
        out = str(tmp_path / "test.svg")  # type: ignore[operator]
        diagram.save(out)
        assert Path(out).read_text(encoding="utf-8") == _SAMPLE_SVG

    def test_save_nonexistent_dir(self) -> None:
        diagram = DAGDiagram(_SAMPLE_SVG)
        with pytest.raises(FileNotFoundError):
            diagram.save("/nonexistent/dir/test.svg")


class TestRenderDagSvg:
    def test_single_thread(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])
        svg = render_dag_svg(plan)
        assert "<svg" in svg
        assert "a" in svg
        # No edges for a single thread
        assert 'class="dag-edge"' not in svg

    def test_linear_chain(self) -> None:
        plan = _make_plan(
            threads=["a", "b", "c"],
            dependencies={"a": [], "b": ["a"], "c": ["b"]},
            execution_order=[["a"], ["b"], ["c"]],
        )
        svg = render_dag_svg(plan)
        # 3 nodes
        assert svg.count('class="dag-node"') == 3
        # 2 edges
        assert svg.count('class="dag-edge"') == 2

    def test_parallel_groups(self) -> None:
        plan = _make_plan(
            threads=["a", "b", "c"],
            dependencies={"a": [], "b": [], "c": ["a", "b"]},
            execution_order=[["a", "b"], ["c"]],
        )
        svg = render_dag_svg(plan)
        assert svg.count('class="dag-edge"') == 2

    def test_cache_markers(self) -> None:
        plan = _make_plan(
            threads=["a", "b", "c"],
            dependencies={"a": [], "b": ["a"], "c": ["a"]},
            execution_order=[["a"], ["b", "c"]],
            cache_targets=["a"],
        )
        svg = render_dag_svg(plan)
        assert 'class="dag-node-cached"' in svg
        assert 'class="dag-label-cached"' in svg

    def test_lookup_schedule(self) -> None:
        plan = _make_plan(
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            execution_order=[["a"], ["b"]],
            lookup_schedule={0: ["ext_ref"]},
        )
        svg = render_dag_svg(plan)
        assert "ext_ref" in svg
        assert 'class="dag-lookup-line"' in svg

    def test_valid_xml(self) -> None:
        plan = _make_plan(
            threads=["a", "b", "c"],
            dependencies={"a": [], "b": ["a"], "c": ["b"]},
            execution_order=[["a"], ["b"], ["c"]],
        )
        svg = render_dag_svg(plan)
        # Should parse as valid XML
        ET.fromstring(svg)

    def test_20_threads(self) -> None:
        threads = [f"thread_{i:02d}" for i in range(20)]
        # 4 groups of 5
        order = [threads[i : i + 5] for i in range(0, 20, 5)]
        deps: dict[str, list[str]] = {t: [] for t in threads}
        # Each group depends on first thread of previous group
        for g in range(1, 4):
            for t in order[g]:
                deps[t] = [order[g - 1][0]]
        plan = _make_plan(threads=threads, dependencies=deps, execution_order=order)
        svg = render_dag_svg(plan)
        # All 20 nodes present
        for t in threads:
            assert t in svg
        # Verify no overlapping X positions within same group
        from weevr.engine.display import _compute_layout

        nodes, _edges, _w, _h, _lk_nodes, _lk_edges = _compute_layout(plan)
        for group in order:
            positions = [(nodes[n][0], nodes[n][0] + nodes[n][2]) for n in group]
            positions.sort()
            for i in range(len(positions) - 1):
                assert positions[i][1] <= positions[i + 1][0], (
                    f"Overlap in group: {positions[i]} overlaps {positions[i + 1]}"
                )

    def test_dark_theme_css(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])
        svg = render_dag_svg(plan)
        assert "prefers-color-scheme:dark" in svg

    def test_forced_dark(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])
        svg = render_dag_svg(plan, dark=True)
        assert "prefers-color-scheme:dark" not in svg

    def test_forced_light(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])
        svg = render_dag_svg(plan, dark=False)
        assert "prefers-color-scheme:dark" not in svg

    def test_tooltips_with_resolved_threads(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])

        class _FakeSource:
            type = "delta"
            alias = "raw/customers"
            path = None

        class _FakeTarget:
            alias = "staging/customers"
            path = None

        class _FakeThread:
            sources = {"main": _FakeSource()}
            target = _FakeTarget()

        svg = render_dag_svg(plan, resolved_threads={"a": _FakeThread()})
        assert "raw/customers" in svg
        assert "staging/customers" in svg

    def test_empty_plan(self) -> None:
        plan = _make_plan(threads=[], execution_order=[])
        svg = render_dag_svg(plan)
        assert "<svg" in svg
        assert "Empty plan" in svg


# ---------------------------------------------------------------------------
# HTML rendering tests
# ---------------------------------------------------------------------------


class _FakeResult:
    """Minimal duck-typed result for render_plan_html testing."""

    def __init__(
        self,
        plans: list[ExecutionPlan],
        *,
        resolved_threads: dict | None = None,
    ) -> None:
        self.status = "PLANNED"
        self.mode = "plan"
        self.config_type = "weave"
        self.config_name = "test_weave"
        self.execution_plan = plans
        self._resolved_threads = resolved_threads


class TestRenderExecutionPlanHtml:
    def test_valid_html(self) -> None:
        plan = _make_plan(
            threads=["a", "b", "c"],
            dependencies={"a": [], "b": ["a"], "c": ["b"]},
            execution_order=[["a"], ["b"], ["c"]],
        )
        html_out = render_execution_plan_html(plan)
        assert "<table" in html_out
        assert "</table>" in html_out
        assert "test_weave" in html_out

    def test_dependency_table_rows(self) -> None:
        plan = _make_plan(
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            execution_order=[["a"], ["b"]],
        )
        html_out = render_execution_plan_html(plan)
        # Header row + 2 data rows
        assert html_out.count("<tr>") == 3

    def test_embedded_svg(self) -> None:
        plan = _make_plan(
            threads=["a"],
            execution_order=[["a"]],
        )
        html_out = render_execution_plan_html(plan)
        assert "<svg" in html_out

    def test_cache_badge(self) -> None:
        plan = _make_plan(
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            execution_order=[["a"], ["b"]],
            cache_targets=["a"],
        )
        html_out = render_execution_plan_html(plan)
        assert "#ebf8ff" in html_out  # cache badge background
        assert "cached" in html_out

    def test_dependency_provenance_badges(self) -> None:
        plan = _make_plan(
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            execution_order=[["a"], ["b"]],
        )
        html_out = render_execution_plan_html(plan)
        # "a" is an inferred dependency of "b" — inline style includes green bg
        assert "#f0fff4" in html_out


class TestLookupNodes:
    """Tests for lookup node rendering with producer/consumer data."""

    def test_lookup_nodes_rendered(self) -> None:
        plan = _make_plan(
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            execution_order=[["a"], ["b"]],
            lookup_schedule={1: ["cust_lookup"]},
            lookup_producers={"cust_lookup": "a"},
            lookup_consumers={"cust_lookup": ["b"]},
        )
        svg = render_dag_svg(plan)
        assert 'class="dag-lookup-node"' in svg
        assert "cust_lookup" in svg
        assert 'class="dag-lookup-edge"' in svg

    def test_lookup_node_valid_xml(self) -> None:
        plan = _make_plan(
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            execution_order=[["a"], ["b"]],
            lookup_schedule={0: ["ext_ref"]},
            lookup_producers={"ext_ref": None},
            lookup_consumers={"ext_ref": ["a"]},
        )
        svg = render_dag_svg(plan)
        ET.fromstring(svg)

    def test_external_lookup_no_producer_edge(self) -> None:
        plan = _make_plan(
            threads=["a"],
            dependencies={"a": []},
            execution_order=[["a"]],
            lookup_schedule={0: ["ext_ref"]},
            lookup_producers={"ext_ref": None},
            lookup_consumers={"ext_ref": ["a"]},
        )
        svg = render_dag_svg(plan)
        assert 'class="dag-lookup-node"' in svg
        # Should have a consumer edge (lookup → a) but no producer edge
        assert 'class="dag-lookup-edge"' in svg

    def test_lookup_fallback_without_producers(self) -> None:
        """Plans without lookup_producers/consumers fall back to dashed lines."""
        plan = _make_plan(
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            execution_order=[["a"], ["b"]],
            lookup_schedule={0: ["ext_ref"]},
        )
        svg = render_dag_svg(plan)
        assert 'class="dag-lookup-line"' in svg
        assert 'class="dag-lookup-node"' not in svg

    def test_lookup_name_matches_thread_name(self) -> None:
        """Lookup with same name as a thread should not cause KeyError."""
        plan = _make_plan(
            threads=["fact_orders", "dim_customer"],
            dependencies={"fact_orders": [], "dim_customer": ["fact_orders"]},
            execution_order=[["fact_orders"], ["dim_customer"]],
            lookup_schedule={1: ["fact_orders"]},
            lookup_producers={"fact_orders": "fact_orders"},
            lookup_consumers={"fact_orders": ["dim_customer"]},
        )
        svg = render_dag_svg(plan)
        assert 'class="dag-lookup-node"' in svg
        ET.fromstring(svg)  # valid XML

    def test_lookup_arrowhead_marker(self) -> None:
        plan = _make_plan(
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            execution_order=[["a"], ["b"]],
            lookup_schedule={1: ["lk"]},
            lookup_producers={"lk": "a"},
            lookup_consumers={"lk": ["b"]},
        )
        svg = render_dag_svg(plan)
        assert "dag-arrowhead-lookup" in svg


class TestLoomDagSvg:
    """Tests for the loom-level DAG renderer."""

    def test_single_plan_delegates(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])
        svg = render_loom_dag_svg([plan])
        # Should delegate to render_dag_svg, no swimlane chrome
        assert 'class="dag-swimlane"' not in svg
        assert "<svg" in svg

    def test_multi_weave_swimlanes(self) -> None:
        plan1 = _make_plan(threads=["a"], execution_order=[["a"]], weave_name="dims")
        plan2 = _make_plan(threads=["x"], execution_order=[["x"]], weave_name="facts")
        svg = render_loom_dag_svg([plan1, plan2])
        assert 'class="dag-swimlane"' in svg
        assert "Weave: dims" in svg
        assert "Weave: facts" in svg

    def test_multi_weave_valid_xml(self) -> None:
        plan1 = _make_plan(threads=["a"], execution_order=[["a"]], weave_name="dims")
        plan2 = _make_plan(threads=["x"], execution_order=[["x"]], weave_name="facts")
        svg = render_loom_dag_svg([plan1, plan2])
        ET.fromstring(svg)

    def test_multi_weave_sequential_arrows(self) -> None:
        plan1 = _make_plan(threads=["a"], execution_order=[["a"]], weave_name="dims")
        plan2 = _make_plan(threads=["x"], execution_order=[["x"]], weave_name="facts")
        svg = render_loom_dag_svg([plan1, plan2])
        # Should have at least one edge between containers
        assert svg.count('class="dag-edge"') >= 1

    def test_multi_weave_thread_nodes(self) -> None:
        plan1 = _make_plan(threads=["a", "b"], execution_order=[["a", "b"]], weave_name="w1")
        plan2 = _make_plan(threads=["x"], execution_order=[["x"]], weave_name="w2")
        svg = render_loom_dag_svg([plan1, plan2])
        assert ">a<" in svg
        assert ">b<" in svg
        assert ">x<" in svg

    def test_empty_plans_returns_empty(self) -> None:
        assert render_loom_dag_svg([]) == ""


class TestRenderPlanHtml:
    def test_summary_table(self) -> None:
        plan = _make_plan(
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            execution_order=[["a"], ["b"]],
        )
        result = _FakeResult([plan])
        html_out = render_plan_html(result)
        assert "PLANNED" in html_out
        assert "weave: test_weave" in html_out
        assert "2 threads" in html_out
        assert "0 cached" in html_out

    def test_multi_weave(self) -> None:
        plan1 = _make_plan(
            threads=["a"],
            execution_order=[["a"]],
        )
        plan2 = ExecutionPlan(
            weave_name="second_weave",
            threads=["x"],
            dependencies={"x": []},
            dependents={"x": []},
            execution_order=[["x"]],
            cache_targets=[],
            inferred_dependencies={"x": []},
            explicit_dependencies={"x": []},
            lookup_schedule=None,
        )
        result = _FakeResult([plan1, plan2])
        html_out = render_plan_html(result)
        # Should have loom header
        assert "2 weaves" in html_out
        # Loom DAG + 2 per-weave SVGs = 3 total
        assert html_out.count("<svg") == 3

    def test_html_escaping(self) -> None:
        plan = _make_plan(
            threads=["<script>alert(1)</script>"],
            execution_order=[["<script>alert(1)</script>"]],
        )
        result = _FakeResult([plan])
        html_out = render_plan_html(result)
        assert "<script>" not in html_out
        assert "&lt;script&gt;" in html_out

    def test_lookup_counts(self) -> None:
        plan = _make_plan(
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            execution_order=[["a"], ["b"]],
            lookup_schedule={0: ["ext_ref", "ext_ref2"]},
        )
        result = _FakeResult([plan])
        html_out = render_plan_html(result)
        assert "2 lookups" in html_out

    def test_inline_styles(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])
        result = _FakeResult([plan])
        html_out = render_plan_html(result)
        # Uses inline styles (no <style> block) for notebook sanitizer compat
        assert "<style>" not in html_out or "<style>" in html_out.split("<svg")[1]
        # Container has explicit background
        assert "background:#ffffff" in html_out


class TestRenderResultHtml:
    """Tests for the unified render_result_html dispatcher."""

    def test_execute_mode(self) -> None:
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "success",
                "config_type": "weave",
                "config_name": "dims",
                "duration_ms": 1200,
                "detail": None,
                "warnings": [],
            },
        )()
        html_out = render_result_html(result)
        assert "Execution Summary" in html_out
        assert "success" in html_out
        assert "1.2s" in html_out

    def test_execute_with_thread_detail(self) -> None:
        thread = type(
            "TR",
            (),
            {
                "status": "success",
                "thread_name": "dim_cust",
                "rows_written": 500,
                "write_mode": "overwrite",
                "target_path": "Tables/dim_cust",
                "error": None,
            },
        )()
        detail = type("WR", (), {"thread_results": [thread]})()
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "success",
                "config_type": "weave",
                "config_name": "dims",
                "duration_ms": 800,
                "detail": detail,
                "warnings": [],
            },
        )()
        html_out = render_result_html(result)
        assert "dim_cust" in html_out
        assert "500" in html_out
        assert "overwrite" in html_out

    def test_execute_loom_level(self) -> None:
        t1 = type(
            "TR",
            (),
            {
                "status": "success",
                "thread_name": "dim_a",
                "rows_written": 100,
                "write_mode": "overwrite",
                "target_path": "Tables/dim_a",
                "error": None,
            },
        )()
        t2 = type(
            "TR",
            (),
            {
                "status": "success",
                "thread_name": "fact_b",
                "rows_written": 2000,
                "write_mode": "append",
                "target_path": "Tables/fact_b",
                "error": None,
            },
        )()
        w1 = type("WR", (), {"thread_results": [t1]})()
        w2 = type("WR", (), {"thread_results": [t2]})()
        detail = type("LR", (), {"weave_results": [w1, w2]})()
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "success",
                "config_type": "loom",
                "config_name": "pipeline",
                "duration_ms": 5000,
                "detail": detail,
                "warnings": [],
            },
        )()
        html_out = render_result_html(result)
        assert "dim_a" in html_out
        assert "fact_b" in html_out
        assert "2,100" in html_out  # total rows: 100 + 2000
        assert "Threads" in html_out

    def test_execute_with_errors(self) -> None:
        thread = type(
            "TR",
            (),
            {
                "status": "failure",
                "thread_name": "bad_thread",
                "rows_written": 0,
                "write_mode": "append",
                "target_path": "t",
                "error": "something broke",
            },
        )()
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "failure",
                "config_type": "thread",
                "config_name": "bad_thread",
                "duration_ms": 50,
                "detail": thread,
                "warnings": [],
            },
        )()
        html_out = render_result_html(result)
        assert "Errors" in html_out
        assert "something broke" in html_out

    def test_validate_success(self) -> None:
        result = type(
            "R",
            (),
            {
                "mode": "validate",
                "status": "success",
                "config_type": "weave",
                "config_name": "test",
                "validation_errors": [],
                "warnings": [],
            },
        )()
        html_out = render_result_html(result)
        assert "Validation Summary" in html_out
        assert "\u2713" in html_out

    def test_validate_failure(self) -> None:
        result = type(
            "R",
            (),
            {
                "mode": "validate",
                "status": "failure",
                "config_type": "weave",
                "config_name": "test",
                "validation_errors": ["bad config"],
                "warnings": [],
            },
        )()
        html_out = render_result_html(result)
        assert "bad config" in html_out

    def test_preview_mode(self) -> None:
        result = type(
            "R",
            (),
            {
                "mode": "preview",
                "status": "success",
                "config_type": "weave",
                "config_name": "test",
                "preview_data": {},
                "warnings": [],
            },
        )()
        html_out = render_result_html(result)
        assert "Preview Summary" in html_out

    def test_plan_delegates(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])
        result = _FakeResult([plan])
        html_out = render_result_html(result)
        assert "Plan Summary" in html_out

    def test_warnings_rendered(self) -> None:
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "success",
                "config_type": "weave",
                "config_name": "test",
                "duration_ms": 100,
                "detail": None,
                "warnings": ["No threads matched filter"],
            },
        )()
        html_out = render_result_html(result)
        assert "Warnings" in html_out
        assert "No threads matched filter" in html_out

    def test_inline_styles_only(self) -> None:
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "success",
                "config_type": "weave",
                "config_name": "test",
                "duration_ms": 100,
                "detail": None,
                "warnings": [],
            },
        )()
        html_out = render_result_html(result)
        assert "<style>" not in html_out
        assert "background:#ffffff" in html_out

    def test_html_escaping(self) -> None:
        result = type(
            "R",
            (),
            {
                "mode": "validate",
                "status": "failure",
                "config_type": "weave",
                "config_name": "<script>x</script>",
                "validation_errors": ["<img onerror=alert(1)>"],
                "warnings": [],
            },
        )()
        html_out = render_result_html(result)
        assert "<script>" not in html_out
        assert "<img" not in html_out


# ---------------------------------------------------------------------------
# Helpers for M103 Phase 5 tests
# ---------------------------------------------------------------------------


def _fake_source(*, type_: str = "delta", alias: str = "src") -> object:
    """Build a duck-typed Source for flow diagram tests."""
    return type("S", (), {"type": type_, "alias": alias, "path": None, "lookup": None})()


def _fake_target(*, alias: str = "tgt", write_mode: str = "overwrite") -> object:
    """Build a duck-typed Target for flow diagram tests."""
    return type("T", (), {"alias": alias, "path": None})()


def _fake_step(step_type: str, detail: object | None = None) -> object:
    """Build a duck-typed pipeline Step of given type."""
    attrs: dict[str, object] = {st: None for st in ("filter", "rename", "cast", "join", "derive")}
    attrs[step_type] = detail or type("P", (), {})()
    return type("Step", (), attrs)()


def _fake_join_step(jtype: str = "inner", keys: list[str] | None = None) -> object:
    """Build a duck-typed JoinStep with join params."""
    key_objs = [type("K", (), {"left": k})() for k in (keys or ["id"])]
    params = type("JP", (), {"type": jtype, "on": key_objs, "source": "right"})()
    return _fake_step("join", params)


def _fake_thread(
    *,
    name: str = "test_thread",
    sources: dict[str, object] | None = None,
    steps: list[object] | None = None,
    target: object | None = None,
    write: object | None = None,
    validations: list[object] | None = None,
    assertions: list[object] | None = None,
    exports: list[object] | None = None,
) -> Any:
    """Build a duck-typed Thread model for flow diagram tests."""
    return type(
        "Thread",
        (),
        {
            "name": name,
            "sources": sources or {"main": _fake_source()},
            "steps": steps or [],
            "target": target or _fake_target(),
            "write": write or type("W", (), {"mode": "overwrite"})(),
            "validations": validations,
            "assertions": assertions,
            "exports": exports,
        },
    )()


def _fake_span(*, start_ms: int = 0, end_ms: int = 1000) -> object:
    """Build a duck-typed ExecutionSpan."""
    from datetime import datetime, timedelta

    base = datetime(2026, 1, 1, tzinfo=UTC)
    return type(
        "Span",
        (),
        {
            "start_time": base + timedelta(milliseconds=start_ms),
            "end_time": base + timedelta(milliseconds=end_ms),
        },
    )()


def _fake_thread_telemetry(
    *,
    rows_read: int = 100,
    rows_written: int = 90,
    rows_quarantined: int = 0,
    rows_after_transforms: int = 95,
    load_mode: str = "full",
    span: object | None = None,
    validation_results: list[object] | None = None,
    assertion_results: list[object] | None = None,
    resolved_params: dict[str, object] | None = None,
) -> object:
    """Build a duck-typed ThreadTelemetry."""
    return type(
        "TT",
        (),
        {
            "span": span or _fake_span(),
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_quarantined": rows_quarantined,
            "rows_after_transforms": rows_after_transforms,
            "load_mode": load_mode,
            "validation_results": validation_results or [],
            "assertion_results": assertion_results or [],
            "resolved_params": resolved_params,
            "watermark_column": None,
            "cdc_inserts": None,
            "cdc_updates": None,
            "cdc_deletes": None,
            "export_results": [],
        },
    )()


def _fake_export_result(
    *,
    name: str = "archive",
    type_: str = "parquet",
    rows_written: int = 100,
) -> object:
    """Build a duck-typed ExportResult."""
    return type(
        "ER",
        (),
        {"name": name, "type": type_, "rows_written": rows_written, "status": "success"},
    )()


def _fake_thread_result(
    *,
    thread_name: str = "dim_cust",
    status: str = "success",
    rows_written: int = 500,
    write_mode: str = "overwrite",
    telemetry: object | None = None,
    error: str | None = None,
    output_schema: list[tuple[str, str]] | None = None,
    samples: dict[str, list[dict[str, object]]] | None = None,
) -> object:
    """Build a duck-typed ThreadResult."""
    return type(
        "TR",
        (),
        {
            "thread_name": thread_name,
            "status": status,
            "rows_written": rows_written,
            "write_mode": write_mode,
            "target_path": f"Tables/{thread_name}",
            "telemetry": telemetry,
            "error": error,
            "output_schema": output_schema,
            "samples": samples,
        },
    )()


# ---------------------------------------------------------------------------
# Task 16 — FlowDiagram class tests
# ---------------------------------------------------------------------------


class TestFlowDiagram:
    def test_svg_property(self) -> None:
        diagram = FlowDiagram(_SAMPLE_SVG)
        assert diagram.svg == _SAMPLE_SVG

    def test_repr_svg(self) -> None:
        diagram = FlowDiagram(_SAMPLE_SVG)
        assert diagram._repr_svg_() == _SAMPLE_SVG

    def test_repr_html(self) -> None:
        diagram = FlowDiagram(_SAMPLE_SVG)
        html_out = diagram._repr_html_()
        assert _SAMPLE_SVG in html_out
        assert "<div" in html_out

    def test_str(self) -> None:
        diagram = FlowDiagram(_SAMPLE_SVG)
        assert str(diagram) == _SAMPLE_SVG

    def test_save(self, tmp_path: pytest.TempPathFactory) -> None:
        diagram = FlowDiagram(_SAMPLE_SVG)
        out = str(tmp_path / "flow.svg")  # type: ignore[operator]
        diagram.save(out)
        assert Path(out).read_text(encoding="utf-8") == _SAMPLE_SVG


# ---------------------------------------------------------------------------
# Task 17 — render_flow_svg() tests
# ---------------------------------------------------------------------------


class TestRenderFlowSvg:
    def test_single_source_no_transforms(self) -> None:
        thread = _fake_thread()
        svg = render_flow_svg(thread)
        assert "<svg" in svg
        assert 'class="flow-source"' in svg
        assert 'class="flow-target"' in svg

    def test_single_source_with_transforms(self) -> None:
        thread = _fake_thread(steps=[_fake_step("filter"), _fake_step("rename")])
        svg = render_flow_svg(thread)
        assert 'class="flow-transform"' in svg

    def test_multiple_sources_with_join(self) -> None:
        sources = {"main": _fake_source(alias="src_a"), "right": _fake_source(alias="src_b")}
        steps = [_fake_join_step("inner", ["id"]), _fake_step("filter")]
        thread = _fake_thread(sources=sources, steps=steps)
        svg = render_flow_svg(thread)
        assert "main" in svg
        assert "right" in svg
        assert 'class="flow-join"' in svg

    def test_consecutive_same_type_grouped(self) -> None:
        steps = [_fake_step("rename"), _fake_step("rename"), _fake_step("rename")]
        thread = _fake_thread(steps=steps)
        svg = render_flow_svg(thread)
        # Grouped with count badge — "(3)" in the SVG
        assert "(3)" in svg

    def test_mixed_transforms_not_grouped(self) -> None:
        steps = [_fake_step("rename"), _fake_step("filter"), _fake_step("rename")]
        thread = _fake_thread(steps=steps)
        svg = render_flow_svg(thread)
        # 3 separate transform nodes, no grouping badge
        assert svg.count('class="flow-transform"') == 3
        assert "(2)" not in svg
        assert "(3)" not in svg

    def test_valid_xml(self) -> None:
        steps = [_fake_join_step(), _fake_step("filter"), _fake_step("rename")]
        sources = {"main": _fake_source(), "right": _fake_source(alias="lookup")}
        thread = _fake_thread(sources=sources, steps=steps)
        svg = render_flow_svg(thread)
        ET.fromstring(svg)

    def test_dark_mode_css(self) -> None:
        thread = _fake_thread()
        svg = render_flow_svg(thread)
        assert "prefers-color-scheme:dark" in svg

    def test_forced_dark(self) -> None:
        thread = _fake_thread()
        svg = render_flow_svg(thread, dark=True)
        assert "prefers-color-scheme:dark" not in svg

    def test_forced_light(self) -> None:
        thread = _fake_thread()
        svg = render_flow_svg(thread, dark=False)
        assert "prefers-color-scheme:dark" not in svg

    def test_xml_escaping(self) -> None:
        thread = _fake_thread(
            sources={"<script>": _fake_source(alias="src")},
            target=_fake_target(alias="target&name"),
        )
        svg = render_flow_svg(thread)
        assert "<script>" not in svg
        assert "&lt;script&gt;" in svg

    def test_validation_node_present(self) -> None:
        """Thread with validations shows validation gate and quarantine nodes."""
        thread = _fake_thread(validations=[{"rule": "test"}])
        svg = render_flow_svg(thread)
        assert 'class="flow-validation"' in svg
        assert 'class="flow-quarantine"' in svg
        assert ">validation<" in svg
        assert ">quarantine<" in svg

    def test_assertions_node_present(self) -> None:
        """Thread with assertions shows assertions checkpoint node."""
        thread = _fake_thread(assertions=[{"type": "row_count"}])
        svg = render_flow_svg(thread)
        assert 'class="flow-assertion"' in svg
        assert ">assertions<" in svg

    def test_export_nodes_present(self) -> None:
        """Thread with exports shows export destination nodes."""
        exports = [
            type("E", (), {"name": "archive", "type": "parquet"})(),
            type("E", (), {"name": "csv_feed", "type": "csv"})(),
        ]
        thread = _fake_thread(exports=exports)
        svg = render_flow_svg(thread)
        assert 'class="flow-export"' in svg
        assert ">archive<" in svg
        assert ">csv_feed<" in svg
        assert ">parquet<" in svg
        assert ">csv<" in svg

    def test_all_new_nodes_together(self) -> None:
        """Thread with validations, assertions, and exports renders all nodes."""
        exports = [type("E", (), {"name": "backup", "type": "delta"})()]
        thread = _fake_thread(
            validations=[{"rule": "test"}],
            assertions=[{"type": "row_count"}],
            exports=exports,
        )
        svg = render_flow_svg(thread)
        assert 'class="flow-validation"' in svg
        assert 'class="flow-quarantine"' in svg
        assert 'class="flow-assertion"' in svg
        assert 'class="flow-export"' in svg
        # Valid XML
        ET.fromstring(svg)

    def test_no_new_nodes_backward_compatible(self) -> None:
        """Thread without validations/assertions/exports has no new node types."""
        thread = _fake_thread()
        svg = render_flow_svg(thread)
        assert 'class="flow-validation"' not in svg
        assert 'class="flow-quarantine"' not in svg
        assert 'class="flow-assertion"' not in svg
        assert 'class="flow-export"' not in svg


# ---------------------------------------------------------------------------
# Task 18 — Timeline, waterfall, and annotated DAG SVG tests
# ---------------------------------------------------------------------------


class TestRenderTimelineSvg:
    def _make_weave_result(
        self,
        thread_names: list[str],
        *,
        statuses: list[str] | None = None,
    ) -> object:
        """Build a duck-typed WeaveResult with thread_results."""
        statuses = statuses or ["success"] * len(thread_names)
        trs = []
        for i, name in enumerate(thread_names):
            tr = _fake_thread_result(
                thread_name=name,
                status=statuses[i],
                telemetry=_fake_thread_telemetry(
                    span=_fake_span(start_ms=i * 100, end_ms=(i + 1) * 200)
                ),
            )
            trs.append(tr)
        return type("WR", (), {"thread_results": trs, "weave_name": "test_weave"})()

    def test_basic_timeline(self) -> None:
        wr = self._make_weave_result(["a", "b"])
        svg = render_timeline_svg(wr, None, None)
        assert "<svg" in svg
        assert "a" in svg
        assert "b" in svg

    def test_with_hooks_and_lookups(self) -> None:
        wr = self._make_weave_result(["a"])
        hooks = [
            type(
                "HR",
                (),
                {
                    "phase": "pre",
                    "step_name": "pre_hook",
                    "step_type": "sql",
                    "status": "passed",
                    "duration_ms": 50,
                    "gate_result": None,
                },
            )(),
        ]
        lookups = [
            type("LR", (), {"name": "ext_ref", "duration_ms": 100})(),
        ]
        svg = render_timeline_svg(wr, hooks, lookups)
        assert "Pre-hooks" in svg or "pre" in svg.lower()
        assert "Lookups" in svg or "ext_ref" in svg

    def test_empty_threads(self) -> None:
        wr = type("WR", (), {"thread_results": [], "weave_name": "empty"})()
        svg = render_timeline_svg(wr, None, None)
        assert "<svg" in svg

    def test_valid_xml(self) -> None:
        wr = self._make_weave_result(["a", "b", "c"])
        svg = render_timeline_svg(wr, None, None)
        ET.fromstring(svg)

    def test_dark_mode_css(self) -> None:
        wr = self._make_weave_result(["a"])
        svg = render_timeline_svg(wr, None, None)
        assert "prefers-color-scheme:dark" in svg

    def test_forced_dark(self) -> None:
        wr = self._make_weave_result(["a"])
        svg = render_timeline_svg(wr, None, None, dark=True)
        assert "prefers-color-scheme:dark" not in svg

    def test_forced_light(self) -> None:
        wr = self._make_weave_result(["a"])
        svg = render_timeline_svg(wr, None, None, dark=False)
        assert "prefers-color-scheme:dark" not in svg


class TestRenderWaterfallSvg:
    def test_execute_mode_full(self) -> None:
        tr = _fake_thread_result(rows_written=90)
        tt = _fake_thread_telemetry(rows_read=100, rows_after_transforms=95, rows_quarantined=5)
        svg = render_waterfall_svg(tr, tt, mode="execute")
        assert "Rows read" in svg
        assert "After transforms" in svg
        assert "Quarantined" in svg
        assert "Target" in svg

    def test_execute_mode_no_quarantine(self) -> None:
        tr = _fake_thread_result(rows_written=100)
        tt = _fake_thread_telemetry(rows_read=100, rows_after_transforms=100, rows_quarantined=0)
        svg = render_waterfall_svg(tr, tt, mode="execute")
        assert "Quarantined" not in svg
        assert "Target" in svg

    def test_preview_mode(self) -> None:
        tr = _fake_thread_result(rows_written=0)
        tt = _fake_thread_telemetry(rows_read=50, rows_after_transforms=45)
        svg = render_waterfall_svg(tr, tt, mode="preview")
        assert "Rows read" in svg
        assert "After transforms" in svg
        assert "Target" not in svg

    def test_zero_rows(self) -> None:
        tr = _fake_thread_result(rows_written=0)
        tt = _fake_thread_telemetry(rows_read=0, rows_after_transforms=0)
        svg = render_waterfall_svg(tr, tt, mode="execute")
        assert "<svg" in svg

    def test_valid_xml(self) -> None:
        tr = _fake_thread_result(rows_written=90)
        tt = _fake_thread_telemetry()
        svg = render_waterfall_svg(tr, tt, mode="execute")
        ET.fromstring(svg)

    def test_thousands_separator(self) -> None:
        tr = _fake_thread_result(rows_written=1_500_000)
        tt = _fake_thread_telemetry(rows_read=2_000_000, rows_after_transforms=1_800_000)
        svg = render_waterfall_svg(tr, tt, mode="execute")
        assert "2,000,000" in svg
        assert "1,500,000" in svg

    def test_dark_mode_css(self) -> None:
        tr = _fake_thread_result(rows_written=90)
        tt = _fake_thread_telemetry()
        svg = render_waterfall_svg(tr, tt, mode="execute")
        assert "prefers-color-scheme:dark" in svg

    def test_forced_dark(self) -> None:
        tr = _fake_thread_result(rows_written=90)
        tt = _fake_thread_telemetry()
        svg = render_waterfall_svg(tr, tt, mode="execute", dark=True)
        assert "prefers-color-scheme:dark" not in svg

    def test_with_exports(self) -> None:
        """Sankey shows export bands alongside target."""
        er = _fake_export_result(name="archive", rows_written=90)
        tr = _fake_thread_result(rows_written=90)
        tt = _fake_thread_telemetry(rows_read=100, rows_after_transforms=95)
        tt.export_results = [er]  # type: ignore[attr-defined]
        svg = render_waterfall_svg(tr, tt, mode="execute")
        assert "Target" in svg
        assert "archive" in svg

    def test_forced_light(self) -> None:
        tr = _fake_thread_result(rows_written=90)
        tt = _fake_thread_telemetry()
        svg = render_waterfall_svg(tr, tt, mode="execute", dark=False)
        assert "prefers-color-scheme:dark" not in svg


class TestRenderAnnotatedDagSvg:
    def test_status_badges_present(self) -> None:
        plan = _make_plan(
            threads=["a", "b"], dependencies={"a": [], "b": ["a"]}, execution_order=[["a"], ["b"]]
        )
        trs = {
            "a": _fake_thread_result(
                thread_name="a", status="success", telemetry=_fake_thread_telemetry()
            ),
            "b": _fake_thread_result(
                thread_name="b", status="failure", telemetry=_fake_thread_telemetry()
            ),
        }
        svg = render_annotated_dag_svg(plan, trs)
        # Green for success, red for failure
        assert "#48bb78" in svg  # success badge color
        assert "#fc8181" in svg  # failure badge color

    def test_duration_labels(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])
        trs = {
            "a": _fake_thread_result(
                thread_name="a",
                telemetry=_fake_thread_telemetry(span=_fake_span(start_ms=0, end_ms=2500)),
            )
        }
        svg = render_annotated_dag_svg(plan, trs)
        assert "2.5s" in svg

    def test_missing_thread_results(self) -> None:
        plan = _make_plan(
            threads=["a", "b"], dependencies={"a": [], "b": ["a"]}, execution_order=[["a"], ["b"]]
        )
        trs = {"a": _fake_thread_result(thread_name="a", telemetry=_fake_thread_telemetry())}
        svg = render_annotated_dag_svg(plan, trs)
        # Should render without error — "b" has no badge
        assert "<svg" in svg

    def test_valid_xml(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])
        trs = {"a": _fake_thread_result(thread_name="a", telemetry=_fake_thread_telemetry())}
        svg = render_annotated_dag_svg(plan, trs)
        ET.fromstring(svg)

    def test_empty_plan_fallback(self) -> None:
        plan = _make_plan(threads=[], execution_order=[])
        svg = render_annotated_dag_svg(plan, {})
        assert "<svg" in svg

    def test_dark_mode_css(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])
        trs = {"a": _fake_thread_result(thread_name="a", telemetry=_fake_thread_telemetry())}
        svg = render_annotated_dag_svg(plan, trs)
        assert "prefers-color-scheme:dark" in svg

    def test_forced_dark(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])
        trs = {"a": _fake_thread_result(thread_name="a", telemetry=_fake_thread_telemetry())}
        svg = render_annotated_dag_svg(plan, trs, dark=True)
        assert "prefers-color-scheme:dark" not in svg

    def test_forced_light(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])
        trs = {"a": _fake_thread_result(thread_name="a", telemetry=_fake_thread_telemetry())}
        svg = render_annotated_dag_svg(plan, trs, dark=False)
        assert "prefers-color-scheme:dark" not in svg


# ---------------------------------------------------------------------------
# Task 19 — HTML section renderer tests
# ---------------------------------------------------------------------------


class TestHtmlSectionRenderers:
    def test_params_table(self) -> None:
        from weevr.engine.display import _render_params_table

        html_out = _render_params_table({"env": "prod", "date": "2026-01-01"})
        assert "Resolved Parameters" in html_out
        assert "env" in html_out
        assert "prod" in html_out

    def test_params_table_empty(self) -> None:
        from weevr.engine.display import _render_params_table

        assert _render_params_table(None) == ""
        assert _render_params_table({}) == ""

    def test_variables_table(self) -> None:
        from weevr.engine.display import _render_variables_table

        html_out = _render_variables_table({"max_date": "2026-01-01"})
        assert "Variables" in html_out
        assert "max_date" in html_out

    def test_hook_results_table(self) -> None:
        from weevr.engine.display import _render_hook_results_table

        hooks = [
            type(
                "HR",
                (),
                {
                    "phase": "pre",
                    "step_type": "sql",
                    "step_name": "check",
                    "status": "passed",
                    "duration_ms": 50,
                    "gate_result": None,
                },
            )(),
        ]
        html_out = _render_hook_results_table(hooks)
        assert "Hook Results" in html_out
        assert "check" in html_out
        assert "pre" in html_out

    def test_hook_results_gate_failure(self) -> None:
        from weevr.engine.display import _render_hook_results_table

        gate = type("G", (), {"passed": False, "message": "threshold exceeded"})()
        hooks = [
            type(
                "HR",
                (),
                {
                    "phase": "pre",
                    "step_type": "gate",
                    "step_name": "check",
                    "status": "aborted",
                    "duration_ms": 10,
                    "gate_result": gate,
                },
            )(),
        ]
        html_out = _render_hook_results_table(hooks)
        assert "threshold exceeded" in html_out

    def test_lookup_results_table(self) -> None:
        from weevr.engine.display import _render_lookup_results_table

        lookups = [
            type(
                "LR",
                (),
                {
                    "name": "dim_product",
                    "strategy": "broadcast",
                    "row_count": 500,
                    "duration_ms": 200,
                    "unique_key_checked": True,
                    "unique_key_passed": True,
                },
            )(),
        ]
        html_out = _render_lookup_results_table(lookups)
        assert "Lookup Results" in html_out
        assert "dim_product" in html_out
        assert "broadcast" in html_out

    def test_validation_table(self) -> None:
        from weevr.engine.display import _render_validation_table

        rules = [
            type(
                "VR",
                (),
                {
                    "rule_name": "not_null_id",
                    "expression": "id IS NOT NULL",
                    "severity": "error",
                    "rows_passed": 99,
                    "rows_failed": 1,
                },
            )(),
        ]
        html_out = _render_validation_table(rules)
        assert "Validation Rules" in html_out
        assert "not_null_id" in html_out
        assert "id IS NOT NULL" in html_out

    def test_validation_table_highlights_failures(self) -> None:
        from weevr.engine.display import _render_validation_table

        rules = [
            type(
                "VR",
                (),
                {
                    "rule_name": "r",
                    "expression": "x",
                    "severity": "error",
                    "rows_passed": 0,
                    "rows_failed": 100,
                },
            )(),
        ]
        html_out = _render_validation_table(rules)
        assert "#9b2c2c" in html_out  # red color for failures

    def test_assertion_table(self) -> None:
        from weevr.engine.display import _render_assertion_table

        assertions = [
            type(
                "AR",
                (),
                {
                    "assertion_type": "row_count",
                    "severity": "error",
                    "passed": True,
                    "details": "count=500, min=1",
                },
            )(),
        ]
        html_out = _render_assertion_table(assertions)
        assert "Assertions" in html_out
        assert "row_count" in html_out

    def test_schema_table(self) -> None:
        from weevr.engine.display import _render_schema_table

        schema = [("id", "bigint"), ("name", "string")]
        html_out = _render_schema_table(schema)
        assert "Output Schema" in html_out
        assert "bigint" in html_out
        assert "name" in html_out

    def test_schema_table_empty(self) -> None:
        from weevr.engine.display import _render_schema_table

        assert _render_schema_table(None) == ""
        assert _render_schema_table([]) == ""

    def test_sample_table(self) -> None:
        from weevr.engine.display import _render_sample_table

        samples = {"output": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}
        html_out = _render_sample_table(samples, "output")
        assert "Data Sample" in html_out
        assert "Alice" in html_out
        assert "Bob" in html_out

    def test_sample_table_truncation(self) -> None:
        from weevr.engine.display import _render_sample_table

        long_val = "x" * 100
        samples = {"output": [{"col": long_val}]}
        html_out = _render_sample_table(samples, "output")
        assert "\u2026" in html_out  # ellipsis for truncation

    def test_sample_table_missing_key(self) -> None:
        from weevr.engine.display import _render_sample_table

        assert _render_sample_table({"output": []}, "quarantine") == ""
        assert _render_sample_table(None, "output") == ""

    def test_watermark_section(self) -> None:
        from weevr.engine.display import _render_watermark_section

        telem = type(
            "TT",
            (),
            {
                "watermark_column": "updated_at",
                "watermark_previous_value": "2026-01-01",
                "watermark_new_value": "2026-01-15",
                "watermark_first_run": False,
                "watermark_persisted": True,
            },
        )()
        html_out = _render_watermark_section(telem)
        assert "Watermark State" in html_out
        assert "updated_at" in html_out
        assert "2026-01-15" in html_out

    def test_watermark_section_first_run(self) -> None:
        from weevr.engine.display import _render_watermark_section

        telem = type(
            "TT",
            (),
            {
                "watermark_column": "ts",
                "watermark_previous_value": None,
                "watermark_new_value": "2026-01-01",
                "watermark_first_run": True,
                "watermark_persisted": True,
            },
        )()
        html_out = _render_watermark_section(telem)
        assert "first run" in html_out

    def test_watermark_section_empty(self) -> None:
        from weevr.engine.display import _render_watermark_section

        telem = type("TT", (), {"watermark_column": None})()
        assert _render_watermark_section(telem) == ""

    def test_cdc_section(self) -> None:
        from weevr.engine.display import _render_cdc_section

        telem = type("TT", (), {"cdc_inserts": 100, "cdc_updates": 50, "cdc_deletes": 5})()
        html_out = _render_cdc_section(telem)
        assert "CDC Breakdown" in html_out
        assert "100" in html_out
        assert "50" in html_out

    def test_cdc_section_empty(self) -> None:
        from weevr.engine.display import _render_cdc_section

        telem = type("TT", (), {"cdc_inserts": None, "cdc_updates": None, "cdc_deletes": None})()
        assert _render_cdc_section(telem) == ""

    def test_row_counts(self) -> None:
        from weevr.engine.display import _render_row_counts

        telem = _fake_thread_telemetry(rows_read=100, rows_after_transforms=95, rows_quarantined=5)
        html_out = _render_row_counts(telem, rows_written=90)
        assert "Row Counts" in html_out
        assert "100" in html_out
        assert "90" in html_out

    def test_row_counts_empty(self) -> None:
        from weevr.engine.display import _render_row_counts

        telem = type(
            "TT", (), {"rows_read": 0, "rows_quarantined": 0, "rows_after_transforms": 0}
        )()
        assert _render_row_counts(telem, rows_written=0) == ""


# ---------------------------------------------------------------------------
# Task 19 (continued) — Collapsible layout and mode matrix tests
# ---------------------------------------------------------------------------


class TestCollapsibleLayout:
    def test_details_elements_in_execute(self) -> None:
        tr = _fake_thread_result(telemetry=_fake_thread_telemetry())
        detail = type("WR", (), {"thread_results": [tr]})()
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "success",
                "config_type": "weave",
                "config_name": "test",
                "duration_ms": 100,
                "detail": detail,
                "warnings": [],
                "telemetry": None,
                "_resolved_threads": {},
                "execution_plan": None,
            },
        )()
        html_out = render_result_html(result)
        assert "<details" in html_out
        assert "<summary" in html_out

    def test_failed_thread_auto_expanded(self) -> None:
        tr = _fake_thread_result(status="failure", error="boom", telemetry=_fake_thread_telemetry())
        detail = type("WR", (), {"thread_results": [tr]})()
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "failure",
                "config_type": "weave",
                "config_name": "test",
                "duration_ms": 100,
                "detail": detail,
                "warnings": [],
                "telemetry": None,
                "_resolved_threads": {},
                "execution_plan": None,
            },
        )()
        html_out = render_result_html(result)
        assert "open" in html_out

    def test_successful_thread_collapsed(self) -> None:
        tr = _fake_thread_result(status="success", telemetry=_fake_thread_telemetry())
        detail = type("WR", (), {"thread_results": [tr]})()
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "success",
                "config_type": "weave",
                "config_name": "test",
                "duration_ms": 100,
                "detail": detail,
                "warnings": [],
                "telemetry": None,
                "_resolved_threads": {},
                "execution_plan": None,
            },
        )()
        html_out = render_result_html(result)
        # <details style="..."> without open attribute
        assert '<details style="' in html_out
        # The details tag should NOT have " open" for successful threads
        import re

        details_tags = re.findall(r"<details[^>]*>", html_out)
        assert all(" open" not in tag for tag in details_tags)

    def test_loom_two_level_collapse(self) -> None:
        tr = _fake_thread_result(thread_name="dim_a")
        w1 = type("WR", (), {"thread_results": [tr], "weave_name": "dims", "status": "success"})()
        detail = type("LR", (), {"weave_results": [w1]})()
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "success",
                "config_type": "loom",
                "config_name": "pipeline",
                "duration_ms": 500,
                "detail": detail,
                "warnings": [],
                "telemetry": None,
                "_resolved_threads": {},
                "execution_plan": None,
            },
        )()
        html_out = render_result_html(result)
        # Nested details: weave level + thread level
        assert html_out.count("<details") >= 2


class TestModeMatrix:
    def test_execute_mode_sections(self) -> None:
        telem = _fake_thread_telemetry(
            validation_results=[
                type(
                    "VR",
                    (),
                    {
                        "rule_name": "r1",
                        "expression": "x",
                        "severity": "warn",
                        "rows_passed": 100,
                        "rows_failed": 0,
                    },
                )(),
            ],
            assertion_results=[
                type(
                    "AR",
                    (),
                    {
                        "assertion_type": "row_count",
                        "severity": "error",
                        "passed": True,
                        "details": "ok",
                    },
                )(),
            ],
        )
        tr = _fake_thread_result(
            telemetry=telem,
            output_schema=[("id", "bigint")],
            samples={"output": [{"id": 1}]},
        )
        detail = type("WR", (), {"thread_results": [tr]})()
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "success",
                "config_type": "weave",
                "config_name": "test",
                "duration_ms": 100,
                "detail": detail,
                "warnings": [],
                "telemetry": None,
                "_resolved_threads": {},
                "execution_plan": None,
            },
        )()
        html_out = render_result_html(result)
        assert "Validation Rules" in html_out
        assert "Assertions" in html_out
        assert "Output Schema" in html_out
        assert "Data Sample" in html_out
        assert "Row Counts" in html_out

    def test_preview_mode_sections(self) -> None:
        result = type(
            "R",
            (),
            {
                "mode": "preview",
                "status": "success",
                "config_type": "weave",
                "config_name": "test",
                "duration_ms": 100,
                "preview_data": {},
                "_preview_metadata": {},
                "telemetry": None,
                "_resolved_threads": {},
                "warnings": [],
            },
        )()
        html_out = render_result_html(result)
        assert "Preview Summary" in html_out
        # No execute-only sections
        assert "Hook Results" not in html_out
        assert "CDC Breakdown" not in html_out

    def test_validate_mode_with_assertions(self) -> None:
        v_rule = type(
            "V", (), {"name": "not_null", "expression": "id IS NOT NULL", "severity": "error"}
        )()
        a_def = type(
            "A",
            (),
            {
                "type": "row_count",
                "severity": "error",
                "columns": None,
                "expression": None,
                "min": 1,
                "max": None,
            },
        )()
        thread_model = type("TM", (), {"validations": [v_rule], "assertions": [a_def]})()
        result = type(
            "R",
            (),
            {
                "mode": "validate",
                "status": "success",
                "config_type": "weave",
                "config_name": "test",
                "validation_errors": [],
                "warnings": [],
                "telemetry": None,
                "_resolved_threads": {"dim_cust": thread_model},
            },
        )()
        html_out = render_result_html(result)
        assert "Validation Rules" in html_out
        assert "not_null" in html_out
        assert "Assertions" in html_out
        assert "row_count" in html_out

    def test_plan_mode_with_flow_diagrams(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])
        thread_model = _fake_thread(name="a")
        result = type(
            "R",
            (),
            {
                "mode": "plan",
                "status": "PLANNED",
                "config_type": "weave",
                "config_name": "test_weave",
                "execution_plan": [plan],
                "_resolved_threads": {"a": thread_model},
                "telemetry": None,
            },
        )()
        html_out = render_result_html(result)
        assert "Thread Pipelines" in html_out
        assert 'class="flow-source"' in html_out


# ---------------------------------------------------------------------------
# Task 20 — Data model tests
# ---------------------------------------------------------------------------


class TestDataModelFields:
    def test_thread_result_new_fields(self) -> None:
        from weevr.engine.result import ThreadResult

        tr = ThreadResult(
            status="success",
            thread_name="test",
            rows_written=100,
            write_mode="overwrite",
            target_path="Tables/test",
            output_schema=[("id", "bigint"), ("name", "string")],
            samples={"output": [{"id": 1, "name": "a"}]},
        )
        assert tr.output_schema == [("id", "bigint"), ("name", "string")]
        assert tr.samples is not None
        assert len(tr.samples["output"]) == 1

    def test_thread_result_defaults(self) -> None:
        from weevr.engine.result import ThreadResult

        tr = ThreadResult(
            status="success",
            thread_name="test",
            rows_written=0,
            write_mode="overwrite",
            target_path="Tables/test",
        )
        assert tr.output_schema is None
        assert tr.samples is None

    def test_thread_telemetry_new_fields(self) -> None:
        from weevr.telemetry.results import ThreadTelemetry
        from weevr.telemetry.span import ExecutionSpan

        span = ExecutionSpan(trace_id="a" * 32, span_id="b" * 16, name="test")
        tt = ThreadTelemetry(
            span=span,
            rows_after_transforms=500,
            resolved_params={"env": "prod"},
        )
        assert tt.rows_after_transforms == 500
        assert tt.resolved_params == {"env": "prod"}

    def test_weave_telemetry_resolved_params(self) -> None:
        from weevr.telemetry.results import WeaveTelemetry
        from weevr.telemetry.span import ExecutionSpan

        span = ExecutionSpan(trace_id="a" * 32, span_id="b" * 16, name="test")
        wt = WeaveTelemetry(
            span=span,
            resolved_params={"batch": "daily"},
        )
        assert wt.resolved_params == {"batch": "daily"}

    def test_loom_telemetry_resolved_params(self) -> None:
        from weevr.telemetry.results import LoomTelemetry
        from weevr.telemetry.span import ExecutionSpan

        span = ExecutionSpan(trace_id="a" * 32, span_id="b" * 16, name="test")
        lt = LoomTelemetry(
            span=span,
            resolved_params={"region": "us"},
        )
        assert lt.resolved_params == {"region": "us"}


# ---------------------------------------------------------------------------
# Task 21 — Edge case tests (partial data, failures, empty states)
# ---------------------------------------------------------------------------


class TestPartialData:
    def test_failed_thread_shows_error_banner(self) -> None:
        tr = _fake_thread_result(
            status="failure",
            error="Connection timeout",
            rows_written=0,
            telemetry=_fake_thread_telemetry(rows_read=0, rows_after_transforms=0),
        )
        detail = type("WR", (), {"thread_results": [tr]})()
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "failure",
                "config_type": "weave",
                "config_name": "test",
                "duration_ms": 100,
                "detail": detail,
                "warnings": [],
                "telemetry": None,
                "_resolved_threads": {},
                "execution_plan": None,
            },
        )()
        html_out = render_result_html(result)
        assert "Connection timeout" in html_out

    def test_missing_sections_omitted(self) -> None:
        # Thread with no schema, no samples, no watermark, no CDC
        tr = _fake_thread_result(telemetry=_fake_thread_telemetry())
        detail = type("WR", (), {"thread_results": [tr]})()
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "success",
                "config_type": "weave",
                "config_name": "test",
                "duration_ms": 100,
                "detail": detail,
                "warnings": [],
                "telemetry": None,
                "_resolved_threads": {},
                "execution_plan": None,
            },
        )()
        html_out = render_result_html(result)
        assert "Output Schema" not in html_out
        assert "Data Sample" not in html_out
        assert "Watermark" not in html_out
        assert "CDC" not in html_out

    def test_empty_weave_result(self) -> None:
        detail = type("WR", (), {"thread_results": []})()
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "success",
                "config_type": "weave",
                "config_name": "test",
                "duration_ms": 0,
                "detail": detail,
                "warnings": [],
                "telemetry": None,
                "_resolved_threads": {},
                "execution_plan": None,
            },
        )()
        html_out = render_result_html(result)
        assert "Execution Summary" in html_out

    def test_thread_with_partial_telemetry(self) -> None:
        # rows_read captured but no samples or schema
        tr = _fake_thread_result(
            status="failure",
            error="Write failed",
            rows_written=0,
            telemetry=_fake_thread_telemetry(rows_read=100, rows_after_transforms=95),
        )
        detail = type("WR", (), {"thread_results": [tr]})()
        result = type(
            "R",
            (),
            {
                "mode": "execute",
                "status": "failure",
                "config_type": "weave",
                "config_name": "test",
                "duration_ms": 100,
                "detail": detail,
                "warnings": [],
                "telemetry": None,
                "_resolved_threads": {},
                "execution_plan": None,
            },
        )()
        html_out = render_result_html(result)
        # Row counts should still be present
        assert "Row Counts" in html_out
        assert "100" in html_out
