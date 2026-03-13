"""Unit tests for the plan display module — DAGDiagram, SVG renderer, and HTML builder."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from weevr.engine.display import (
    DAGDiagram,
    render_dag_svg,
    render_execution_plan_html,
    render_loom_dag_svg,
    render_plan_html,
    render_result_html,
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
