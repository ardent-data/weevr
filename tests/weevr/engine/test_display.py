"""Unit tests for the plan display module — DAGDiagram, SVG renderer, and HTML builder."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from weevr.engine.display import (
    DAGDiagram,
    render_dag_svg,
    render_execution_plan_html,
    render_plan_html,
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
        weave_name="test_weave",
        threads=t,
        dependencies=deps,
        dependents=dependents,
        execution_order=execution_order or [],
        cache_targets=cache_targets or [],
        inferred_dependencies={n: deps.get(n, []) for n in t},
        explicit_dependencies={n: [] for n in t},
        lookup_schedule=lookup_schedule,
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

        nodes, _edges, _w, _h = _compute_layout(plan)
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
        assert "<table>" in html_out
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
        assert "badge-cache" in html_out
        assert "cached" in html_out

    def test_dependency_provenance_badges(self) -> None:
        plan = _make_plan(
            threads=["a", "b"],
            dependencies={"a": [], "b": ["a"]},
            execution_order=[["a"], ["b"]],
        )
        html_out = render_execution_plan_html(plan)
        # "a" is an inferred dependency of "b" (via _make_plan)
        assert "badge-inferred" in html_out


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
        # Both SVGs present
        assert html_out.count("<svg") == 2

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

    def test_style_block(self) -> None:
        plan = _make_plan(threads=["a"], execution_order=[["a"]])
        result = _FakeResult([plan])
        html_out = render_plan_html(result)
        assert "<style>" in html_out
        assert "prefers-color-scheme:dark" in html_out
