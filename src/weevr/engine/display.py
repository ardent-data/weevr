"""Plan display — DAG visualization and rich rendering for execution plans."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from weevr.engine.planner import ExecutionPlan

# ---------------------------------------------------------------------------
# SVG layout constants
# ---------------------------------------------------------------------------

_NODE_HEIGHT = 36
_NODE_PADDING = 20  # horizontal text padding inside node
_NODE_CORNER_RADIUS = 6
_H_SPACING = 24  # between nodes in same group
_V_SPACING = 80  # between execution groups
_ARROWHEAD_SIZE = 8
_FONT_FAMILY = "system-ui, -apple-system, sans-serif"
_FONT_SIZE_LABEL = 13
_FONT_SIZE_ANNOTATION = 11
_CHAR_WIDTH_ESTIMATE = 7.8  # approximate px per character at 13px
_SVG_PADDING = 20  # canvas padding around all content
_LOOKUP_MARKER_HEIGHT = 20  # height reserved for lookup markers between groups

# Color palettes
_LIGHT = {
    "bg": "#ffffff",
    "node_fill": "#f7fafc",
    "node_stroke": "#a0aec0",
    "node_text": "#2d3748",
    "cached_fill": "#ebf8ff",
    "cached_stroke": "#3182ce",
    "cached_text": "#2c5282",
    "edge_stroke": "#a0aec0",
    "lookup_fill": "#fffaf0",
    "lookup_stroke": "#dd6b20",
    "lookup_text": "#c05621",
}

_DARK = {
    "bg": "#1a202c",
    "node_fill": "#2d3748",
    "node_stroke": "#718096",
    "node_text": "#e2e8f0",
    "cached_fill": "#2a4365",
    "cached_stroke": "#63b3ed",
    "cached_text": "#bee3f8",
    "edge_stroke": "#718096",
    "lookup_fill": "#744210",
    "lookup_stroke": "#ed8936",
    "lookup_text": "#fbd38d",
}


class DAGDiagram:
    """Inline SVG diagram of a weave execution plan.

    Auto-renders in IPython/Jupyter notebooks via ``_repr_svg_()``.
    SVG markup available as string via ``__str__()`` or the ``svg`` property.
    Export to file via ``save()``.

    Args:
        svg: The raw SVG markup string.
    """

    __slots__ = ("_svg",)

    def __init__(self, svg: str) -> None:
        """Initialize with SVG markup."""
        self._svg = svg

    @property
    def svg(self) -> str:
        """The raw SVG markup."""
        return self._svg

    def _repr_svg_(self) -> str:
        """IPython SVG display protocol."""
        return self._svg

    def __str__(self) -> str:
        """Return SVG markup as string."""
        return self._svg

    def save(self, path: str) -> None:
        """Write SVG to a file.

        Args:
            path: File path to write. Parent directory must exist.

        Raises:
            FileNotFoundError: If the parent directory does not exist.
        """
        Path(path).write_text(self._svg, encoding="utf-8")


# ---------------------------------------------------------------------------
# SVG layout helpers
# ---------------------------------------------------------------------------


def _xml_escape(text: str) -> str:
    """Escape special characters for XML/SVG text content."""
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


def _estimate_node_width(name: str) -> float:
    """Estimate node width in pixels from the thread name length."""
    text_width = len(name) * _CHAR_WIDTH_ESTIMATE
    return max(text_width + _NODE_PADDING * 2, 100.0)


def _compute_layout(
    plan: ExecutionPlan,
    resolved_threads: dict[str, Any] | None = None,
) -> tuple[dict[str, tuple[float, float, float, float]], list[tuple[str, str]], float, float]:
    """Compute node positions and edges for the DAG layout.

    Uses the execution_order groups as Y layers and a barycenter heuristic
    for horizontal ordering within each layer.

    Args:
        plan: The execution plan to lay out.
        resolved_threads: Optional thread models (unused by layout, reserved).

    Returns:
        A tuple of (nodes, edges, total_width, total_height) where:
        - nodes maps thread_name to (x, y, width, height)
        - edges is a list of (source_thread, target_thread)
        - total_width and total_height are the canvas dimensions
    """
    nodes: dict[str, tuple[float, float, float, float]] = {}
    edges: list[tuple[str, str]] = []

    if not plan.execution_order:
        return nodes, edges, 0.0, 0.0

    # Pre-compute node widths
    widths: dict[str, float] = {name: _estimate_node_width(name) for name in plan.threads}

    # Determine lookup marker slots (group boundaries that have lookups)
    lookup_groups: set[int] = set()
    if plan.lookup_schedule:
        lookup_groups = set(plan.lookup_schedule.keys())

    # Barycenter ordering: for each layer, order nodes by average X of upstream neighbors
    # First layer uses alphabetical order (no upstream)
    node_x_center: dict[str, float] = {}

    y = float(_SVG_PADDING)
    max_row_width = 0.0

    for group_idx, group in enumerate(plan.execution_order):
        # Add space for lookup marker if this group boundary has one
        if group_idx in lookup_groups:
            y += _LOOKUP_MARKER_HEIGHT

        # Barycenter ordering
        if group_idx == 0:
            ordered = sorted(group)
        else:

            def _barycenter(name: str) -> float:
                ups = plan.dependencies.get(name, [])
                if not ups:
                    return 0.0
                return sum(node_x_center.get(u, 0.0) for u in ups) / len(ups)

            ordered = sorted(group, key=_barycenter)

        # Compute row width and center it
        row_width = sum(widths[n] for n in ordered) + _H_SPACING * max(len(ordered) - 1, 0)
        max_row_width = max(max_row_width, row_width)

        x = float(_SVG_PADDING)
        for name in ordered:
            w = widths[name]
            nodes[name] = (x, y, w, float(_NODE_HEIGHT))
            node_x_center[name] = x + w / 2
            x += w + _H_SPACING

        y += _NODE_HEIGHT + _V_SPACING

    # Center rows horizontally
    canvas_width = max_row_width + _SVG_PADDING * 2
    for name, (nx, ny, nw, nh) in list(nodes.items()):
        group_idx = _find_group_index(name, plan.execution_order)
        group = plan.execution_order[group_idx]
        ordered_in_group = [
            n for n in nodes if _find_group_index(n, plan.execution_order) == group_idx
        ]
        row_width = sum(nodes[n][2] for n in ordered_in_group) + _H_SPACING * max(
            len(ordered_in_group) - 1, 0
        )
        offset = (canvas_width - row_width) / 2 - _SVG_PADDING
        nodes[name] = (nx + offset, ny, nw, nh)
        node_x_center[name] = nx + offset + nw / 2

    # Build edges from dependencies
    for name, deps in plan.dependencies.items():
        for dep in deps:
            if dep in nodes and name in nodes:
                edges.append((dep, name))

    canvas_height = y - _V_SPACING + _NODE_HEIGHT + _SVG_PADDING
    return nodes, edges, canvas_width, canvas_height


def _find_group_index(name: str, execution_order: list[list[str]]) -> int:
    """Find which execution group a thread belongs to."""
    for idx, group in enumerate(execution_order):
        if name in group:
            return idx
    return 0


def _build_svg_style() -> str:
    """Build the CSS style block with prefers-color-scheme media query."""
    lt = _LIGHT
    dk = _DARK
    font = _FONT_FAMILY
    fs = _FONT_SIZE_LABEL
    fa = _FONT_SIZE_ANNOTATION
    ta = "text-anchor:middle;dominant-baseline:central"

    # Pre-build common CSS fragments to stay within line limits
    nf = f"fill:{lt['node_fill']};stroke:{lt['node_stroke']}"
    cf = f"fill:{lt['cached_fill']};stroke:{lt['cached_stroke']}"
    lbl = f"fill:{lt['node_text']};font-family:{font};font-size:{fs}px"
    clbl = f"fill:{lt['cached_text']};font-family:{font};font-size:{fs}px"
    lkl = f"fill:{lt['lookup_text']};font-family:{font};font-size:{fa}px"

    lines = [
        "<style>",
        f"  .dag-node{{{nf};stroke-width:1.5}}",
        f"  .dag-node-cached{{{cf};stroke-width:2}}",
        f"  .dag-label{{{lbl};{ta}}}",
        f"  .dag-label-cached{{{clbl};{ta}}}",
        f"  .dag-edge{{stroke:{lt['edge_stroke']};stroke-width:1.5;fill:none}}",
        f"  .dag-arrow{{fill:{lt['edge_stroke']}}}",
        f"  .dag-lookup-line{{stroke:{lt['lookup_stroke']};stroke-width:1;stroke-dasharray:6 3}}",
        f"  .dag-lookup-label{{{lkl};{ta}}}",
        f"  .dag-bg{{fill:{lt['bg']}}}",
        "  @media(prefers-color-scheme:dark){",
        f"    .dag-node{{fill:{dk['node_fill']};stroke:{dk['node_stroke']}}}",
        f"    .dag-node-cached{{fill:{dk['cached_fill']};stroke:{dk['cached_stroke']}}}",
        f"    .dag-label{{fill:{dk['node_text']}}}",
        f"    .dag-label-cached{{fill:{dk['cached_text']}}}",
        f"    .dag-edge{{stroke:{dk['edge_stroke']}}}",
        f"    .dag-arrow{{fill:{dk['edge_stroke']}}}",
        f"    .dag-lookup-line{{stroke:{dk['lookup_stroke']}}}",
        f"    .dag-lookup-label{{fill:{dk['lookup_text']}}}",
        f"    .dag-bg{{fill:{dk['bg']}}}",
        "  }",
        "</style>",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# SVG renderer
# ---------------------------------------------------------------------------


def render_dag_svg(
    plan: ExecutionPlan,
    resolved_threads: dict[str, Any] | None = None,
) -> str:
    """Generate inline SVG markup for an execution plan DAG.

    Args:
        plan: The execution plan to visualize.
        resolved_threads: Optional thread models for tooltip content.

    Returns:
        Complete SVG markup as a string.
    """
    nodes, edges, width, height = _compute_layout(plan, resolved_threads)

    if not nodes:
        return (
            '<svg xmlns="http://www.w3.org/2000/svg" '
            'viewBox="0 0 200 40" width="200" height="40">'
            f"{_build_svg_style()}"
            '<text x="100" y="20" class="dag-label">Empty plan</text>'
            "</svg>"
        )

    parts: list[str] = []
    vb = f"0 0 {width:.0f} {height:.0f}"
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vb}" '
        f'width="{width:.0f}" height="{height:.0f}">'
    )

    # Style block
    parts.append(_build_svg_style())

    # Arrowhead marker
    parts.append("<defs>")
    parts.append(
        f'<marker id="dag-arrowhead" markerWidth="{_ARROWHEAD_SIZE}" '
        f'markerHeight="{_ARROWHEAD_SIZE}" refX="{_ARROWHEAD_SIZE}" refY="{_ARROWHEAD_SIZE // 2}" '
        f'orient="auto-start-reverse">'
        f'<polygon points="0 0, {_ARROWHEAD_SIZE} {_ARROWHEAD_SIZE // 2}, 0 {_ARROWHEAD_SIZE}" '
        f'class="dag-arrow"/>'
        "</marker>"
    )
    parts.append("</defs>")

    # Background
    parts.append(f'<rect width="{width:.0f}" height="{height:.0f}" class="dag-bg"/>')

    # Lookup materialization markers
    if plan.lookup_schedule:
        for group_idx, lookup_names in sorted(plan.lookup_schedule.items()):
            if group_idx == 0:
                # Before first group — place marker just above first group
                marker_y = _SVG_PADDING - _LOOKUP_MARKER_HEIGHT / 2
            else:
                # Find Y of the group that this marker precedes
                # Marker goes between group (group_idx-1) and group_idx
                first_in_group = (
                    plan.execution_order[group_idx][0]
                    if group_idx < len(plan.execution_order)
                    else None
                )
                if first_in_group and first_in_group in nodes:
                    marker_y = nodes[first_in_group][1] - _LOOKUP_MARKER_HEIGHT / 2 - 10
                else:
                    continue

            label = ", ".join(lookup_names)
            parts.append(
                f'<line x1="{_SVG_PADDING}" y1="{marker_y:.0f}" '
                f'x2="{width - _SVG_PADDING}" y2="{marker_y:.0f}" class="dag-lookup-line"/>'
            )
            parts.append(
                f'<text x="{width / 2:.0f}" y="{marker_y - 6:.0f}" '
                f'class="dag-lookup-label">{_xml_escape(label)}</text>'
            )

    # Edges (draw before nodes so they appear behind)
    for src, tgt in edges:
        sx, sy, sw, sh = nodes[src]
        tx, ty, tw, _th = nodes[tgt]
        x1 = sx + sw / 2
        y1 = sy + sh
        x2 = tx + tw / 2
        y2 = ty
        parts.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
            f'class="dag-edge" marker-end="url(#dag-arrowhead)"/>'
        )

    # Nodes
    cache_set = set(plan.cache_targets)
    for name, (x, y, w, h) in nodes.items():
        is_cached = name in cache_set
        node_class = "dag-node-cached" if is_cached else "dag-node"
        label_class = "dag-label-cached" if is_cached else "dag-label"

        parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" '
            f'rx="{_NODE_CORNER_RADIUS}" class="{node_class}"/>'
        )
        parts.append(
            f'<text x="{x + w / 2:.1f}" y="{y + h / 2:.1f}" '
            f'class="{label_class}">{_xml_escape(name)}</text>'
        )

        # Tooltip with source/target info
        if resolved_threads and name in resolved_threads:
            thread = resolved_threads[name]
            tooltip_parts: list[str] = [name]
            sources = getattr(thread, "sources", {})
            if sources:
                first_src = next(iter(sources.values()))
                src_type = getattr(first_src, "type", None) or "lookup"
                src_path = getattr(first_src, "alias", None) or getattr(first_src, "path", "")
                tooltip_parts.append(f"Source: {src_type}:{src_path}")
            target = getattr(thread, "target", None)
            if target:
                tgt_path = getattr(target, "alias", None) or getattr(target, "path", "")
                tooltip_parts.append(f"Target: {tgt_path}")
            tooltip = "\n".join(tooltip_parts)
            # Insert title before the closing rect — wrap rect+text+title in a group
            parts.insert(
                len(parts) - 2,
                f"<title>{_xml_escape(tooltip)}</title>",
            )

    parts.append("</svg>")
    return "".join(parts)
