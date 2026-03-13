"""Plan display — DAG visualization and rich rendering for execution results."""

from __future__ import annotations

import html
from pathlib import Path
from typing import TYPE_CHECKING, Any

from weevr.engine.formatting import format_duration as _format_duration

if TYPE_CHECKING:
    from weevr.engine.planner import ExecutionPlan
    from weevr.model.thread import Thread

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
_LOOKUP_NODE_HEIGHT = 26
_LOOKUP_NODE_PADDING = 14
_LOOKUP_CORNER_RADIUS = 13  # pill shape

# Loom-level DAG swimlane constants
_SWIMLANE_PADDING = 16
_SWIMLANE_HEADER_HEIGHT = 28
_SWIMLANE_GAP = 40

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

# Thread flow diagram constants
_FLOW_NODE_HEIGHT = 36
_FLOW_H_GAP = 40  # horizontal gap between pipeline stages
_FLOW_V_GAP = 12  # vertical gap between stacked sources
_FLOW_ARROW_SIZE = 8

# Flow node type palettes
_FLOW_LIGHT = {
    "source_fill": "#f7fafc",
    "source_stroke": "#a0aec0",
    "join_fill": "#fefcbf",
    "join_stroke": "#d69e2e",
    "transform_fill": "#f0fff4",
    "transform_stroke": "#48bb78",
    "target_fill": "#ebf8ff",
    "target_stroke": "#3182ce",
    "text": "#2d3748",
    "edge": "#a0aec0",
    "bg": "#ffffff",
    "badge_bg": "#e2e8f0",
}
_FLOW_DARK = {
    "source_fill": "#2d3748",
    "source_stroke": "#718096",
    "join_fill": "#744210",
    "join_stroke": "#d69e2e",
    "transform_fill": "#22543d",
    "transform_stroke": "#48bb78",
    "target_fill": "#2a4365",
    "target_stroke": "#63b3ed",
    "text": "#e2e8f0",
    "edge": "#718096",
    "bg": "#1a202c",
    "badge_bg": "#4a5568",
}


class DAGDiagram:
    """Inline SVG diagram of a weave execution plan.

    Auto-renders in notebooks via the ``_repr_svg_()`` display protocol.
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
        """Notebook SVG display protocol."""
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


class FlowDiagram:
    """Inline SVG diagram of a thread's processing pipeline.

    Visualizes the flow from sources through transforms to the target.
    Auto-renders in notebooks via the ``_repr_svg_()`` display protocol.

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
        """Notebook SVG display protocol."""
        return self._svg

    def _repr_html_(self) -> str:
        """Notebook HTML display protocol — wraps SVG in a div."""
        return f"<div>{self._svg}</div>"

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


def _estimate_lookup_node_width(name: str) -> float:
    """Estimate lookup node width in pixels from the lookup name length."""
    text_width = len(name) * _CHAR_WIDTH_ESTIMATE
    return max(text_width + _LOOKUP_NODE_PADDING * 2, 80.0)


def _compute_layout(
    plan: ExecutionPlan,
    resolved_threads: dict[str, Any] | None = None,
) -> tuple[
    dict[str, tuple[float, float, float, float]],
    list[tuple[str, str]],
    float,
    float,
    dict[str, tuple[float, float, float, float]],
    list[tuple[str, str, bool]],
]:
    """Compute node positions and edges for the DAG layout.

    Uses the execution_order groups as Y layers and a barycenter heuristic
    for horizontal ordering within each layer.

    Args:
        plan: The execution plan to lay out.
        resolved_threads: Optional thread models (unused by layout, reserved).

    Returns:
        A tuple of (nodes, edges, width, height, lookup_nodes, lookup_edges):
        - nodes maps thread_name to (x, y, width, height)
        - edges is a list of (source_thread, target_thread)
        - total_width and total_height are the canvas dimensions
        - lookup_nodes maps lookup_name to (x, y, width, height)
        - lookup_edges is a list of (source_id, target_id, is_producer_edge)
          where source/target are thread or lookup names
    """
    nodes: dict[str, tuple[float, float, float, float]] = {}
    edges: list[tuple[str, str]] = []
    lookup_nodes: dict[str, tuple[float, float, float, float]] = {}
    lookup_edges: list[tuple[str, str, bool]] = []

    if not plan.execution_order:
        return nodes, edges, 0.0, 0.0, lookup_nodes, lookup_edges

    # Pre-compute node widths
    widths: dict[str, float] = {name: _estimate_node_width(name) for name in plan.threads}

    # Check if we have full lookup node data
    has_lookup_nodes = (
        plan.lookup_producers is not None
        and plan.lookup_consumers is not None
        and plan.lookup_schedule is not None
    )

    # Determine lookup marker slots (group boundaries that have lookups)
    lookup_groups: set[int] = set()
    if plan.lookup_schedule:
        lookup_groups = set(plan.lookup_schedule.keys())

    # Pre-compute lookup node widths
    lookup_widths: dict[str, float] = {}
    if has_lookup_nodes and plan.lookup_schedule:
        for lk_names in plan.lookup_schedule.values():
            for lk in lk_names:
                lookup_widths[lk] = _estimate_lookup_node_width(lk)

    # Barycenter ordering: for each layer, order nodes by average X of upstream neighbors
    # First layer uses alphabetical order (no upstream)
    node_x_center: dict[str, float] = {}

    y = float(_SVG_PADDING)
    max_row_width = 0.0

    for group_idx, group in enumerate(plan.execution_order):
        # Add space for lookup row if this group boundary has lookups
        if group_idx in lookup_groups:
            if has_lookup_nodes:
                y += _V_SPACING / 2
            else:
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

        # Place lookup nodes between groups if we have full data
        if has_lookup_nodes and group_idx in lookup_groups and plan.lookup_schedule:
            lk_names = plan.lookup_schedule[group_idx]
            lk_row_width = sum(lookup_widths[lk] for lk in lk_names) + _H_SPACING * max(
                len(lk_names) - 1, 0
            )
            max_row_width = max(max_row_width, lk_row_width)
            lk_x = float(_SVG_PADDING)
            for lk in lk_names:
                lk_w = lookup_widths[lk]
                lookup_nodes[lk] = (lk_x, y, lk_w, float(_LOOKUP_NODE_HEIGHT))
                node_x_center[f"__lk__{lk}"] = lk_x + lk_w / 2
                lk_x += lk_w + _H_SPACING
            y += _LOOKUP_NODE_HEIGHT + _V_SPACING / 2

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
        ordered_in_group = [
            n for n in nodes if _find_group_index(n, plan.execution_order) == group_idx
        ]
        row_width = sum(nodes[n][2] for n in ordered_in_group) + _H_SPACING * max(
            len(ordered_in_group) - 1, 0
        )
        offset = (canvas_width - row_width) / 2 - _SVG_PADDING
        nodes[name] = (nx + offset, ny, nw, nh)
        node_x_center[name] = nx + offset + nw / 2

    # Center lookup node rows
    for lk_name, (lx, ly, lw, lh) in list(lookup_nodes.items()):
        # Find all lookups at the same Y
        same_row = [ln for ln, (_, ry, _, _) in lookup_nodes.items() if ry == ly]
        row_width = sum(lookup_nodes[ln][2] for ln in same_row) + _H_SPACING * max(
            len(same_row) - 1, 0
        )
        offset = (canvas_width - row_width) / 2 - _SVG_PADDING
        lookup_nodes[lk_name] = (lx + offset, ly, lw, lh)
        node_x_center[f"__lk__{lk_name}"] = lx + offset + lw / 2

    # Build edges from dependencies
    for name, deps in plan.dependencies.items():
        for dep in deps:
            if dep in nodes and name in nodes:
                edges.append((dep, name))

    # Build lookup edges: producer → lookup, lookup → consumer
    if has_lookup_nodes and plan.lookup_producers and plan.lookup_consumers:
        for lk_name in lookup_nodes:
            producer = plan.lookup_producers.get(lk_name)
            if producer is not None and producer in nodes:
                lookup_edges.append((producer, lk_name, True))
            consumers = plan.lookup_consumers.get(lk_name, [])
            for consumer in consumers:
                if consumer in nodes:
                    lookup_edges.append((lk_name, consumer, False))

    canvas_height = y - _V_SPACING + _NODE_HEIGHT + _SVG_PADDING
    return nodes, edges, canvas_width, canvas_height, lookup_nodes, lookup_edges


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

    lkn = (
        f"fill:{lt['lookup_fill']};stroke:{lt['lookup_stroke']};"
        "stroke-width:1.5;stroke-dasharray:4 2"
    )

    lines = [
        "<style>",
        f"  .dag-node{{{nf};stroke-width:1.5}}",
        f"  .dag-node-cached{{{cf};stroke-width:2}}",
        f"  .dag-label{{{lbl};{ta}}}",
        f"  .dag-label-cached{{{clbl};{ta}}}",
        f"  .dag-edge{{stroke:{lt['edge_stroke']};stroke-width:1.5;fill:none}}",
        f"  .dag-arrow{{fill:{lt['edge_stroke']}}}",
        f"  .dag-lookup-node{{{lkn}}}",
        f"  .dag-lookup-node-label{{{lkl};{ta}}}",
        (
            f"  .dag-lookup-edge{{stroke:{lt['lookup_stroke']};stroke-width:1.5;"
            "fill:none;stroke-dasharray:6 3}"
        ),
        f"  .dag-lookup-line{{stroke:{lt['lookup_stroke']};stroke-width:1;stroke-dasharray:6 3}}",
        f"  .dag-lookup-label{{{lkl};{ta}}}",
        f"  .dag-swimlane{{fill:none;stroke:{lt['node_stroke']};stroke-width:1;rx:8}}",
        f"  .dag-swimlane-header{{{lbl};{ta};font-weight:600}}",
        f"  .dag-bg{{fill:{lt['bg']}}}",
        "  @media(prefers-color-scheme:dark){",
        f"    .dag-node{{fill:{dk['node_fill']};stroke:{dk['node_stroke']}}}",
        f"    .dag-node-cached{{fill:{dk['cached_fill']};stroke:{dk['cached_stroke']}}}",
        f"    .dag-label{{fill:{dk['node_text']}}}",
        f"    .dag-label-cached{{fill:{dk['cached_text']}}}",
        f"    .dag-edge{{stroke:{dk['edge_stroke']}}}",
        f"    .dag-arrow{{fill:{dk['edge_stroke']}}}",
        (f"    .dag-lookup-node{{fill:{dk['lookup_fill']};stroke:{dk['lookup_stroke']}}}"),
        f"    .dag-lookup-node-label{{fill:{dk['lookup_text']}}}",
        f"    .dag-lookup-edge{{stroke:{dk['lookup_stroke']}}}",
        f"    .dag-lookup-line{{stroke:{dk['lookup_stroke']}}}",
        f"    .dag-lookup-label{{fill:{dk['lookup_text']}}}",
        f"    .dag-swimlane{{stroke:{dk['node_stroke']}}}",
        f"    .dag-swimlane-header{{fill:{dk['node_text']}}}",
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
    nodes, edges, width, height, lookup_nodes, lookup_edges = _compute_layout(
        plan, resolved_threads
    )

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

    # Arrowhead markers
    lt = _LIGHT
    parts.append("<defs>")
    parts.append(
        f'<marker id="dag-arrowhead" markerWidth="{_ARROWHEAD_SIZE}" '
        f'markerHeight="{_ARROWHEAD_SIZE}" refX="{_ARROWHEAD_SIZE}" refY="{_ARROWHEAD_SIZE // 2}" '
        f'orient="auto-start-reverse">'
        f'<polygon points="0 0, {_ARROWHEAD_SIZE} {_ARROWHEAD_SIZE // 2}, 0 {_ARROWHEAD_SIZE}" '
        f'class="dag-arrow"/>'
        "</marker>"
    )
    parts.append(
        f'<marker id="dag-arrowhead-lookup" markerWidth="{_ARROWHEAD_SIZE}" '
        f'markerHeight="{_ARROWHEAD_SIZE}" refX="{_ARROWHEAD_SIZE}" refY="{_ARROWHEAD_SIZE // 2}" '
        f'orient="auto-start-reverse">'
        f'<polygon points="0 0, {_ARROWHEAD_SIZE} {_ARROWHEAD_SIZE // 2}, 0 {_ARROWHEAD_SIZE}" '
        f'fill="{lt["lookup_stroke"]}"/>'
        "</marker>"
    )
    parts.append("</defs>")

    # Background
    parts.append(f'<rect width="{width:.0f}" height="{height:.0f}" class="dag-bg"/>')

    # Lookup rendering — use nodes if available, fall back to dashed lines
    if lookup_nodes:
        # Draw lookup edges (dashed, orange)
        for src, tgt, is_producer in lookup_edges:
            if is_producer:
                # producer edge: thread → lookup node
                if src not in nodes or tgt not in lookup_nodes:
                    continue
                sx, sy, sw, sh = nodes[src]
                tx, ty, tw, _th = lookup_nodes[tgt]
            else:
                # consumer edge: lookup node → thread
                if src not in lookup_nodes or tgt not in nodes:
                    continue
                sx, sy, sw, sh = lookup_nodes[src]
                tx, ty, tw, _th = nodes[tgt]
            x1 = sx + sw / 2
            y1 = sy + sh
            x2 = tx + tw / 2
            y2 = ty
            parts.append(
                f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
                f'class="dag-lookup-edge" marker-end="url(#dag-arrowhead-lookup)"/>'
            )

        # Draw lookup node rects (pill shape)
        for lk_name, (lx, ly, lw, lh) in lookup_nodes.items():
            parts.append(
                f'<rect x="{lx:.1f}" y="{ly:.1f}" width="{lw:.1f}" height="{lh:.1f}" '
                f'rx="{_LOOKUP_CORNER_RADIUS}" class="dag-lookup-node"/>'
            )
            parts.append(
                f'<text x="{lx + lw / 2:.1f}" y="{ly + lh / 2:.1f}" '
                f'class="dag-lookup-node-label">{_xml_escape(lk_name)}</text>'
            )
    elif plan.lookup_schedule:
        # Fallback: dashed-line rendering for plans without producer/consumer data
        for group_idx, lookup_names in sorted(plan.lookup_schedule.items()):
            if group_idx == 0:
                marker_y = _SVG_PADDING - _LOOKUP_MARKER_HEIGHT / 2
            else:
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


def render_loom_dag_svg(
    plans: list[ExecutionPlan],
    resolved_threads: dict[str, Any] | None = None,
) -> str:
    """Generate a loom-level DAG SVG with swimlanes for each weave.

    For a single plan, delegates to :func:`render_dag_svg`. For multiple
    plans, stacks each weave's DAG inside a labelled swimlane container
    with sequential arrows between them.

    Args:
        plans: Execution plans for each weave in execution order.
        resolved_threads: Optional thread models for tooltip content.

    Returns:
        Complete SVG markup as a string.
    """
    if len(plans) <= 1:
        return render_dag_svg(plans[0], resolved_threads) if plans else ""

    # Compute internal layouts for each weave
    weave_layouts: list[
        tuple[
            str,
            dict[str, tuple[float, float, float, float]],
            list[tuple[str, str]],
            float,
            float,
            dict[str, tuple[float, float, float, float]],
            list[tuple[str, str, bool]],
        ]
    ] = []
    for plan in plans:
        result = _compute_layout(plan, resolved_threads)
        weave_layouts.append((plan.weave_name, *result))

    # Calculate container dimensions
    max_inner_width = max(w for _, _, _, w, _, _, _ in weave_layouts)
    container_width = max_inner_width + _SWIMLANE_PADDING * 2
    canvas_width = container_width + _SVG_PADDING * 2

    # Build SVG
    parts: list[str] = []

    # First pass: compute total height
    total_height = float(_SVG_PADDING)
    container_tops: list[float] = []
    container_heights: list[float] = []
    for _, nodes, _, _w, h, _lk_nodes, _ in weave_layouts:
        container_tops.append(total_height)
        inner_h = max(h, _NODE_HEIGHT + _SVG_PADDING * 2) if nodes else _NODE_HEIGHT * 2
        ch = _SWIMLANE_HEADER_HEIGHT + inner_h + _SWIMLANE_PADDING
        container_heights.append(ch)
        total_height += ch + _SWIMLANE_GAP
    total_height = total_height - _SWIMLANE_GAP + _SVG_PADDING

    vb = f"0 0 {canvas_width:.0f} {total_height:.0f}"
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vb}" '
        f'width="{canvas_width:.0f}" height="{total_height:.0f}">'
    )
    parts.append(_build_svg_style())

    # Arrowhead markers
    lt = _LIGHT
    parts.append("<defs>")
    parts.append(
        f'<marker id="dag-arrowhead" markerWidth="{_ARROWHEAD_SIZE}" '
        f'markerHeight="{_ARROWHEAD_SIZE}" refX="{_ARROWHEAD_SIZE}" refY="{_ARROWHEAD_SIZE // 2}" '
        f'orient="auto-start-reverse">'
        f'<polygon points="0 0, {_ARROWHEAD_SIZE} {_ARROWHEAD_SIZE // 2}, 0 {_ARROWHEAD_SIZE}" '
        f'class="dag-arrow"/>'
        "</marker>"
    )
    parts.append(
        f'<marker id="dag-arrowhead-lookup" markerWidth="{_ARROWHEAD_SIZE}" '
        f'markerHeight="{_ARROWHEAD_SIZE}" refX="{_ARROWHEAD_SIZE}" refY="{_ARROWHEAD_SIZE // 2}" '
        f'orient="auto-start-reverse">'
        f'<polygon points="0 0, {_ARROWHEAD_SIZE} {_ARROWHEAD_SIZE // 2}, 0 {_ARROWHEAD_SIZE}" '
        f'fill="{lt["lookup_stroke"]}"/>'
        "</marker>"
    )
    parts.append("</defs>")

    # Background
    parts.append(f'<rect width="{canvas_width:.0f}" height="{total_height:.0f}" class="dag-bg"/>')

    # Draw each weave container
    container_x = float(_SVG_PADDING)
    for idx, (wname, nodes, edges, _w, _h, lk_nodes, lk_edges) in enumerate(weave_layouts):
        cy = container_tops[idx]
        ch = container_heights[idx]

        # Swimlane container rect
        parts.append(
            f'<rect x="{container_x:.1f}" y="{cy:.1f}" '
            f'width="{container_width:.1f}" height="{ch:.1f}" class="dag-swimlane"/>'
        )

        # Header label
        header_y = cy + _SWIMLANE_HEADER_HEIGHT / 2
        parts.append(
            f'<text x="{container_x + container_width / 2:.1f}" y="{header_y:.1f}" '
            f'class="dag-swimlane-header">{_xml_escape("Weave: " + wname)}</text>'
        )

        # Translate internal content
        content_y = cy + _SWIMLANE_HEADER_HEIGHT
        offset_x = container_x + _SWIMLANE_PADDING
        offset_y = content_y

        # Lookup edges
        for src, tgt, is_producer in lk_edges:
            if is_producer:
                if src not in nodes or tgt not in lk_nodes:
                    continue
                sx, sy, sw, sh = nodes[src]
                tx, ty, tw, _th = lk_nodes[tgt]
            else:
                if src not in lk_nodes or tgt not in nodes:
                    continue
                sx, sy, sw, sh = lk_nodes[src]
                tx, ty, tw, _th = nodes[tgt]
            x1 = offset_x + sx + sw / 2
            y1 = offset_y + sy + sh
            x2 = offset_x + tx + tw / 2
            y2 = offset_y + ty
            parts.append(
                f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
                f'class="dag-lookup-edge" marker-end="url(#dag-arrowhead-lookup)"/>'
            )

        # Lookup nodes
        for lk_name, (lx, ly, lw, lh) in lk_nodes.items():
            ax = offset_x + lx
            ay = offset_y + ly
            parts.append(
                f'<rect x="{ax:.1f}" y="{ay:.1f}" width="{lw:.1f}" height="{lh:.1f}" '
                f'rx="{_LOOKUP_CORNER_RADIUS}" class="dag-lookup-node"/>'
            )
            parts.append(
                f'<text x="{ax + lw / 2:.1f}" y="{ay + lh / 2:.1f}" '
                f'class="dag-lookup-node-label">{_xml_escape(lk_name)}</text>'
            )

        # Thread edges
        for src, tgt in edges:
            sx, sy, sw, sh = nodes[src]
            tx, ty, tw, _th = nodes[tgt]
            x1 = offset_x + sx + sw / 2
            y1 = offset_y + sy + sh
            x2 = offset_x + tx + tw / 2
            y2 = offset_y + ty
            parts.append(
                f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
                f'class="dag-edge" marker-end="url(#dag-arrowhead)"/>'
            )

        # Thread nodes
        plan = plans[idx]
        cache_set = set(plan.cache_targets)
        for name, (nx, ny, nw, nh) in nodes.items():
            ax = offset_x + nx
            ay = offset_y + ny
            is_cached = name in cache_set
            node_class = "dag-node-cached" if is_cached else "dag-node"
            label_class = "dag-label-cached" if is_cached else "dag-label"
            parts.append(
                f'<rect x="{ax:.1f}" y="{ay:.1f}" width="{nw:.1f}" height="{nh:.1f}" '
                f'rx="{_NODE_CORNER_RADIUS}" class="{node_class}"/>'
            )
            parts.append(
                f'<text x="{ax + nw / 2:.1f}" y="{ay + nh / 2:.1f}" '
                f'class="{label_class}">{_xml_escape(name)}</text>'
            )

    # Sequential arrows between containers
    mid_x = container_x + container_width / 2
    for idx in range(len(weave_layouts) - 1):
        y1 = container_tops[idx] + container_heights[idx]
        y2 = container_tops[idx + 1]
        parts.append(
            f'<line x1="{mid_x:.1f}" y1="{y1:.1f}" x2="{mid_x:.1f}" y2="{y2:.1f}" '
            f'class="dag-edge" marker-end="url(#dag-arrowhead)"/>'
        )

    parts.append("</svg>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Thread flow SVG renderer
# ---------------------------------------------------------------------------

# Step types recognized as the key attribute on discriminated-union step models.
_STEP_TYPES = frozenset(
    {
        "filter",
        "derive",
        "join",
        "select",
        "drop",
        "rename",
        "cast",
        "dedup",
        "sort",
        "union",
        "aggregate",
        "window",
        "pivot",
        "unpivot",
        "case_when",
        "fill_null",
        "coalesce",
        "string_ops",
        "date_ops",
    }
)


def _get_step_type(step: Any) -> str:
    """Determine the step type key from a discriminated-union step model."""
    for attr in _STEP_TYPES:
        if hasattr(step, attr) and getattr(step, attr) is not None:
            return attr
    return "unknown"


def _get_join_detail(step: Any) -> str:
    """Extract a short label for a join step (type + key columns)."""
    params = getattr(step, "join", None)
    if params is None:
        return "join"
    jtype = getattr(params, "type", "inner")
    keys = getattr(params, "on", [])
    key_names = [getattr(k, "left", str(k)) for k in keys[:3]]
    suffix = ", \u2026" if len(keys) > 3 else ""
    return f"{jtype}({', '.join(key_names)}{suffix})"


def _group_transforms(
    steps: list[Any],
) -> list[tuple[str, str, int]]:
    """Walk pipeline steps and group consecutive same-type transforms.

    Returns a list of ``(node_type, label, count)`` tuples.
    ``node_type`` is ``"join"`` or ``"transform"``.
    """
    nodes: list[tuple[str, str, int]] = []
    for step in steps:
        stype = _get_step_type(step)
        if stype == "join":
            nodes.append(("join", _get_join_detail(step), 1))
        elif nodes and nodes[-1][0] == "transform" and nodes[-1][1] == stype:
            # Merge with previous same-type transform
            prev_type, prev_label, prev_count = nodes[-1]
            nodes[-1] = (prev_type, prev_label, prev_count + 1)
        else:
            nodes.append(("transform", stype, 1))
    return nodes


def _build_flow_svg_style() -> str:
    """Build CSS style block for thread flow diagrams."""
    lt = _FLOW_LIGHT
    dk = _FLOW_DARK
    font = _FONT_FAMILY
    fs = _FONT_SIZE_LABEL
    fa = _FONT_SIZE_ANNOTATION
    ta = "text-anchor:middle;dominant-baseline:central"

    lines = [
        "<style>",
        f"  .flow-bg{{fill:{lt['bg']}}}",
        f"  .flow-source{{fill:{lt['source_fill']};stroke:{lt['source_stroke']};stroke-width:1.5}}",
        f"  .flow-join{{fill:{lt['join_fill']};stroke:{lt['join_stroke']};stroke-width:1.5}}",
        (
            f"  .flow-transform{{fill:{lt['transform_fill']};"
            f"stroke:{lt['transform_stroke']};stroke-width:1.5}}"
        ),
        f"  .flow-target{{fill:{lt['target_fill']};stroke:{lt['target_stroke']};stroke-width:2}}",
        f"  .flow-label{{fill:{lt['text']};font-family:{font};font-size:{fs}px;{ta}}}",
        f"  .flow-sublabel{{fill:{lt['text']};font-family:{font};font-size:{fa}px;{ta};"
        "opacity:0.7}}",
        f"  .flow-badge{{fill:{lt['badge_bg']};rx:8}}",
        f"  .flow-edge{{stroke:{lt['edge']};stroke-width:1.5;fill:none}}",
        f"  .flow-arrow{{fill:{lt['edge']}}}",
        "  @media(prefers-color-scheme:dark){",
        f"    .flow-bg{{fill:{dk['bg']}}}",
        f"    .flow-source{{fill:{dk['source_fill']};stroke:{dk['source_stroke']}}}",
        f"    .flow-join{{fill:{dk['join_fill']};stroke:{dk['join_stroke']}}}",
        f"    .flow-transform{{fill:{dk['transform_fill']};stroke:{dk['transform_stroke']}}}",
        f"    .flow-target{{fill:{dk['target_fill']};stroke:{dk['target_stroke']}}}",
        f"    .flow-label{{fill:{dk['text']}}}",
        f"    .flow-sublabel{{fill:{dk['text']}}}",
        f"    .flow-badge{{fill:{dk['badge_bg']}}}",
        f"    .flow-edge{{stroke:{dk['edge']}}}",
        f"    .flow-arrow{{fill:{dk['edge']}}}",
        "  }",
        "</style>",
    ]
    return "\n".join(lines)


def render_flow_svg(thread: Thread) -> str:
    """Generate an inline SVG showing a thread's processing pipeline.

    Renders a horizontal flow diagram: sources on the left, transforms in
    the middle, target on the right. Consecutive same-type transforms are
    grouped with a count badge.

    Args:
        thread: Thread configuration model.

    Returns:
        Complete SVG markup as a string.
    """
    pad = _SVG_PADDING
    nh = _FLOW_NODE_HEIGHT
    hgap = _FLOW_H_GAP
    vgap = _FLOW_V_GAP
    cw = _CHAR_WIDTH_ESTIMATE
    np = _NODE_PADDING

    # --- Build node list ---
    # Source nodes
    source_nodes: list[tuple[str, str]] = []  # (alias, type_label)
    for alias, src in thread.sources.items():
        src_type = getattr(src, "type", None) or "lookup"
        src_ref = getattr(src, "alias", None) or getattr(src, "path", "") or ""
        # Shorten long paths
        if len(src_ref) > 25:
            src_ref = "\u2026" + src_ref[-22:]
        label = f"{alias}" if alias == src_ref or not src_ref else alias
        source_nodes.append((label, src_type))

    # Pipeline nodes (joins + transforms, grouped)
    pipeline_nodes = _group_transforms(list(thread.steps))

    # Target node
    target = thread.target
    tgt_label = getattr(target, "alias", None) or getattr(target, "path", "") or "target"
    if len(tgt_label) > 30:
        tgt_label = "\u2026" + tgt_label[-27:]
    write_mode = thread.write.mode if thread.write else "overwrite"

    # --- Estimate widths ---
    def _est_w(text: str) -> float:
        return max(len(text) * cw + np * 2, 80.0)

    source_widths = [_est_w(label) for label, _ in source_nodes]
    max_source_w = max(source_widths) if source_widths else 80.0

    pipe_widths: list[float] = []
    for ntype, label, count in pipeline_nodes:
        if ntype == "join":
            pipe_widths.append(_est_w(label))
        elif count > 1:
            pipe_widths.append(_est_w(f"{label} ({count})"))
        else:
            pipe_widths.append(_est_w(label))

    target_w = _est_w(tgt_label)

    # --- Compute positions ---
    # Sources: stacked vertically at x=pad
    src_total_h = len(source_nodes) * nh + max(len(source_nodes) - 1, 0) * vgap
    src_positions: list[tuple[float, float, float]] = []  # (x, y, w)
    sy = pad
    for _label, _ in source_nodes:
        src_positions.append((pad, sy, max_source_w))
        sy += nh + vgap

    # Pipeline center Y (vertically centered with source stack)
    center_y = pad + src_total_h / 2 - nh / 2
    if not source_nodes:
        center_y = pad

    # Pipeline nodes: left to right after sources
    px = pad + max_source_w + hgap
    pipe_positions: list[tuple[float, float, float]] = []  # (x, y, w)
    for idx, (_ntype, _label, _count) in enumerate(pipeline_nodes):
        w = pipe_widths[idx]
        pipe_positions.append((px, center_y, w))
        px += w + hgap

    # Target: after pipeline
    if not pipeline_nodes:
        px = pad + max_source_w + hgap
    target_pos = (px, center_y, target_w)

    # Canvas dimensions
    canvas_w = px + target_w + pad
    canvas_h = max(src_total_h, nh) + pad * 2

    # --- Render SVG ---
    parts: list[str] = []
    vb = f"0 0 {canvas_w:.0f} {canvas_h:.0f}"
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vb}" '
        f'width="{canvas_w:.0f}" height="{canvas_h:.0f}">'
    )
    parts.append(_build_flow_svg_style())

    # Arrowhead marker
    asz = _FLOW_ARROW_SIZE
    parts.append("<defs>")
    parts.append(
        f'<marker id="flow-arrow" markerWidth="{asz}" '
        f'markerHeight="{asz}" refX="{asz}" refY="{asz // 2}" '
        f'orient="auto-start-reverse">'
        f'<polygon points="0 0, {asz} {asz // 2}, 0 {asz}" class="flow-arrow"/>'
        "</marker>"
    )
    parts.append("</defs>")

    # Background
    parts.append(f'<rect width="{canvas_w:.0f}" height="{canvas_h:.0f}" class="flow-bg"/>')

    # --- Edges (draw first, behind nodes) ---
    # Source → first pipeline node (or target if no pipeline)
    first_target_x = pipe_positions[0][0] if pipe_positions else target_pos[0]
    first_target_y = pipe_positions[0][1] if pipe_positions else target_pos[1]
    for sx, sy, sw in src_positions:
        x1 = sx + sw
        y1 = sy + nh / 2
        x2 = first_target_x
        y2 = first_target_y + nh / 2
        parts.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
            f'class="flow-edge" marker-end="url(#flow-arrow)"/>'
        )

    # Pipeline node → next pipeline node
    for i in range(len(pipe_positions) - 1):
        x1 = pipe_positions[i][0] + pipe_positions[i][2]
        y1 = pipe_positions[i][1] + nh / 2
        x2 = pipe_positions[i + 1][0]
        y2 = pipe_positions[i + 1][1] + nh / 2
        parts.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
            f'class="flow-edge" marker-end="url(#flow-arrow)"/>'
        )

    # Last pipeline node → target
    if pipe_positions:
        last = pipe_positions[-1]
        x1 = last[0] + last[2]
        y1 = last[1] + nh / 2
        x2 = target_pos[0]
        y2 = target_pos[1] + nh / 2
        parts.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
            f'class="flow-edge" marker-end="url(#flow-arrow)"/>'
        )

    # --- Source nodes ---
    cr = _NODE_CORNER_RADIUS
    for (label, stype), (sx, sy, sw) in zip(source_nodes, src_positions, strict=True):
        parts.append(
            f'<rect x="{sx:.1f}" y="{sy:.1f}" width="{sw:.1f}" height="{nh}" '
            f'rx="{cr}" class="flow-source"/>'
        )
        parts.append(
            f'<text x="{sx + sw / 2:.1f}" y="{sy + nh / 2 - 5:.1f}" '
            f'class="flow-label">{_xml_escape(label)}</text>'
        )
        parts.append(
            f'<text x="{sx + sw / 2:.1f}" y="{sy + nh / 2 + 8:.1f}" '
            f'class="flow-sublabel">{_xml_escape(stype)}</text>'
        )

    # --- Pipeline nodes ---
    for (ntype, label, count), (px, py, pw) in zip(pipeline_nodes, pipe_positions, strict=True):
        css_class = "flow-join" if ntype == "join" else "flow-transform"
        display_label = label if count <= 1 else f"{label} ({count})"

        if ntype == "join":
            # Hexagon shape for joins
            hh = nh / 2
            indent = 12
            points = (
                f"{px + indent:.1f},{py:.1f} "
                f"{px + pw - indent:.1f},{py:.1f} "
                f"{px + pw:.1f},{py + hh:.1f} "
                f"{px + pw - indent:.1f},{py + nh:.1f} "
                f"{px + indent:.1f},{py + nh:.1f} "
                f"{px:.1f},{py + hh:.1f}"
            )
            parts.append(f'<polygon points="{points}" class="{css_class}"/>')
        else:
            parts.append(
                f'<rect x="{px:.1f}" y="{py:.1f}" width="{pw:.1f}" height="{nh}" '
                f'rx="{cr}" class="{css_class}"/>'
            )

        parts.append(
            f'<text x="{px + pw / 2:.1f}" y="{py + nh / 2:.1f}" '
            f'class="flow-label">{_xml_escape(display_label)}</text>'
        )

    # --- Target node (double border via nested rects) ---
    tx, ty, tw = target_pos
    parts.append(
        f'<rect x="{tx:.1f}" y="{ty:.1f}" width="{tw:.1f}" height="{nh}" '
        f'rx="{cr}" class="flow-target"/>'
    )
    parts.append(
        f'<rect x="{tx + 3:.1f}" y="{ty + 3:.1f}" width="{tw - 6:.1f}" height="{nh - 6}" '
        f'rx="{max(cr - 2, 0)}" class="flow-target" style="fill:none"/>'
    )
    parts.append(
        f'<text x="{tx + tw / 2:.1f}" y="{ty + nh / 2 - 5:.1f}" '
        f'class="flow-label">{_xml_escape(tgt_label)}</text>'
    )
    parts.append(
        f'<text x="{tx + tw / 2:.1f}" y="{ty + nh / 2 + 8:.1f}" '
        f'class="flow-sublabel">{_xml_escape(write_mode)}</text>'
    )

    parts.append("</svg>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Execution timeline SVG renderer
# ---------------------------------------------------------------------------


def render_timeline_svg(
    weave_result: Any,
    hook_results: list[Any] | None = None,
    lookup_results: list[Any] | None = None,
) -> str:
    """Generate a phased execution timeline Gantt SVG.

    Shows lookups, pre-hooks, thread execution groups, and post-hooks as
    sequential phases with wall-clock duration bars.

    Args:
        weave_result: A WeaveResult with thread_results and telemetry.
        hook_results: Hook execution results (from WeaveTelemetry).
        lookup_results: Lookup materialization results (from WeaveTelemetry).

    Returns:
        Complete SVG markup as a string.
    """
    lt = _FLOW_LIGHT
    dk = _FLOW_DARK
    font = _FONT_FAMILY
    pad = _SVG_PADDING
    bar_h = 24
    phase_gap = 16
    label_w = 120
    bar_area_w = 400

    # Collect phases with bars: [(phase_label, [(bar_label, duration_ms, status)])]
    phases: list[tuple[str, list[tuple[str, int, str]]]] = []

    # Lookup phase
    if lookup_results:
        lk_bars: list[tuple[str, int, str]] = []
        for lk in lookup_results:
            name = getattr(lk, "name", "lookup")
            dur = getattr(lk, "duration_ms", 0)
            status = "success" if getattr(lk, "success", True) else "failure"
            lk_bars.append((name, dur, status))
        if lk_bars:
            phases.append(("Lookups", lk_bars))

    # Pre/post-hooks phases
    post_bars: list[tuple[str, int, str]] = []
    if hook_results:
        pre_bars: list[tuple[str, int, str]] = []
        for hr in hook_results:
            phase = getattr(hr, "phase", "")
            name = getattr(hr, "step_type", "hook")
            dur = getattr(hr, "duration_ms", 0)
            status = "success" if getattr(hr, "success", True) else "failure"
            if phase == "pre":
                pre_bars.append((name, dur, status))
            elif phase == "post":
                post_bars.append((name, dur, status))
        if pre_bars:
            phases.append(("Pre-hooks", pre_bars))

    # Thread execution phase
    thread_results = getattr(weave_result, "thread_results", []) or []
    if thread_results:
        thread_bars: list[tuple[str, int, str]] = []
        for tr in thread_results:
            name = getattr(tr, "thread_name", "thread")
            telem = getattr(tr, "telemetry", None)
            dur = 0
            if telem:
                span = getattr(telem, "span", None)
                if span:
                    start = getattr(span, "start_time", None)
                    end = getattr(span, "end_time", None)
                    if start and end:
                        dur = int((end - start).total_seconds() * 1000)
            status = str(getattr(tr, "status", "success"))
            thread_bars.append((name, dur, status))
        if thread_bars:
            phases.append(("Threads", thread_bars))

    # Post-hooks phase
    if hook_results and post_bars:
        phases.append(("Post-hooks", post_bars))

    if not phases:
        return (
            '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 40" '
            'width="200" height="40">'
            f'<text x="100" y="20" text-anchor="middle" '
            f'font-family="{font}" font-size="13" fill="{lt["text"]}">'
            "No timeline data</text></svg>"
        )

    # Find max duration for scaling
    max_dur = max(dur for _, bars in phases for _, dur, _ in bars) or 1

    # Compute canvas height
    total_bars = sum(len(bars) for _, bars in phases)
    canvas_h = pad * 2 + total_bars * (bar_h + 4) + (len(phases) - 1) * phase_gap
    canvas_w = pad * 2 + label_w + bar_area_w + 60  # 60 for duration text

    status_colors = {
        "success": ("#c6f6d5", "#276749", "#22543d", "#48bb78"),
        "failure": ("#feb2b2", "#9b2c2c", "#742a2a", "#fc8181"),
        "skipped": ("#e2e8f0", "#718096", "#4a5568", "#a0aec0"),
    }

    parts: list[str] = []
    vb = f"0 0 {canvas_w:.0f} {canvas_h:.0f}"
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vb}" '
        f'width="{canvas_w:.0f}" height="{canvas_h:.0f}">'
    )

    # Inline style for dark/light
    parts.append("<style>")
    parts.append(f"  .tl-bg{{fill:{lt['bg']}}}")
    parts.append(f"  .tl-label{{fill:{lt['text']};font-family:{font};font-size:12px}}")
    parts.append(
        f"  .tl-phase{{fill:{lt['text']};font-family:{font};font-size:13px;font-weight:600}}}}"
    )
    parts.append(f"  .tl-dur{{fill:{lt['text']};font-family:{font};font-size:11px;opacity:0.7}}")
    parts.append("  @media(prefers-color-scheme:dark){")
    parts.append(f"    .tl-bg{{fill:{dk['bg']}}}")
    parts.append(f"    .tl-label{{fill:{dk['text']}}}")
    parts.append(f"    .tl-phase{{fill:{dk['text']}}}")
    parts.append(f"    .tl-dur{{fill:{dk['text']}}}")
    parts.append("  }")
    parts.append("</style>")

    # Background
    parts.append(f'<rect width="{canvas_w:.0f}" height="{canvas_h:.0f}" class="tl-bg"/>')

    y = float(pad)
    for phase_label, bars in phases:
        # Phase label
        parts.append(
            f'<text x="{pad}" y="{y + bar_h / 2:.1f}" '
            f'dominant-baseline="central" class="tl-phase">'
            f"{_xml_escape(phase_label)}</text>"
        )
        y += bar_h + 2

        for bar_label, dur, status in bars:
            colors = status_colors.get(status, status_colors["success"])
            fill_lt = colors[0]
            bar_w = max((dur / max_dur) * bar_area_w, 4)

            bx = pad + label_w
            parts.append(
                f'<rect x="{bx:.1f}" y="{y:.1f}" width="{bar_w:.1f}" '
                f'height="{bar_h}" rx="4" fill="{fill_lt}"/>'
            )
            parts.append(
                f'<text x="{bx + 6:.1f}" y="{y + bar_h / 2:.1f}" '
                f'dominant-baseline="central" class="tl-label">'
                f"{_xml_escape(bar_label)}</text>"
            )
            # Duration text after bar
            dur_text = _format_duration(dur)
            parts.append(
                f'<text x="{bx + bar_w + 6:.1f}" y="{y + bar_h / 2:.1f}" '
                f'dominant-baseline="central" class="tl-dur">'
                f"{_xml_escape(dur_text)}</text>"
            )
            y += bar_h + 4

        y += phase_gap

    parts.append("</svg>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Data flow waterfall SVG renderer
# ---------------------------------------------------------------------------


def render_waterfall_svg(
    thread_result: Any,
    thread_telemetry: Any | None,
    mode: str = "execute",
) -> str:
    """Generate a data flow waterfall SVG showing row counts at each stage.

    Execute mode shows 4 stages: read, transforms, quarantine, written.
    Preview mode shows 2 stages: read, transforms.

    Args:
        thread_result: A ThreadResult (used for rows_written).
        thread_telemetry: ThreadTelemetry with row counts.
        mode: ``"execute"`` or ``"preview"``.

    Returns:
        Complete SVG markup as a string.
    """
    lt = _FLOW_LIGHT
    dk = _FLOW_DARK
    font = _FONT_FAMILY
    pad = _SVG_PADDING
    bar_h = 28
    bar_gap = 8
    label_w = 140
    bar_area_w = 300

    # Extract row counts
    rows_read = 0
    rows_after = 0
    rows_quarantined = 0
    rows_written = getattr(thread_result, "rows_written", 0)
    if thread_telemetry:
        rows_read = getattr(thread_telemetry, "rows_read", 0)
        rows_after = getattr(thread_telemetry, "rows_after_transforms", 0)
        rows_quarantined = getattr(thread_telemetry, "rows_quarantined", 0)

    # Build stages
    stages: list[tuple[str, int, str]] = []  # (label, count, color)
    stages.append(("Rows read", rows_read, "#bee3f8"))
    stages.append(("After transforms", rows_after, "#c6f6d5"))
    if mode == "execute":
        if rows_quarantined > 0:
            stages.append(("Quarantined", rows_quarantined, "#fefcbf"))
        stages.append(("Rows written", rows_written, "#bee3f8"))

    max_count = max((c for _, c, _ in stages), default=1) or 1
    canvas_h = pad * 2 + len(stages) * (bar_h + bar_gap)
    canvas_w = pad * 2 + label_w + bar_area_w + 80

    parts: list[str] = []
    vb = f"0 0 {canvas_w:.0f} {canvas_h:.0f}"
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vb}" '
        f'width="{canvas_w:.0f}" height="{canvas_h:.0f}">'
    )
    parts.append("<style>")
    parts.append(f"  .wf-bg{{fill:{lt['bg']}}}")
    parts.append(f"  .wf-label{{fill:{lt['text']};font-family:{font};font-size:13px}}")
    parts.append(f"  .wf-count{{fill:{lt['text']};font-family:{font};font-size:12px;opacity:0.7}}")
    parts.append("  @media(prefers-color-scheme:dark){")
    parts.append(f"    .wf-bg{{fill:{dk['bg']}}}")
    parts.append(f"    .wf-label{{fill:{dk['text']}}}")
    parts.append(f"    .wf-count{{fill:{dk['text']}}}")
    parts.append("  }")
    parts.append("</style>")

    parts.append(f'<rect width="{canvas_w:.0f}" height="{canvas_h:.0f}" class="wf-bg"/>')

    y = float(pad)
    for stage_label, count, color in stages:
        bar_w = max((count / max_count) * bar_area_w, 4) if count > 0 else 4
        bx = pad + label_w

        # Label
        parts.append(
            f'<text x="{bx - 8:.1f}" y="{y + bar_h / 2:.1f}" '
            f'text-anchor="end" dominant-baseline="central" class="wf-label">'
            f"{_xml_escape(stage_label)}</text>"
        )
        # Bar
        parts.append(
            f'<rect x="{bx:.1f}" y="{y:.1f}" width="{bar_w:.1f}" '
            f'height="{bar_h}" rx="4" fill="{color}" opacity="0.8"/>'
        )
        # Count text
        parts.append(
            f'<text x="{bx + bar_w + 6:.1f}" y="{y + bar_h / 2:.1f}" '
            f'dominant-baseline="central" class="wf-count">{count:,}</text>'
        )
        y += bar_h + bar_gap

    parts.append("</svg>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Annotated DAG SVG renderer
# ---------------------------------------------------------------------------


def render_annotated_dag_svg(
    plan: ExecutionPlan,
    thread_results: dict[str, Any] | None = None,
    resolved_threads: dict[str, Any] | None = None,
) -> str:
    """Generate a DAG SVG with status badges and duration annotations.

    Builds on the standard DAG layout, overlaying colored status indicators
    and timing information on each thread node.

    Args:
        plan: The execution plan to visualize.
        thread_results: Mapping of thread name to ThreadResult.
        resolved_threads: Optional thread models for tooltip content.

    Returns:
        Complete SVG markup as a string.
    """
    nodes, edges, width, height, lookup_nodes, lookup_edges = _compute_layout(
        plan, resolved_threads
    )
    if not nodes:
        return render_dag_svg(plan, resolved_threads)

    # Extend height for duration annotations below nodes
    annotation_space = 18
    height += annotation_space

    parts: list[str] = []
    vb = f"0 0 {width:.0f} {height:.0f}"
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vb}" '
        f'width="{width:.0f}" height="{height:.0f}">'
    )
    parts.append(_build_svg_style())

    # Additional styles for annotations
    lt = _LIGHT
    dk = _DARK
    parts.append("<style>")
    parts.append(
        f"  .dag-dur{{fill:{lt['node_text']};font-family:{_FONT_FAMILY};"
        f"font-size:{_FONT_SIZE_ANNOTATION}px;text-anchor:middle;opacity:0.7}}"
    )
    parts.append(f"  @media(prefers-color-scheme:dark){{.dag-dur{{fill:{dk['node_text']}}}}}")
    parts.append("</style>")

    # Arrowhead markers
    parts.append("<defs>")
    parts.append(
        f'<marker id="dag-arrowhead" markerWidth="{_ARROWHEAD_SIZE}" '
        f'markerHeight="{_ARROWHEAD_SIZE}" refX="{_ARROWHEAD_SIZE}" '
        f'refY="{_ARROWHEAD_SIZE // 2}" orient="auto-start-reverse">'
        f'<polygon points="0 0, {_ARROWHEAD_SIZE} {_ARROWHEAD_SIZE // 2}, '
        f'0 {_ARROWHEAD_SIZE}" class="dag-arrow"/></marker>'
    )
    parts.append("</defs>")

    # Background
    parts.append(f'<rect width="{width:.0f}" height="{height:.0f}" class="dag-bg"/>')

    # Edges
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

    # Nodes with status badges
    cache_set = set(plan.cache_targets)
    status_colors = {
        "success": "#48bb78",
        "failure": "#fc8181",
        "skipped": "#a0aec0",
    }

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

        # Status badge (small circle in top-right corner)
        tr = (thread_results or {}).get(name)
        if tr is not None:
            status = str(getattr(tr, "status", ""))
            badge_color = status_colors.get(status, "#a0aec0")
            bx = x + w - 8
            by = y + 8
            parts.append(
                f'<circle cx="{bx:.1f}" cy="{by:.1f}" r="5" '
                f'fill="{badge_color}" stroke="white" stroke-width="1.5"/>'
            )

            # Duration annotation below node
            telem = getattr(tr, "telemetry", None)
            if telem:
                span = getattr(telem, "span", None)
                if span:
                    start = getattr(span, "start_time", None)
                    end = getattr(span, "end_time", None)
                    if start and end:
                        dur_ms = int((end - start).total_seconds() * 1000)
                        dur_text = _format_duration(dur_ms)
                        parts.append(
                            f'<text x="{x + w / 2:.1f}" y="{y + h + 14:.1f}" '
                            f'class="dag-dur">{_xml_escape(dur_text)}</text>'
                        )

    parts.append("</svg>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

# Inline style constants — notebook HTML sanitizers strip <style> blocks,
# so all styling must be applied directly on elements.  The plan renders as
# a self-contained card with explicit backgrounds to avoid inheriting the
# host page's theme (which caused unreadable text on Fabric dark mode).

_S_CONTAINER = (
    "font-family:system-ui,-apple-system,sans-serif;font-size:14px;"
    "color:#2d3748;background:#ffffff;padding:12px 16px;border-radius:8px"
)
_S_TABLE = "border-collapse:collapse;width:100%;margin:8px 0"
_S_TH = (
    "border:1px solid #e2e8f0;padding:6px 10px;text-align:left;"
    "background:#f7fafc;font-weight:600;color:#2d3748"
)
_S_TD = "border:1px solid #e2e8f0;padding:6px 10px;text-align:left;background:#ffffff;color:#2d3748"
_S_H3 = "margin:16px 0 8px;font-size:16px;color:#2d3748"
_S_H4 = "margin:12px 0 6px;font-size:14px;color:#2d3748"
_S_BADGE = "display:inline-block;padding:1px 6px;border-radius:3px;font-size:12px"
_S_BADGE_CACHE = f"{_S_BADGE};background:#ebf8ff;color:#2c5282;border:1px solid #bee3f8"
_S_BADGE_INFERRED = f"{_S_BADGE};background:#f0fff4;color:#276749;border:1px solid #c6f6d5"
_S_BADGE_EXPLICIT = f"{_S_BADGE};background:#faf5ff;color:#553c9a;border:1px solid #e9d8fd"
_S_BADGE_STATUS = f"{_S_BADGE};background:#f0fff4;color:#276749;border:1px solid #c6f6d5"
_S_NONE = "color:#a0aec0;font-style:italic"
_S_ERROR_BOX = (
    "background:#fff5f5;border:1px solid #feb2b2;border-radius:4px;"
    "padding:8px 12px;color:#9b2c2c;font-family:monospace;font-size:13px;margin:4px 0"
)
_S_WARN_BOX = (
    "background:#fffff0;border:1px solid #fefcbf;border-radius:4px;"
    "padding:8px 12px;color:#975a16;font-size:13px;margin:4px 0"
)
_S_CHECK_BOX = (
    "background:#f0fff4;border:1px solid #c6f6d5;border-radius:4px;"
    "padding:8px 12px;color:#276749;font-size:13px;margin:8px 0"
)


def _html_dep_badges(
    thread_name: str,
    plan: ExecutionPlan,
) -> str:
    """Build HTML badges for a thread's dependencies with provenance."""
    deps = plan.dependencies.get(thread_name, [])
    if not deps:
        return f'<span style="{_S_NONE}">(none)</span>'

    inferred = set(plan.inferred_dependencies.get(thread_name, []))
    explicit = set(plan.explicit_dependencies.get(thread_name, []))

    badges: list[str] = []
    for dep in deps:
        name = html.escape(dep)
        if dep in explicit:
            badges.append(f'<span style="{_S_BADGE_EXPLICIT}">{name}</span>')
        elif dep in inferred:
            badges.append(f'<span style="{_S_BADGE_INFERRED}">{name}</span>')
        else:
            badges.append(html.escape(dep))
    return " ".join(badges)


def render_execution_plan_html(
    plan: ExecutionPlan,
    resolved_threads: dict[str, Any] | None = None,
) -> str:
    """Generate styled HTML for a single ExecutionPlan.

    Includes DAG visualization and dependency table.

    Args:
        plan: The execution plan to render.
        resolved_threads: Optional thread models for detail rows.

    Returns:
        Complete HTML fragment as a string.
    """
    parts: list[str] = []

    weave_name = html.escape(plan.weave_name)
    parts.append(f'<h4 style="{_S_H4}">Weave: {weave_name}</h4>')

    # Dependency table
    cache_set = set(plan.cache_targets)
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(
        f'<tr><th style="{_S_TH}">Group</th><th style="{_S_TH}">Thread</th>'
        f'<th style="{_S_TH}">Dependencies</th><th style="{_S_TH}">Cache</th></tr>'
    )
    for group_idx, group in enumerate(plan.execution_order):
        for thread_name in sorted(group):
            name_esc = html.escape(thread_name)
            cache_badge = (
                f'<span style="{_S_BADGE_CACHE}">cached</span>' if thread_name in cache_set else ""
            )
            dep_badges = _html_dep_badges(thread_name, plan)
            parts.append(
                f'<tr><td style="{_S_TD}">{group_idx}</td>'
                f'<td style="{_S_TD}">{name_esc}</td>'
                f'<td style="{_S_TD}">{dep_badges}</td>'
                f'<td style="{_S_TD}">{cache_badge}</td></tr>'
            )
    parts.append("</table>")

    # Embedded DAG SVG
    svg = render_dag_svg(plan, resolved_threads)
    parts.append(f"<div>{svg}</div>")

    return "\n".join(parts)


def render_plan_html(
    result: Any,
) -> str:
    """Generate styled HTML for a plan mode RunResult.

    Accepts a duck-typed result object to avoid circular imports.
    Uses attributes: ``status``, ``mode``, ``config_type``, ``config_name``,
    ``execution_plan``, ``_resolved_threads``.

    Args:
        result: A RunResult in plan mode.

    Returns:
        Complete HTML fragment as a string.
    """
    parts: list[str] = [f'<div style="{_S_CONTAINER}">']

    # Summary table
    status = html.escape(str(getattr(result, "status", "")))
    config_type = html.escape(str(getattr(result, "config_type", "")))
    config_name = html.escape(str(getattr(result, "config_name", "")))
    plans: list[Any] = getattr(result, "execution_plan", []) or []
    resolved = getattr(result, "_resolved_threads", None)

    total_threads = sum(len(p.threads) for p in plans)
    total_cached = sum(len(p.cache_targets) for p in plans)
    total_lookups = sum(sum(len(v) for v in (p.lookup_schedule or {}).values()) for p in plans)

    parts.append(f'<h3 style="{_S_H3}">Plan Summary</h3>')
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Status</strong></td><td style="{_S_TD}">'
        f'<span style="{_S_BADGE_STATUS}">{status}</span></td></tr>'
    )
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Scope</strong></td>'
        f'<td style="{_S_TD}">{config_type}: {config_name}</td></tr>'
    )
    counts = f"{total_threads} threads | {total_cached} cached"
    if total_lookups > 0:
        counts += f" | {total_lookups} lookups"
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Counts</strong></td>'
        f'<td style="{_S_TD}">{counts}</td></tr>'
    )
    parts.append("</table>")

    # Loom header and combined DAG for multiple plans
    if len(plans) > 1:
        parts.append(f'<h3 style="{_S_H3}">Loom: {config_name} &mdash; {len(plans)} weaves</h3>')
        loom_svg = render_loom_dag_svg(plans, resolved)
        parts.append(f"<div>{loom_svg}</div>")

    # Per-plan sections
    for plan in plans:
        parts.append(render_execution_plan_html(plan, resolved))

    parts.append("</div>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Status badge styling
# ---------------------------------------------------------------------------

_S_BADGE_SUCCESS = f"{_S_BADGE};background:#f0fff4;color:#276749;border:1px solid #c6f6d5"
_S_BADGE_FAILURE = f"{_S_BADGE};background:#fff5f5;color:#9b2c2c;border:1px solid #feb2b2"
_S_BADGE_PARTIAL = f"{_S_BADGE};background:#fffff0;color:#975a16;border:1px solid #fefcbf"
_S_BADGE_SKIPPED = f"{_S_BADGE};background:#f7fafc;color:#718096;border:1px solid #e2e8f0"

_STATUS_BADGE_MAP = {
    "success": _S_BADGE_SUCCESS,
    "failure": _S_BADGE_FAILURE,
    "partial": _S_BADGE_PARTIAL,
    "skipped": _S_BADGE_SKIPPED,
}


def _status_badge(status: str) -> str:
    """Return an inline-styled badge for a status value."""
    style = _STATUS_BADGE_MAP.get(status, _S_BADGE)
    return f'<span style="{style}">{html.escape(status)}</span>'


# ---------------------------------------------------------------------------
# Rich HTML renderers for non-plan modes
# ---------------------------------------------------------------------------


def render_result_html(result: Any) -> str:
    """Generate styled HTML for any RunResult mode.

    Dispatches to mode-specific renderers based on ``result.mode``.
    Accepts a duck-typed result object to avoid circular imports.

    Args:
        result: A RunResult instance.

    Returns:
        Complete HTML fragment as a string.
    """
    mode = str(getattr(result, "mode", ""))
    if mode == "plan":
        return render_plan_html(result)
    if mode == "execute":
        return _render_execute_html(result)
    if mode == "validate":
        return _render_validate_html(result)
    if mode == "preview":
        return _render_preview_html(result)
    # Unknown mode — fall back to escaped summary
    escaped = html.escape(str(getattr(result, "summary", lambda: "")()))
    return f'<pre style="font-family:monospace;padding:12px;border-radius:4px">{escaped}</pre>'


def _render_execute_html(result: Any) -> str:
    """Render a styled HTML report for execute mode results."""
    parts: list[str] = [f'<div style="{_S_CONTAINER}">']

    status = str(getattr(result, "status", ""))
    config_type = html.escape(str(getattr(result, "config_type", "")))
    config_name = html.escape(str(getattr(result, "config_name", "")))
    duration_ms = getattr(result, "duration_ms", 0) or 0
    warnings: list[str] = getattr(result, "warnings", []) or []

    parts.append(f'<h3 style="{_S_H3}">Execution Summary</h3>')

    # Summary table
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Status</strong></td>'
        f'<td style="{_S_TD}">{_status_badge(status)}</td></tr>'
    )
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Scope</strong></td>'
        f'<td style="{_S_TD}">{config_type}: {config_name}</td></tr>'
    )
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Duration</strong></td>'
        f'<td style="{_S_TD}">{_format_duration(duration_ms)}</td></tr>'
    )

    # Total rows
    total_rows = _count_total_rows(result)
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Rows Written</strong></td>'
        f'<td style="{_S_TD}">{total_rows:,}</td></tr>'
    )
    parts.append("</table>")

    # Thread results table
    thread_results = _collect_thread_results(result)
    if thread_results:
        parts.append(f'<h4 style="{_S_H4}">Threads</h4>')
        parts.append(f'<table style="{_S_TABLE}">')
        parts.append(
            f'<tr><th style="{_S_TH}">Thread</th><th style="{_S_TH}">Status</th>'
            f'<th style="{_S_TH}">Rows</th><th style="{_S_TH}">Mode</th>'
            f'<th style="{_S_TH}">Target</th></tr>'
        )
        for tr in thread_results:
            name = html.escape(getattr(tr, "thread_name", ""))
            tr_status = str(getattr(tr, "status", ""))
            rows = getattr(tr, "rows_written", 0)
            write_mode = html.escape(getattr(tr, "write_mode", ""))
            target = html.escape(getattr(tr, "target_path", ""))
            # Truncate long paths
            target_display = target if len(target) <= 60 else "\u2026" + target[-57:]
            parts.append(
                f'<tr><td style="{_S_TD}">{name}</td>'
                f'<td style="{_S_TD}">{_status_badge(tr_status)}</td>'
                f'<td style="{_S_TD}">{rows:,}</td>'
                f'<td style="{_S_TD}">{write_mode}</td>'
                f'<td style="{_S_TD}" title="{target}">{target_display}</td></tr>'
            )
        parts.append("</table>")

    # Errors
    errors = _collect_errors(result)
    if errors:
        parts.append(f'<h4 style="{_S_H4}">Errors</h4>')
        for err in errors:
            parts.append(f'<div style="{_S_ERROR_BOX}">{html.escape(err)}</div>')

    # Warnings
    if warnings:
        parts.append(f'<h4 style="{_S_H4}">Warnings</h4>')
        for w in warnings:
            parts.append(f'<div style="{_S_WARN_BOX}">{html.escape(w)}</div>')

    parts.append("</div>")
    return "\n".join(parts)


def _render_validate_html(result: Any) -> str:
    """Render a styled HTML report for validate mode results."""
    parts: list[str] = [f'<div style="{_S_CONTAINER}">']

    status = str(getattr(result, "status", ""))
    config_type = html.escape(str(getattr(result, "config_type", "")))
    config_name = html.escape(str(getattr(result, "config_name", "")))
    validation_errors: list[str] = getattr(result, "validation_errors", []) or []
    warnings: list[str] = getattr(result, "warnings", []) or []

    parts.append(f'<h3 style="{_S_H3}">Validation Summary</h3>')

    # Summary table
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Status</strong></td>'
        f'<td style="{_S_TD}">{_status_badge(status)}</td></tr>'
    )
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Scope</strong></td>'
        f'<td style="{_S_TD}">{config_type}: {config_name}</td></tr>'
    )
    parts.append("</table>")

    if not validation_errors:
        # Checks passed
        parts.append(
            f'<div style="{_S_CHECK_BOX}">'
            "\u2713 config schema &nbsp; \u2713 DAG &nbsp; \u2713 sources"
            "</div>"
        )
    else:
        parts.append(f'<h4 style="{_S_H4}">Errors</h4>')
        for err in validation_errors:
            parts.append(f'<div style="{_S_ERROR_BOX}">{html.escape(err)}</div>')

    if warnings:
        parts.append(f'<h4 style="{_S_H4}">Warnings</h4>')
        for w in warnings:
            parts.append(f'<div style="{_S_WARN_BOX}">{html.escape(w)}</div>')

    parts.append("</div>")
    return "\n".join(parts)


def _render_preview_html(result: Any) -> str:
    """Render a styled HTML report for preview mode results."""
    parts: list[str] = [f'<div style="{_S_CONTAINER}">']

    status = str(getattr(result, "status", ""))
    config_type = html.escape(str(getattr(result, "config_type", "")))
    config_name = html.escape(str(getattr(result, "config_name", "")))
    preview_data: dict[str, Any] = getattr(result, "preview_data", None) or {}
    warnings: list[str] = getattr(result, "warnings", []) or []

    parts.append(f'<h3 style="{_S_H3}">Preview Summary</h3>')

    # Summary table
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Status</strong></td>'
        f'<td style="{_S_TD}">{_status_badge(status)}</td></tr>'
    )
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Scope</strong></td>'
        f'<td style="{_S_TD}">{config_type}: {config_name}</td></tr>'
    )
    if preview_data:
        parts.append(
            f'<tr><td style="{_S_TD}"><strong>Threads</strong></td>'
            f'<td style="{_S_TD}">{len(preview_data)}</td></tr>'
        )
    parts.append("</table>")

    # Preview data table
    if preview_data:
        parts.append(f'<h4 style="{_S_H4}">Output Shape</h4>')
        parts.append(f'<table style="{_S_TABLE}">')
        parts.append(
            f'<tr><th style="{_S_TH}">Thread</th>'
            f'<th style="{_S_TH}">Columns</th>'
            f'<th style="{_S_TH}">Rows</th></tr>'
        )
        for name, df in preview_data.items():
            name_esc = html.escape(name)
            try:
                cols = len(df.columns)
                rows = df.count()
                parts.append(
                    f'<tr><td style="{_S_TD}">{name_esc}</td>'
                    f'<td style="{_S_TD}">{cols}</td>'
                    f'<td style="{_S_TD}">{rows:,}</td></tr>'
                )
            except Exception:
                parts.append(
                    f'<tr><td style="{_S_TD}">{name_esc}</td>'
                    f'<td style="{_S_TD}" colspan="2">'
                    f'<span style="{_S_NONE}">(unavailable)</span></td></tr>'
                )
        parts.append("</table>")

    if warnings:
        parts.append(f'<h4 style="{_S_H4}">Warnings</h4>')
        for w in warnings:
            parts.append(f'<div style="{_S_WARN_BOX}">{html.escape(w)}</div>')

    parts.append("</div>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Helpers for extracting result data (duck-typed to avoid circular imports)
# ---------------------------------------------------------------------------


def _count_total_rows(result: Any) -> int:
    """Sum rows_written across all threads in the result detail tree."""
    detail = getattr(result, "detail", None)
    if detail is None:
        return 0
    config_type = str(getattr(result, "config_type", ""))
    if config_type == "thread":
        return getattr(detail, "rows_written", 0)
    if config_type == "weave":
        return sum(getattr(tr, "rows_written", 0) for tr in getattr(detail, "thread_results", []))
    if config_type == "loom":
        total = 0
        for wr in getattr(detail, "weave_results", []):
            total += sum(getattr(tr, "rows_written", 0) for tr in getattr(wr, "thread_results", []))
        return total
    return 0


def _collect_thread_results(result: Any) -> list[Any]:
    """Flatten all ThreadResult objects from the result detail tree."""
    detail = getattr(result, "detail", None)
    if detail is None:
        return []
    config_type = str(getattr(result, "config_type", ""))
    if config_type == "thread":
        return [detail]
    if config_type == "weave":
        return list(getattr(detail, "thread_results", []))
    if config_type == "loom":
        results: list[Any] = []
        for wr in getattr(detail, "weave_results", []):
            results.extend(getattr(wr, "thread_results", []))
        return results
    return []


def _collect_errors(result: Any) -> list[str]:
    """Extract error messages from failed threads."""
    errors: list[str] = []
    for tr in _collect_thread_results(result):
        error = getattr(tr, "error", None)
        if error:
            name = getattr(tr, "thread_name", "unknown")
            errors.append(f"[{name}] {error}")
    return errors
