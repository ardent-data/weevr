"""Plan display — DAG visualization and rich rendering for execution results."""

from __future__ import annotations

import contextlib
import html
import types
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
    "validation_fill": "#fffbeb",
    "validation_stroke": "#d97706",
    "quarantine_fill": "#fef3c7",
    "quarantine_stroke": "#b45309",
    "assertion_fill": "#eff6ff",
    "assertion_stroke": "#6366f1",
    "export_fill": "#f0fdf4",
    "export_stroke": "#22c55e",
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
    "validation_fill": "#451a03",
    "validation_stroke": "#d97706",
    "quarantine_fill": "#78350f",
    "quarantine_stroke": "#f59e0b",
    "assertion_fill": "#312e81",
    "assertion_stroke": "#818cf8",
    "export_fill": "#14532d",
    "export_stroke": "#4ade80",
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


def _build_svg_style(dark: bool | None = None) -> str:
    """Build the CSS style block with prefers-color-scheme media query.

    Args:
        dark: Force dark (True) or light (False) palette. None uses
            ``prefers-color-scheme`` media query for automatic switching.
    """
    lt = _DARK if dark is True else _LIGHT
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
    ]
    if dark is None:
        lines.extend(
            [
                "  @media(prefers-color-scheme:dark){",
                f"    .dag-node{{fill:{dk['node_fill']};stroke:{dk['node_stroke']}}}",
                f"    .dag-node-cached{{fill:{dk['cached_fill']};stroke:{dk['cached_stroke']}}}",
                f"    .dag-label{{fill:{dk['node_text']}}}",
                f"    .dag-label-cached{{fill:{dk['cached_text']}}}",
                f"    .dag-edge{{stroke:{dk['edge_stroke']}}}",
                f"    .dag-arrow{{fill:{dk['edge_stroke']}}}",
                f"    .dag-lookup-node{{fill:{dk['lookup_fill']};stroke:{dk['lookup_stroke']}}}",
                f"    .dag-lookup-node-label{{fill:{dk['lookup_text']}}}",
                f"    .dag-lookup-edge{{stroke:{dk['lookup_stroke']}}}",
                f"    .dag-lookup-line{{stroke:{dk['lookup_stroke']}}}",
                f"    .dag-lookup-label{{fill:{dk['lookup_text']}}}",
                f"    .dag-swimlane{{stroke:{dk['node_stroke']}}}",
                f"    .dag-swimlane-header{{fill:{dk['node_text']}}}",
                f"    .dag-bg{{fill:{dk['bg']}}}",
                "  }",
            ]
        )
    lines.append("</style>")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# SVG renderer
# ---------------------------------------------------------------------------


def render_dag_svg(
    plan: ExecutionPlan,
    resolved_threads: dict[str, Any] | None = None,
    *,
    dark: bool | None = None,
) -> str:
    """Generate inline SVG markup for an execution plan DAG.

    Args:
        plan: The execution plan to visualize.
        resolved_threads: Optional thread models for tooltip content.
        dark: Force dark (``True``) or light (``False``) palette.
            ``None`` (default) uses the browser's color scheme preference.

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
            f"{_build_svg_style(dark)}"
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
    parts.append(_build_svg_style(dark))

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
    *,
    dark: bool | None = None,
) -> str:
    """Generate a loom-level DAG SVG with swimlanes for each weave.

    For a single plan, delegates to :func:`render_dag_svg`. For multiple
    plans, stacks each weave's DAG inside a labelled swimlane container
    with sequential arrows between them.

    Args:
        plans: Execution plans for each weave in execution order.
        resolved_threads: Optional thread models for tooltip content.
        dark: Force dark (``True``) or light (``False``) palette.
            ``None`` (default) uses the browser's color scheme preference.

    Returns:
        Complete SVG markup as a string.
    """
    if len(plans) <= 1:
        return render_dag_svg(plans[0], resolved_threads, dark=dark) if plans else ""

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
        "concat",
        "map",
        "format",
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
) -> list[tuple[str, str, int, str | None]]:
    """Walk pipeline steps and group consecutive same-type transforms.

    Returns a list of ``(node_type, label, count, join_source)`` tuples.
    ``node_type`` is ``"join"`` or ``"transform"``.
    ``join_source`` is the source name for join steps, ``None`` otherwise.
    """
    nodes: list[tuple[str, str, int, str | None]] = []
    for step in steps:
        stype = _get_step_type(step)
        if stype == "join":
            params = getattr(step, "join", None)
            join_src = getattr(params, "source", None) if params else None
            nodes.append(("join", _get_join_detail(step), 1, join_src))
        elif nodes and nodes[-1][0] == "transform" and nodes[-1][1] == stype:
            # Merge with previous same-type transform
            prev_type, prev_label, prev_count, prev_src = nodes[-1]
            nodes[-1] = (prev_type, prev_label, prev_count + 1, prev_src)
        else:
            nodes.append(("transform", stype, 1, None))
    return nodes


def _build_flow_svg_style(dark: bool | None = None) -> str:
    """Build CSS style block for thread flow diagrams.

    Args:
        dark: Force dark (True) or light (False) palette. None uses
            ``prefers-color-scheme`` media query for automatic switching.
    """
    lt = _FLOW_DARK if dark is True else _FLOW_LIGHT
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
        (
            f"  .flow-validation{{fill:{lt['validation_fill']};"
            f"stroke:{lt['validation_stroke']};stroke-width:1.5}}"
        ),
        (
            f"  .flow-quarantine{{fill:{lt['quarantine_fill']};"
            f"stroke:{lt['quarantine_stroke']};stroke-width:1.5;stroke-dasharray:4,3}}"
        ),
        (
            f"  .flow-assertion{{fill:{lt['assertion_fill']};"
            f"stroke:{lt['assertion_stroke']};stroke-width:1.5}}"
        ),
        (
            f"  .flow-export{{fill:{lt['export_fill']};"
            f"stroke:{lt['export_stroke']};stroke-width:1.5;stroke-dasharray:6,3}}"
        ),
        f"  .flow-label{{fill:{lt['text']};font-family:{font};font-size:{fs}px;{ta}}}",
        f"  .flow-sublabel{{fill:{lt['text']};font-family:{font};font-size:{fa}px;{ta};"
        "opacity:0.7}}",
        f"  .flow-badge{{fill:{lt['badge_bg']};rx:8}}",
        f"  .flow-edge{{stroke:{lt['edge']};stroke-width:1.5;fill:none}}",
        f"  .flow-arrow{{fill:{lt['edge']}}}",
    ]
    if dark is None:
        lines.extend(
            [
                "  @media(prefers-color-scheme:dark){",
                f"    .flow-bg{{fill:{dk['bg']}}}",
                f"    .flow-source{{fill:{dk['source_fill']};stroke:{dk['source_stroke']}}}",
                f"    .flow-join{{fill:{dk['join_fill']};stroke:{dk['join_stroke']}}}",
                f"    .flow-transform{{fill:{dk['transform_fill']};"
                f"stroke:{dk['transform_stroke']}}}",
                f"    .flow-target{{fill:{dk['target_fill']};stroke:{dk['target_stroke']}}}",
                f"    .flow-validation{{fill:{dk['validation_fill']};"
                f"stroke:{dk['validation_stroke']}}}",
                f"    .flow-quarantine{{fill:{dk['quarantine_fill']};"
                f"stroke:{dk['quarantine_stroke']}}}",
                f"    .flow-assertion{{fill:{dk['assertion_fill']};"
                f"stroke:{dk['assertion_stroke']}}}",
                f"    .flow-export{{fill:{dk['export_fill']};stroke:{dk['export_stroke']}}}",
                f"    .flow-label{{fill:{dk['text']}}}",
                f"    .flow-sublabel{{fill:{dk['text']}}}",
                f"    .flow-badge{{fill:{dk['badge_bg']}}}",
                f"    .flow-edge{{stroke:{dk['edge']}}}",
                f"    .flow-arrow{{fill:{dk['edge']}}}",
                "  }",
            ]
        )
    lines.append("</style>")
    return "\n".join(lines)


def render_flow_svg(thread: Thread, *, dark: bool | None = None) -> str:
    """Generate an inline SVG showing a thread's processing pipeline.

    Renders a horizontal flow diagram: sources on the left, transforms in
    the middle, target on the right. Consecutive same-type transforms are
    grouped with a count badge.

    Args:
        thread: Thread configuration model.
        dark: Force dark (``True``) or light (``False``) palette. ``None``
            uses ``prefers-color-scheme`` media query (default).

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

    source_widths = {label: _est_w(label) for label, _ in source_nodes}

    pipe_widths: list[float] = []
    for ntype, label, count, _jsrc in pipeline_nodes:
        if ntype == "join":
            pipe_widths.append(_est_w(label))
        elif count > 1:
            pipe_widths.append(_est_w(f"{label} ({count})"))
        else:
            pipe_widths.append(_est_w(label))

    target_w = _est_w(tgt_label)

    # --- Classify sources: primary (left-stacked) vs join (above their join node) ---
    join_source_names: set[str] = set()
    join_source_to_pipe_idx: dict[str, int] = {}
    for pidx, (_ntype, _label, _count, jsrc) in enumerate(pipeline_nodes):
        if _ntype == "join" and jsrc:
            for slabel, _stype in source_nodes:
                if slabel == jsrc:
                    join_source_names.add(slabel)
                    join_source_to_pipe_idx[slabel] = pidx
                    break

    primary_sources = [(sn, st) for sn, st in source_nodes if sn not in join_source_names]
    join_sources = [(sn, st) for sn, st in source_nodes if sn in join_source_names]

    # --- Compute positions ---
    # Primary sources: stacked vertically at x=pad
    max_primary_w = max((source_widths[sn] for sn, _ in primary_sources), default=80.0)
    primary_count = len(primary_sources)
    primary_total_h = primary_count * nh + max(primary_count - 1, 0) * vgap

    # Join sources sit above the pipeline, so the pipeline row needs vertical room
    has_join_sources = len(join_sources) > 0
    join_row_h = (nh + vgap) if has_join_sources else 0

    # Pipeline center Y
    center_y = pad + join_row_h
    if primary_sources:
        center_y = max(center_y, pad + join_row_h + primary_total_h / 2 - nh / 2)
        # If primary stack is taller, it determines center_y
        if primary_total_h > nh:
            center_y = pad + join_row_h

    primary_positions: dict[str, tuple[float, float, float]] = {}
    sy = center_y + nh / 2 - primary_total_h / 2 if primary_count > 0 else center_y
    for slabel, _stype in primary_sources:
        primary_positions[slabel] = (pad, sy, max_primary_w)
        sy += nh + vgap

    # Pipeline nodes: left to right after primary sources
    px = pad + (max_primary_w + hgap if primary_sources else 0.0)
    pipe_positions: list[tuple[float, float, float]] = []
    for idx, (_ntype, _label, _count, _jsrc) in enumerate(pipeline_nodes):
        w = pipe_widths[idx]
        pipe_positions.append((px, center_y, w))
        px += w + hgap

    # Join source positions: centered above their join node
    join_positions: dict[str, tuple[float, float, float]] = {}
    for slabel in join_source_names:
        pidx = join_source_to_pipe_idx[slabel]
        jpx, _jpy, jpw = pipe_positions[pidx]
        sw = source_widths[slabel]
        # Center the source above the join node
        jx = jpx + jpw / 2 - sw / 2
        jy = center_y - nh - vgap
        join_positions[slabel] = (jx, jy, sw)

    # Build combined source position list (in original order, for node rendering)
    src_positions: list[tuple[float, float, float]] = []
    for slabel, _stype in source_nodes:
        if slabel in primary_positions:
            src_positions.append(primary_positions[slabel])
        else:
            src_positions.append(join_positions[slabel])

    # --- Validation and quarantine nodes (if thread has validations) ---
    has_validations = getattr(thread, "validations", None) is not None
    validation_pos: tuple[float, float, float] | None = None
    quarantine_pos: tuple[float, float, float] | None = None
    if has_validations:
        val_w = _est_w("validation")
        validation_pos = (px, center_y, val_w)
        px += val_w + hgap
        # Quarantine branches down from validation
        quar_w = _est_w("quarantine")
        quarantine_pos = (
            validation_pos[0] + val_w / 2 - quar_w / 2,
            center_y + nh + vgap,
            quar_w,
        )

    # Target: after pipeline (and validation if present)
    if not pipeline_nodes and not has_validations:
        px = pad + (max_primary_w + hgap if primary_sources else 0.0)
    target_pos = (px, center_y, target_w)

    # --- Export nodes (fan out from the same fan-out point as target) ---
    export_list = getattr(thread, "exports", None) or []
    export_positions: list[tuple[float, float, float, str, str]] = []
    for i, exp in enumerate(export_list):
        exp_name = getattr(exp, "name", f"export_{i}")
        exp_type = getattr(exp, "type", "")
        exp_w = _est_w(exp_name)
        exp_y = center_y + (i + 1) * (nh + vgap)
        export_positions.append((px, exp_y, exp_w, exp_name, exp_type))

    # --- Assertions node (after target) ---
    has_assertions = getattr(thread, "assertions", None) is not None
    assertion_pos: tuple[float, float, float] | None = None
    if has_assertions:
        assert_x = px + target_w + hgap
        assert_w = _est_w("assertions")
        assertion_pos = (assert_x, center_y, assert_w)

    # Canvas dimensions
    top_edge = min(y for _, y, _ in src_positions) if src_positions else pad
    bot_edge = max(y + nh for _, y, _ in src_positions) if src_positions else center_y + nh
    bot_edge = max(bot_edge, center_y + nh)
    # Account for quarantine branch
    if quarantine_pos:
        bot_edge = max(bot_edge, quarantine_pos[1] + nh)
    # Account for export nodes
    for _, ey, _, _, _ in export_positions:
        bot_edge = max(bot_edge, ey + nh)
    # Account for assertions
    right_edge = px + target_w
    if assertion_pos:
        right_edge = assertion_pos[0] + assertion_pos[2]
    canvas_w = right_edge + pad
    canvas_h = bot_edge - top_edge + pad * 2

    # Shift everything so top_edge aligns with pad
    y_offset = pad - top_edge
    src_positions = [(x, y + y_offset, w) for x, y, w in src_positions]
    pipe_positions = [(x, y + y_offset, w) for x, y, w in pipe_positions]
    target_pos = (target_pos[0], target_pos[1] + y_offset, target_pos[2])
    if validation_pos:
        validation_pos = (validation_pos[0], validation_pos[1] + y_offset, validation_pos[2])
    if quarantine_pos:
        quarantine_pos = (quarantine_pos[0], quarantine_pos[1] + y_offset, quarantine_pos[2])
    if assertion_pos:
        assertion_pos = (assertion_pos[0], assertion_pos[1] + y_offset, assertion_pos[2])
    export_positions = [(x, y + y_offset, w, n, t) for x, y, w, n, t in export_positions]
    center_y += y_offset

    # --- Render SVG ---
    parts: list[str] = []
    vb = f"0 0 {canvas_w:.0f} {canvas_h:.0f}"
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vb}" '
        f'width="{canvas_w:.0f}" height="{canvas_h:.0f}">'
    )
    parts.append(_build_flow_svg_style(dark))

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
    # Primary sources → first pipeline node, validation, or target
    if pipe_positions:
        first_target_x = pipe_positions[0][0]
        first_target_y = pipe_positions[0][1]
    elif validation_pos:
        first_target_x = validation_pos[0]
        first_target_y = validation_pos[1]
    else:
        first_target_x = target_pos[0]
        first_target_y = target_pos[1]
    for (slabel, _stype), (sx, sy, sw) in zip(source_nodes, src_positions, strict=True):
        if slabel in join_source_names:
            # Join source → its join node (vertical arrow down)
            pidx = join_source_to_pipe_idx[slabel]
            tx, ty, tw = pipe_positions[pidx]
            x1, y1 = sx + sw / 2, sy + nh
            x2, y2 = tx + tw / 2, ty
        else:
            # Primary source → first pipeline node (or target)
            x1, y1 = sx + sw, sy + nh / 2
            x2, y2 = first_target_x, first_target_y + nh / 2
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

    # Last pipeline node → validation (or target if no validation)
    if pipe_positions:
        last = pipe_positions[-1]
        x1 = last[0] + last[2]
        y1 = last[1] + nh / 2
        if validation_pos:
            x2 = validation_pos[0]
            y2 = validation_pos[1] + nh / 2
        else:
            x2 = target_pos[0]
            y2 = target_pos[1] + nh / 2
        parts.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
            f'class="flow-edge" marker-end="url(#flow-arrow)"/>'
        )

    # Validation → target (main flow continues)
    if validation_pos:
        x1 = validation_pos[0] + validation_pos[2]
        y1 = validation_pos[1] + nh / 2
        x2 = target_pos[0]
        y2 = target_pos[1] + nh / 2
        parts.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
            f'class="flow-edge" marker-end="url(#flow-arrow)"/>'
        )

    # Validation → quarantine (branch down)
    if validation_pos and quarantine_pos:
        x1 = validation_pos[0] + validation_pos[2] / 2
        y1 = validation_pos[1] + nh
        x2 = quarantine_pos[0] + quarantine_pos[2] / 2
        y2 = quarantine_pos[1]
        parts.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
            f'class="flow-edge" marker-end="url(#flow-arrow)"/>'
        )

    # Fan-out point → export nodes (same x as target, different y)
    for ex, ey, _ew, _en, _et in export_positions:
        # Edge from the preparation stage (left of target column) to export
        if validation_pos:
            fx = validation_pos[0] + validation_pos[2]
            fy = validation_pos[1] + nh / 2
        elif pipe_positions:
            last = pipe_positions[-1]
            fx = last[0] + last[2]
            fy = last[1] + nh / 2
        else:
            fx = src_positions[0][0] + src_positions[0][2] if src_positions else pad
            fy = center_y + nh / 2
        parts.append(
            f'<line x1="{fx:.1f}" y1="{fy:.1f}" x2="{ex:.1f}" y2="{ey + nh / 2:.1f}" '
            f'class="flow-edge" marker-end="url(#flow-arrow)"/>'
        )

    # Target → assertions
    if assertion_pos:
        x1 = target_pos[0] + target_pos[2]
        y1 = target_pos[1] + nh / 2
        x2 = assertion_pos[0]
        y2 = assertion_pos[1] + nh / 2
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
    for (ntype, label, count, _jsrc), (px, py, pw) in zip(
        pipeline_nodes, pipe_positions, strict=True
    ):
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

    # --- Validation gate node (diamond-ish shape) ---
    if validation_pos:
        vx, vy, vw = validation_pos
        parts.append(
            f'<rect x="{vx:.1f}" y="{vy:.1f}" width="{vw:.1f}" height="{nh}" '
            f'rx="{cr}" class="flow-validation"/>'
        )
        parts.append(
            f'<text x="{vx + vw / 2:.1f}" y="{vy + nh / 2 - 5:.1f}" '
            f'class="flow-label">validation</text>'
        )
        parts.append(
            f'<text x="{vx + vw / 2:.1f}" y="{vy + nh / 2 + 8:.1f}" '
            f'class="flow-sublabel">gate</text>'
        )

    # --- Quarantine destination node ---
    if quarantine_pos:
        qx, qy, qw = quarantine_pos
        parts.append(
            f'<rect x="{qx:.1f}" y="{qy:.1f}" width="{qw:.1f}" height="{nh}" '
            f'rx="{cr}" class="flow-quarantine"/>'
        )
        parts.append(
            f'<text x="{qx + qw / 2:.1f}" y="{qy + nh / 2:.1f}" '
            f'class="flow-label">quarantine</text>'
        )

    # --- Export destination nodes ---
    for ex, ey, ew, en, et in export_positions:
        parts.append(
            f'<rect x="{ex:.1f}" y="{ey:.1f}" width="{ew:.1f}" height="{nh}" '
            f'rx="{cr}" class="flow-export"/>'
        )
        parts.append(
            f'<text x="{ex + ew / 2:.1f}" y="{ey + nh / 2 - 5:.1f}" '
            f'class="flow-label">{_xml_escape(en)}</text>'
        )
        parts.append(
            f'<text x="{ex + ew / 2:.1f}" y="{ey + nh / 2 + 8:.1f}" '
            f'class="flow-sublabel">{_xml_escape(et)}</text>'
        )

    # --- Assertions checkpoint node ---
    if assertion_pos:
        ax, ay, aw = assertion_pos
        parts.append(
            f'<rect x="{ax:.1f}" y="{ay:.1f}" width="{aw:.1f}" height="{nh}" '
            f'rx="{cr}" class="flow-assertion"/>'
        )
        parts.append(
            f'<text x="{ax + aw / 2:.1f}" y="{ay + nh / 2 - 5:.1f}" '
            f'class="flow-label">assertions</text>'
        )
        parts.append(
            f'<text x="{ax + aw / 2:.1f}" y="{ay + nh / 2 + 8:.1f}" '
            f'class="flow-sublabel">post-write</text>'
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
    *,
    dark: bool | None = None,
) -> str:
    """Generate a phased execution timeline Gantt SVG.

    Shows lookups, pre-hooks, thread execution groups, and post-hooks as
    sequential phases with wall-clock duration bars.

    Args:
        weave_result: A WeaveResult with thread_results and telemetry.
        hook_results: Hook execution results (from WeaveTelemetry).
        lookup_results: Lookup materialization results (from WeaveTelemetry).
        dark: Force dark (``True``) or light (``False``) palette. ``None``
            uses ``prefers-color-scheme`` media query (default).

    Returns:
        Complete SVG markup as a string.
    """
    lt = _FLOW_DARK if dark is True else _FLOW_LIGHT
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
            dur = int(getattr(lk, "duration_ms", 0))
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
            dur = int(getattr(hr, "duration_ms", 0))
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
    phase_label_h = len(phases) * (bar_h + 2)  # each phase has a label row
    canvas_h = pad * 2 + total_bars * (bar_h + 4) + phase_label_h + (len(phases) - 1) * phase_gap
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
        f"  .tl-phase{{fill:{lt['text']};font-family:{font};font-size:13px;font-weight:600}}"
    )
    parts.append(f"  .tl-dur{{fill:{lt['text']};font-family:{font};font-size:11px;opacity:0.7}}")
    if dark is None:
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
            # Name label left of bar
            parts.append(
                f'<text x="{bx - 6:.1f}" y="{y + bar_h / 2:.1f}" '
                f'dominant-baseline="central" text-anchor="end" class="tl-label">'
                f"{_xml_escape(bar_label)}</text>"
            )
            parts.append(
                f'<rect x="{bx:.1f}" y="{y:.1f}" width="{bar_w:.1f}" '
                f'height="{bar_h}" rx="4" fill="{fill_lt}"/>'
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
    *,
    dark: bool | None = None,
) -> str:
    """Generate a Sankey-style data flow SVG showing row counts at each stage.

    Renders proportional bands flowing left-to-right: rows read → after
    transforms → validation split (quarantine branch + clean rows) →
    target + export fan-out.

    Execute mode shows the full pipeline including target and exports.
    Preview mode shows only read → transforms.

    Args:
        thread_result: A ThreadResult (used for rows_written).
        thread_telemetry: ThreadTelemetry with row counts and export_results.
        mode: ``"execute"`` or ``"preview"``.
        dark: Force dark (``True``) or light (``False``) palette. ``None``
            uses ``prefers-color-scheme`` media query (default).

    Returns:
        Complete SVG markup as a string.
    """
    lt = _FLOW_DARK if dark is True else _FLOW_LIGHT
    dk = _FLOW_DARK
    font = _FONT_FAMILY
    pad = _SVG_PADDING

    # Extract row counts
    rows_read = 0
    rows_after = 0
    rows_quarantined = 0
    rows_written = getattr(thread_result, "rows_written", 0)
    export_results: list[Any] = []
    if thread_telemetry:
        rows_read = getattr(thread_telemetry, "rows_read", 0)
        rows_after = getattr(thread_telemetry, "rows_after_transforms", 0)
        rows_quarantined = getattr(thread_telemetry, "rows_quarantined", 0)
        export_results = getattr(thread_telemetry, "export_results", [])

    # Sankey layout constants
    col_w = 110  # width of each stage column
    col_gap = 80  # gap between columns (room for Bezier curves)
    max_band_h = 80  # max band height at full row count
    band_gap = 24  # vertical gap between stacked bands (room for labels)
    band_min = 34  # minimum band height (must fit count + label text)

    max_count = max(rows_read, rows_after, 1)

    def _bh(count: int) -> float:
        """Proportional band height."""
        if count <= 0:
            return band_min
        return max((count / max_count) * max_band_h, band_min)

    # --- Compute band heights ---
    clean_rows = max(rows_after - rows_quarantined, 0) if mode == "execute" else 0
    output_dests: list[tuple[str, int]] = []
    if mode == "execute":
        output_dests.append(("Target", rows_written))
        for er in export_results:
            er_name = getattr(er, "name", "export")
            er_rows = getattr(er, "rows_written", 0)
            output_dests.append((er_name, er_rows))

    has_quarantine = mode == "execute" and rows_quarantined > 0
    n_cols = 2 if mode == "preview" else (4 if has_quarantine else 3)

    h_read = _bh(rows_read)
    h_after = _bh(rows_after)
    h_clean = _bh(clean_rows) if has_quarantine else _bh(rows_after)
    h_quar = _bh(rows_quarantined) if has_quarantine else 0.0
    h_outputs = [_bh(c) for _, c in output_dests]

    # Canvas sizing
    max_stack = max(
        h_read + 16,
        h_after + 16,
        (h_clean + band_gap + h_quar + 16) if has_quarantine else h_clean + 16,
        (sum(h_outputs) + band_gap * max(len(h_outputs) - 1, 0) + 16 if h_outputs else 0.0),
    )
    canvas_w = pad * 2 + n_cols * col_w + (n_cols - 1) * col_gap
    canvas_h = pad * 2 + max_stack + 20

    # --- Theme-aware colors (aligned with flow SVG palette) ---
    band_colors_lt = {
        "read": lt["source_fill"],
        "transform": lt["transform_fill"],
        "clean": lt["transform_fill"],
        "quarantine": lt["quarantine_fill"],
        "target": lt["target_fill"],
        "export": lt["export_fill"],
    }
    band_stroke_lt = {
        "read": lt["source_stroke"],
        "transform": lt["transform_stroke"],
        "clean": lt["transform_stroke"],
        "quarantine": lt["quarantine_stroke"],
        "target": lt["target_stroke"],
        "export": lt["export_stroke"],
    }
    band_colors_dk = {
        "read": dk["source_fill"],
        "transform": dk["transform_fill"],
        "clean": dk["transform_fill"],
        "quarantine": dk["quarantine_fill"],
        "target": dk["target_fill"],
        "export": dk["export_fill"],
    }
    band_stroke_dk = {
        "read": dk["source_stroke"],
        "transform": dk["transform_stroke"],
        "clean": dk["transform_stroke"],
        "quarantine": dk["quarantine_stroke"],
        "target": dk["target_stroke"],
        "export": dk["export_stroke"],
    }
    flow_opacity_lt = "0.3"
    flow_opacity_dk = "0.4"
    bc = band_colors_dk if dark is True else band_colors_lt
    bs = band_stroke_dk if dark is True else band_stroke_lt

    parts: list[str] = []
    vb = f"0 0 {canvas_w:.0f} {canvas_h:.0f}"
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vb}" '
        f'width="{canvas_w:.0f}" height="{canvas_h:.0f}">'
    )

    # --- CSS ---
    fop = flow_opacity_dk if dark is True else flow_opacity_lt
    parts.append("<style>")
    parts.append(f"  .wf-bg{{fill:{lt['bg']}}}")
    parts.append(
        f"  .wf-label{{fill:{lt['text']};font-family:{font};"
        "font-size:10px;text-anchor:middle;font-weight:500}}"
    )
    parts.append(
        f"  .wf-count{{fill:{lt['text']};font-family:{font};"
        "font-size:11px;text-anchor:middle;font-weight:600}}"
    )
    parts.append(f"  .wf-flow{{opacity:{fop}}}")
    if dark is None:
        parts.append("  @media(prefers-color-scheme:dark){")
        parts.append(f"    .wf-bg{{fill:{dk['bg']}}}")
        parts.append(f"    .wf-label{{fill:{dk['text']}}}")
        parts.append(f"    .wf-count{{fill:{dk['text']}}}")
        parts.append(f"    .wf-flow{{opacity:{flow_opacity_dk}}}")
        # Band color overrides per role (fill + stroke)
        for role in band_colors_dk:
            dc = band_colors_dk[role]
            ds = band_stroke_dk[role]
            parts.append(f"    .wf-band-{role}{{fill:{dc};stroke:{ds}}}")
        parts.append("  }")
    parts.append("</style>")
    parts.append(f'<rect width="{canvas_w:.0f}" height="{canvas_h:.0f}" class="wf-bg"/>')

    # --- Helpers ---
    def _draw_band(
        x: float,
        y: float,
        w: float,
        h: float,
        label: str,
        count: int,
        role: str,
    ) -> None:
        color = bc[role]
        stroke = bs[role]
        parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{w}" '
            f'height="{h:.1f}" rx="5" fill="{color}" '
            f'stroke="{stroke}" stroke-width="1.5" '
            f'opacity="0.8" class="wf-band-{role}"/>'
        )
        # Count and label stacked inside the band
        parts.append(
            f'<text x="{x + w / 2:.1f}" y="{y + h / 2 - 4:.1f}" '
            f'dominant-baseline="central" class="wf-count">'
            f"{count:,}</text>"
        )
        parts.append(
            f'<text x="{x + w / 2:.1f}" y="{y + h / 2 + 8:.1f}" '
            f'dominant-baseline="central" '
            f'class="wf-label">{_xml_escape(label)}</text>'
        )

    def _draw_flow(
        lx: float,
        lt_: float,
        lb: float,
        rx: float,
        rt: float,
        rb: float,
        role: str,
    ) -> None:
        """Smooth Bezier Sankey band from left edge to right edge."""
        color = bc[role]
        mx = (lx + rx) / 2
        parts.append(
            f'<path d="M{lx:.1f},{lt_:.1f} '
            f"C{mx:.1f},{lt_:.1f} {mx:.1f},{rt:.1f} "
            f"{rx:.1f},{rt:.1f} "
            f"L{rx:.1f},{rb:.1f} "
            f"C{mx:.1f},{rb:.1f} {mx:.1f},{lb:.1f} "
            f"{lx:.1f},{lb:.1f}Z"
            f'" fill="{color}" class="wf-flow wf-band-{role}"/>'
        )

    # --- Position and draw ---
    cy = pad + max_stack / 2

    # Col 0: Rows read
    cx0 = pad
    y_read = cy - h_read / 2
    _draw_band(cx0, y_read, col_w, h_read, "Rows read", rows_read, "read")

    # Col 1: After transforms
    cx1 = pad + col_w + col_gap
    y_after = cy - h_after / 2
    _draw_band(
        cx1,
        y_after,
        col_w,
        h_after,
        "After transforms",
        rows_after,
        "transform",
    )
    _draw_flow(
        cx0 + col_w,
        y_read,
        y_read + h_read,
        cx1,
        y_after,
        y_after + h_after,
        "read",
    )

    if mode == "execute" and has_quarantine:
        # Col 2: Split — clean rows (top) + quarantine (bottom)
        cx2 = pad + 2 * (col_w + col_gap)
        split_h = h_clean + band_gap + h_quar
        y_clean = cy - split_h / 2
        y_quar = y_clean + h_clean + band_gap
        _draw_band(
            cx2,
            y_clean,
            col_w,
            h_clean,
            "Clean rows",
            clean_rows,
            "clean",
        )
        _draw_band(
            cx2,
            y_quar,
            col_w,
            h_quar,
            "Quarantined",
            rows_quarantined,
            "quarantine",
        )

        # Proportional split from "after transforms" band
        ratio = clean_rows / rows_after if rows_after > 0 else 1.0
        split_y = y_after + h_after * ratio
        _draw_flow(
            cx1 + col_w,
            y_after,
            split_y,
            cx2,
            y_clean,
            y_clean + h_clean,
            "clean",
        )
        _draw_flow(
            cx1 + col_w,
            split_y,
            y_after + h_after,
            cx2,
            y_quar,
            y_quar + h_quar,
            "quarantine",
        )
        # Quarantine is terminal — no forward flow

        # Col 3: Outputs (flow from clean rows only)
        cx3 = pad + 3 * (col_w + col_gap)
        total_out = sum(h_outputs) + band_gap * max(len(h_outputs) - 1, 0)
        y_out = cy - total_out / 2
        n_out = len(output_dests)
        for oi, (olabel, ocount) in enumerate(output_dests):
            oh = h_outputs[oi]
            role = "target" if oi == 0 else "export"
            _draw_band(cx3, y_out, col_w, oh, olabel, ocount, role)
            l_top = y_clean + h_clean * oi / n_out
            l_bot = y_clean + h_clean * (oi + 1) / n_out
            _draw_flow(
                cx2 + col_w,
                l_top,
                l_bot,
                cx3,
                y_out,
                y_out + oh,
                role,
            )
            y_out += oh + band_gap

    elif mode == "execute":
        # No quarantine — after transforms → outputs directly
        cx2 = pad + 2 * (col_w + col_gap)
        total_out = sum(h_outputs) + band_gap * max(len(h_outputs) - 1, 0)
        y_out = cy - total_out / 2
        n_out = len(output_dests)
        for oi, (olabel, ocount) in enumerate(output_dests):
            oh = h_outputs[oi]
            role = "target" if oi == 0 else "export"
            _draw_band(cx2, y_out, col_w, oh, olabel, ocount, role)
            l_top = y_after + h_after * oi / n_out
            l_bot = y_after + h_after * (oi + 1) / n_out
            _draw_flow(
                cx1 + col_w,
                l_top,
                l_bot,
                cx2,
                y_out,
                y_out + oh,
                role,
            )
            y_out += oh + band_gap

    parts.append("</svg>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Annotated DAG SVG renderer
# ---------------------------------------------------------------------------


def render_annotated_dag_svg(
    plan: ExecutionPlan,
    thread_results: dict[str, Any] | None = None,
    resolved_threads: dict[str, Any] | None = None,
    *,
    dark: bool | None = None,
) -> str:
    """Generate a DAG SVG with status badges and duration annotations.

    Builds on the standard DAG layout, overlaying colored status indicators
    and timing information on each thread node.

    Args:
        plan: The execution plan to visualize.
        thread_results: Mapping of thread name to ThreadResult.
        resolved_threads: Optional thread models for tooltip content.
        dark: Force dark (``True``) or light (``False``) palette. ``None``
            uses ``prefers-color-scheme`` media query (default).

    Returns:
        Complete SVG markup as a string.
    """
    nodes, edges, width, height, lookup_nodes, lookup_edges = _compute_layout(
        plan, resolved_threads
    )
    if not nodes:
        return render_dag_svg(plan, resolved_threads, dark=dark)

    # Extend height for duration annotations below nodes
    annotation_space = 18
    height += annotation_space

    parts: list[str] = []
    vb = f"0 0 {width:.0f} {height:.0f}"
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vb}" '
        f'width="{width:.0f}" height="{height:.0f}">'
    )
    parts.append(_build_svg_style(dark))

    # Additional styles for annotations
    lt = _DARK if dark is True else _LIGHT
    dk = _DARK
    parts.append("<style>")
    parts.append(
        f"  .dag-dur{{fill:{lt['node_text']};font-family:{_FONT_FAMILY};"
        f"font-size:{_FONT_SIZE_ANNOTATION}px;text-anchor:middle;opacity:0.7}}"
    )
    if dark is None:
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
    "padding:8px 12px;color:#9b2c2c;font-family:monospace;font-size:13px;margin:4px 0;"
    "word-wrap:break-word;overflow-wrap:break-word;white-space:pre-wrap"
)
_S_WARN_BOX = (
    "background:#fffff0;border:1px solid #fefcbf;border-radius:4px;"
    "padding:8px 12px;color:#975a16;font-size:13px;margin:4px 0"
)
_S_CHECK_BOX = (
    "background:#f0fff4;border:1px solid #c6f6d5;border-radius:4px;"
    "padding:8px 12px;color:#276749;font-size:13px;margin:8px 0"
)
_S_DETAILS = "margin:8px 0;border:1px solid #e2e8f0;border-radius:6px;overflow:hidden"
_S_SUMMARY = (
    "padding:8px 12px;cursor:pointer;font-weight:600;font-size:14px;"
    "color:#2d3748;background:#f7fafc;list-style:none"
)
_S_DETAIL_BODY = "padding:8px 12px"


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

    # Resolved parameters
    telemetry = getattr(result, "telemetry", None)
    if telemetry:
        parts.append(_render_params_table(getattr(telemetry, "resolved_params", None)))

    # Loom header and combined DAG for multiple plans
    if len(plans) > 1:
        parts.append(f'<h3 style="{_S_H3}">Loom: {config_name} &mdash; {len(plans)} weaves</h3>')
        loom_svg = render_loom_dag_svg(plans, resolved)
        parts.append(f"<div>{loom_svg}</div>")

    # Per-plan sections
    for plan in plans:
        parts.append(render_execution_plan_html(plan, resolved))

    # Per-thread flow diagrams
    if resolved:
        parts.append(f'<h3 style="{_S_H3}">Thread Pipelines</h3>')
        for tname, thread_model in resolved.items():
            tname_esc = html.escape(tname)
            parts.append(f'<details style="{_S_DETAILS}">')
            parts.append(f'<summary style="{_S_SUMMARY}">{tname_esc}</summary>')
            parts.append(f'<div style="{_S_DETAIL_BODY}">')
            try:
                flow_svg = render_flow_svg(thread_model)
                parts.append(f"<div>{flow_svg}</div>")
            except Exception:
                parts.append(f'<span style="{_S_NONE}">(flow diagram unavailable)</span>')
            parts.append("</div></details>")

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
# Shared HTML section renderers
# ---------------------------------------------------------------------------


def _render_params_table(params: dict[str, Any] | None) -> str:
    """Render resolved parameters as an HTML table. Empty if no params."""
    if not params:
        return ""
    parts = [f'<h4 style="{_S_H4}">Resolved Parameters</h4>']
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(f'<tr><th style="{_S_TH}">Parameter</th><th style="{_S_TH}">Value</th></tr>')
    for name, value in params.items():
        parts.append(
            f'<tr><td style="{_S_TD}">{html.escape(str(name))}</td>'
            f'<td style="{_S_TD}">{html.escape(str(value))}</td></tr>'
        )
    parts.append("</table>")
    return "\n".join(parts)


def _render_variables_table(variables: dict[str, Any] | None) -> str:
    """Render weave-scoped variable values as an HTML table."""
    if not variables:
        return ""
    parts = [f'<h4 style="{_S_H4}">Variables</h4>']
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(f'<tr><th style="{_S_TH}">Variable</th><th style="{_S_TH}">Value</th></tr>')
    for name, value in variables.items():
        parts.append(
            f'<tr><td style="{_S_TD}">{html.escape(str(name))}</td>'
            f'<td style="{_S_TD}">{html.escape(str(value))}</td></tr>'
        )
    parts.append("</table>")
    return "\n".join(parts)


def _render_hook_results_table(hook_results: list[Any] | None) -> str:
    """Render hook execution results as an HTML table."""
    if not hook_results:
        return ""
    parts = [f'<h4 style="{_S_H4}">Hook Results</h4>']
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(
        f'<tr><th style="{_S_TH}">Phase</th>'
        f'<th style="{_S_TH}">Type</th>'
        f'<th style="{_S_TH}">Name</th>'
        f'<th style="{_S_TH}">Status</th>'
        f'<th style="{_S_TH}">Duration</th></tr>'
    )
    for hr in hook_results:
        phase = html.escape(str(getattr(hr, "phase", "")))
        step_type = html.escape(str(getattr(hr, "step_type", "")))
        step_name = html.escape(str(getattr(hr, "step_name", "") or ""))
        status = str(getattr(hr, "status", "passed"))
        dur = _format_duration(int(getattr(hr, "duration_ms", 0)))
        gate = getattr(hr, "gate_result", None)
        status_html = _status_badge(status)
        if gate and not getattr(gate, "passed", True):
            msg = html.escape(str(getattr(gate, "message", "")))
            status_html += f' <span style="font-size:12px;color:#9b2c2c">{msg}</span>'
        parts.append(
            f'<tr><td style="{_S_TD}">{phase}</td>'
            f'<td style="{_S_TD}">{step_type}</td>'
            f'<td style="{_S_TD}">{step_name}</td>'
            f'<td style="{_S_TD}">{status_html}</td>'
            f'<td style="{_S_TD}">{dur}</td></tr>'
        )
    parts.append("</table>")
    return "\n".join(parts)


def _render_lookup_results_table(lookup_results: list[Any] | None) -> str:
    """Render lookup materialization results as an HTML table."""
    if not lookup_results:
        return ""
    parts = [f'<h4 style="{_S_H4}">Lookup Results</h4>']
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(
        f'<tr><th style="{_S_TH}">Lookup</th>'
        f'<th style="{_S_TH}">Strategy</th>'
        f'<th style="{_S_TH}">Rows</th>'
        f'<th style="{_S_TH}">Duration</th>'
        f'<th style="{_S_TH}">Key Check</th></tr>'
    )
    for lk in lookup_results:
        name = html.escape(str(getattr(lk, "name", "")))
        strategy = html.escape(str(getattr(lk, "strategy", "cache")))
        rows = getattr(lk, "row_count", 0)
        dur = _format_duration(int(getattr(lk, "duration_ms", 0)))
        uk_checked = getattr(lk, "unique_key_checked", False)
        if uk_checked:
            uk_passed = getattr(lk, "unique_key_passed", None)
            key_check = _status_badge("success" if uk_passed else "failure")
        else:
            key_check = f'<span style="{_S_NONE}">—</span>'
        parts.append(
            f'<tr><td style="{_S_TD}">{name}</td>'
            f'<td style="{_S_TD}">{strategy}</td>'
            f'<td style="{_S_TD}">{rows:,}</td>'
            f'<td style="{_S_TD}">{dur}</td>'
            f'<td style="{_S_TD}">{key_check}</td></tr>'
        )
    parts.append("</table>")
    return "\n".join(parts)


def _render_validation_table(validation_results: list[Any] | None) -> str:
    """Render validation rule results as an HTML table."""
    if not validation_results:
        return ""
    parts = [f'<h4 style="{_S_H4}">Validation Rules</h4>']
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(
        f'<tr><th style="{_S_TH}">Rule</th>'
        f'<th style="{_S_TH}">Expression</th>'
        f'<th style="{_S_TH}">Severity</th>'
        f'<th style="{_S_TH}">Passed</th>'
        f'<th style="{_S_TH}">Failed</th></tr>'
    )
    for vr in validation_results:
        rule_name = html.escape(str(getattr(vr, "rule_name", "")))
        expr = html.escape(str(getattr(vr, "expression", "")))
        severity = html.escape(str(getattr(vr, "severity", "")))
        passed = getattr(vr, "rows_passed", 0)
        failed = getattr(vr, "rows_failed", 0)
        failed_style = _S_TD
        if failed > 0:
            failed_style = f"{_S_TD};color:#9b2c2c;font-weight:600"
        parts.append(
            f'<tr><td style="{_S_TD}">{rule_name}</td>'
            f'<td style="{_S_TD}"><code>{expr}</code></td>'
            f'<td style="{_S_TD}">{severity}</td>'
            f'<td style="{_S_TD}">{passed:,}</td>'
            f'<td style="{failed_style}">{failed:,}</td></tr>'
        )
    parts.append("</table>")
    return "\n".join(parts)


def _render_assertion_table(assertion_results: list[Any] | None) -> str:
    """Render post-execution assertion results as an HTML table."""
    if not assertion_results:
        return ""
    parts = [f'<h4 style="{_S_H4}">Assertions</h4>']
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(
        f'<tr><th style="{_S_TH}">Type</th>'
        f'<th style="{_S_TH}">Severity</th>'
        f'<th style="{_S_TH}">Result</th>'
        f'<th style="{_S_TH}">Details</th></tr>'
    )
    for ar in assertion_results:
        atype = html.escape(str(getattr(ar, "assertion_type", "")))
        severity = html.escape(str(getattr(ar, "severity", "")))
        passed = getattr(ar, "passed", False)
        details = html.escape(str(getattr(ar, "details", "")))
        result_badge = _status_badge("success" if passed else "failure")
        parts.append(
            f'<tr><td style="{_S_TD}">{atype}</td>'
            f'<td style="{_S_TD}">{severity}</td>'
            f'<td style="{_S_TD}">{result_badge}</td>'
            f'<td style="{_S_TD}">{details}</td></tr>'
        )
    parts.append("</table>")
    return "\n".join(parts)


def _render_schema_table(output_schema: list[tuple[str, str]] | None) -> str:
    """Render output schema as an HTML table."""
    if not output_schema:
        return ""
    parts = [f'<h4 style="{_S_H4}">Output Schema</h4>']
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(f'<tr><th style="{_S_TH}">Column</th><th style="{_S_TH}">Type</th></tr>')
    for col_name, col_type in output_schema:
        parts.append(
            f'<tr><td style="{_S_TD}">{html.escape(str(col_name))}</td>'
            f'<td style="{_S_TD}"><code>{html.escape(str(col_type))}</code></td></tr>'
        )
    parts.append("</table>")
    return "\n".join(parts)


def _render_sample_table(
    samples: dict[str, list[dict[str, Any]]] | None,
    key: str,
    title: str | None = None,
) -> str:
    """Render a data sample as an HTML table.

    Args:
        samples: Combined samples dict from ThreadResult.
        key: Sample category key (``"output"`` or ``"quarantine"``).
        title: Optional section title override.
    """
    if not samples or key not in samples:
        return ""
    rows = samples[key]
    if not rows:
        return ""
    heading = title or f"Data Sample ({key.title()})"
    parts = [f'<h4 style="{_S_H4}">{html.escape(heading)}</h4>']
    # Build table from list of dicts
    columns = list(rows[0].keys())
    parts.append(f'<table style="{_S_TABLE}">')
    header = f'<th style="{_S_TH}">#</th>'
    header += "".join(f'<th style="{_S_TH}">{html.escape(str(c))}</th>' for c in columns)
    parts.append(f"<tr>{header}</tr>")
    for idx, row in enumerate(rows, 1):
        cells = f'<td style="{_S_TD}">{idx}</td>'
        for col in columns:
            val = str(row.get(col, ""))
            # Truncate long values for display
            if len(val) > 80:
                val = val[:77] + "\u2026"
            cells += f'<td style="{_S_TD}">{html.escape(val)}</td>'
        parts.append(f"<tr>{cells}</tr>")
    parts.append("</table>")
    return "\n".join(parts)


def _render_watermark_section(telemetry: Any) -> str:
    """Render watermark state for incremental threads."""
    wm_col = getattr(telemetry, "watermark_column", None)
    if not wm_col:
        return ""
    parts = [f'<h4 style="{_S_H4}">Watermark State</h4>']
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(f'<tr><th style="{_S_TH}">Property</th><th style="{_S_TH}">Value</th></tr>')
    parts.append(
        f'<tr><td style="{_S_TD}">Column</td>'
        f'<td style="{_S_TD}">{html.escape(str(wm_col))}</td></tr>'
    )
    prev = getattr(telemetry, "watermark_previous_value", None)
    new = getattr(telemetry, "watermark_new_value", None)
    first = getattr(telemetry, "watermark_first_run", False)
    if first:
        parts.append(
            f'<tr><td style="{_S_TD}">Previous</td><td style="{_S_TD}"><em>first run</em></td></tr>'
        )
    elif prev is not None:
        parts.append(
            f'<tr><td style="{_S_TD}">Previous</td>'
            f'<td style="{_S_TD}">{html.escape(str(prev))}</td></tr>'
        )
    if new is not None:
        parts.append(
            f'<tr><td style="{_S_TD}">New</td><td style="{_S_TD}">{html.escape(str(new))}</td></tr>'
        )
    persisted = getattr(telemetry, "watermark_persisted", False)
    parts.append(
        f'<tr><td style="{_S_TD}">Persisted</td>'
        f'<td style="{_S_TD}">{"yes" if persisted else "no"}</td></tr>'
    )
    parts.append("</table>")
    return "\n".join(parts)


def _render_cdc_section(telemetry: Any) -> str:
    """Render CDC breakdown for merge writes."""
    inserts = getattr(telemetry, "cdc_inserts", None)
    updates = getattr(telemetry, "cdc_updates", None)
    deletes = getattr(telemetry, "cdc_deletes", None)
    if inserts is None and updates is None and deletes is None:
        return ""
    parts = [f'<h4 style="{_S_H4}">CDC Breakdown</h4>']
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(f'<tr><th style="{_S_TH}">Operation</th><th style="{_S_TH}">Rows</th></tr>')
    if inserts is not None:
        parts.append(
            f'<tr><td style="{_S_TD}">Inserts</td><td style="{_S_TD}">{inserts:,}</td></tr>'
        )
    if updates is not None:
        parts.append(
            f'<tr><td style="{_S_TD}">Updates</td><td style="{_S_TD}">{updates:,}</td></tr>'
        )
    if deletes is not None:
        parts.append(
            f'<tr><td style="{_S_TD}">Deletes</td><td style="{_S_TD}">{deletes:,}</td></tr>'
        )
    parts.append("</table>")
    return "\n".join(parts)


def _render_row_counts(telemetry: Any, rows_written: int = 0) -> str:
    """Render rows read vs written summary."""
    rows_read = getattr(telemetry, "rows_read", 0)
    rows_quarantined = getattr(telemetry, "rows_quarantined", 0)
    if rows_read == 0 and rows_written == 0:
        return ""
    parts = [f'<h4 style="{_S_H4}">Row Counts</h4>']
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(f'<tr><th style="{_S_TH}">Metric</th><th style="{_S_TH}">Count</th></tr>')
    parts.append(
        f'<tr><td style="{_S_TD}">Rows read</td><td style="{_S_TD}">{rows_read:,}</td></tr>'
    )
    rows_after = getattr(telemetry, "rows_after_transforms", 0)
    if rows_after > 0:
        parts.append(
            f'<tr><td style="{_S_TD}">After transforms</td>'
            f'<td style="{_S_TD}">{rows_after:,}</td></tr>'
        )
    if rows_quarantined > 0:
        parts.append(
            f'<tr><td style="{_S_TD}">Quarantined</td>'
            f'<td style="{_S_TD}">{rows_quarantined:,}</td></tr>'
        )
    parts.append(
        f'<tr><td style="{_S_TD}">Rows written</td><td style="{_S_TD}">{rows_written:,}</td></tr>'
    )
    parts.append("</table>")
    return "\n".join(parts)


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


def _render_execute_thread_detail(
    tr: Any,
    telemetry: Any | None,
    thread_model: Any | None,
) -> str:
    """Render a single thread's collapsible detail section for execute mode."""
    name = html.escape(getattr(tr, "thread_name", ""))
    tr_status = str(getattr(tr, "status", ""))
    rows = getattr(tr, "rows_written", 0)
    dur_text = ""
    if telemetry:
        span = getattr(telemetry, "span", None)
        if span:
            start = getattr(span, "start_time", None)
            end = getattr(span, "end_time", None)
            if start and end:
                dur_text = _format_duration(int((end - start).total_seconds() * 1000))

    # Auto-expand failed threads (DEC-001)
    open_attr = " open" if tr_status == "failure" else ""
    parts = [f'<details style="{_S_DETAILS}"{open_attr}>']
    summary_detail = f"{rows:,} rows"
    if dur_text:
        summary_detail += f", {dur_text}"
    parts.append(
        f'<summary style="{_S_SUMMARY}">'
        f"{_status_badge(tr_status)} {name} &mdash; {summary_detail}"
        "</summary>"
    )
    parts.append(f'<div style="{_S_DETAIL_BODY}">')

    # Error banner for failed threads (DEC-014)
    error = getattr(tr, "error", None)
    if error:
        parts.append(f'<div style="{_S_ERROR_BOX}">{html.escape(str(error))}</div>')

    # Thread flow SVG
    if thread_model is not None:
        try:
            flow_svg = render_flow_svg(thread_model)
            parts.append(f"<div>{flow_svg}</div>")
        except Exception:
            pass  # SVG is best-effort; don't break the report

    # Data flow waterfall SVG
    if telemetry:
        try:
            wf_svg = render_waterfall_svg(tr, telemetry, mode="execute")
            parts.append(f"<div>{wf_svg}</div>")
        except Exception:
            pass  # SVG is best-effort; don't break the report

    # Validation rules
    if telemetry:
        parts.append(_render_validation_table(getattr(telemetry, "validation_results", None)))

    # Assertions
    if telemetry:
        parts.append(_render_assertion_table(getattr(telemetry, "assertion_results", None)))

    # Output schema
    parts.append(_render_schema_table(getattr(tr, "output_schema", None)))

    # Data sample (output)
    parts.append(_render_sample_table(getattr(tr, "samples", None), "output"))

    # Quarantine sample
    parts.append(
        _render_sample_table(getattr(tr, "samples", None), "quarantine", "Quarantine Sample")
    )

    # Watermark state
    if telemetry:
        parts.append(_render_watermark_section(telemetry))

    # CDC breakdown
    if telemetry:
        parts.append(_render_cdc_section(telemetry))

    # Row counts
    if telemetry:
        parts.append(_render_row_counts(telemetry, rows_written=rows))

    parts.append("</div></details>")
    return "\n".join(parts)


def _render_execute_html(result: Any) -> str:
    """Render a styled HTML report for execute mode results."""
    parts: list[str] = [f'<div style="{_S_CONTAINER}">']

    status = str(getattr(result, "status", ""))
    config_type = str(getattr(result, "config_type", ""))
    config_type_esc = html.escape(config_type)
    config_name = html.escape(str(getattr(result, "config_name", "")))
    duration_ms = getattr(result, "duration_ms", 0) or 0
    warnings: list[str] = getattr(result, "warnings", []) or []
    detail = getattr(result, "detail", None)
    telemetry = getattr(result, "telemetry", None)
    resolved = getattr(result, "_resolved_threads", None) or {}

    parts.append(f'<h3 style="{_S_H3}">Execution Summary</h3>')

    # Summary table
    thread_results = _collect_thread_results(result)
    total_rows = _count_total_rows(result)
    parts.append(f'<table style="{_S_TABLE}">')
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Status</strong></td>'
        f'<td style="{_S_TD}">{_status_badge(status)}</td></tr>'
    )
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Scope</strong></td>'
        f'<td style="{_S_TD}">{config_type_esc}: {config_name}</td></tr>'
    )
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Duration</strong></td>'
        f'<td style="{_S_TD}">{_format_duration(duration_ms)}</td></tr>'
    )
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Rows Written</strong></td>'
        f'<td style="{_S_TD}">{total_rows:,}</td></tr>'
    )
    if len(thread_results) > 1:
        parts.append(
            f'<tr><td style="{_S_TD}"><strong>Threads</strong></td>'
            f'<td style="{_S_TD}">{len(thread_results)}</td></tr>'
        )
    parts.append("</table>")

    # Resolved parameters (from outermost telemetry)
    if telemetry:
        parts.append(_render_params_table(getattr(telemetry, "resolved_params", None)))

    # Annotated DAG (weave/loom — requires execution plan)
    plans: list[Any] = getattr(result, "execution_plan", None) or []
    if plans and thread_results:
        try:
            tr_map = {getattr(t, "thread_name", ""): t for t in thread_results}
            dag_svg = render_annotated_dag_svg(plans[0], tr_map, resolved)
            parts.append(f'<h4 style="{_S_H4}">Execution DAG</h4>')
            parts.append(f"<div>{dag_svg}</div>")
        except Exception:
            pass  # SVG is best-effort; don't break the report

    # Execution timeline (weave/loom)
    if config_type == "weave" and detail:
        weave_telem = telemetry
        hook_results = getattr(weave_telem, "hook_results", None)
        lookup_results = getattr(weave_telem, "lookup_results", None)
        try:
            tl_svg = render_timeline_svg(detail, hook_results, lookup_results)
            parts.append(f'<h4 style="{_S_H4}">Execution Timeline</h4>')
            parts.append(f"<div>{tl_svg}</div>")
        except Exception:
            pass  # SVG is best-effort; don't break the report
    elif config_type == "loom" and detail:
        # Render timeline for each weave in the loom
        weave_results = getattr(detail, "weave_results", []) or []
        loom_telem = telemetry
        weave_telem_map = getattr(loom_telem, "weave_telemetry", {}) or {}
        for wr in weave_results:
            wname = getattr(wr, "weave_name", "")
            wt = weave_telem_map.get(wname)
            hook_results = getattr(wt, "hook_results", None) if wt else None
            lookup_results = getattr(wt, "lookup_results", None) if wt else None
            try:
                tl_svg = render_timeline_svg(wr, hook_results, lookup_results)
                wname_esc = html.escape(wname)
                parts.append(f'<h4 style="{_S_H4}">Timeline: {wname_esc}</h4>')
                parts.append(f"<div>{tl_svg}</div>")
            except Exception:
                pass  # SVG is best-effort; don't break the report

    # Thread results summary table
    if thread_results:
        parts.append(f'<h4 style="{_S_H4}">Thread Results</h4>')
        parts.append(f'<table style="{_S_TABLE}">')
        parts.append(
            f'<tr><th style="{_S_TH}">Thread</th>'
            f'<th style="{_S_TH}">Status</th>'
            f'<th style="{_S_TH}">Rows</th>'
            f'<th style="{_S_TH}">Duration</th>'
            f'<th style="{_S_TH}">Write Mode</th>'
            f'<th style="{_S_TH}">Load Mode</th></tr>'
        )
        for tr in thread_results:
            name = html.escape(getattr(tr, "thread_name", ""))
            tr_status = str(getattr(tr, "status", ""))
            rows = getattr(tr, "rows_written", 0)
            write_mode = html.escape(str(getattr(tr, "write_mode", "") or ""))
            telem = getattr(tr, "telemetry", None)
            load_mode = html.escape(str(getattr(telem, "load_mode", "") or ""))
            dur_text = ""
            if telem:
                span = getattr(telem, "span", None)
                if span:
                    start = getattr(span, "start_time", None)
                    end = getattr(span, "end_time", None)
                    if start and end:
                        dur_text = _format_duration(int((end - start).total_seconds() * 1000))
            parts.append(
                f'<tr><td style="{_S_TD}">{name}</td>'
                f'<td style="{_S_TD}">{_status_badge(tr_status)}</td>'
                f'<td style="{_S_TD}">{rows:,}</td>'
                f'<td style="{_S_TD}">{dur_text}</td>'
                f'<td style="{_S_TD}">{write_mode}</td>'
                f'<td style="{_S_TD}">{load_mode}</td></tr>'
            )
        parts.append("</table>")

    # Weave-level context: variables, hooks, lookups
    if config_type == "weave" and telemetry:
        parts.append(_render_variables_table(getattr(telemetry, "variables", None)))
        parts.append(_render_hook_results_table(getattr(telemetry, "hook_results", None)))
        parts.append(_render_lookup_results_table(getattr(telemetry, "lookup_results", None)))

    # Per-thread detail sections (collapsible)
    if config_type == "loom" and detail:
        # Two-level collapse for loom: weave → thread (DEC-010)
        weave_results = getattr(detail, "weave_results", []) or []
        loom_telem = telemetry
        weave_telem_map = getattr(loom_telem, "weave_telemetry", {}) or {}
        for wr in weave_results:
            wname = getattr(wr, "weave_name", "")
            wr_status = str(getattr(wr, "status", ""))
            open_attr = " open" if wr_status == "failure" else ""
            wname_esc = html.escape(wname)
            parts.append(f'<details style="{_S_DETAILS}"{open_attr}>')
            parts.append(
                f'<summary style="{_S_SUMMARY}">'
                f"{_status_badge(wr_status)} Weave: {wname_esc}</summary>"
            )
            parts.append(f'<div style="{_S_DETAIL_BODY}">')
            # Weave-level sections
            wt = weave_telem_map.get(wname)
            if wt:
                parts.append(_render_variables_table(getattr(wt, "variables", None)))
                parts.append(_render_hook_results_table(getattr(wt, "hook_results", None)))
                parts.append(_render_lookup_results_table(getattr(wt, "lookup_results", None)))
            # Per-thread details within weave
            wt_thread_telem = getattr(wt, "thread_telemetry", {}) or {} if wt else {}
            for wtr in getattr(wr, "thread_results", []):
                tname = getattr(wtr, "thread_name", "")
                t_telem = wt_thread_telem.get(tname) or getattr(wtr, "telemetry", None)
                t_model = resolved.get(tname)
                parts.append(_render_execute_thread_detail(wtr, t_telem, t_model))
            parts.append("</div></details>")
    else:
        # Weave or thread: flat per-thread details
        for tr in thread_results:
            tname = getattr(tr, "thread_name", "")
            t_telem = getattr(tr, "telemetry", None)
            t_model = resolved.get(tname)
            parts.append(_render_execute_thread_detail(tr, t_telem, t_model))

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
    telemetry = getattr(result, "telemetry", None)
    resolved = getattr(result, "_resolved_threads", None) or {}

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

    # Resolved parameters
    if telemetry:
        parts.append(_render_params_table(getattr(telemetry, "resolved_params", None)))

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

    # Per-thread validation rules and assertions from config (DEC-012)
    for tname, thread_model in resolved.items():
        tname_esc = html.escape(tname)
        validations = getattr(thread_model, "validations", None) or []
        assertions = getattr(thread_model, "assertions", None) or []
        if not validations and not assertions:
            continue
        parts.append(f'<details style="{_S_DETAILS}">')
        parts.append(f'<summary style="{_S_SUMMARY}">{tname_esc}</summary>')
        parts.append(f'<div style="{_S_DETAIL_BODY}">')
        # Validation rules defined in config
        if validations:
            parts.append(f'<h4 style="{_S_H4}">Validation Rules</h4>')
            parts.append(f'<table style="{_S_TABLE}">')
            parts.append(
                f'<tr><th style="{_S_TH}">Rule</th>'
                f'<th style="{_S_TH}">Expression</th>'
                f'<th style="{_S_TH}">Severity</th></tr>'
            )
            for v in validations:
                rule_name = html.escape(str(getattr(v, "name", "")))
                expr = html.escape(str(getattr(v, "expression", "")))
                severity = html.escape(str(getattr(v, "severity", "")))
                parts.append(
                    f'<tr><td style="{_S_TD}">{rule_name}</td>'
                    f'<td style="{_S_TD}"><code>{expr}</code></td>'
                    f'<td style="{_S_TD}">{severity}</td></tr>'
                )
            parts.append("</table>")
        # Assertion definitions from config
        if assertions:
            parts.append(f'<h4 style="{_S_H4}">Assertions</h4>')
            parts.append(f'<table style="{_S_TABLE}">')
            parts.append(
                f'<tr><th style="{_S_TH}">Type</th>'
                f'<th style="{_S_TH}">Severity</th>'
                f'<th style="{_S_TH}">Details</th></tr>'
            )
            for a in assertions:
                atype = html.escape(str(getattr(a, "type", "")))
                severity = html.escape(str(getattr(a, "severity", "")))
                # Build detail string from assertion config
                detail_parts = []
                for attr in ("columns", "expression", "min", "max"):
                    val = getattr(a, attr, None)
                    if val is not None:
                        detail_parts.append(f"{attr}={val}")
                details = html.escape(", ".join(detail_parts)) if detail_parts else ""
                parts.append(
                    f'<tr><td style="{_S_TD}">{atype}</td>'
                    f'<td style="{_S_TD}">{severity}</td>'
                    f'<td style="{_S_TD}">{details}</td></tr>'
                )
            parts.append("</table>")
        parts.append("</div></details>")

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
    duration_ms = getattr(result, "duration_ms", 0) or 0
    preview_data: dict[str, Any] = getattr(result, "preview_data", None) or {}
    preview_meta: dict[str, dict[str, Any]] = getattr(result, "_preview_metadata", None) or {}
    telemetry = getattr(result, "telemetry", None)
    resolved = getattr(result, "_resolved_threads", None) or {}
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
    parts.append(
        f'<tr><td style="{_S_TD}"><strong>Duration</strong></td>'
        f'<td style="{_S_TD}">{_format_duration(duration_ms)}</td></tr>'
    )
    if preview_data:
        parts.append(
            f'<tr><td style="{_S_TD}"><strong>Threads</strong></td>'
            f'<td style="{_S_TD}">{len(preview_data)}</td></tr>'
        )
    parts.append("</table>")

    # Resolved parameters
    if telemetry:
        parts.append(_render_params_table(getattr(telemetry, "resolved_params", None)))

    # Output shape table
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

    # Per-thread detail sections
    for name in preview_data:
        name_esc = html.escape(name)
        meta = preview_meta.get(name, {})
        thread_model = resolved.get(name)

        parts.append(f'<details style="{_S_DETAILS}">')
        parts.append(f'<summary style="{_S_SUMMARY}">{name_esc}</summary>')
        parts.append(f'<div style="{_S_DETAIL_BODY}">')

        # Thread flow SVG
        if thread_model is not None:
            try:
                flow_svg = render_flow_svg(thread_model)
                parts.append(f"<div>{flow_svg}</div>")
            except Exception:
                pass  # SVG is best-effort; don't break the report

        # Data flow waterfall SVG (partial: preview mode)
        if meta:
            try:
                row_count = 0
                df = preview_data.get(name)
                if df is not None:
                    with contextlib.suppress(Exception):
                        row_count = df.count()
                pt = types.SimpleNamespace(
                    rows_read=row_count,
                    rows_after_transforms=row_count,
                    rows_quarantined=0,
                )
                pr = types.SimpleNamespace(rows_written=0)
                wf_svg = render_waterfall_svg(pr, pt, mode="preview")
                parts.append(f"<div>{wf_svg}</div>")
            except Exception:
                pass  # SVG is best-effort; don't break the report

        # Variables (weave-level, from telemetry)
        if telemetry:
            parts.append(_render_variables_table(getattr(telemetry, "variables", None)))

        # Output schema (from preview metadata per AMD-001)
        output_schema = meta.get("output_schema")
        parts.append(_render_schema_table(output_schema))

        # Data sample (from preview metadata per AMD-001, DEC-017: no annotation)
        samples = meta.get("samples")
        parts.append(_render_sample_table(samples, "output", "Data Sample"))

        parts.append("</div></details>")

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
