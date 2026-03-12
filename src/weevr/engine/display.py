"""Plan display — DAG visualization and rich rendering for execution plans."""

from __future__ import annotations

from pathlib import Path


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
