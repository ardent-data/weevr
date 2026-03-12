"""Unit tests for the plan display module — DAGDiagram, SVG renderer, and HTML builder."""

from __future__ import annotations

from pathlib import Path

import pytest

from weevr.engine.display import DAGDiagram

_SAMPLE_SVG = '<svg xmlns="http://www.w3.org/2000/svg"><rect/></svg>'


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
