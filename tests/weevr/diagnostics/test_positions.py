"""Position maps place config diagnostics at the line the author wrote."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from weevr.config import load_config
from weevr.diagnostics import diagnostic
from weevr.diagnostics.positions import (
    PositionMap,
    _PositionLoader,
    build_position_map,
    fill_lines,
    parse_pointer,
    place,
    pointed,
    read_position_map,
)
from weevr.errors import ModelValidationError

FIXTURES = Path(__file__).parent / "fixtures" / "positions"


def _map(name: str) -> PositionMap:
    return build_position_map((FIXTURES / name).read_text())


GOLDEN: dict[str, dict[str, tuple[int, int]]] = {
    "block.yaml": {
        "": (1, 1),
        "/config_version": (1, 1),
        "/sources": (2, 1),
        "/sources/orders": (3, 3),
        "/sources/orders/alias": (5, 5),
        "/steps": (6, 1),
        "/steps/0": (7, 5),
        "/steps/0/filter/expr": (8, 7),
        "/steps/1": (9, 5),
        "/steps/1/derive/columns/total": (11, 9),
    },
    "flow.yaml": {
        "/sources": (2, 1),
        "/sources/orders": (2, 11),
        "/sources/orders/type": (2, 20),
        "/sources/orders/alias": (2, 33),
        "/steps/0": (3, 9),
        "/steps/0/filter/expr": (3, 19),
        "/steps/1": (3, 41),
        "/steps/1/drop/columns/1": (3, 62),
    },
    "anchors.yaml": {
        "/defaults/type": (3, 3),
        # An alias is placed where it is written ...
        "/sources/orders": (6, 3),
        # ... and what it holds, at the anchor that defines it.
        "/sources/orders/type": (3, 3),
        # A merged key is placed at the anchor; an explicit one overrides it.
        "/sources/returns/type": (3, 3),
        "/sources/returns/alias": (9, 5),
        "/tags/0": (11, 5),
        "/tags/1": (12, 5),
    },
    "scalars.yaml": {
        "/quoted key": (2, 1),
        "/single~1quoted": (3, 1),
        "/description": (4, 1),
        "/next": (7, 1),
        "/folded": (8, 1),
        "/last": (11, 1),
    },
}


@pytest.mark.parametrize(
    ("name", "pointer", "position"),
    [(name, ptr, pos) for name, golden in GOLDEN.items() for ptr, pos in golden.items()],
)
def test_golden_positions(name: str, pointer: str, position: tuple[int, int]) -> None:
    assert _map(name).positions[pointer] == position


@pytest.mark.parametrize("name", list(GOLDEN))
def test_position_loader_constructs_what_safe_load_does(name: str) -> None:
    """Giving aliases their own nodes does not change the loaded values."""
    text = (FIXTURES / name).read_text()
    assert yaml.load(text, Loader=_PositionLoader) == yaml.safe_load(text)  # noqa: S506


def test_list_lengths_and_substitutions() -> None:
    assert _map("block.yaml").lengths["/steps"] == 2
    assert _map("scalars.yaml").substituted == frozenset({"/last"})


def test_empty_document() -> None:
    assert build_position_map("") == PositionMap()


def test_unreadable_file_has_no_map(tmp_path: Path) -> None:
    assert read_position_map(str(tmp_path / "missing.yaml")) is None
    assert read_position_map("abfss://ws@onelake/x.thread") is None
    bad = tmp_path / "bad.yaml"
    bad.write_text("a: [1\n")
    assert read_position_map(str(bad)) is None


def test_parse_pointer() -> None:
    assert parse_pointer("") == ()
    assert parse_pointer("/steps/0/a~1b/c~0d") == ("steps", 0, "a/b", "c~d")


class TestPlace:
    """``place`` maps a resolved path to the authored pointer, line and column."""

    def test_value_written_in_the_file(self) -> None:
        diag = place(
            diagnostic("C102", "x", resolved_path="/sources/orders/alias"), _map("block.yaml")
        )
        assert (diag.pointer, diag.line, diag.column) == ("/sources/orders/alias", 5, 5)
        assert not diag.location_fallback

    def test_missing_key_points_at_its_mapping(self) -> None:
        diag = place(
            diagnostic("C101", "x", resolved_path="/sources/orders/key"), _map("block.yaml")
        )
        assert (diag.pointer, diag.line) == ("/sources/orders/key", 3)
        assert not diag.location_fallback

    def test_value_from_elsewhere_keeps_pointer_without_line(self) -> None:
        diag = place(diagnostic("C103", "x", resolved_path="/write/mode"), _map("block.yaml"))
        assert (diag.pointer, diag.line, diag.column) == ("/write/mode", None, None)
        assert diag.location_fallback

    def test_substituted_value_is_placed_and_flagged(self) -> None:
        diag = place(diagnostic("C103", "x", resolved_path="/last"), _map("scalars.yaml"))
        assert (diag.line, diag.location_fallback) == (11, True)

    def test_expanded_list_is_placed_at_the_list(self) -> None:
        resolved: dict[str, Any] = {"steps": [{}, {}, {}, {"derive": {"columns": 1}}]}
        diag = place(
            diagnostic("C102", "x", resolved_path="/steps/3/derive/columns"),
            _map("block.yaml"),
            resolved,
        )
        assert (diag.pointer, diag.line, diag.location_fallback) == ("/steps", 6, True)

    def test_inline_document_is_placed_below_its_entry(self) -> None:
        text = "threads:\n  - name: a\n    sources: 5\n"
        diag = place(
            diagnostic("C102", "x", resolved_path="/sources"),
            build_position_map(text),
            {"name": "a", "sources": 5},
            at=("threads", 0),
        )
        assert (diag.pointer, diag.line, diag.column) == ("/threads/0/sources", 3, 5)

    def test_without_a_map_only_the_pointer_is_set(self) -> None:
        diag = place(diagnostic("C102", "x", resolved_path="/a"), None, at=("threads", 1))
        assert (diag.pointer, diag.line) == ("/threads/1/a", None)

    def test_existing_pointer_is_kept(self) -> None:
        diag = place(diagnostic("C202", "x", pointer="/config_version"), _map("block.yaml"))
        assert (diag.pointer, diag.line) == ("/config_version", 1)


def test_pointed_sets_pointer_below_at() -> None:
    [diag] = pointed([diagnostic("C102", "x", resolved_path="/a")], ("weaves", 2))
    assert diag.pointer == "/weaves/2/a"


def test_fill_lines_only_touches_its_file() -> None:
    file = str(FIXTURES / "block.yaml")
    mine = diagnostic("C202", "x", file=file, pointer="/config_version")
    other = diagnostic("C202", "x", file="other.yaml", pointer="/config_version")
    assert [d.line for d in fill_lines([mine, other], file)] == [1, None]


class TestLoad:
    """Loading a broken project names the authored file and line."""

    def _write(self, path: Path, data: dict[str, Any]) -> Path:
        path.write_text(yaml.safe_dump(data, sort_keys=False))
        return path

    def _thread(self, **extra: Any) -> dict[str, Any]:
        return {
            "config_version": "1.0",
            "sources": {"a": {"type": "delta", "alias": "x.y"}},
            "target": {"alias": "x.z"},
            **extra,
        }

    def test_value_in_the_thread_file(self, tmp_path: Path) -> None:
        thread = self._write(tmp_path / "t.thread", self._thread(write={"mode": "bogus"}))
        with pytest.raises(ModelValidationError) as exc_info:
            load_config(thread)
        [diag] = exc_info.value.diagnostics
        assert (diag.file, diag.pointer, diag.line, diag.column) == (
            str(thread),
            "/write/mode",
            9,
            3,
        )
        assert not diag.location_fallback

    def test_value_inherited_from_the_weave(self, tmp_path: Path) -> None:
        thread = self._write(tmp_path / "t.thread", self._thread())
        weave = self._write(
            tmp_path / "w.weave",
            {
                "config_version": "1.0",
                "name": "w",
                "defaults": {"write": {"mode": "bogus"}},
                "threads": [{"ref": "t.thread"}],
            },
        )
        with pytest.raises(ModelValidationError) as exc_info:
            load_config(weave)
        [diag] = exc_info.value.diagnostics
        assert (diag.file, diag.pointer, diag.line) == (str(thread), "/write/mode", None)
        assert diag.location_fallback

    def test_foreach_steps_fall_back_to_the_list(self, tmp_path: Path) -> None:
        steps = [
            {
                "foreach": {
                    "values": ["a", "b"],
                    "as": "c",
                    "steps": [{"drop": {"columns": ["{c}"]}}],
                }
            },
            {"derive": {"columns": 5}},
        ]
        thread = self._write(tmp_path / "t.thread", self._thread(steps=steps))
        with pytest.raises(ModelValidationError) as exc_info:
            load_config(thread)
        [diag] = exc_info.value.diagnostics
        assert diag.resolved_path == "/steps/2/derive/columns"
        assert (diag.pointer, diag.location_fallback) == ("/steps", True)
        assert diag.line is not None

    def test_version_error_gets_a_line(self, tmp_path: Path) -> None:
        thread = tmp_path / "t.thread"
        thread.write_text("sources: {}\nconfig_version: '9.0'\n")
        with pytest.raises(Exception) as exc_info:
            load_config(thread)
        [diag] = exc_info.value.diagnostics  # type: ignore[attr-defined]
        assert (diag.code, diag.line) == ("C203", 2)
