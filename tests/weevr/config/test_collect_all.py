"""One load reports every broken file in a project."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from weevr.config import load_config, load_resolved
from weevr.config.collect import collecting, combine, skip_on_error
from weevr.diagnostics import REGISTRY, diagnostic
from weevr.errors import (
    ConfigError,
    ConfigParseError,
    ModelValidationError,
    ReferenceResolutionError,
)


def _thread(**extra: Any) -> dict[str, Any]:
    return {
        "config_version": "1.0",
        "sources": {"a": {"type": "delta", "alias": "x.y"}},
        "target": {"alias": "x.z"},
        **extra,
    }


def _write(path: Path, data: dict[str, Any] | str) -> Path:
    path.write_text(data if isinstance(data, str) else yaml.safe_dump(data, sort_keys=False))
    return path


@pytest.fixture
def broken_project(tmp_path: Path) -> Path:
    """A weave whose three threads each break in a different stage."""
    _write(tmp_path / "a_parse.thread", "config_version: '1.0'\nsources: [1, 2\n")
    _write(tmp_path / "b_schema.thread", _thread(write={"mode": "bogus"}))
    _write(
        tmp_path / "c_lookup.thread",
        _thread(steps=[{"resolve": {"name": "k", "lookup": "missing", "match": "m", "pk": "p"}}]),
    )
    _write(tmp_path / "d_ok.thread", _thread())
    return _write(
        tmp_path / "w.weave",
        {
            "config_version": "1.0",
            "threads": [
                {"ref": "a_parse.thread"},
                {"ref": "b_schema.thread"},
                {"ref": "c_lookup.thread"},
                {"ref": "d_ok.thread"},
            ],
        },
    )


@pytest.mark.parametrize("load", [load_config, load_resolved])
def test_every_broken_file_is_reported_once(broken_project: Path, load: Any) -> None:
    with pytest.raises(ConfigError) as exc_info:
        load(broken_project)
    exc = exc_info.value
    by_file = {Path(d.file or "").name: d.code for d in exc.diagnostics}
    assert by_file == {
        "a_parse.thread": "C005",
        "b_schema.thread": "C103",
        "c_lookup.thread": "R120",
    }
    assert all(d.code in REGISTRY for d in exc.diagnostics)


def test_class_is_the_first_diagnostics_stage(broken_project: Path) -> None:
    with pytest.raises(ConfigParseError) as exc_info:
        load_config(broken_project)
    files = [d.file or "" for d in exc_info.value.diagnostics]
    assert files == sorted(files)


def test_a_file_that_fails_to_parse_is_not_checked_further(tmp_path: Path) -> None:
    _write(tmp_path / "t.thread", "config_version: '1.0'\nwrite: {mode: bogus\n")
    weave = _write(
        tmp_path / "w.weave", {"config_version": "1.0", "threads": [{"ref": "t.thread"}]}
    )
    with pytest.raises(ConfigParseError) as exc_info:
        load_config(weave)
    assert [d.code for d in exc_info.value.diagnostics] == ["C005"]


def test_a_single_problem_raises_as_before(tmp_path: Path) -> None:
    weave = _write(
        tmp_path / "w.weave", {"config_version": "1.0", "threads": [{"ref": "nope.thread"}]}
    )
    with pytest.raises(ReferenceResolutionError) as exc_info:
        load_config(weave)
    assert "nope.thread" in exc_info.value.message
    assert [d.code for d in exc_info.value.diagnostics] == ["C301"]


def test_inline_threads_are_collected_too(tmp_path: Path) -> None:
    weave = _write(
        tmp_path / "w.weave",
        {
            "config_version": "1.0",
            "threads": [
                {"name": "one", **_thread(write={"mode": "bogus"})},
                {"name": "two", **_thread(cache="maybe")},
            ],
        },
    )
    with pytest.raises(ModelValidationError) as exc_info:
        load_config(weave)
    assert sorted(d.pointer or "" for d in exc_info.value.diagnostics) == [
        "/threads/0/write/mode",
        "/threads/1/cache",
    ]


class TestCollector:
    """The collector records child errors and combines them at the end."""

    def test_skip_on_error_outside_collecting_raises(self) -> None:
        with pytest.raises(ConfigError), skip_on_error():
            raise ConfigError("boom")

    def test_collecting_raises_everything_recorded(self) -> None:
        with pytest.raises(ConfigError) as exc_info, collecting():
            for code in ("C001", "C004"):
                with skip_on_error():
                    raise ConfigError.from_diagnostic(diagnostic(code, "x", file=f"{code}.y"))
        assert [d.code for d in exc_info.value.diagnostics] == ["C001", "C004"]

    def test_nested_collecting_joins_the_outer(self) -> None:
        with pytest.raises(ConfigError) as exc_info, collecting():
            with skip_on_error():
                raise ConfigError.from_diagnostic(diagnostic("C001", "x"))
            with collecting(), skip_on_error():
                raise ConfigError.from_diagnostic(diagnostic("C004", "y"))
        assert len(exc_info.value.diagnostics) == 2

    def test_stray_error_after_a_skip_reports_the_recorded_problems(self) -> None:
        with pytest.raises(ConfigError) as exc_info, collecting():
            with skip_on_error():
                raise ConfigError.from_diagnostic(diagnostic("C001", "x"))
            raise KeyError("missing child")
        assert isinstance(exc_info.value.__cause__, KeyError)

    def test_stray_error_with_nothing_recorded_propagates(self) -> None:
        with pytest.raises(KeyError), collecting():
            raise KeyError("bug")

    def test_combine_orders_by_file_then_line(self) -> None:
        late = ConfigParseError.from_diagnostic(diagnostic("C005", "x", file="b", line=3))
        early = ModelValidationError(
            "y",
            diagnostics=[
                diagnostic("C103", "y", file="a", line=9),
                diagnostic("C102", "y", file="b", line=1),
            ],
        )
        combined = combine([late, early])
        assert isinstance(combined, ModelValidationError)
        assert [(d.file, d.line) for d in combined.diagnostics] == [("a", 9), ("b", 1), ("b", 3)]

    def test_combine_of_one_is_unchanged(self) -> None:
        error = ConfigError("boom")
        assert combine([error]) is error
