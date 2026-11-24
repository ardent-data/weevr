"""A removed or renamed v1 key names its replacement or the reason it went."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from documents import write_document
from weevr.config import load_config
from weevr.diagnostics import removed_keys
from weevr.diagnostics.removed_keys import REMOVED_KEYS, RemovedKey, removed_key_diagnostics
from weevr.errors import ConfigError, ConfigSchemaError

TABLE = (
    RemovedKey(
        "thread", "/legacy_mode", reason="Incremental loads are set in `load`.", removed_in="2.0"
    ),
    RemovedKey("thread", "/sources/*/table_name", replacement="/sources/*/table", removed_in="2.0"),
    RemovedKey("weave", "/threads/*/old_flag", reason="No longer needed.", removed_in="2.0"),
)


@pytest.fixture
def table(monkeypatch: pytest.MonkeyPatch) -> tuple[RemovedKey, ...]:
    monkeypatch.setattr(removed_keys, "REMOVED_KEYS", TABLE)
    return TABLE


def _thread(**extra: Any) -> dict[str, Any]:
    return {
        "config_version": "2.0",
        "sources": {"a": {"type": "delta", "alias": "x.y"}},
        "target": {"alias": "x.z"},
        **extra,
    }


def test_shipped_table_is_valid() -> None:
    for row in REMOVED_KEYS:
        assert (row.replacement is None) != (row.reason is None)
        assert row.path.startswith("/")


@pytest.mark.parametrize("kind", ["thread", "weave", "loom", "params", "warp"])
def test_root_type_is_renamed_to_kind(kind: str) -> None:
    """Every kind that accepted a root ``type:`` in v1 reports it renamed to ``/kind``."""
    [diag] = removed_key_diagnostics({"type": kind, "config_version": "2.0"}, kind)
    assert (diag.code, diag.pointer, diag.expected) == ("C112", "/type", ("/kind",))


def test_nested_type_is_not_renamed() -> None:
    assert removed_key_diagnostics(_thread(), "thread") == []


def test_row_needs_a_replacement_or_a_reason() -> None:
    with pytest.raises(ValueError):
        RemovedKey("thread", "/x", removed_in="2.0")
    with pytest.raises(ValueError):
        RemovedKey("thread", "/x", replacement="/y", reason="both", removed_in="2.0")


def test_topic_is_not_part_of_the_public_row() -> None:
    assert "topic" not in RemovedKey.__dataclass_fields__


class TestCheck:
    """``removed_key_diagnostics`` walks an authored document for table rows."""

    def test_removed_key(self, table: Any) -> None:
        [diag] = removed_key_diagnostics(_thread(legacy_mode=True), "thread")
        assert (diag.code, diag.pointer) == ("C111", "/legacy_mode")
        assert diag.hint == "Incremental loads are set in `load`."

    def test_renamed_key_under_a_wildcard(self, table: Any) -> None:
        data = _thread(sources={"a": {"type": "delta", "table_name": "t"}})
        [diag] = removed_key_diagnostics(data, "thread")
        assert (diag.code, diag.pointer) == ("C112", "/sources/a/table_name")
        assert diag.expected == ("/sources/a/table",)

    def test_rows_of_other_kinds_are_ignored(self, table: Any) -> None:
        assert removed_key_diagnostics({"old_flag": 1}, "thread") == []

    def test_wildcard_matches_list_items(self, table: Any) -> None:
        data = {"threads": [{"ref": "a.thread"}, {"ref": "b.thread", "old_flag": True}]}
        [diag] = removed_key_diagnostics(data, "weave")
        assert diag.pointer == "/threads/1/old_flag"

    def test_clean_document(self, table: Any) -> None:
        assert removed_key_diagnostics(_thread(), "thread") == []

    def test_no_rows_no_findings(self) -> None:
        assert removed_key_diagnostics(_thread(legacy_mode=True), "thread", table=()) == []


class TestLoad:
    """Loading reports a removed key at its authored location."""

    def test_thread_file(self, table: Any, tmp_path: Path) -> None:
        path = tmp_path / "t.thread"
        write_document(path, yaml.safe_dump(_thread(legacy_mode=True), sort_keys=False))
        with pytest.raises(ConfigSchemaError) as exc_info:
            load_config(path)
        [diag] = exc_info.value.diagnostics
        assert (diag.code, diag.file, diag.pointer, diag.line) == (
            "C111",
            str(path),
            "/legacy_mode",
            9,
        )

    def test_reported_with_other_schema_errors(self, table: Any, tmp_path: Path) -> None:
        path = tmp_path / "t.thread"
        data = _thread(legacy_mode=True)
        data["sources"] = 5
        write_document(path, yaml.safe_dump(data, sort_keys=False))
        with pytest.raises(ConfigError) as exc_info:
            load_config(path)
        assert {d.code for d in exc_info.value.diagnostics} == {"C111", "C102"}

    def test_inline_thread(self, table: Any, tmp_path: Path) -> None:
        weave = tmp_path / "w.weave"
        write_document(
            weave,
            yaml.safe_dump(
                {"config_version": "2.0", "threads": [{"name": "t", **_thread(legacy_mode=1)}]}
            ),
        )
        with pytest.raises(ConfigError) as exc_info:
            load_config(weave)
        [diag] = [d for d in exc_info.value.diagnostics if d.code == "C111"]
        assert (diag.file, diag.pointer) == (str(weave), "/threads/0/legacy_mode")
