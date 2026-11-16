"""The ``Diagnostic`` record and its JSON form."""

from __future__ import annotations

import json

import pytest

from weevr.diagnostics import Category, Diagnostic, Severity, diagnostic


def _full() -> Diagnostic:
    return Diagnostic(
        code="C001",
        name="config-file-not-found",
        severity=Severity.ERROR,
        category=Category.CONFIG,
        message="Config file not found",
        file="weaves/a.thread",
        pointer="/steps/0/filter",
        line=12,
        column=5,
        resolved_path="/steps/0/filter",
        location_fallback=True,
        node="loom/weave/a",
        step_index=0,
        step_type="filter",
        expected=("a", "b"),
        hint="Rename it.",
        docs_url="https://example.test/#c001",
    )


class TestRoundTrip:
    """A diagnostic survives ``to_dict`` → JSON → ``from_dict`` unchanged."""

    def test_full_record(self) -> None:
        diag = _full()
        assert Diagnostic.from_dict(json.loads(json.dumps(diag.to_dict()))) == diag

    def test_minimal_record(self) -> None:
        diag = diagnostic("C001", "missing")
        assert Diagnostic.from_dict(json.loads(json.dumps(diag.to_dict()))) == diag

    def test_dict_has_every_field(self) -> None:
        keys = set(diagnostic("C001", "missing").to_dict())
        assert keys == {
            "code",
            "name",
            "severity",
            "category",
            "message",
            "file",
            "pointer",
            "line",
            "column",
            "resolved_path",
            "location_fallback",
            "node",
            "step_index",
            "step_type",
            "expected",
            "hint",
            "docs_url",
        }

    def test_enums_serialize_as_strings(self) -> None:
        data = _full().to_dict()
        assert data["severity"] == "error"
        assert data["category"] == "config"
        assert data["expected"] == ["a", "b"]


def test_is_immutable() -> None:
    diag = diagnostic("C001", "missing")
    with pytest.raises(AttributeError):
        diag.code = "C002"  # type: ignore[misc]


def test_expected_is_frozen_to_a_tuple() -> None:
    diag = diagnostic("C001", "missing", expected=["x", "y"])
    assert diag.expected == ("x", "y")


def test_located_fills_only_missing_file() -> None:
    diag = diagnostic("C001", "missing")
    assert diag.located("a.thread").file == "a.thread"
    assert diag.located("a.thread").located("b.thread").file == "a.thread"


def test_is_error() -> None:
    assert diagnostic("C001", "missing").is_error
    assert not diagnostic("C001", "missing", severity=Severity.WARNING).is_error
