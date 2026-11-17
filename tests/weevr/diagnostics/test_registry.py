"""Invariants of the diagnostic code registry."""

from __future__ import annotations

import re

import pytest

from weevr.diagnostics import (
    REGISTRY,
    RETIRED_CODES,
    Category,
    Severity,
    Stability,
    diagnostic,
    track_usage,
)
from weevr.diagnostics.registry import RegistryEntry, build_registry

CODE_PATTERN = re.compile(r"^[A-Z]\d{3}$")
NAME_PATTERN = re.compile(r"^[a-z][a-z0-9]*(-[a-z0-9]+)*$")
LETTER_CATEGORY = {
    "C": Category.CONFIG,
    "P": Category.CONFIG,
    "R": Category.CONFIG,
    "D": Category.CONFIG,
    "S": Category.PLATFORM,
    "E": Category.PLATFORM,
    "Q": Category.DATA,
}


@pytest.mark.parametrize("entry", list(REGISTRY.values()), ids=lambda e: e.code)
class TestEveryEntry:
    """Each registered code is well formed and documented."""

    def test_code_is_letter_and_three_digits(self, entry: RegistryEntry) -> None:
        assert CODE_PATTERN.match(entry.code)

    def test_name_is_kebab_case(self, entry: RegistryEntry) -> None:
        assert NAME_PATTERN.match(entry.name)

    def test_category_matches_letter(self, entry: RegistryEntry) -> None:
        assert entry.category is LETTER_CATEGORY[entry.code[0]]

    def test_has_summary_and_anchor(self, entry: RegistryEntry) -> None:
        assert entry.summary.strip()
        assert entry.docs_anchor == entry.code.lower()

    def test_deprecated_entries_name_a_removal_version(self, entry: RegistryEntry) -> None:
        assert (entry.stability is Stability.DEPRECATED) == (entry.removed_in is not None)


def test_codes_and_names_are_unique() -> None:
    codes = [entry.code for entry in REGISTRY.values()]
    names = [entry.name for entry in REGISTRY.values()]
    assert len(codes) == len(set(codes))
    assert len(names) == len(set(names))


def test_retired_codes_are_never_registered() -> None:
    assert not RETIRED_CODES & set(REGISTRY)


def test_generic_fallback_codes_exist() -> None:
    assert REGISTRY["C100"].name == "schema-violation"
    assert REGISTRY["C400"].name == "invalid-config"


def _entry(code: str, name: str) -> RegistryEntry:
    return RegistryEntry(code, name, Category.CONFIG, Severity.ERROR, "A summary.")


class TestBuildRegistry:
    """The registry builder rejects a table that breaks an invariant."""

    def test_duplicate_code(self) -> None:
        with pytest.raises(ValueError, match="C001"):
            build_registry([_entry("C001", "one"), _entry("C001", "two")], frozenset())

    def test_duplicate_name(self) -> None:
        with pytest.raises(ValueError, match="same-name"):
            build_registry([_entry("C001", "same-name"), _entry("C002", "same-name")], frozenset())

    def test_retired_code_reused(self) -> None:
        with pytest.raises(ValueError, match="C009"):
            build_registry([_entry("C009", "back-again")], frozenset({"C009"}))

    def test_malformed_code(self) -> None:
        with pytest.raises(ValueError, match="C01"):
            build_registry([_entry("C01", "short")], frozenset())


class TestFactory:
    """``diagnostic()`` builds a record from a registered code."""

    def test_fills_registry_fields(self) -> None:
        diag = diagnostic("C001", "missing", file="a.thread")
        entry = REGISTRY["C001"]
        assert diag.code == "C001"
        assert diag.name == entry.name
        assert diag.severity is entry.severity
        assert diag.category is entry.category
        assert diag.file == "a.thread"
        assert diag.docs_url is not None
        assert diag.docs_url.endswith("#c001")

    def test_severity_can_be_overridden(self) -> None:
        diag = diagnostic("C001", "missing", severity=Severity.WARNING)
        assert diag.severity is Severity.WARNING

    def test_rejects_unregistered_code(self) -> None:
        with pytest.raises(ValueError, match="Z999"):
            diagnostic("Z999", "nope")


class TestTrackUsage:
    """The usage tracker records which module emitted which code."""

    def test_records_code_and_calling_module(self) -> None:
        with track_usage() as usage:
            diagnostic("C001", "missing")
        assert usage[("C001", __name__)] == 1

    def test_explicit_origin_wins(self) -> None:
        with track_usage() as usage:
            diagnostic("C001", "missing", origin="weevr.model.pipeline")
        assert ("C001", "weevr.model.pipeline") in usage

    def test_records_nothing_outside_the_block(self) -> None:
        with track_usage() as usage:
            pass
        diagnostic("C001", "missing")
        assert not usage
