"""Tests for reserved word presets and resolution."""

import pytest

from weevr.model.column_set import ReservedWordConfig, ReservedWordPreset
from weevr.operations.reserved_words import (
    ANSI_RESERVED_WORDS,
    DAX_RESERVED_WORDS,
    M_RESERVED_WORDS,
    PRESET_REGISTRY,
    TSQL_RESERVED_WORDS,
    resolve_effective_words,
)


class TestAnsiReservedWords:
    """Test ANSI_RESERVED_WORDS frozenset."""

    def test_is_frozenset(self):
        """ANSI_RESERVED_WORDS is a frozenset."""
        assert isinstance(ANSI_RESERVED_WORDS, frozenset)

    def test_contains_expected_words(self):
        """ANSI_RESERVED_WORDS contains all expected words."""
        expected_words = {
            "select",
            "from",
            "where",
            "insert",
            "update",
            "delete",
            "table",
            "order",
            "group",
            "by",
            "join",
            "on",
            "and",
            "or",
            "not",
            "null",
            "as",
            "in",
            "between",
            "like",
            "having",
            "union",
            "create",
            "drop",
            "alter",
            "index",
            "grant",
            "revoke",
        }
        assert expected_words.issubset(ANSI_RESERVED_WORDS)

    def test_all_entries_are_lowercase_strings(self):
        """All entries are lowercase strings."""
        for word in ANSI_RESERVED_WORDS:
            assert isinstance(word, str)
            assert word == word.lower()

    def test_frozenset_immutable(self):
        """Frozenset is immutable."""
        with pytest.raises(AttributeError):
            ANSI_RESERVED_WORDS.add("test")  # type: ignore[attr-defined]

    def test_contains_extended_words(self):
        """ANSI_RESERVED_WORDS contains extended SQL words."""
        extended_words = {
            "into",
            "values",
            "set",
            "exists",
            "case",
            "when",
            "then",
            "else",
            "end",
            "is",
            "distinct",
            "all",
            "any",
            "some",
            "primary",
            "foreign",
            "key",
            "references",
            "check",
            "constraint",
            "default",
            "add",
            "column",
            "asc",
            "desc",
            "limit",
            "offset",
            "fetch",
            "cross",
            "inner",
            "outer",
            "left",
            "right",
            "full",
            "natural",
            "using",
            "except",
            "intersect",
            "with",
            "recursive",
            "true",
            "false",
            "current_date",
            "current_time",
            "current_timestamp",
            "current_user",
            "commit",
            "rollback",
            "begin",
            "declare",
            "cursor",
            "open",
            "close",
            "for",
        }
        assert extended_words.issubset(ANSI_RESERVED_WORDS)

    def test_minimum_size(self):
        """ANSI_RESERVED_WORDS has at least ~80 words."""
        assert len(ANSI_RESERVED_WORDS) >= 70


class TestPresetRegistry:
    """Test PRESET_REGISTRY and individual word lists."""

    def test_registry_has_all_presets(self):
        """Registry contains entries for all five preset values."""
        for preset in ReservedWordPreset:
            assert preset.value in PRESET_REGISTRY

    def test_each_preset_is_nonempty_frozenset(self):
        """Each preset maps to a non-empty frozenset of lowercase strings."""
        for name, words in PRESET_REGISTRY.items():
            assert isinstance(words, frozenset), f"{name} is not a frozenset"
            assert len(words) > 0, f"{name} is empty"
            for word in words:
                assert isinstance(word, str), f"{name}: {word!r} is not a str"
                assert word == word.lower(), f"{name}: {word!r} is not lowercase"

    def test_ansi_preset_matches_constant(self):
        """The ansi preset is the same object as ANSI_RESERVED_WORDS."""
        assert PRESET_REGISTRY["ansi"] is ANSI_RESERVED_WORDS

    def test_powerbi_equals_dax_union_m(self):
        """The powerbi preset is the exact union of dax and m."""
        assert PRESET_REGISTRY["powerbi"] == DAX_RESERVED_WORDS | M_RESERVED_WORDS

    def test_dax_contains_dax_specific_words(self):
        """DAX preset contains words unique to DAX (not in ANSI)."""
        dax_specific = {"calculate", "measure", "var", "blank"}
        assert dax_specific.issubset(DAX_RESERVED_WORDS)

    def test_m_contains_m_specific_words(self):
        """M preset contains words unique to M language (not in ANSI)."""
        m_specific = {"each", "let", "otherwise", "try"}
        assert m_specific.issubset(M_RESERVED_WORDS)

    def test_tsql_is_self_contained(self):
        """T-SQL preset includes ANSI overlap words (self-contained)."""
        ansi_core = {"select", "from", "where", "insert", "update", "delete"}
        assert ansi_core.issubset(TSQL_RESERVED_WORDS)

    def test_tsql_contains_tsql_specific_words(self):
        """T-SQL preset contains T-SQL-specific words beyond ANSI."""
        tsql_specific = {"exec", "top", "identity", "pivot", "merge"}
        assert tsql_specific.issubset(TSQL_RESERVED_WORDS)


class TestResolveEffectiveWords:
    """Test resolve_effective_words function."""

    def test_no_preset_returns_ansi(self):
        """When preset is None, returns ANSI words."""
        cfg = ReservedWordConfig()
        result = resolve_effective_words(cfg)
        assert result == ANSI_RESERVED_WORDS

    def test_single_preset_dax(self):
        """Single preset returns only that preset's words."""
        cfg = ReservedWordConfig(preset=["dax"])  # type: ignore[arg-type]
        result = resolve_effective_words(cfg)
        assert result == DAX_RESERVED_WORDS

    def test_multi_preset_union(self):
        """Multiple presets produce the union of their word sets."""
        cfg = ReservedWordConfig(preset=["ansi", "dax"])  # type: ignore[arg-type]
        result = resolve_effective_words(cfg)
        assert result == ANSI_RESERVED_WORDS | DAX_RESERVED_WORDS

    def test_powerbi_same_as_dax_m(self):
        """Preset powerbi produces the same result as [dax, m]."""
        cfg_pbi = ReservedWordConfig(preset=["powerbi"])  # type: ignore[arg-type]
        cfg_dm = ReservedWordConfig(preset=["dax", "m"])  # type: ignore[arg-type]
        assert resolve_effective_words(cfg_pbi) == resolve_effective_words(cfg_dm)

    def test_extend_adds_words(self):
        """extend adds words to the preset base."""
        cfg = ReservedWordConfig(preset=["dax"], extend=["fiscal_year"])  # type: ignore[arg-type]
        result = resolve_effective_words(cfg)
        assert "fiscal_year" in result
        assert DAX_RESERVED_WORDS.issubset(result)

    def test_exclude_removes_words(self):
        """exclude removes words from the preset base."""
        cfg = ReservedWordConfig(preset=["ansi"], exclude=["by"])  # type: ignore[arg-type]
        result = resolve_effective_words(cfg)
        assert "by" not in result
        assert "select" in result

    def test_extend_and_exclude_compose(self):
        """extend and exclude compose on the same preset."""
        cfg = ReservedWordConfig(preset=["ansi"], extend=["custom_word"], exclude=["by"])  # type: ignore[arg-type]
        result = resolve_effective_words(cfg)
        assert "custom_word" in result
        assert "by" not in result
        assert "select" in result

    def test_empty_preset_list(self):
        """Empty preset list produces no base words; only extend applies."""
        cfg = ReservedWordConfig(preset=[], extend=["custom_only"])
        result = resolve_effective_words(cfg)
        assert result == frozenset({"custom_only"})

    def test_redundant_presets_harmless(self):
        """Redundant presets (dax + powerbi) produce same as powerbi."""
        cfg_redundant = ReservedWordConfig(preset=["dax", "powerbi"])  # type: ignore[arg-type]
        cfg_powerbi = ReservedWordConfig(preset=["powerbi"])  # type: ignore[arg-type]
        assert resolve_effective_words(cfg_redundant) == resolve_effective_words(cfg_powerbi)

    def test_no_preset_with_extend(self):
        """Default ANSI + extend when no preset specified."""
        cfg = ReservedWordConfig(extend=["custom_word"])
        result = resolve_effective_words(cfg)
        assert "custom_word" in result
        assert ANSI_RESERVED_WORDS.issubset(result)

    def test_exclude_case_insensitive(self):
        """exclude matches are case-insensitive (lowered)."""
        cfg = ReservedWordConfig(preset=["ansi"], exclude=["SELECT"])  # type: ignore[arg-type]
        result = resolve_effective_words(cfg)
        assert "select" not in result
