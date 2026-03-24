"""Tests for ANSI SQL reserved words."""

import pytest

from weevr.operations.reserved_words import ANSI_RESERVED_WORDS


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
