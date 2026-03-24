"""Tests for NamingPattern and NamingConfig models."""

import pytest
from pydantic import ValidationError

from weevr.model.column_set import ReservedWordConfig
from weevr.model.naming import NamingConfig, NamingPattern


class TestNamingPattern:
    """Test NamingPattern enum values."""

    def test_snake_case_value(self):
        """SNAKE_CASE value is 'snake_case'."""
        assert NamingPattern.SNAKE_CASE == "snake_case"

    def test_camel_case_value(self):
        """CAMEL_CASE value is 'camelCase'."""
        assert NamingPattern.CAMEL_CASE == "camelCase"

    def test_pascal_case_value(self):
        """PASCAL_CASE value is 'PascalCase'."""
        assert NamingPattern.PASCAL_CASE == "PascalCase"

    def test_upper_snake_case_value(self):
        """UPPER_SNAKE_CASE value is 'UPPER_SNAKE_CASE'."""
        assert NamingPattern.UPPER_SNAKE_CASE == "UPPER_SNAKE_CASE"

    def test_title_snake_case_value(self):
        """TITLE_SNAKE_CASE value is 'Title_Snake_Case'."""
        assert NamingPattern.TITLE_SNAKE_CASE == "Title_Snake_Case"

    def test_title_case_value(self):
        """TITLE_CASE value is 'Title Case'."""
        assert NamingPattern.TITLE_CASE == "Title Case"

    def test_lowercase_value(self):
        """LOWERCASE value is 'lowercase'."""
        assert NamingPattern.LOWERCASE == "lowercase"

    def test_uppercase_value(self):
        """UPPERCASE value is 'UPPERCASE'."""
        assert NamingPattern.UPPERCASE == "UPPERCASE"

    def test_none_value(self):
        """NONE value is 'none'."""
        assert NamingPattern.NONE == "none"

    def test_kebab_case_value(self):
        """KEBAB_CASE value is 'kebab-case'."""
        assert NamingPattern.KEBAB_CASE == "kebab-case"

    def test_kebab_case_is_str(self):
        """KEBAB_CASE is usable as a plain string."""
        assert str(NamingPattern.KEBAB_CASE) == "kebab-case"


class TestNamingConfig:
    """Test NamingConfig construction and validation."""

    def test_defaults(self):
        """All fields default to documented values."""
        cfg = NamingConfig()
        assert cfg.columns is None
        assert cfg.tables is None
        assert cfg.exclude == []
        assert cfg.on_collision == "error"
        assert cfg.reserved_words is None

    def test_columns_pattern(self):
        """columns accepts a NamingPattern."""
        cfg = NamingConfig(columns=NamingPattern.SNAKE_CASE)
        assert cfg.columns == NamingPattern.SNAKE_CASE

    def test_tables_pattern(self):
        """tables accepts a NamingPattern."""
        cfg = NamingConfig(tables=NamingPattern.PASCAL_CASE)
        assert cfg.tables == NamingPattern.PASCAL_CASE

    def test_kebab_case_pattern(self):
        """columns accepts KEBAB_CASE pattern."""
        cfg = NamingConfig(columns=NamingPattern.KEBAB_CASE)
        assert cfg.columns == "kebab-case"

    def test_exclude_list(self):
        """exclude stores a list of patterns."""
        cfg = NamingConfig(exclude=["id", "ts_*"])
        assert cfg.exclude == ["id", "ts_*"]

    def test_on_collision_default(self):
        """on_collision defaults to 'error'."""
        cfg = NamingConfig()
        assert cfg.on_collision == "error"

    def test_on_collision_suffix(self):
        """on_collision accepts 'suffix'."""
        cfg = NamingConfig(on_collision="suffix")
        assert cfg.on_collision == "suffix"

    def test_on_collision_error_explicit(self):
        """on_collision accepts 'error' explicitly."""
        cfg = NamingConfig(on_collision="error")
        assert cfg.on_collision == "error"

    def test_reserved_words_none_by_default(self):
        """reserved_words defaults to None."""
        cfg = NamingConfig()
        assert cfg.reserved_words is None

    def test_reserved_words_config(self):
        """reserved_words accepts a ReservedWordConfig."""
        rw = ReservedWordConfig(strategy="prefix", prefix="col_")
        cfg = NamingConfig(reserved_words=rw)
        assert cfg.reserved_words is not None
        assert cfg.reserved_words.strategy == "prefix"
        assert cfg.reserved_words.prefix == "col_"

    def test_reserved_words_defaults_propagate(self):
        """ReservedWordConfig nested defaults are preserved."""
        cfg = NamingConfig(reserved_words=ReservedWordConfig())
        assert cfg.reserved_words is not None
        assert cfg.reserved_words.strategy == "quote"
        assert cfg.reserved_words.prefix == "_"

    def test_frozen(self):
        """NamingConfig is immutable."""
        cfg = NamingConfig()
        with pytest.raises(ValidationError):
            cfg.columns = NamingPattern.SNAKE_CASE  # type: ignore[misc]

    def test_frozen_on_collision(self):
        """NamingConfig.on_collision is immutable."""
        cfg = NamingConfig()
        with pytest.raises(ValidationError):
            cfg.on_collision = "suffix"  # type: ignore[misc]
