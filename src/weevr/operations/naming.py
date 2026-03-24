"""Naming normalization functions."""

from __future__ import annotations

import fnmatch
import logging
import re
from typing import TYPE_CHECKING

from weevr.errors.exceptions import ConfigError
from weevr.model.naming import NamingConfig, NamingPattern
from weevr.operations.reserved_words import ANSI_RESERVED_WORDS

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from weevr.model.column_set import ReservedWordConfig

_log = logging.getLogger(__name__)


def _tokenize(name: str) -> list[str]:
    """Split a name into lowercase word tokens.

    Handles underscores, hyphens, spaces, camelCase boundaries,
    and acronym boundaries (e.g., HTTPStatus -> [http, status]).
    """
    # Replace underscores, hyphens, spaces with a uniform separator
    s = re.sub(r"[_\-\s]+", " ", name).strip()
    if not s:
        return []

    # Split on camelCase and acronym boundaries
    # Insert space before: uppercase followed by lowercase (acronym end)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", s)
    # Insert space before: lowercase/digit followed by uppercase
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", s)

    return [t.lower() for t in s.split() if t]


def _snake_case(tokens: list[str]) -> str:
    return "_".join(tokens)


def _camel_case(tokens: list[str]) -> str:
    if not tokens:
        return ""
    return tokens[0] + "".join(t.capitalize() for t in tokens[1:])


def _pascal_case(tokens: list[str]) -> str:
    return "".join(t.capitalize() for t in tokens)


def _upper_snake_case(tokens: list[str]) -> str:
    return "_".join(t.upper() for t in tokens)


def _title_snake_case(tokens: list[str]) -> str:
    return "_".join(t.capitalize() for t in tokens)


def _title_case(tokens: list[str]) -> str:
    return " ".join(t.capitalize() for t in tokens)


def _lowercase(tokens: list[str]) -> str:
    return "".join(tokens)


def _uppercase(tokens: list[str]) -> str:
    return "".join(t.upper() for t in tokens)


def _kebab_case(tokens: list[str]) -> str:
    return "-".join(tokens)


_PATTERN_FUNCS = {
    NamingPattern.SNAKE_CASE: _snake_case,
    NamingPattern.CAMEL_CASE: _camel_case,
    NamingPattern.PASCAL_CASE: _pascal_case,
    NamingPattern.UPPER_SNAKE_CASE: _upper_snake_case,
    NamingPattern.TITLE_SNAKE_CASE: _title_snake_case,
    NamingPattern.TITLE_CASE: _title_case,
    NamingPattern.LOWERCASE: _lowercase,
    NamingPattern.UPPERCASE: _uppercase,
    NamingPattern.KEBAB_CASE: _kebab_case,
}


def normalize_name(name: str, pattern: NamingPattern) -> str:
    """Normalize a name according to the given pattern.

    Args:
        name: The name to normalize.
        pattern: The target naming pattern.

    Returns:
        The normalized name. Returns the original name if pattern is NONE.
    """
    if pattern == NamingPattern.NONE:
        return name
    tokens = _tokenize(name)
    if not tokens:
        return name
    return _PATTERN_FUNCS[pattern](tokens)


def is_excluded(column_name: str, exclude_patterns: list[str]) -> bool:
    """Check if a column name matches any exclusion pattern.

    Args:
        column_name: The column name to check.
        exclude_patterns: List of glob patterns or explicit names.

    Returns:
        True if the column should be excluded from normalization.
    """
    return any(fnmatch.fnmatch(column_name, p) for p in exclude_patterns)


def _apply_reserved_word_protection(
    renames: dict[str, str], config: ReservedWordConfig
) -> dict[str, str]:
    """Apply reserved word protection to a column rename mapping.

    Checks each output column name against the effective reserved word set and
    applies the configured strategy to any conflicts.

    Args:
        renames: Mapping of original column name to normalized output name.
        config: Reserved word configuration with strategy, prefix, extend, and exclude.

    Returns:
        Updated renames dict with reserved word conflicts resolved.

    Raises:
        ConfigError: If ``strategy="error"`` and any output names are reserved words.
    """
    effective_words = (ANSI_RESERVED_WORDS | {w.lower() for w in config.extend}) - {
        w.lower() for w in config.exclude
    }

    if config.strategy == "quote":
        return renames

    conflicts: list[str] = []
    updated: dict[str, str] = {}
    for original, output in renames.items():
        if output.lower() in effective_words:
            if config.strategy == "prefix":
                updated[original] = config.prefix + output
            else:
                # strategy == "error": collect for later reporting
                conflicts.append(output)
                updated[original] = output
        else:
            updated[original] = output

    if conflicts:
        raise ConfigError(f"Column names are SQL reserved words: {', '.join(sorted(conflicts))}")

    return updated


def normalize_columns(df: DataFrame, config: NamingConfig) -> DataFrame:
    """Normalize column names in a DataFrame according to the naming config.

    Args:
        df: Input DataFrame.
        config: Naming configuration with column pattern and exclusions.

    Returns:
        DataFrame with normalized column names.

    Raises:
        ConfigError: If normalization produces duplicate column names and
            ``on_collision`` is ``"error"``.
    """
    if config.columns is None or config.columns == NamingPattern.NONE:
        return df

    if config.columns == NamingPattern.KEBAB_CASE:
        _log.warning("kebab-case column names require backtick-quoting in SQL expressions")

    renames: dict[str, str] = {}
    for col_name in df.columns:
        if is_excluded(col_name, config.exclude):
            renames[col_name] = col_name
        else:
            renames[col_name] = normalize_name(col_name, config.columns)

    # Detect collision groups (preserve source column order via dict iteration order)
    seen: dict[str, list[str]] = {}
    for old, new in renames.items():
        seen.setdefault(new, []).append(old)
    duplicates = {new: sources for new, sources in seen.items() if len(sources) > 1}

    if duplicates:
        if config.on_collision == "error":
            details = "; ".join(f"'{new}' from {srcs}" for new, srcs in duplicates.items())
            raise ConfigError(f"Naming normalization produces duplicate columns: {details}")

        # on_collision == "suffix": keep first occurrence, append _2, _3, ... to the rest
        for base_name, source_cols in duplicates.items():
            for idx, old_col in enumerate(source_cols[1:], start=2):
                suffixed = f"{base_name}_{idx}"
                _log.warning(
                    "Column '%s' normalises to '%s' (already taken); renamed to '%s'",
                    old_col,
                    base_name,
                    suffixed,
                )
                renames[old_col] = suffixed

    if config.reserved_words is not None:
        renames = _apply_reserved_word_protection(renames, config.reserved_words)

    # Rename via toDF() to avoid ambiguous column references when dedup
    # creates output names that collide with other columns' original names.
    return df.toDF(*renames.values())


def normalize_table_name(name: str, config: NamingConfig) -> str:
    """Normalize a table name according to the naming config.

    Args:
        name: The table name to normalize.
        config: Naming configuration with table pattern.

    Returns:
        The normalized table name, or original if no table pattern is set.
    """
    if config.tables is None or config.tables == NamingPattern.NONE:
        return name
    return normalize_name(name, config.tables)
