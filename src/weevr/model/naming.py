"""Naming normalization models."""

from enum import StrEnum
from typing import Literal

from weevr.model.base import FrozenBase
from weevr.model.column_set import ReservedWordConfig


class NamingPattern(StrEnum):
    """Column and table naming patterns.

    Attributes:
        SNAKE_CASE: ``http_status``
        CAMEL_CASE: ``httpStatus``
        PASCAL_CASE: ``HttpStatus``
        UPPER_SNAKE_CASE: ``HTTP_STATUS``
        TITLE_SNAKE_CASE: ``Http_Status``
        TITLE_CASE: ``Http Status``
        LOWERCASE: ``httpstatus``
        UPPERCASE: ``HTTPSTATUS``
        KEBAB_CASE: ``http-status``
        NONE: Opt-out sentinel — no normalization applied.
    """

    SNAKE_CASE = "snake_case"
    CAMEL_CASE = "camelCase"
    PASCAL_CASE = "PascalCase"
    UPPER_SNAKE_CASE = "UPPER_SNAKE_CASE"
    TITLE_SNAKE_CASE = "Title_Snake_Case"
    TITLE_CASE = "Title Case"
    LOWERCASE = "lowercase"
    UPPERCASE = "UPPERCASE"
    KEBAB_CASE = "kebab-case"
    NONE = "none"


class NamingConfig(FrozenBase):
    """Configuration for naming normalization.

    Attributes:
        columns: Pattern to apply to column names. None means inherit from parent.
        tables: Pattern to apply to table names. None means inherit from parent.
        exclude: Glob patterns or explicit names to exclude from column normalization.
        on_collision: Behaviour when two columns normalise to the same name.
            ``"suffix"`` appends a numeric suffix to the later column; ``"error"``
            aborts with a validation error.
        reserved_words: Optional configuration for handling SQL reserved word
            collisions in column names. None means no reserved word handling.
    """

    columns: NamingPattern | None = None
    tables: NamingPattern | None = None
    exclude: list[str] = []
    on_collision: Literal["suffix", "error"] = "error"
    reserved_words: ReservedWordConfig | None = None
