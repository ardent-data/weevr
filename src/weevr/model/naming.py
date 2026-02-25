"""Naming normalization models."""

from enum import StrEnum

from weevr.model.base import FrozenBase


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
    NONE = "none"


class NamingConfig(FrozenBase):
    """Configuration for naming normalization.

    Attributes:
        columns: Pattern to apply to column names. None means inherit from parent.
        tables: Pattern to apply to table names. None means inherit from parent.
        exclude: Glob patterns or explicit names to exclude from column normalization.
    """

    columns: NamingPattern | None = None
    tables: NamingPattern | None = None
    exclude: list[str] = []
