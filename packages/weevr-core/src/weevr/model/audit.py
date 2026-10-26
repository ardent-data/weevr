"""AuditTemplate model for weevr audit column definitions."""

from typing import Any

from pydantic import Field, model_validator

from weevr.model.base import FrozenBase


class AuditTemplate(FrozenBase):
    """A named set of audit columns applied during data shaping.

    A template is written either nested under ``columns`` or as a flat mapping
    of column names to expressions. A mapping with a ``columns`` key is read as
    the nested form, so a flat template cannot define a column named
    ``columns``.

    Attributes:
        columns: Mapping of column names to Spark SQL expressions. Each entry
            defines a column that will be appended to the output dataset.

    Example:
        >>> template = AuditTemplate(
        ...     columns={
        ...         "created_at": "current_timestamp()",
        ...         "created_by": "current_user()",
        ...     }
        ... )
        >>> AuditTemplate.model_validate({"created_at": "current_timestamp()"}).columns
        {'created_at': 'current_timestamp()'}
    """

    columns: dict[str, str] = Field(
        description=(
            "Mapping of column names to Spark SQL expressions. Each entry defines a column"
            " that will be appended to the output dataset."
        )
    )

    @model_validator(mode="before")
    @classmethod
    def _wrap_flat_columns(cls, data: Any) -> Any:
        """Read a flat column mapping as the nested ``columns`` form."""
        if isinstance(data, dict) and "columns" not in data:
            return {"columns": data}
        return data
