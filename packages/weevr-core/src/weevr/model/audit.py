"""AuditTemplate model for weevr audit column definitions."""

from typing import Any

from pydantic import Field, GetJsonSchemaHandler, model_validator
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema

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

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        """Advertise both the nested and the flat form in the JSON schema.

        Serialization schemas keep the nested form only, since dumps always
        emit ``columns``.
        """
        if handler.mode == "serialization":
            return handler(core_schema)
        nested = dict(handler.resolve_ref_schema(handler(core_schema)))
        title = nested.pop("title", None)
        description = nested.pop("description", None)
        flat = {"type": "object", "additionalProperties": {"type": "string"}}
        return {"title": title, "description": description, "anyOf": [nested, flat]}
