"""AuditTemplate model for weevr audit column definitions."""

from weevr.model.base import FrozenBase


class AuditTemplate(FrozenBase):
    """A named set of audit columns applied during data shaping.

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
    """

    columns: dict[str, str]
