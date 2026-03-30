"""FactConfig model for foreign key validation metadata and sentinel conventions."""

from pydantic import Field, field_validator

from weevr.model.base import FrozenBase


class SentinelValueConfig(FrozenBase):
    """Configuration for sentinel value conventions in fact tables.

    Sentinel values are special numeric codes used to represent missing,
    invalid, or explicitly null data within fact table columns.

    Attributes:
        invalid: Sentinel code for invalid data. Defaults to -4.
        missing: Sentinel code for missing data. Defaults to -1.
    """

    invalid: int = Field(
        default=-4,
        description="Sentinel code for invalid data.",
    )
    missing: int = Field(
        default=-1,
        description="Sentinel code for missing data.",
    )


class FactConfig(FrozenBase):
    """Configuration metadata for fact table validation.

    FactConfig declares foreign key columns and sentinel value conventions
    for a fact table, enabling validation and data quality enforcement
    during mapping operations.

    Attributes:
        foreign_keys: Non-empty list of column names that reference
            dimension tables. Required field.
        sentinel_values: Configuration for sentinel value conventions.
            Defaults to SentinelValueConfig() if not provided.
    """

    foreign_keys: list[str] = Field(
        description="Non-empty list of column names that reference dimension tables.",
    )
    sentinel_values: SentinelValueConfig = Field(
        default=SentinelValueConfig(),
        description="Configuration for sentinel value conventions.",
    )

    @field_validator("foreign_keys")
    @classmethod
    def _validate_foreign_keys_non_empty(cls, v: list[str]) -> list[str]:
        """Validate that foreign_keys is a non-empty list.

        Args:
            v: The foreign_keys value to validate.

        Returns:
            The validated foreign_keys value.

        Raises:
            ValueError: If foreign_keys is empty.
        """
        if not v:
            raise ValueError("foreign_keys must be a non-empty list")
        return v
