"""Incremental load configuration model."""

from __future__ import annotations

import logging
from typing import Literal

from pydantic import Field, model_validator

from weevr.model.base import FrozenBase

logger = logging.getLogger(__name__)


class CdcConfig(FrozenBase):
    """CDC-specific configuration.

    Supports two modes:
    - **Preset**: ``preset: delta_cdf`` auto-configures for Delta Change Data Feed conventions.
    - **Explicit**: Declare ``operation_column`` and value mappings directly.

    Preset and explicit column mappings are mutually exclusive.
    """

    preset: Literal["delta_cdf"] | None = Field(
        default=None,
        description=(
            "CDC preset. 'delta_cdf' auto-configures for Delta Change Data Feed. "
            "Mutually exclusive with operation_column."
        ),
    )
    operation_column: str | None = Field(
        default=None,
        description=("Column containing the CDC operation type. Mutually exclusive with preset."),
    )
    insert_value: str | None = Field(
        default=None,
        description="Value in operation_column that indicates an insert operation.",
    )
    update_value: str | None = Field(
        default=None,
        description="Value in operation_column that indicates an update operation.",
    )
    delete_value: str | None = Field(
        default=None,
        description="Value in operation_column that indicates a delete operation.",
    )
    on_delete: Literal["hard_delete", "soft_delete"] = Field(
        default="hard_delete",
        description=(
            "How to handle delete operations: hard_delete removes rows, soft_delete flags them."
        ),
    )

    @model_validator(mode="after")
    def _validate_preset_or_explicit(self) -> CdcConfig:
        has_preset = self.preset is not None
        has_explicit = self.operation_column is not None

        if has_preset and has_explicit:
            raise ValueError(
                "CDC preset and explicit column mapping are mutually exclusive; "
                "set 'preset' or 'operation_column', not both"
            )

        if not has_preset and not has_explicit:
            raise ValueError("CDC config requires either 'preset' or 'operation_column' to be set")

        if has_explicit and not (self.insert_value or self.update_value):
            raise ValueError(
                "Explicit CDC mapping requires at least 'insert_value' or 'update_value'"
            )

        return self


class WatermarkStoreConfig(FrozenBase):
    """Watermark store configuration.

    Inheritable via loom -> weave -> thread cascade.
    Default store type is ``table_properties`` (zero-config).
    """

    type: Literal["table_properties", "metadata_table"] = Field(
        default="table_properties",
        description=(
            "Watermark storage backend. 'table_properties' stores in Delta table "
            "properties (zero-config). 'metadata_table' uses a dedicated table."
        ),
    )
    table_path: str | None = Field(
        default=None,
        description="Path to the metadata table. Required when type is 'metadata_table'.",
    )

    @model_validator(mode="after")
    def _validate_metadata_table_path(self) -> WatermarkStoreConfig:
        if self.type == "metadata_table" and not self.table_path:
            raise ValueError("metadata_table store type requires 'table_path' to be set")
        return self


class LoadConfig(FrozenBase):
    """Incremental load mode and watermark parameters.

    Cross-field validation:
    - ``mode == "incremental_watermark"`` requires ``watermark_column`` to be set.
    - ``mode == "cdc"`` requires ``cdc`` config to be set.
    - ``mode == "cdc"`` must not have ``watermark_column`` set
      (CDF version tracking is automatic).
    """

    mode: Literal["full", "incremental_watermark", "incremental_parameter", "cdc"] = Field(
        default="full",
        description=("Load mode: full, incremental_watermark, incremental_parameter, or cdc."),
    )
    watermark_column: str | None = Field(
        default=None,
        description=(
            "Column used for watermark-based incremental loads. "
            "Required when mode is 'incremental_watermark'."
        ),
    )
    watermark_type: Literal["timestamp", "date", "int", "long"] | None = Field(
        default=None,
        description="Data type of the watermark column.",
    )
    watermark_inclusive: bool = Field(
        default=False,
        description=(
            "When True, include rows equal to the watermark value. "
            "Pair with merge or overwrite for idempotent re-reads."
        ),
    )
    watermark_store: WatermarkStoreConfig | None = Field(
        default=None,
        description="Watermark persistence configuration. Defaults to table_properties.",
    )
    cdc: CdcConfig | None = Field(
        default=None,
        description="CDC-specific configuration. Required when mode is 'cdc'.",
    )

    @model_validator(mode="after")
    def _validate_watermark_fields(self) -> LoadConfig:
        if self.mode == "incremental_watermark" and not self.watermark_column:
            raise ValueError("incremental_watermark mode requires 'watermark_column' to be set")
        return self

    @model_validator(mode="after")
    def _validate_cdc_fields(self) -> LoadConfig:
        if self.mode == "cdc" and not self.cdc:
            raise ValueError("cdc mode requires 'cdc' config to be set")

        if self.mode == "cdc" and self.watermark_column:
            raise ValueError(
                "cdc mode must not set 'watermark_column'; "
                "CDF version tracking is handled automatically"
            )

        if self.mode != "cdc" and self.cdc:
            raise ValueError("'cdc' config is only valid when mode is 'cdc'")

        return self

    @model_validator(mode="after")
    def _warn_inclusive_with_append(self) -> LoadConfig:
        if self.watermark_inclusive and self.mode == "incremental_watermark":
            logger.debug(
                "watermark_inclusive=True; pair with merge or overwrite write mode "
                "for idempotent re-reads"
            )
        return self
