"""Incremental load configuration model."""

from typing import Literal

from pydantic import model_validator

from weevr.model.base import FrozenBase


class LoadConfig(FrozenBase):
    """Incremental load mode and watermark parameters.

    Cross-field validation:
    - ``mode == "incremental_watermark"`` requires ``watermark_column`` to be set.
    """

    mode: Literal["full", "incremental_watermark", "incremental_parameter", "cdc"] = "full"
    watermark_column: str | None = None
    watermark_type: Literal["timestamp", "date", "int", "long"] | None = None

    @model_validator(mode="after")
    def _validate_watermark_fields(self) -> "LoadConfig":
        if self.mode == "incremental_watermark" and not self.watermark_column:
            raise ValueError("incremental_watermark mode requires 'watermark_column' to be set")
        return self
