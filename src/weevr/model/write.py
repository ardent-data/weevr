"""Write configuration model."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from weevr.model.base import FrozenBase


class WriteConfig(FrozenBase):
    """Write mode and merge parameters for a thread target.

    Cross-field validation:
    - ``mode == "merge"`` requires ``match_keys`` to be set.
    - ``on_no_match_source == "soft_delete"`` requires ``soft_delete_column`` to be set.

    Optional fields:
    - ``soft_delete_default_value``: when set, non-deleted rows (matched updates and
      new inserts) are written with this value in ``soft_delete_column`` instead of
      ``null``. Defaults to ``None`` to preserve existing behaviour.
    """

    mode: Literal["overwrite", "append", "merge"] = Field(
        default="overwrite",
        description="Write mode: overwrite, append, or merge (upsert).",
    )
    match_keys: list[str] | None = Field(
        default=None,
        description="Columns to match on for merge mode. Required when mode is 'merge'.",
    )
    on_match: Literal["update", "ignore"] = Field(
        default="update",
        description="Action for matched rows in merge mode.",
    )
    on_no_match_target: Literal["insert", "ignore"] = Field(
        default="insert",
        description="Action for source rows with no target match in merge mode.",
    )
    on_no_match_source: Literal["delete", "soft_delete", "ignore"] = Field(
        default="ignore",
        description=(
            "Action for target rows with no source match in merge mode. "
            "'soft_delete' requires soft_delete_column."
        ),
    )
    soft_delete_column: str | None = Field(
        default=None,
        description=(
            "Column name for soft delete flag. Required when on_no_match_source is 'soft_delete'."
        ),
    )
    soft_delete_value: bool = Field(
        default=True,
        description="Value written to the soft delete column when a row is soft-deleted.",
    )
    soft_delete_default_value: bool | None = Field(
        default=None,
        description=(
            "Default value written to the soft delete column for rows that are not "
            "soft-deleted (matched updates and new inserts). When None (the default), "
            "those rows receive null, preserving existing behaviour."
        ),
    )

    @model_validator(mode="after")
    def _validate_merge_keys(self) -> WriteConfig:
        if self.mode == "merge" and not self.match_keys:
            raise ValueError("merge mode requires 'match_keys' to be set")
        return self

    @model_validator(mode="after")
    def _validate_soft_delete(self) -> WriteConfig:
        if self.on_no_match_source == "soft_delete" and not self.soft_delete_column:
            raise ValueError(
                "on_no_match_source='soft_delete' requires 'soft_delete_column' to be set"
            )
        return self
