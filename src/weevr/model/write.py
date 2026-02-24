"""Write configuration model."""

from __future__ import annotations

from typing import Literal

from pydantic import model_validator

from weevr.model.base import FrozenBase


class WriteConfig(FrozenBase):
    """Write mode and merge parameters for a thread target.

    Cross-field validation:
    - ``mode == "merge"`` requires ``match_keys`` to be set.
    - ``on_no_match_source == "soft_delete"`` requires ``soft_delete_column`` to be set.
    """

    mode: Literal["overwrite", "append", "merge"] = "overwrite"
    match_keys: list[str] | None = None
    on_match: Literal["update", "ignore"] = "update"
    on_no_match_target: Literal["insert", "ignore"] = "insert"
    on_no_match_source: Literal["delete", "soft_delete", "ignore"] = "ignore"
    soft_delete_column: str | None = None
    soft_delete_value: str = "true"

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
