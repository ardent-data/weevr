"""Write configuration model."""

from typing import Literal

from pydantic import model_validator

from weevr.model.base import FrozenBase


class WriteConfig(FrozenBase):
    """Write mode and merge parameters for a thread target.

    Cross-field validation:
    - ``mode == "merge"`` requires ``match_keys`` to be set.
    """

    mode: Literal["overwrite", "append", "merge", "insert_only"] = "overwrite"
    match_keys: list[str] | None = None
    on_match: Literal["update", "ignore"] = "update"
    on_no_match_target: Literal["insert", "ignore"] = "insert"
    on_no_match_source: Literal["delete", "soft_delete", "ignore"] = "ignore"

    @model_validator(mode="after")
    def _validate_merge_keys(self) -> "WriteConfig":
        if self.mode == "merge" and not self.match_keys:
            raise ValueError("merge mode requires 'match_keys' to be set")
        return self
