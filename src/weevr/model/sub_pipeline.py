"""SubPipeline model — named intermediate DataFrame computation."""

from typing import Any

from pydantic import Field, field_validator, model_validator

from weevr.model.base import FrozenBase
from weevr.model.pipeline import Step


class SubPipeline(FrozenBase):
    """A named sub-pipeline that produces an intermediate DataFrame.

    Sub-pipelines are declared under the ``with:`` block of a thread and are
    referenced by name in subsequent join or union steps.
    """

    model_config = {"frozen": True, "populate_by_name": True}

    from_: str = Field(
        alias="from",
        description="Name of the source thread or table this sub-pipeline reads from.",
    )
    steps: list[Step] = Field(  # type: ignore[type-arg]
        description="Ordered list of pipeline steps applied to the source.",
    )

    @model_validator(mode="before")
    @classmethod
    def _remap_from_key(cls, data: Any) -> Any:
        """Remap YAML ``from`` to Python ``from_``."""
        if isinstance(data, dict) and "from" in data and "from_" not in data:
            data = {**data, "from_": data.pop("from")}
        return data

    @field_validator("steps", mode="after")
    @classmethod
    def _steps_not_empty(cls, v: list[Any]) -> list[Any]:
        """Require at least one step."""
        if not v:
            raise ValueError("steps must not be empty")
        return v
