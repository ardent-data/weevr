"""Loom domain model."""

from typing import Any

from pydantic import model_validator

from weevr.model.base import FrozenBase
from weevr.model.execution import ExecutionConfig
from weevr.model.params import ParamSpec
from weevr.model.weave import ConditionSpec


class WeaveEntry(FrozenBase):
    """A weave reference within a loom, with optional condition.

    Attributes:
        name: Weave name as declared in the loom config.
        condition: Optional condition for conditional execution.
    """

    name: str
    condition: ConditionSpec | None = None


class Loom(FrozenBase):
    """A deployment unit containing weave references with optional shared defaults."""

    name: str = ""
    config_version: str
    weaves: list[WeaveEntry]
    defaults: dict[str, Any] | None = None
    params: dict[str, ParamSpec] | None = None
    execution: ExecutionConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_weave_entries(cls, data: Any) -> Any:
        """Normalize weave list to WeaveEntry objects.

        Accepts both plain string entries and dict entries, ensuring backward
        compatibility with existing ``list[str]`` configs.
        """
        if not isinstance(data, dict):
            return data
        weaves = data.get("weaves")
        if not isinstance(weaves, list):
            return data
        normalized = []
        for entry in weaves:
            if isinstance(entry, str):
                normalized.append({"name": entry})
            else:
                normalized.append(entry)
        return {**data, "weaves": normalized}
