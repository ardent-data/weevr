"""Loom domain model."""

from typing import Any

from weevr.model.base import FrozenBase
from weevr.model.params import ParamSpec


class Loom(FrozenBase):
    """A deployment unit containing weave references with optional shared defaults."""

    name: str = ""
    config_version: str
    weaves: list[str]
    defaults: dict[str, Any] | None = None
    params: dict[str, ParamSpec] | None = None
