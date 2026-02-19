"""Weave domain model."""

from typing import Any

from weevr.model.base import FrozenBase
from weevr.model.params import ParamSpec


class Weave(FrozenBase):
    """A collection of thread references with optional shared defaults."""

    config_version: str
    threads: list[str]
    defaults: dict[str, Any] | None = None
    params: dict[str, ParamSpec] | None = None
