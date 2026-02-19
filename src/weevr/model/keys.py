"""Key management models."""

from typing import Literal

from weevr.model.base import FrozenBase


class SurrogateKeyConfig(FrozenBase):
    """Configuration for surrogate key generation."""

    name: str
    algorithm: Literal["sha256", "md5"] = "sha256"


class ChangeDetectionConfig(FrozenBase):
    """Configuration for change detection hash generation."""

    name: str
    columns: list[str]
    algorithm: Literal["md5", "sha256"] = "md5"


class KeyConfig(FrozenBase):
    """Key management configuration for a thread."""

    business_key: list[str] | None = None
    surrogate_key: SurrogateKeyConfig | None = None
    change_detection: ChangeDetectionConfig | None = None
