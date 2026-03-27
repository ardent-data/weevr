"""Step result type for pipeline step handlers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame


@dataclass(frozen=True)
class StepResult:
    """Return type for pipeline step handlers.

    Wraps the transformed DataFrame with optional metadata for
    observability. Existing handlers return empty metadata; new M115
    handlers populate step-specific metrics.
    """

    df: DataFrame
    metadata: dict[str, Any] = field(default_factory=dict)
