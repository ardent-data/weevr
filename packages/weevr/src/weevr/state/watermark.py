"""Watermark state model and store ABC."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Literal

from weevr.model.base import FrozenBase

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class WatermarkState(FrozenBase):
    """Immutable watermark high-water mark snapshot.

    ``last_value`` is serialized as a string for uniform storage across
    all watermark types (timestamp, date, int, long).
    """

    thread_name: str
    watermark_column: str
    watermark_type: Literal["timestamp", "date", "int", "long"]
    last_value: str
    last_updated: datetime
    run_id: str | None = None


class WatermarkStore(ABC):
    """Abstract interface for watermark state persistence."""

    @abstractmethod
    def read(self, spark: SparkSession, thread_name: str) -> WatermarkState | None:
        """Load persisted watermark state.

        Returns ``None`` if no prior state exists for the given thread.
        """

    @abstractmethod
    def write(self, spark: SparkSession, state: WatermarkState) -> None:
        """Persist watermark state.

        Raises ``StateError`` on failure.
        """
