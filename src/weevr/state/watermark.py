"""Watermark state model, store ABC, and store resolution."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Literal

from weevr.model.base import FrozenBase

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from weevr.model.load import LoadConfig


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


def resolve_store(load_config: LoadConfig, target_path: str) -> WatermarkStore:
    """Resolve the appropriate watermark store from config.

    If ``load_config.watermark_store`` is ``None`` or its type is
    ``table_properties``, returns a :class:`TablePropertiesStore`.
    If the type is ``metadata_table``, returns a :class:`MetadataTableStore`.

    Args:
        load_config: Thread-level load configuration.
        target_path: Path to the thread's Delta target table.

    Returns:
        A concrete ``WatermarkStore`` implementation.
    """
    from weevr.state.metadata_table import MetadataTableStore
    from weevr.state.table_properties import TablePropertiesStore

    store_config = load_config.watermark_store

    if store_config is None or store_config.type == "table_properties":
        return TablePropertiesStore(target_path)

    # metadata_table — table_path is validated present by WatermarkStoreConfig
    return MetadataTableStore(store_config.table_path)  # type: ignore[arg-type]
