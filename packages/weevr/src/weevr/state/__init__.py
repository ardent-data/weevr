"""Watermark state persistence for incremental processing."""

from __future__ import annotations

from typing import TYPE_CHECKING

from weevr.state.watermark import WatermarkState, WatermarkStore

if TYPE_CHECKING:
    from weevr.model.load import LoadConfig


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


__all__ = ["WatermarkState", "WatermarkStore", "resolve_store"]
