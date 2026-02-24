"""Watermark state persistence for incremental processing."""

from weevr.state.watermark import WatermarkState, WatermarkStore, resolve_store

__all__ = ["WatermarkState", "WatermarkStore", "resolve_store"]
