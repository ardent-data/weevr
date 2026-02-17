"""Configuration loading and validation."""

from pathlib import Path
from typing import Any


def load_config(
    path: str | Path,
    runtime_params: dict[str, Any] | None = None,
    param_file: str | Path | None = None,
) -> dict[str, Any]:
    """Load and validate a weevr configuration file.

    Args:
        path: Path to the config file (thread, weave, or loom)
        runtime_params: Optional runtime parameter overrides
        param_file: Optional parameter file path

    Returns:
        Fully resolved and validated configuration dictionary

    Raises:
        NotImplementedError: This function is not yet implemented
    """
    raise NotImplementedError("Config loading pipeline not yet implemented")
