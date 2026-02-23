"""Context class — the user-facing entry point for weevr."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from weevr.model.execution import LogLevel
from weevr.telemetry.logging import configure_logging

logger = logging.getLogger(__name__)


class Context:
    """Entry point for all weevr operations.

    Wraps a SparkSession with resolved parameters and execution configuration.
    Provides ``run()`` for execution and ``load()`` for model inspection.

    Args:
        spark: Active SparkSession (required).
        params: Runtime parameter overrides.
        param_file: Path to a YAML parameter file.
        log_level: Logging verbosity — ``"minimal"``, ``"standard"`` (default),
            ``"verbose"``, or ``"debug"``.
    """

    def __init__(
        self,
        spark: SparkSession,
        params: dict[str, Any] | None = None,
        param_file: str | Path | None = None,
        log_level: str = "standard",
    ) -> None:
        if not isinstance(spark, SparkSession):
            raise TypeError(
                f"'spark' must be a SparkSession, got {type(spark).__name__}"
            )

        try:
            resolved_level = LogLevel(log_level)
        except ValueError:
            valid = ", ".join(f"'{v.value}'" for v in LogLevel)
            raise ValueError(
                f"Invalid log_level '{log_level}'. Must be one of: {valid}"
            ) from None

        self._spark = spark
        self._params = params
        self._param_file = Path(param_file) if param_file is not None else None
        self._log_level = resolved_level
        configure_logging(self._log_level)

    @property
    def spark(self) -> SparkSession:
        """The underlying SparkSession."""
        return self._spark

    @property
    def params(self) -> dict[str, Any] | None:
        """Runtime parameter overrides."""
        return self._params

    @property
    def log_level(self) -> LogLevel:
        """Configured logging verbosity."""
        return self._log_level
