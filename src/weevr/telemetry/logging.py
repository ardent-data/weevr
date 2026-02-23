"""Structured JSON logging for weevr telemetry."""

import json
import logging
from datetime import UTC, datetime

from weevr.model.execution import LogLevel

# Mapping from weevr LogLevel to Python logging levels
_LOG_LEVEL_MAP: dict[LogLevel, int] = {
    LogLevel.MINIMAL: logging.WARNING,
    LogLevel.STANDARD: logging.INFO,
    LogLevel.VERBOSE: logging.DEBUG,
    LogLevel.DEBUG: logging.DEBUG,
}


class StructuredJsonFormatter(logging.Formatter):
    """Formats log records as structured JSON with OTel-compatible field names.

    Produces one JSON object per log line with fields: timestamp, level,
    logger, message, and any extra attributes attached to the log record.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as a JSON string."""
        entry: dict[str, str | int | float | bool | None] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include weevr-specific context if attached to the record
        for key in (
            "thread_name",
            "weave_name",
            "loom_name",
            "trace_id",
            "span_id",
        ):
            value = getattr(record, key, None)
            if value is not None:
                entry[key] = value

        # Include extra attributes if present
        extra_attrs = getattr(record, "attributes", None)
        if extra_attrs and isinstance(extra_attrs, dict):
            entry["attributes"] = extra_attrs  # type: ignore[assignment]

        return json.dumps(entry, default=str)


def configure_logging(
    log_level: LogLevel = LogLevel.STANDARD,
    logger_name: str = "weevr",
) -> logging.Logger:
    """Configure a weevr logger with structured JSON output.

    Sets up a logger with a :class:`StructuredJsonFormatter` handler at the
    appropriate Python logging level based on the weevr :class:`LogLevel`.

    Args:
        log_level: weevr log level to configure.
        logger_name: Name of the logger to configure.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(logger_name)
    python_level = _LOG_LEVEL_MAP[log_level]
    logger.setLevel(python_level)

    # Avoid duplicate handlers on repeated calls
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(StructuredJsonFormatter())
        logger.addHandler(handler)
    else:
        # Update existing handler level
        for h in logger.handlers:
            if isinstance(h, logging.StreamHandler):
                h.setLevel(python_level)

    return logger
