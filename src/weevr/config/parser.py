"""YAML parsing and config version validation."""

from pathlib import Path
from typing import Any

import yaml

from weevr.errors import ConfigParseError, ConfigVersionError


def parse_yaml(path: str | Path) -> dict[str, Any]:
    """Parse a YAML file and return its contents.

    Args:
        path: Path to the YAML file

    Returns:
        Parsed YAML content as a dictionary

    Raises:
        ConfigParseError: If file not found, unreadable, or invalid YAML syntax
    """
    try:
        file_path = Path(path)
        with open(file_path, encoding="utf-8") as f:
            content = yaml.safe_load(f)

        if content is None:
            raise ConfigParseError(
                "Config file is empty or contains only null",
                file_path=str(file_path),
            )

        if not isinstance(content, dict):
            raise ConfigParseError(
                f"Expected YAML dict, got {type(content).__name__}",
                file_path=str(file_path),
            )

        return content

    except FileNotFoundError as e:
        raise ConfigParseError(
            f"Config file not found: {path}",
            cause=e,
            file_path=str(path),
        ) from e
    except yaml.YAMLError as e:
        raise ConfigParseError(
            f"Invalid YAML syntax: {e}",
            cause=e,
            file_path=str(path),
        ) from e
    except OSError as e:
        raise ConfigParseError(
            f"Failed to read config file: {e}",
            cause=e,
            file_path=str(path),
        ) from e


def extract_config_version(raw: dict[str, Any]) -> tuple[int, int]:
    """Extract and parse config_version from a config dict.

    Args:
        raw: Parsed config dictionary

    Returns:
        Tuple of (major, minor) version numbers

    Raises:
        ConfigParseError: If config_version is missing or invalid format
    """
    if "config_version" not in raw:
        raise ConfigParseError("Missing required field 'config_version'")

    version_str = raw["config_version"]
    if not isinstance(version_str, str):
        raise ConfigParseError(
            f"config_version must be a string, got {type(version_str).__name__}"
        )

    parts = version_str.split(".")
    if len(parts) != 2:
        raise ConfigParseError(
            f"config_version must be 'major.minor' format, got '{version_str}'"
        )

    try:
        major = int(parts[0])
        minor = int(parts[1])
        return (major, minor)
    except ValueError as e:
        raise ConfigParseError(
            f"config_version parts must be integers, got '{version_str}'",
            cause=e,
        ) from e


# Supported config versions by type
SUPPORTED_VERSIONS: dict[str, tuple[int, int]] = {
    "thread": (1, 0),
    "weave": (1, 0),
    "loom": (1, 0),
    "params": (1, 0),
}


def validate_config_version(version: tuple[int, int], config_type: str) -> None:
    """Validate that the config version is supported.

    Args:
        version: Tuple of (major, minor) version
        config_type: Type of config (thread, weave, loom, params)

    Raises:
        ConfigVersionError: If the major version doesn't match supported version
    """
    if config_type not in SUPPORTED_VERSIONS:
        raise ConfigVersionError(
            f"Unknown config type '{config_type}', expected one of: "
            f"{', '.join(SUPPORTED_VERSIONS.keys())}"
        )

    supported = SUPPORTED_VERSIONS[config_type]
    major, minor = version

    if major != supported[0]:
        raise ConfigVersionError(
            f"Unsupported {config_type} config version {major}.{minor}. "
            f"Expected major version {supported[0]}.x"
        )


def detect_config_type(raw: dict[str, Any]) -> str:
    """Detect the type of config from its structure.

    Args:
        raw: Parsed config dictionary

    Returns:
        Config type: 'thread', 'weave', 'loom', or 'params'

    Raises:
        ConfigParseError: If config type cannot be determined
    """
    # Check for explicit type field
    if "type" in raw:
        config_type = raw["type"]
        if config_type in SUPPORTED_VERSIONS:
            return config_type

    # Detect by structure
    if "weaves" in raw:
        return "loom"
    elif "threads" in raw:
        return "weave"
    elif "sources" in raw or "target" in raw:
        return "thread"
    elif "config_version" in raw and len(raw) >= 1:
        # Likely a params file (just key-value pairs)
        return "params"

    raise ConfigParseError(
        "Unable to detect config type. Expected 'threads', 'weaves', "
        "'sources'/'target', or a params file structure"
    )
