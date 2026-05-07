"""YAML parsing and config version validation."""

from pathlib import Path
from typing import Any

import yaml

from weevr.config.locations import ConfigLocation, make_location
from weevr.errors import ConfigError, ConfigParseError, ConfigVersionError


def parse_yaml(path: str | Path | ConfigLocation) -> dict[str, Any]:
    """Parse a YAML file and return its contents.

    Args:
        path: A local path, a URI string, or a :class:`ConfigLocation`. Local
            path inputs are wrapped in a :class:`LocalConfigLocation`
            automatically. Remote URIs require the caller to construct a
            :class:`RemoteConfigLocation` themselves so they can supply the
            active SparkSession.

    Returns:
        Parsed YAML content as a dictionary.

    Raises:
        ConfigParseError: If the file is not found, unreadable, or contains
            invalid YAML syntax.
    """
    # Local-only path inputs are wrapped here. Remote URIs must already be
    # a ConfigLocation so the caller has supplied a SparkSession; failing
    # here surfaces the missing session early rather than at read time.
    location = path if isinstance(path, ConfigLocation) else make_location(path)

    location_str = str(location)
    try:
        text = location.read_text()
        content = yaml.safe_load(text)

        if content is None:
            raise ConfigParseError(
                "Config file is empty or contains only null",
                file_path=location_str,
            )

        if not isinstance(content, dict):
            raise ConfigParseError(
                f"Expected YAML dict, got {type(content).__name__}",
                file_path=location_str,
            )

        return content

    except FileNotFoundError as e:
        raise ConfigParseError(
            f"Config file not found: {location_str}",
            cause=e,
            file_path=location_str,
        ) from e
    except yaml.YAMLError as e:
        raise ConfigParseError(
            f"Invalid YAML syntax: {e}",
            cause=e,
            file_path=location_str,
        ) from e
    except OSError as e:
        raise ConfigParseError(
            f"Failed to read config file: {e}",
            cause=e,
            file_path=location_str,
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
        raise ConfigParseError(f"config_version must be a string, got {type(version_str).__name__}")

    parts = version_str.split(".")
    if len(parts) != 2:
        raise ConfigParseError(f"config_version must be 'major.minor' format, got '{version_str}'")

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
    "warp": (1, 0),
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


# Typed extensions map file suffix to config type
TYPED_EXTENSIONS: dict[str, str] = {
    ".thread": "thread",
    ".weave": "weave",
    ".loom": "loom",
    ".warp": "warp",
}


def detect_config_type_from_extension(
    path: str | Path | ConfigLocation,
) -> str | None:
    """Detect config type from file extension.

    Args:
        path: Path or location of the config file.

    Returns:
        Config type string if the extension is a typed extension
        (``.thread``, ``.weave``, ``.loom``), or ``None`` for
        ``.yaml``/``.yml`` files.

    Raises:
        ConfigError: If the extension is ``.yaml`` or ``.yml`` and a
            typed extension was expected (caller context).
    """
    display = str(path)
    suffix = path.suffix.lower() if isinstance(path, ConfigLocation) else Path(path).suffix.lower()
    if suffix in TYPED_EXTENSIONS:
        return TYPED_EXTENSIONS[suffix]
    if suffix in (".yaml", ".yml"):
        return None
    raise ConfigError(
        f"Unsupported extension '{suffix}'. Expected .thread, .weave, .loom, or .warp",
        file_path=display,
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
    elif "config_version" in raw and len(raw) > 1:
        # Params file: has config_version plus other key-value pairs
        return "params"

    raise ConfigParseError(
        "Unable to detect config type. Expected 'threads', 'weaves', "
        "'sources'/'target', or a params file structure"
    )
