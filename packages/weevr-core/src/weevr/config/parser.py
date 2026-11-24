"""YAML parsing and config version validation."""

from pathlib import Path
from typing import Any

import yaml

from weevr.config.locations import ConfigLocation, make_location
from weevr.diagnostics import diagnostic
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
    except FileNotFoundError as e:
        raise _parse_error("C001", f"Config file not found: {location_str}", location_str, e) from e
    except yaml.YAMLError as e:
        raise _yaml_error(e, location_str) from e
    except UnicodeDecodeError as e:
        raise _parse_error("C003", f"Config file is not valid UTF-8: {e}", location_str, e) from e
    except OSError as e:
        raise _parse_error("C002", f"Failed to read config file: {e}", location_str, e) from e

    if content is None:
        raise _parse_error("C004", "Config file is empty or contains only null", location_str)
    if not isinstance(content, dict):
        raise ConfigParseError.from_diagnostic(
            diagnostic(
                "C006",
                f"Expected YAML dict, got {type(content).__name__}",
                file=location_str,
                pointer="",
            )
        )
    return content


def _parse_error(
    code: str, message: str, file: str, cause: Exception | None = None
) -> ConfigParseError:
    return ConfigParseError.from_diagnostic(diagnostic(code, message, file=file), cause=cause)


def _yaml_error(error: yaml.YAMLError, file: str) -> ConfigParseError:
    """A YAML syntax error, located at PyYAML's problem mark (1-based)."""
    mark = getattr(error, "problem_mark", None) or getattr(error, "context_mark", None)
    return ConfigParseError.from_diagnostic(
        diagnostic(
            "C005",
            f"Invalid YAML syntax: {error}",
            file=file,
            line=mark.line + 1 if mark is not None else None,
            column=mark.column + 1 if mark is not None else None,
        ),
        cause=error,
    )


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
        raise ConfigParseError.from_diagnostic(
            diagnostic("C201", "Missing required field 'config_version'", pointer="")
        )

    version_str = raw["config_version"]
    if not isinstance(version_str, str):
        raise _version_format_error(
            f"config_version must be a string, got {type(version_str).__name__}"
        )

    parts = version_str.split(".")
    if len(parts) != 2:
        raise _version_format_error(
            f"config_version must be 'major.minor' format, got '{version_str}'"
        )

    try:
        major = int(parts[0])
        minor = int(parts[1])
        return (major, minor)
    except ValueError as e:
        raise _version_format_error(
            f"config_version parts must be integers, got '{version_str}'", cause=e
        ) from e


def _version_format_error(message: str, cause: Exception | None = None) -> ConfigParseError:
    return ConfigParseError.from_diagnostic(
        diagnostic("C202", message, pointer="/config_version", expected=["major.minor"]),
        cause=cause,
    )


# Supported config_version by document kind, in envelope order.
SUPPORTED_VERSIONS: dict[str, tuple[int, int]] = {
    "project": (2, 0),
    "loom": (2, 0),
    "weave": (2, 0),
    "thread": (2, 0),
    "warp": (2, 0),
    "params": (2, 0),
}

# The config_version major this release reads, for every kind.
CONFIG_MAJOR = 2

MIGRATE_HINT = (
    "This is a weevr 1.x document. Run `weevr migrate` (weevr-cli) to rewrite it for "
    "weevr 2, or keep running it on weevr 1.x."
)
"""The hint on a v1 or kind-less document: the weevr-cli command that upgrades it."""


def validate_config_version(version: tuple[int, int], config_type: str) -> None:
    """Validate that the config version is supported.

    Args:
        version: Tuple of (major, minor) version
        config_type: Document kind (see ``SUPPORTED_VERSIONS``)

    Raises:
        ConfigVersionError: If the kind is unknown or the major version is not
            one this release reads.
    """
    if config_type not in SUPPORTED_VERSIONS:
        raise ConfigVersionError.from_diagnostic(
            diagnostic(
                "C204",
                f"Unknown config type '{config_type}', expected one of: "
                f"{', '.join(SUPPORTED_VERSIONS.keys())}",
                expected=list(SUPPORTED_VERSIONS),
            )
        )
    _check_major(version, SUPPORTED_VERSIONS[config_type][0], f"{config_type} config")


def _check_major(version: tuple[int, int], supported: int, label: str) -> None:
    """Raise C203 unless ``version`` has the ``supported`` major.

    An older major is a v1 document, so its error carries the migrate hint.
    """
    major, minor = version
    if major == supported:
        return
    raise ConfigVersionError.from_diagnostic(
        diagnostic(
            "C203",
            f"Unsupported {label} version {major}.{minor}. Expected major version {supported}.x",
            pointer="/config_version",
            expected=[f"{supported}.x"],
            hint=MIGRATE_HINT if major < supported else None,
        )
    )


def read_envelope(raw: dict[str, Any], ext_type: str | None) -> str:
    """Check a document's envelope and return its kind.

    The version is checked before the kind, so a v1 document (which has no
    ``kind``) gets the migration error rather than a missing-kind one; the
    kind is checked before it is matched against the typed extension.

    Args:
        raw: Parsed document.
        ext_type: The kind its typed extension names, or ``None`` for
            ``.yaml``/``.yml``.

    Returns:
        The document kind.

    Raises:
        ConfigParseError: ``config_version`` is missing (C201) or malformed
            (C202); ``kind`` is missing (C206), unknown (C204), or disagrees
            with the extension (C207).
        ConfigVersionError: The major version is not one this release reads
            (C203).
    """
    version = extract_config_version(raw)
    _check_major(version, CONFIG_MAJOR, "config")
    kind = document_kind(raw, ext_type)
    validate_config_version(version, kind)
    return kind


def document_kind(raw: dict[str, Any], ext_type: str | None) -> str:
    """The kind a document declares, checked against its typed extension.

    ``kind`` is required and authoritative; a typed extension is a convention
    it must agree with. Neither the extension nor the document's structure
    stands in for a missing ``kind``.

    Args:
        raw: Parsed document.
        ext_type: The kind its typed extension names, or ``None`` for
            ``.yaml``/``.yml``.

    Returns:
        The document kind.

    Raises:
        ConfigParseError: ``kind`` is missing (C206), not a known kind (C204),
            or disagrees with the typed extension (C207).
    """
    if "kind" not in raw:
        raise ConfigParseError.from_diagnostic(
            diagnostic(
                "C206",
                "Missing required field 'kind'",
                pointer="",
                expected=list(SUPPORTED_VERSIONS),
                hint=MIGRATE_HINT,
            )
        )

    kind = raw["kind"]
    if not isinstance(kind, str) or kind not in SUPPORTED_VERSIONS:
        raise ConfigParseError.from_diagnostic(
            diagnostic(
                "C204",
                f"Unknown kind {kind!r}, expected one of: {', '.join(SUPPORTED_VERSIONS)}",
                pointer="/kind",
                expected=list(SUPPORTED_VERSIONS),
            )
        )
    if ext_type is not None and kind != ext_type:
        raise ConfigParseError.from_diagnostic(
            diagnostic(
                "C207",
                f"Declared kind '{kind}' does not match the .{ext_type} extension",
                pointer="/kind",
                expected=[ext_type],
            )
        )
    return kind


def declared_kind(location: ConfigLocation) -> str | None:
    """The known kind a document declares, read for display without validating it.

    Args:
        location: The document's file.

    Returns:
        Its declared ``kind``, or ``None`` when the file can't be read or
        declares no known kind.
    """
    try:
        kind = parse_yaml(location).get("kind")
    except ConfigError:
        return None
    return kind if isinstance(kind, str) and kind in SUPPORTED_VERSIONS else None


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
    raise ConfigError.from_diagnostic(
        diagnostic(
            "C007",
            f"Unsupported extension '{suffix}'. Expected .thread, .weave, .loom, or .warp",
            file=display,
            expected=[*TYPED_EXTENSIONS, ".yaml", ".yml"],
        )
    )
