"""Read one config document from its file: parse, envelope, schema."""

from __future__ import annotations

from typing import TYPE_CHECKING

from weevr.config.attribution import attribute_errors
from weevr.config.parser import (
    TYPED_EXTENSIONS,
    detect_config_type_from_extension,
    parse_yaml,
    read_envelope,
)
from weevr.config.validation import validate_schema
from weevr.diagnostics import diagnostic
from weevr.errors import ConfigError

if TYPE_CHECKING:
    from pydantic import BaseModel

    from weevr.config.locations import ConfigLocation


def load_document(
    location: ConfigLocation, *, require_typed_extension: bool = False
) -> tuple[str, BaseModel]:
    """Read and validate one document, in the order every load shares.

    The file is parsed, its envelope checked (version, then kind, then the
    match with a typed extension), then its body validated against the
    kind's schema, so an unversioned or v1 document gets the migration
    error rather than a schema error. Errors name ``location``.

    Args:
        location: The document's file.
        require_typed_extension: Reject a ``.yaml``/``.yml`` file instead of
            reading its kind from the envelope alone.

    Returns:
        The document's kind and its validated model.

    Raises:
        ConfigError: The file cannot be read or parsed, has an unsupported
            extension, or fails the envelope or schema checks.
    """
    with attribute_errors(location):
        raw = parse_yaml(location)
        ext_type = detect_config_type_from_extension(location)
        if ext_type is None and require_typed_extension:
            raise _untyped_extension(location)
        kind = read_envelope(raw, ext_type)
        return kind, validate_schema(raw, kind)


def _untyped_extension(location: ConfigLocation) -> ConfigError:
    """C007 for a ``.yaml``/``.yml`` file where a typed extension is required."""
    message = f"Unsupported extension '{location.suffix}'. Expected .thread, .weave, or .loom"
    return ConfigError(
        message,
        file_path=str(location),
        diagnostics=[
            diagnostic(
                "C007",
                message,
                file=str(location),
                pointer="",
                expected=sorted(TYPED_EXTENSIONS),
            )
        ],
    )
