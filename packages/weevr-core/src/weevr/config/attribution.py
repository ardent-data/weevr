"""Attach the source file to config errors raised while loading it."""

from collections.abc import Iterator
from contextlib import contextmanager

from weevr.config.locations import ConfigLocation
from weevr.errors.exceptions import ConfigError


@contextmanager
def attribute_errors(location: ConfigLocation) -> Iterator[None]:
    """Name ``location`` on any ``ConfigError`` raised inside the block.

    Only errors that don't already name a file are updated, so when blocks
    nest (a weave loading a referenced thread), the innermost file wins.

    Args:
        location: The config file being processed.

    Yields:
        Nothing; the block runs unchanged.
    """
    try:
        yield
    except ConfigError as exc:
        if exc.file_path is None:
            exc.file_path = str(location)
        raise
