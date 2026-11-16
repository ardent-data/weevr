"""Attach the source file to config errors raised while loading it."""

from collections.abc import Iterator
from contextlib import contextmanager

from weevr.config.locations import ConfigLocation
from weevr.diagnostics.positions import fill_lines
from weevr.errors.exceptions import ConfigError


@contextmanager
def attribute_errors(location: ConfigLocation | str) -> Iterator[None]:
    """Name ``location`` on any ``ConfigError`` raised inside the block.

    Only errors and diagnostics that don't already name a file are updated,
    so when blocks nest (a weave loading a referenced thread), the innermost
    file wins. Diagnostics in that file with an authored pointer also gain
    their line and column.

    Args:
        location: The config file being processed.

    Yields:
        Nothing; the block runs unchanged.
    """
    try:
        yield
    except ConfigError as exc:
        exc.locate(str(location))
        exc.diagnostics = fill_lines(exc.diagnostics, str(location))
        raise
