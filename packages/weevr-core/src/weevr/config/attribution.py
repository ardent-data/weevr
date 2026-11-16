"""Attach the source file to config errors raised while loading it."""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace

from weevr.config.locations import ConfigLocation
from weevr.diagnostics.positions import Segments, fill_lines, json_pointer, parse_pointer
from weevr.errors.exceptions import ConfigError


@contextmanager
def attribute_errors(location: ConfigLocation | str, at: Segments = ()) -> Iterator[None]:
    """Name ``location`` on any ``ConfigError`` raised inside the block.

    Only errors and diagnostics that don't already name a file are updated,
    so when blocks nest (a weave loading a referenced thread), the innermost
    file wins. Diagnostics in that file with an authored pointer also gain
    their line and column.

    Args:
        location: The config file being processed.
        at: Where the document being processed sits in ``location`` (an
            inline thread's ``("threads", 2)``). Pointers raised inside the
            block are relative to that document and are prefixed with it.

    Yields:
        Nothing; the block runs unchanged.
    """
    try:
        yield
    except ConfigError as exc:
        if at:
            exc.diagnostics = [
                replace(d, pointer=json_pointer(at + parse_pointer(d.pointer)))
                if d.file is None and d.pointer is not None
                else d
                for d in exc.diagnostics
            ]
        exc.locate(str(location))
        exc.diagnostics = fill_lines(exc.diagnostics, str(location))
        raise
