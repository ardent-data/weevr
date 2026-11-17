"""Attach the source file to config errors raised while loading it."""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace

from weevr.config.collect import recorded
from weevr.config.locations import ConfigLocation
from weevr.diagnostics.positions import Segments, fill_lines, json_pointer, parse_pointer
from weevr.errors.exceptions import ConfigError


@contextmanager
def attribute_errors(location: ConfigLocation | str, at: Segments = ()) -> Iterator[None]:
    """Name ``location`` on any ``ConfigError`` raised inside the block.

    Only errors and diagnostics that don't already name a file are updated,
    so when blocks nest (a weave loading a referenced thread), the innermost
    file wins. Diagnostics in that file with an authored pointer also gain
    their line and column. Inside a collecting load, errors that children
    recorded and skipped past within the block are attributed the same way.

    Args:
        location: The config file being processed.
        at: Where the document being processed sits in ``location`` (an
            inline thread's ``("threads", 2)``). Pointers raised inside the
            block are relative to that document and are prefixed with it.

    Yields:
        Nothing; the block runs unchanged.
    """
    file = str(location)
    collected = recorded()
    start = len(collected) if collected is not None else 0
    try:
        yield
    except ConfigError as exc:
        _attribute(exc, file, at)
        raise
    finally:
        # Errors a child recorded and skipped past inside this block belong
        # to this file too; the innermost block claims them first.
        for exc in (collected or [])[start:]:
            _attribute(exc, file, at)


def _attribute(exc: ConfigError, file: str, at: Segments) -> None:
    if at:
        exc.diagnostics = [
            replace(d, pointer=json_pointer(at + parse_pointer(d.pointer)))
            if d.file is None and d.pointer is not None
            else d
            for d in exc.diagnostics
        ]
    exc.locate(file)
    exc.diagnostics = fill_lines(exc.diagnostics, file)
