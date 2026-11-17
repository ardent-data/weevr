"""Collect every config problem in one load instead of stopping at the first.

A load runs inside :func:`collecting`. Where it processes children one by
one, each child runs inside :func:`skip_on_error`: a ``ConfigError`` from
that child is recorded and the child is left out, so its later stages are
skipped while its siblings carry on. When the load ends, the recorded
errors are raised together as one error (see :func:`combine`).

Outside :func:`collecting`, :func:`skip_on_error` does nothing and errors
propagate as they always have.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar

from weevr.diagnostics import Diagnostic, diagnostic
from weevr.errors.exceptions import ConfigError

_collected: ContextVar[list[ConfigError] | None] = ContextVar("weevr_collected", default=None)


@contextmanager
def collecting(into: list[ConfigError] | None = None) -> Iterator[None]:
    """Run a load that reports every problem it finds in one error.

    Nested uses join the outermost one.

    Args:
        into: Collect instead of raising: the errors found, including one
            that ends the block, are appended here and nothing is raised.
            For a caller that reports problems rather than stopping on them.

    Raises:
        ConfigError: Without ``into``, the errors recorded inside the block,
            combined, plus any error that ends the block.
    """
    if _collected.get() is not None:
        yield
        return
    errors: list[ConfigError] = []
    token = _collected.set(errors)
    try:
        yield
    except ConfigError as exc:
        errors.append(exc)
    except Exception as exc:
        if not errors:
            raise
        if into is None:
            # A child was skipped, and a later stage tripped over its
            # absence: the recorded problems are the cause worth reporting.
            raise combine(errors) from exc
    finally:
        _collected.reset(token)
    if into is not None:
        into.extend(errors)
    elif errors:
        raise combine(errors)


@contextmanager
def skip_on_error() -> Iterator[None]:
    """Record a ``ConfigError`` raised in the block and carry on, inside :func:`collecting`."""
    errors = _collected.get()
    if errors is None:
        yield
        return
    try:
        yield
    except ConfigError as exc:
        errors.append(exc)


def recorded() -> list[ConfigError] | None:
    """The errors recorded so far in the active :func:`collecting`, or ``None`` outside one."""
    return _collected.get()


def _position(diagnostic: Diagnostic) -> tuple[str, int, int]:
    return (diagnostic.file or "", diagnostic.line or 0, diagnostic.column or 0)


def combine(errors: Sequence[ConfigError]) -> ConfigError:
    """Merge config errors into one carrying every diagnostic.

    A single error is returned unchanged. Otherwise the result has the class
    of the error behind the first diagnostic (by file, then position), so an
    ``except`` for that class still catches it, and its diagnostics are
    every error's, in that order.

    Args:
        errors: The errors, in the order they were found.

    Returns:
        The combined error.
    """
    if len(errors) == 1:
        return errors[0]
    owned = [(d, error) for error in errors for d in error.diagnostics]
    owned.sort(key=lambda pair: _position(pair[0]))
    first = owned[0][1] if owned else errors[0]
    message = f"{len(errors)} config problems:\n" + "\n".join(f"- {e}" for e in errors)
    return type(first)(
        message,
        first,
        file_path=first.file_path,
        diagnostics=[d for d, _ in owned],
    )


def diagnostics_of(errors: Sequence[ConfigError]) -> list[Diagnostic]:
    """Every diagnostic the errors carry, ordered by file, then position.

    An error that carries none is reported under the generic ``C400`` with
    its message and file, so no problem is dropped from a report.

    Args:
        errors: Config errors.

    Returns:
        The diagnostics.
    """
    found: list[Diagnostic] = []
    for error in errors:
        found.extend(
            error.diagnostics
            or [diagnostic("C400", error.message, file=error.file_path, origin=__name__)]
        )
    return sorted(found, key=_position)
