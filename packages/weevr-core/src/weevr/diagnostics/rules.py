"""Raise a coded error from a pydantic model validator."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, LiteralString, cast

from pydantic_core import PydanticCustomError

from weevr.diagnostics.registry import REGISTRY


def rule_error(
    code: str,
    message: str,
    *,
    expected: Sequence[Any] | None = None,
    hint: str | None = None,
) -> PydanticCustomError:
    """Build the error a model validator raises for a broken config rule.

    The code becomes the pydantic error type, so it reaches the diagnostic
    unchanged; ``expected`` and ``hint`` travel in the error context.
    Raise it from a ``field_validator`` or ``model_validator``::

        raise rule_error("C401", f"Step '{name}' sets both 'a' and 'b'")

    Args:
        code: A registered code.
        message: Human-readable description. Braces need no escaping.
        expected: Allowed keys or values, when the rule has a fixed set.
        hint: What to change.

    Returns:
        The error to raise.

    Raises:
        ValueError: If ``code`` is not registered.
    """
    if code not in REGISTRY:
        raise ValueError(f"Diagnostic code '{code}' is not registered")
    context: dict[str, Any] = {"message": message}
    if expected is not None:
        context["expected"] = [str(value) for value in expected]
    if hint is not None:
        context["hint"] = hint
    # Checked against the registry above, so it is one of the registry's literals.
    return PydanticCustomError(cast(LiteralString, code), "{message}", context)
