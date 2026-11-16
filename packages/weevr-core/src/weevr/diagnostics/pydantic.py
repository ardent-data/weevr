"""Turn a pydantic ``ValidationError`` into coded diagnostics.

Each pydantic error becomes one diagnostic, with ``resolved_path`` a JSON
pointer into the validated document. Union-member labels that pydantic puts
in error locations (step tags, ``str``, ``literal[False]``) are dropped, and
the errors of a plain union's members collapse into one wrong-type
diagnostic for the value.

Model validators raise ``PydanticCustomError`` with a registered code as
its type; the code passes through unchanged. Other validator errors become
the generic ``C400``, and error types with no mapping become ``C100``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError
from pydantic_core import ErrorDetails

from weevr.diagnostics.diagnostic import Diagnostic
from weevr.diagnostics.positions import ABSENT, child_of, json_pointer
from weevr.diagnostics.registry import REGISTRY, diagnostic

_CONSTRAINTS = frozenset(
    {
        "greater_than",
        "greater_than_equal",
        "less_than",
        "less_than_equal",
        "multiple_of",
        "too_short",
        "too_long",
        "string_too_short",
        "string_too_long",
        "string_pattern_mismatch",
    }
)
_VALIDATOR_PREFIXES = ("Value error, ", "Assertion failed, ")
_QUOTED = re.compile(r"'((?:[^'\\]|\\.)*)'")


@dataclass(frozen=True)
class _Located:
    error: ErrorDetails
    path: tuple[str | int, ...]
    labels: tuple[str, ...]


def from_validation_error(
    error: ValidationError,
    data: Any,
    *,
    file: str | None = None,
) -> list[Diagnostic]:
    """Convert every error in a pydantic ``ValidationError``.

    Args:
        error: The validation failure.
        data: The input that was validated, used to tell real keys from
            union-member labels in error locations.
        file: The file the input came from.

    Returns:
        One diagnostic per problem, in pydantic's order.
    """
    located = [_locate(details, data) for details in error.errors()]
    groups: dict[tuple[str | int, ...], list[_Located]] = {}
    for item in located:
        groups.setdefault(item.path, []).append(item)

    diagnostics: list[Diagnostic] = []
    for path, items in groups.items():
        if len(items) > 1 and all(item.labels for item in items):
            diagnostics.append(_union_mismatch(path, items, file))
        else:
            diagnostics.extend(_convert(item, file) for item in items)
    return diagnostics


def _locate(details: ErrorDetails, data: Any) -> _Located:
    """Split an error location into the real path and the union labels in it."""
    path: list[str | int] = []
    labels: list[str] = []
    node = data
    loc = details["loc"]
    for index, segment in enumerate(loc):
        child = child_of(node, segment)
        if child is not ABSENT:
            path.append(segment)
            node = child
        elif index == len(loc) - 1 and isinstance(node, dict):
            path.append(segment)  # a key the input lacks: a missing field
            node = None
        else:
            labels.append(str(segment))
    return _Located(details, tuple(path), tuple(labels))


def _step_context(path: tuple[str | int, ...]) -> dict[str, Any]:
    """Step index and type for a path into a thread's ``steps``."""
    if "steps" not in path:
        return {}
    at = path.index("steps")
    rest = path[at + 1 :]
    if not rest or not isinstance(rest[0], int):
        return {}
    context: dict[str, Any] = {"step_index": rest[0]}
    if len(rest) > 1 and isinstance(rest[1], str):
        context["step_type"] = rest[1]
    return context


def _code(error_type: str) -> str:
    if error_type in REGISTRY:
        return error_type
    if error_type == "missing":
        return "C101"
    if error_type.endswith(("_type", "_parsing")) or error_type in {
        "int_from_float",
        "is_instance_of",
    }:
        return "C102"
    if error_type in {"literal_error", "enum"}:
        return "C103"
    if error_type in {"union_tag_invalid", "union_tag_not_found"}:
        return "C104"
    if error_type in _CONSTRAINTS:
        return "C105"
    if error_type == "extra_forbidden":
        return "C110"
    if error_type in {"value_error", "assertion_error"}:
        return "C400"
    return "C100"


def _expected(details: ErrorDetails) -> list[str] | None:
    ctx = details.get("ctx") or {}
    if details["type"] in {"union_tag_invalid", "union_tag_not_found"}:
        return _QUOTED.findall(str(ctx.get("expected_tags", ""))) or None
    if details["type"] in {"literal_error", "enum"}:
        text = str(ctx.get("expected", ""))
        return _QUOTED.findall(text) or ([text] if text else None)
    return None


def _message(details: ErrorDetails) -> str:
    message = details["msg"]
    for prefix in _VALIDATOR_PREFIXES:
        if message.startswith(prefix):
            return message[len(prefix) :]
    return message


def _convert(item: _Located, file: str | None) -> Diagnostic:
    return diagnostic(
        _code(item.error["type"]),
        _message(item.error),
        file=file,
        resolved_path=json_pointer(item.path),
        expected=_expected(item.error),
        **_step_context(item.path),
    )


def _union_mismatch(
    path: tuple[str | int, ...], items: list[_Located], file: str | None
) -> Diagnostic:
    """One wrong-type diagnostic for a value no member of a plain union accepts."""
    expected: list[str] = []
    for item in items:
        values = _expected(item.error) if item.error["type"] == "literal_error" else None
        for value in values or [item.labels[-1]]:
            if value not in expected:
                expected.append(value)
    return diagnostic(
        "C102",
        f"Value matches none of the allowed types ({', '.join(expected)})",
        file=file,
        resolved_path=json_pointer(path),
        expected=expected,
        **_step_context(path),
    )
