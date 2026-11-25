"""Param values by declared type: the binding coercions and the text a value renders.

:func:`coerce_param` checks and coerces a bound value against its declared
``ParamSpec.type``; :func:`param_text` is the text a ``${param.x}`` reference
embedded in a larger string renders for a value, at load and at execution.
"""

import math
import re
from collections.abc import Callable
from datetime import date, datetime
from typing import Any


def param_text(value: Any) -> str:
    """The text a ``${param.x}`` reference embedded in a larger string renders for ``value``.

    A bool renders as YAML writes it, ``true`` or ``false``; any other
    value, a string included, by ``str()``.

    Args:
        value: The param's value.

    Returns:
        The text to substitute.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


_INT = re.compile(r"[+-]?[0-9]+")
_FLOAT = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")


def _mismatch(value: Any, type_name: str) -> TypeError:
    return TypeError(f"{type(value).__name__} is not a {type_name}")


def _to_string(value: Any) -> str:
    if isinstance(value, str):
        return value
    raise _mismatch(value, "string")


def _to_int(value: Any) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and _INT.fullmatch(value):
        try:
            return int(value)
        except ValueError:  # more digits than int() converts
            pass
    raise _mismatch(value, "int")


def _to_float(value: Any) -> float:
    numeric = isinstance(value, (int, float)) and not isinstance(value, bool)
    if numeric or (isinstance(value, str) and _FLOAT.fullmatch(value)):
        try:
            result = float(value)
        except OverflowError:  # an int too large for a float
            result = math.inf
        if math.isfinite(result):
            return value if isinstance(value, float) else result
    raise _mismatch(value, "float")


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ("true", "false"):
        return value.lower() == "true"
    raise _mismatch(value, "bool")


def _to_date(value: Any) -> str:
    """A date as its ISO string: a ``YYYY-MM-DD`` string as written, a date's ``isoformat()``.

    A string is checked with ``strptime("%Y-%m-%d")``, so an unpadded
    ``2024-1-5`` is accepted as in v1; values bind as written. Binding keeps
    dates as strings; native date objects are left to later field-type
    coercion.
    """
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        datetime.strptime(value, "%Y-%m-%d")
        return value
    raise _mismatch(value, "date")


def _to_timestamp(value: Any) -> str:
    """A timestamp as its ISO string: an ISO string as written, a datetime's ``isoformat()``.

    A string is checked with ``datetime.fromisoformat``, so a date-only
    ``2024-01-05``, a space separator, and the compact (``20240105T1030``)
    and week (``2024-W01-1``) forms are accepted as in v1; values bind as
    written. Binding keeps timestamps as strings, so ``T`` and ``Z`` survive
    in embedded references; native datetime objects are left to later
    field-type coercion.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        datetime.fromisoformat(value)
        return value
    raise _mismatch(value, "timestamp")


def _to_string_list(value: Any) -> list[str]:
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    raise _mismatch(value, "list[string]")


ISO_TYPES = frozenset({"date", "timestamp"})
"""The types whose malformed strings fail ``P103`` rather than ``P102``."""

_COERCIONS: dict[str, Callable[[Any], Any]] = {
    "string": _to_string,
    "int": _to_int,
    "float": _to_float,
    "bool": _to_bool,
    "date": _to_date,
    "timestamp": _to_timestamp,
    "list[string]": _to_string_list,
}


def coerce_param(value: Any, type_name: str) -> Any:
    """Coerce ``value`` to the param type ``type_name``, by the narrow binding table.

    - ``string``: a string.
    - ``int``: an int (not a bool), or a base-10 string (``"42"``, ``"-3"``).
    - ``float``: a finite float, an int (not a bool), or a numeric string.
    - ``bool``: a bool, or ``"true"`` / ``"false"`` in any case.
    - ``date``: a ``YYYY-MM-DD`` string (bound as written), or a date
      (a datetime's date part), bound as its ``isoformat()``.
    - ``timestamp``: an ISO 8601 string (bound as written), or a datetime,
      bound as its ``isoformat()``.

    Date and timestamp strings are accepted as in v1; values bind as written.
    A date may be unpadded (``2024-1-5``). A timestamp may be date-only, use a
    space separator, or take the compact (``20240105T1030``) or week
    (``2024-W01-1``) forms ``datetime.fromisoformat`` reads on Python 3.12
    and 3.13.
    - ``list[string]``: a list of strings.

    A type name outside the table passes the value through unchanged.

    Args:
        value: The value to coerce.
        type_name: A ``ParamSpec.type`` name.

    Returns:
        The value as the type's native Python value; for ``date`` and
        ``timestamp``, its ISO string. A value already of the bound form is
        returned as the same object. The coerced value is what a whole-value
        reference reads; an embedded reference renders the value as supplied
        (:attr:`weevr.config.params.Binding.text`).

    Raises:
        TypeError: The value is not one the type accepts.
        ValueError: A date or timestamp string is not in ISO format.
    """
    convert = _COERCIONS.get(type_name)
    return value if convert is None else convert(value)
