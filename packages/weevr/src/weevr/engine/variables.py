"""Runtime variable context for weave-scoped variables."""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from weevr.model.variable import VariableSpec

_VAR_PATTERN = re.compile(r"\$\{var\.([a-zA-Z_][a-zA-Z0-9_]*)\}")

# Maps VariableSpec type names to Python types for validation
_TYPE_CASTERS: dict[str, type] = {
    "string": str,
    "int": int,
    "long": int,
    "float": float,
    "double": float,
    "boolean": bool,
    "timestamp": datetime,
    "date": date,
}


class VariableContext:
    """Mutable runtime store for weave-scoped variables.

    Initialised from ``VariableSpec`` definitions, with optional defaults.
    Values are set during hook execution (e.g. via ``set_var`` on
    ``SqlStatementStep``) and referenced in downstream config as
    ``${var.name}``.

    Args:
        specs: Variable name → spec mapping from the weave definition.
    """

    def __init__(self, specs: dict[str, VariableSpec] | None = None) -> None:
        """Initialise from variable spec definitions.

        Args:
            specs: Variable name to spec mapping. Defaults to empty.
        """
        self._specs: dict[str, VariableSpec] = specs or {}
        self._values: dict[str, Any] = {}
        for name, spec in self._specs.items():
            if spec.default is not None:
                self._values[name] = spec.default

    def get(self, name: str) -> Any:
        """Return the current value of a variable.

        Args:
            name: Variable name.

        Returns:
            Current value, or ``None`` if the variable has no value set.

        Raises:
            KeyError: If the variable name is not declared.
        """
        if name not in self._specs:
            raise KeyError(f"Variable '{name}' is not declared")
        return self._values.get(name)

    def set(self, name: str, value: Any) -> None:
        """Set the value of a variable with type validation.

        If ``value`` is ``None``, the variable is set to ``None`` regardless of
        declared type (null handling for empty SQL results).

        Args:
            name: Variable name.
            value: Value to store. Will be cast to the declared type.

        Raises:
            KeyError: If the variable name is not declared.
            TypeError: If the value cannot be cast to the declared type.
        """
        if name not in self._specs:
            raise KeyError(f"Variable '{name}' is not declared")
        if value is None:
            self._values[name] = None
            return
        spec = self._specs[name]
        caster = _TYPE_CASTERS.get(spec.type)
        if caster is None:
            self._values[name] = value
            return
        if isinstance(value, caster):
            self._values[name] = value
            return
        try:
            self._values[name] = caster(value)
        except (ValueError, TypeError) as exc:
            msg = f"Cannot cast {value!r} to type '{spec.type}' for variable '{name}'"
            raise TypeError(msg) from exc

    def resolve_refs(self, text: str) -> str:
        """Replace ``${var.name}`` placeholders in a string.

        Undeclared or unset variables are left as-is (no error).

        Args:
            text: Input string potentially containing ``${var.name}`` refs.

        Returns:
            String with resolved variable references.
        """

        def _replacer(match: re.Match[str]) -> str:
            name = match.group(1)
            if name in self._specs and name in self._values:
                val = self._values[name]
                if val is not None:
                    return str(val)
            return match.group(0)

        return _VAR_PATTERN.sub(_replacer, text)

    def snapshot(self) -> dict[str, Any]:
        """Return a copy of current variable values for telemetry.

        Returns:
            Dict mapping variable names to their current values.
        """
        return dict(self._values)
