"""Config macro expansion — foreach loop processing."""

import copy
from typing import Any

from weevr.errors.exceptions import ConfigError


def _substitute_value(obj: Any, var_name: str, value: Any) -> Any:
    """Recursively substitute ``{var_name}`` placeholders in strings within a nested structure."""
    if isinstance(obj, str):
        return obj.replace(f"{{{var_name}}}", str(value))
    if isinstance(obj, dict):
        return {k: _substitute_value(v, var_name, value) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_substitute_value(item, var_name, value) for item in obj]
    return obj


def expand_foreach(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Expand foreach macro blocks into repeated step sequences.

    Each foreach block in the steps list is replaced by its template steps
    repeated once per value, with ``{var}`` placeholders substituted.

    Non-foreach entries pass through unchanged.

    Args:
        steps: Raw step list (dicts), possibly containing foreach blocks.

    Returns:
        Expanded step list with all foreach blocks replaced.

    Raises:
        ConfigError: If a foreach block is missing required fields.
    """
    result: list[dict[str, Any]] = []

    for i, item in enumerate(steps):
        if not isinstance(item, dict):
            result.append(item)
            continue

        if "foreach" not in item:
            result.append(item)
            continue

        foreach = item["foreach"]
        if not isinstance(foreach, dict):
            raise ConfigError(
                f"foreach block at step index {i}: 'foreach' must be a mapping"
            )

        values = foreach.get("values")
        var_name = foreach.get("as")
        template_steps = foreach.get("steps")

        if values is None or (isinstance(values, list) and len(values) == 0):
            raise ConfigError(
                f"foreach block at step index {i}: 'values' must be a non-empty list"
            )
        if not var_name:
            raise ConfigError(
                f"foreach block at step index {i}: 'as' variable name is required"
            )
        if not template_steps or not isinstance(template_steps, list):
            raise ConfigError(
                f"foreach block at step index {i}: 'steps' must be a non-empty list"
            )

        for value in values:
            for template in template_steps:
                expanded = _substitute_value(copy.deepcopy(template), var_name, value)
                result.append(expanded)

    return result
