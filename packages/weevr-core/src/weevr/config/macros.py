"""Config macro expansion — foreach loop processing."""

import copy
from typing import Any

from weevr.diagnostics import diagnostic
from weevr.diagnostics.positions import json_pointer
from weevr.errors.exceptions import ConfigError


def _foreach_error(
    code: str, message: str, pointer: str | None = None, hint: str | None = None
) -> ConfigError:
    return ConfigError(
        message,
        diagnostics=[diagnostic(code, message, pointer=pointer, hint=hint, origin=__name__)],
    )


def _substitute_value(obj: Any, var_name: str, value: Any) -> Any:
    """Recursively substitute ``{var_name}`` placeholders in strings within a nested structure."""
    if isinstance(obj, str):
        return obj.replace(f"{{{var_name}}}", str(value))
    if isinstance(obj, dict):
        return {
            _substitute_value(k, var_name, value): _substitute_value(v, var_name, value)
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_substitute_value(item, var_name, value) for item in obj]
    return obj


def reject_foreach_in_with(thread: dict[str, Any]) -> None:
    """Reject ``foreach`` in a ``with:`` sub-pipeline, which isn't expanded.

    Args:
        thread: Raw or resolved thread dict.

    Raises:
        ConfigError: Naming the sub-pipeline that holds a ``foreach`` block.
    """
    sub_pipelines = thread.get("with_") or thread.get("with") or {}
    if not isinstance(sub_pipelines, dict):
        return
    for name, sub_pipeline in sub_pipelines.items():
        steps = sub_pipeline.get("steps") if isinstance(sub_pipeline, dict) else None
        if isinstance(steps, list) and any(isinstance(s, dict) and "foreach" in s for s in steps):
            raise _foreach_error(
                "C309",
                f"foreach is not supported in with.{name}.steps; "
                f"use it in the thread's top-level steps",
                json_pointer(("with", name, "steps")),
                hint="Use foreach in the thread's top-level steps.",
            )


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
            raise _foreach_error(
                "C308",
                f"foreach block at step index {i}: 'foreach' must be a mapping",
                json_pointer(("steps", i, "foreach")),
            )

        values = foreach.get("values")
        var_name = foreach.get("as")
        template_steps = foreach.get("steps")

        if not isinstance(values, list) or len(values) == 0:
            raise _foreach_error(
                "C308",
                f"foreach block at step index {i}: 'values' must be a non-empty list",
                json_pointer(("steps", i, "foreach", "values")),
            )
        if not var_name:
            raise _foreach_error(
                "C308",
                f"foreach block at step index {i}: 'as' variable name is required",
                json_pointer(("steps", i, "foreach", "as")),
            )
        if not template_steps or not isinstance(template_steps, list):
            raise _foreach_error(
                "C308",
                f"foreach block at step index {i}: 'steps' must be a non-empty list",
                json_pointer(("steps", i, "foreach", "steps")),
            )

        for value in values:
            for template in template_steps:
                expanded = _substitute_value(copy.deepcopy(template), var_name, value)
                result.append(expanded)

    return result
