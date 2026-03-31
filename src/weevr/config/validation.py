"""Pre-resolution schema validation for the config loading pipeline.

These loose schemas validate config structure *before* variable substitution.
They intentionally use ``dict[str, Any]`` and ``extra="allow"`` so that
``${...}`` placeholder strings pass validation.  Strict domain-model
validation happens as the final step of ``load_config()`` via
``model_validate()``.

Post-resolution cross-cutting validators (``validate_incremental_config``,
``validate_dimension_overrides``) run after variable substitution on the
resolved thread config dict.
"""

from __future__ import annotations

import json
import re
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from weevr.errors import ConfigSchemaError
from weevr.model.params import ParamsConfig


class BaseConfig(BaseModel):
    """Base configuration model with shared fields."""

    model_config = {"extra": "allow"}

    config_version: str
    params: dict[str, Any] | None = None
    defaults: dict[str, Any] | None = None


class ThreadConfig(BaseConfig):
    """Thread configuration schema (pre-resolution, Phase 0 fields)."""

    sources: dict[str, Any]
    steps: list[dict[str, Any]] = Field(default_factory=list)
    target: dict[str, Any]
    write: dict[str, Any] | None = None
    keys: dict[str, Any] | None = None
    validations: list[dict[str, Any]] | None = None
    assertions: list[dict[str, Any]] | None = None
    load: dict[str, Any] | None = None
    tags: list[str] | None = None
    exports: list[dict[str, Any]] | None = None
    execution: dict[str, Any] | None = None
    connections: dict[str, Any] | None = None
    audit_templates: dict[str, Any] | None = None


class WeaveConfig(BaseConfig):
    """Weave configuration schema (pre-resolution)."""

    threads: list[str | dict[str, Any]]
    defaults: dict[str, Any] | None = None
    execution: dict[str, Any] | None = None
    lookups: dict[str, Any] | None = None
    column_sets: dict[str, Any] | None = None
    variables: dict[str, Any] | None = None
    connections: dict[str, Any] | None = None
    pre_steps: list[dict[str, Any]] | None = None
    post_steps: list[dict[str, Any]] | None = None
    audit_templates: dict[str, Any] | None = None


class LoomConfig(BaseConfig):
    """Loom configuration schema (pre-resolution)."""

    weaves: list[str | dict[str, Any]]
    defaults: dict[str, Any] | None = None
    execution: dict[str, Any] | None = None
    column_sets: dict[str, Any] | None = None
    lookups: dict[str, Any] | None = None
    variables: dict[str, Any] | None = None
    connections: dict[str, Any] | None = None
    pre_steps: list[dict[str, Any]] | None = None
    post_steps: list[dict[str, Any]] | None = None
    audit_templates: dict[str, Any] | None = None


def validate_schema(raw: dict[str, Any], config_type: str) -> BaseModel:
    """Validate a raw config dict against the appropriate pre-resolution schema.

    Args:
        raw: Raw config dictionary (variables not yet resolved)
        config_type: Type of config (thread, weave, loom, params)

    Returns:
        Validated Pydantic model instance

    Raises:
        ConfigSchemaError: If validation fails
    """
    model_map: dict[str, type[BaseModel]] = {
        "thread": ThreadConfig,
        "weave": WeaveConfig,
        "loom": LoomConfig,
        "params": ParamsConfig,
    }

    if config_type not in model_map:
        raise ConfigSchemaError(
            f"Unknown config type '{config_type}', expected one of: {', '.join(model_map.keys())}"
        )

    model_class = model_map[config_type]

    try:
        return model_class.model_validate(raw)
    except ValidationError as exc:
        raise ConfigSchemaError(
            f"Schema validation failed for {config_type} config: {exc}",
            cause=exc,
        ) from exc


# ---------------------------------------------------------------------------
# Post-resolution cross-cutting validators for incremental processing
# ---------------------------------------------------------------------------

_PARAM_REF_PATTERN = re.compile(r"\$\{param\.")


def _contains_param_reference(obj: Any) -> bool:
    """Recursively check if an object tree contains ``${param.`` references."""
    if isinstance(obj, str):
        return bool(_PARAM_REF_PATTERN.search(obj))
    if isinstance(obj, dict):
        return any(_contains_param_reference(v) for v in obj.values())
    if isinstance(obj, list):
        return any(_contains_param_reference(item) for item in obj)
    return False


def validate_incremental_config(thread_config: dict[str, Any]) -> list[str]:
    """Validate incremental processing cross-cutting constraints.

    Called after variable resolution on the full thread config dict.

    Returns a list of diagnostic messages. Messages starting with
    ``"ERROR:"`` are fatal; ``"WARN:"`` are advisory.

    Rules:
    - ``incremental_parameter`` mode should reference at least one ``${param.*}``
      variable in steps or sources (WARN if missing).
    - ``cdc`` mode requires ``write.mode: merge`` (ERROR if not).
    - ``watermark_inclusive=True`` with ``write.mode: append`` is risky (WARN).
    """
    diagnostics: list[str] = []

    load = thread_config.get("load")
    if not load:
        return diagnostics

    mode = load.get("mode", "full")
    write = thread_config.get("write", {}) or {}
    write_mode = write.get("mode", "overwrite")

    if mode == "incremental_parameter":
        # Scan steps + sources for ${param.} references
        has_param = _contains_param_reference(
            thread_config.get("steps", [])
        ) or _contains_param_reference(thread_config.get("sources", {}))
        if not has_param:
            # Also check serialized config as string fallback
            config_str = json.dumps(thread_config)
            if "${param." not in config_str:
                diagnostics.append(
                    "WARN: incremental_parameter mode but no ${param.*} "
                    "references found in steps or sources"
                )

    if mode == "cdc" and write_mode != "merge":
        diagnostics.append(f"ERROR: cdc mode requires write.mode='merge', got '{write_mode}'")

    if load.get("watermark_inclusive") and write_mode == "append":
        diagnostics.append(
            "WARN: watermark_inclusive=True with write.mode='append' "
            "may cause duplicate rows on re-reads; prefer merge or overwrite"
        )

    return diagnostics


# ---------------------------------------------------------------------------
# Post-resolution cross-cutting validators for dimension mode
# ---------------------------------------------------------------------------

#: write fields that the engine derives automatically from the dimension block
_DIMENSION_FORBIDDEN_WRITE_FIELDS = ("match_keys", "on_match")

#: keys fields declared inside the dimension block; forbidden at top level
_DIMENSION_FORBIDDEN_KEYS_FIELDS = ("business_key", "surrogate_key", "change_detection")


def validate_dimension_overrides(thread_config: dict[str, Any]) -> list[str]:
    """Validate write/keys override constraints when a dimension block is present.

    Called after variable resolution on the full thread config dict.

    Returns a list of diagnostic messages. Messages starting with
    ``"ERROR:"`` are fatal; ``"WARN:"`` are advisory.

    Rules (only applied when ``target.dimension`` is present):

    - ``write.match_keys`` → ERROR (engine derives from business_key)
    - ``write.on_match`` → ERROR (engine derives from on_change behaviour)
    - ``write.mode`` not ``"merge"`` → ERROR (dimension requires merge)
    - ``keys.business_key`` → ERROR (declared in dimension block)
    - ``keys.surrogate_key`` → ERROR (declared in dimension block)
    - ``keys.change_detection`` → ERROR (declared in dimension block)
    """
    diagnostics: list[str] = []

    target = thread_config.get("target") or {}
    if "dimension" not in target:
        return diagnostics

    write = thread_config.get("write") or {}
    keys = thread_config.get("keys") or {}

    for field in _DIMENSION_FORBIDDEN_WRITE_FIELDS:
        if field in write:
            diagnostics.append(
                f"ERROR: write.{field} cannot be set when target.dimension is present "
                f"(the engine derives it from the dimension block)"
            )

    if "mode" in write and write["mode"] != "merge":
        diagnostics.append(
            f"ERROR: write.mode must be 'merge' when target.dimension is present, "
            f"got '{write['mode']}'"
        )

    for field in _DIMENSION_FORBIDDEN_KEYS_FIELDS:
        if field in keys:
            diagnostics.append(
                f"ERROR: keys.{field} cannot be set when target.dimension is present "
                f"(declare it inside the dimension block instead)"
            )

    return diagnostics


# ---------------------------------------------------------------------------
# Post-resolution cross-cutting validators for resolve step lookups
# ---------------------------------------------------------------------------


def validate_resolve_lookups(
    thread_config: dict[str, Any],
    *,
    available_lookups: set[str],
) -> list[str]:
    """Validate that resolve steps reference defined lookup names.

    Called after variable resolution on the full thread config dict.

    Args:
        thread_config: Resolved thread configuration dictionary.
        available_lookups: Set of lookup names available from
            weave, loom, and thread levels.

    Returns:
        List of diagnostic messages. ``"ERROR:"`` prefixed entries
        are fatal.
    """
    diagnostics: list[str] = []
    steps = thread_config.get("steps") or []

    for step in steps:
        if not isinstance(step, dict) or "resolve" not in step:
            continue

        resolve = step["resolve"]
        if not isinstance(resolve, dict):
            continue

        # Collect all lookup names referenced by this resolve step
        lookup_names: list[str] = []

        # Single-mode lookup
        if "lookup" in resolve and isinstance(resolve["lookup"], str):
            lookup_names.append(resolve["lookup"])

        # Batch-mode lookups
        batch = resolve.get("batch")
        if isinstance(batch, list):
            for item in batch:
                if isinstance(item, dict) and "lookup" in item:
                    lookup_names.append(item["lookup"])

        # Validate each referenced lookup
        for name in lookup_names:
            if name not in available_lookups:
                diagnostics.append(
                    f"ERROR: resolve step references undefined lookup "
                    f"'{name}'; available lookups: "
                    f"{sorted(available_lookups) or '(none)'}"
                )

    return diagnostics
