"""Pre-resolution schema validation for the config loading pipeline.

These loose schemas validate config structure *before* variable substitution.
They intentionally use ``dict[str, Any]`` and ``extra="allow"`` so that
``${...}`` placeholder strings pass validation.  Strict domain-model
validation happens as the final step of ``load_config()`` via
``model_validate()``.

Post-resolution cross-cutting validators (``validate_incremental_config``)
run after variable substitution on the resolved thread config dict.
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
    audit_templates: dict[str, Any] | None = None


class WeaveConfig(BaseConfig):
    """Weave configuration schema (pre-resolution)."""

    threads: list[str | dict[str, Any]]
    defaults: dict[str, Any] | None = None
    execution: dict[str, Any] | None = None
    lookups: dict[str, Any] | None = None
    column_sets: dict[str, Any] | None = None
    variables: dict[str, Any] | None = None
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
