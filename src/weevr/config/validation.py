"""Pre-resolution schema validation for the config loading pipeline.

These loose schemas validate config structure *before* variable substitution.
They intentionally use ``dict[str, Any]`` and ``extra="allow"`` so that
``${...}`` placeholder strings pass validation.  Strict domain-model
validation happens as the final step of ``load_config()`` via
``model_validate()``.
"""

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
    execution: dict[str, Any] | None = None


class WeaveConfig(BaseConfig):
    """Weave configuration schema (pre-resolution)."""

    threads: list[str]
    defaults: dict[str, Any] | None = None
    execution: dict[str, Any] | None = None


class LoomConfig(BaseConfig):
    """Loom configuration schema (pre-resolution)."""

    weaves: list[str]
    defaults: dict[str, Any] | None = None
    execution: dict[str, Any] | None = None


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
