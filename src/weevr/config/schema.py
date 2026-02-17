"""Pydantic schema models for config validation."""

from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError

from weevr.errors import ConfigSchemaError


class ParamSpec(BaseModel):
    """Parameter specification for typed validation."""

    name: str
    type: Literal["string", "int", "float", "bool", "date", "timestamp", "list[string]"]
    required: bool = True
    default: Any = None
    description: str = ""


class BaseConfig(BaseModel):
    """Base configuration model with shared fields."""

    model_config = {"extra": "allow"}

    config_version: str
    params: dict[str, ParamSpec] | None = None
    defaults: dict[str, Any] | None = None


class ThreadConfig(BaseConfig):
    """Thread configuration schema (Phase 0 fields)."""

    sources: dict[str, Any]
    steps: list[dict[str, Any]] = Field(default_factory=list)
    target: dict[str, Any]
    write: dict[str, Any] | None = None
    keys: dict[str, Any] | None = None
    validations: list[dict[str, Any]] | None = None
    assertions: list[dict[str, Any]] | None = None
    load: dict[str, Any] | None = None
    tags: list[str] | None = None


class WeaveConfig(BaseConfig):
    """Weave configuration schema."""

    threads: list[str]
    defaults: dict[str, Any] | None = None


class LoomConfig(BaseConfig):
    """Loom configuration schema."""

    weaves: list[str]
    defaults: dict[str, Any] | None = None


class ParamsConfig(BaseModel):
    """Parameter file schema (flat key-value structure)."""

    config_version: str
    # Allow arbitrary additional fields for params
    model_config = {"extra": "allow"}


def validate_schema(raw: dict[str, Any], config_type: str) -> BaseModel:
    """Validate config dict against appropriate Pydantic schema.

    Args:
        raw: Raw config dictionary
        config_type: Type of config (thread, weave, loom, params)

    Returns:
        Validated Pydantic model instance

    Raises:
        ConfigSchemaError: If validation fails
    """
    model_map = {
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
    except ValidationError as e:
        raise ConfigSchemaError(
            f"Schema validation failed for {config_type} config: {e}",
            cause=e,
        ) from e
