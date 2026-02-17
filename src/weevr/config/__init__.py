"""Configuration loading and validation."""

from pathlib import Path
from typing import Any

from weevr.config.inheritance import apply_inheritance
from weevr.config.parser import (
    detect_config_type,
    extract_config_version,
    parse_yaml,
    validate_config_version,
)
from weevr.config.resolver import build_param_context, resolve_references, resolve_variables
from weevr.config.schema import validate_schema


def load_config(
    path: str | Path,
    runtime_params: dict[str, Any] | None = None,
    param_file: str | Path | None = None,
) -> dict[str, Any]:
    """Load and validate a weevr configuration file.

    This function orchestrates the full config loading pipeline:
    1. Parse YAML file
    2. Extract and validate config_version
    3. Detect config type (thread, weave, loom, params)
    4. Validate schema with Pydantic
    5. Build parameter context (runtime > param_file > defaults)
    6. Resolve variable references (${var} and ${var:-default})
    7. Resolve references to child configs (threads, weaves)
    8. Apply inheritance cascade (loom -> weave -> thread)

    Args:
        path: Path to the config file (thread, weave, or loom)
        runtime_params: Optional runtime parameter overrides
        param_file: Optional parameter file path

    Returns:
        Fully resolved and validated configuration dictionary with:
        - All variables interpolated
        - References resolved (child configs in _resolved_threads/_resolved_weaves)
        - Inheritance applied where applicable

    Raises:
        ConfigParseError: YAML syntax errors, file not found
        ConfigVersionError: Unsupported config version
        ConfigSchemaError: Schema validation failures
        VariableResolutionError: Unresolved variables without defaults
        ReferenceResolutionError: Missing referenced files, circular dependencies
    """
    # Step 1: Parse YAML
    raw = parse_yaml(path)

    # Step 2: Extract and validate version
    version = extract_config_version(raw)

    # Step 3: Detect config type
    config_type = detect_config_type(raw)

    # Step 4: Validate schema
    validate_config_version(version, config_type)
    validated = validate_schema(raw, config_type)
    config_dict = validated.model_dump(exclude_unset=True)

    # Step 5: Build parameter context
    param_file_data = None
    if param_file:
        param_file_data = parse_yaml(param_file)

    context = build_param_context(
        runtime_params,
        param_file_data,
        config_dict.get("defaults") or config_dict.get("params"),
    )

    # Step 6: Resolve variables
    resolved = resolve_variables(config_dict, context)

    # Step 7: Resolve references to child configs
    base_path = Path(path).parent
    # Walk up to find project root (assume config files are in subdirs)
    if config_type == "thread":
        # threads/path/to/thread.yaml -> go up to project root
        base_path = base_path.parent
        if base_path.name == "threads":
            base_path = base_path.parent
    elif config_type == "weave":
        # weaves/weave.yaml -> go up to project root
        if base_path.name == "weaves":
            base_path = base_path.parent
    elif config_type == "loom":
        # looms/loom.yaml -> go up to project root
        if base_path.name == "looms":
            base_path = base_path.parent

    param_file_path = Path(param_file) if param_file else None
    resolved_with_refs = resolve_references(
        resolved,
        config_type,
        base_path,
        runtime_params,
        param_file_path,
    )

    # Step 8: Apply inheritance for child configs
    if config_type == "loom" and "_resolved_weaves" in resolved_with_refs:
        loom_defaults = resolved_with_refs.get("defaults")
        for weave in resolved_with_refs["_resolved_weaves"]:
            if "_resolved_threads" in weave:
                weave_defaults = weave.get("defaults")
                for i, thread in enumerate(weave["_resolved_threads"]):
                    merged = apply_inheritance(loom_defaults, weave_defaults, thread)
                    weave["_resolved_threads"][i] = merged

    elif config_type == "weave" and "_resolved_threads" in resolved_with_refs:
        weave_defaults = resolved_with_refs.get("defaults")
        for i, thread in enumerate(resolved_with_refs["_resolved_threads"]):
            merged = apply_inheritance(None, weave_defaults, thread)
            resolved_with_refs["_resolved_threads"][i] = merged

    return resolved_with_refs
