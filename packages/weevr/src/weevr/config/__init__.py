"""Configuration loading and validation."""

from pathlib import Path
from typing import Any

from pydantic import ValidationError

from weevr.config.inheritance import apply_inheritance
from weevr.config.locations import ConfigLocation, LocalConfigLocation, make_location
from weevr.config.macros import expand_foreach
from weevr.config.parser import (
    detect_config_type,
    detect_config_type_from_extension,
    extract_config_version,
    parse_yaml,
    validate_config_version,
)
from weevr.config.resolver import (
    build_param_context,
    resolve_declared_params,
    resolve_references,
    resolve_variables,
    validate_params,
)
from weevr.config.validation import validate_schema
from weevr.errors import ConfigError, ModelValidationError
from weevr.model import Loom, Thread, Weave


def _derive_config_name(path: ConfigLocation) -> str:
    """Derive the component name from a config location.

    Returns the filename stem — the filename without the typed extension.
    For example, ``dim_customer.thread`` returns ``'dim_customer'``.
    """
    return path.stem


def load_config(
    path: str | Path | ConfigLocation,
    runtime_params: dict[str, Any] | None = None,
    project_root: Path | ConfigLocation | None = None,
) -> Thread | Weave | Loom | dict[str, Any]:
    """Load and validate a weevr configuration file.

    This function orchestrates the full config loading pipeline:
    1. Parse YAML file
    2. Extract and validate config_version
    3. Detect config type (extension-based for components, content-based for params)
    4. Validate schema with Pydantic
    5. Build parameter context (runtime > defaults)
    6. Resolve variable references (${var} and ${var:-default})
    7. Resolve references to child configs (threads, weaves)
    8. Apply inheritance cascade (loom -> weave -> thread)
    9. Validate name against filename stem
    10. Hydrate into typed domain model (thread, weave, loom only)

    Args:
        path: Path or :class:`ConfigLocation` for the config file (thread,
            weave, or loom). Local paths are wrapped automatically; remote
            URIs must be supplied as a :class:`RemoteConfigLocation` so the
            caller can attach a SparkSession.
        runtime_params: Optional runtime parameter overrides.
        project_root: The ``.weevr`` project directory. Required for configs
            that reference other files. Accepts a :class:`ConfigLocation` or
            a bare :class:`pathlib.Path`.

    Returns:
        A frozen, typed domain model instance (Thread, Weave, or Loom) for
        thread/weave/loom config types. Returns a plain dict for params configs.

    Raises:
        ConfigParseError: YAML syntax errors, file not found
        ConfigVersionError: Unsupported config version
        ConfigSchemaError: Schema validation failures
        ConfigError: Extension or name validation failures
        VariableResolutionError: Unresolved variables without defaults
        ReferenceResolutionError: Missing referenced files, circular dependencies
        ModelValidationError: Semantic validation failures during model hydration
    """
    file_path = make_location(path)

    # Step 1: Parse YAML
    raw = parse_yaml(file_path)

    # Step 2: Extract and validate version
    version = extract_config_version(raw)

    # Step 3: Detect config type — extension-based for typed files, content-based for .yaml
    ext_type = detect_config_type_from_extension(file_path)
    config_type = ext_type if ext_type is not None else detect_config_type(raw)

    # Step 4: Validate schema
    validate_config_version(version, config_type)
    validated = validate_schema(raw, config_type)
    config_dict = validated.model_dump(exclude_unset=True)

    # Step 5: Build parameter context (no more param_file)
    declared_param_specs = config_dict.get("params")
    resolved_declared_params = resolve_declared_params(
        declared_param_specs,
        runtime_params,
        file_path=str(file_path),
    )
    # Type validation only — required-missing already raised above.
    validate_params(declared_param_specs, resolved_declared_params)
    context = build_param_context(
        runtime_params,
        config_dict.get("defaults"),
        entry_params=resolved_declared_params or None,
    )

    # Step 6: Resolve variables
    resolved = resolve_variables(config_dict, context)

    # Step 7: Resolve references to child configs
    if project_root is None:
        effective_root: ConfigLocation = file_path.parent
    elif isinstance(project_root, ConfigLocation):
        effective_root = project_root
    else:
        effective_root = LocalConfigLocation(Path(project_root))
    resolved_with_refs = resolve_references(
        resolved,
        config_type,
        effective_root,
        runtime_params,
    )

    # Step 8: Apply inheritance for child configs
    if config_type == "loom" and "_resolved_weaves" in resolved_with_refs:
        loom_defaults = resolved_with_refs.get("defaults")
        loom_audit_templates = resolved_with_refs.get("audit_templates")
        loom_connections = resolved_with_refs.get("connections")
        for weave in resolved_with_refs["_resolved_weaves"]:
            if "_resolved_threads" in weave:
                weave_defaults = weave.get("defaults")
                weave_audit_templates = weave.get("audit_templates")
                weave_connections = weave.get("connections")
                for i, thread in enumerate(weave["_resolved_threads"]):
                    merged = apply_inheritance(
                        loom_defaults,
                        weave_defaults,
                        thread,
                        loom_audit_templates=loom_audit_templates,
                        weave_audit_templates=weave_audit_templates,
                        loom_connections=loom_connections,
                        weave_connections=weave_connections,
                    )
                    weave["_resolved_threads"][i] = merged

    elif config_type == "weave" and "_resolved_threads" in resolved_with_refs:
        weave_defaults = resolved_with_refs.get("defaults")
        weave_audit_templates = resolved_with_refs.get("audit_templates")
        weave_connections = resolved_with_refs.get("connections")
        for i, thread in enumerate(resolved_with_refs["_resolved_threads"]):
            merged = apply_inheritance(
                None,
                weave_defaults,
                thread,
                weave_audit_templates=weave_audit_templates,
                weave_connections=weave_connections,
            )
            resolved_with_refs["_resolved_threads"][i] = merged

    # Step 8b: Expand foreach macros in thread steps
    if config_type == "thread" and isinstance(resolved_with_refs.get("steps"), list):
        resolved_with_refs["steps"] = expand_foreach(resolved_with_refs["steps"])

    # Step 9: Name validation and injection
    if config_type in ("thread", "weave", "loom") and ext_type is not None:
        stem = _derive_config_name(file_path)
        declared_name = resolved_with_refs.get("name", "")
        if declared_name and declared_name != stem:
            raise ConfigError(
                f"Declared name '{declared_name}' does not match filename stem '{stem}'",
                file_path=str(file_path),
            )
        if not declared_name:
            resolved_with_refs["name"] = stem

        # Compute qualified key for standalone files
        if project_root is not None and ext_type is not None:
            if file_path.is_relative_to(effective_root):
                # Strip the project_root prefix to produce a relative key.
                # For local locations this matches the previous Path.resolve()
                # / Path.relative_to behavior; for remote locations it is a
                # normalized URI prefix strip.
                if isinstance(file_path, LocalConfigLocation) and isinstance(
                    effective_root, LocalConfigLocation
                ):
                    rel = file_path.path.resolve().relative_to(effective_root.path.resolve())
                    resolved_with_refs["qualified_key"] = str(rel)
                else:
                    file_str = str(file_path)
                    root_str = str(effective_root).rstrip("/")
                    resolved_with_refs["qualified_key"] = (
                        file_str[len(root_str) + 1 :]
                        if file_str.startswith(root_str + "/")
                        else file_str
                    )
            else:
                resolved_with_refs["qualified_key"] = str(file_path)
    elif not resolved_with_refs.get("name"):
        resolved_with_refs["name"] = file_path.stem

    # Step 10: Hydrate into typed domain model
    model_map: dict[str, type[Thread | Weave | Loom]] = {
        "thread": Thread,
        "weave": Weave,
        "loom": Loom,
    }
    if config_type in model_map:
        try:
            return model_map[config_type].model_validate(resolved_with_refs)
        except ValidationError as exc:
            raise ModelValidationError(
                f"Model hydration failed for {config_type}: {exc}",
                cause=exc,
                file_path=str(path),
            ) from exc

    # params config type returns a plain dict
    return resolved_with_refs
