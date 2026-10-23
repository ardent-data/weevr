"""The shared config load: parse, resolve, cascade, and hydrate one file.

Every entry point — ``load_config``, ``load_resolved``, and the engine's
``Context`` — runs these functions, so a config loads the same way whichever
one is used. Engine-bound input (the Fabric context) is passed in, never
imported.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from weevr.config.attribution import attribute_errors
from weevr.config.hydration import hydrate_children, hydrate_model
from weevr.config.inheritance import apply_inheritance
from weevr.config.locations import (
    ConfigLocation,
    LocalConfigLocation,
    RemoteConfigLocation,
    make_location,
)
from weevr.config.macros import expand_foreach
from weevr.config.parser import (
    detect_config_type,
    detect_config_type_from_extension,
    extract_config_version,
    parse_yaml,
    validate_config_version,
)
from weevr.config.resolver import (
    resolve_config_variables,
    resolve_declared_params,
    resolve_references,
    validate_params,
)
from weevr.config.validation import (
    collect_execution_scope_warnings,
    collect_trace_composition_warnings,
    validate_schema,
)
from weevr.errors import ConfigError, VariableResolutionError
from weevr.model import Loom, Thread, Weave

logger = logging.getLogger("weevr.config")

COMPONENT_TYPES = ("thread", "weave", "loom")


@dataclass
class ResolvedConfig:
    """A config file resolved to a dict, before hydration.

    Attributes:
        location: The file the config came from.
        project_root: The root child references and Source paths resolved against.
        config_type: Config kind (``"thread"``, ``"weave"``, ``"loom"``, ``"params"``).
        config_name: Name of the config (declared, else the filename stem).
        data: The resolved config dict, with child configs attached under the
            resolver's ``_resolved_*`` keys.
        warnings: Load warnings collected so far.
    """

    location: ConfigLocation
    project_root: ConfigLocation
    config_type: str
    config_name: str
    data: dict[str, Any]
    warnings: list[str] = field(default_factory=list)


@dataclass
class HydratedConfig:
    """A hydrated config with the children it pulls in.

    The loader's own result. It deliberately is not
    :class:`~weevr.result.LoadedConfig`: importing ``weevr.result`` reaches the
    engine wheel's ``weevr.engine`` package, and ``load_config`` must stay
    importable and runnable without PySpark in a full install.

    Attributes:
        model: The hydrated Thread, Weave, or Loom.
        config_type: Config kind (``"thread"``, ``"weave"``, ``"loom"``).
        config_name: Name of the config.
        weaves: Hydrated weaves by name (looms only).
        threads: Each weave's hydrated threads by weave name, then effective
            thread name (weaves and looms).
        warnings: Load warnings, in emission order.
    """

    model: Thread | Weave | Loom
    config_type: str
    config_name: str
    weaves: dict[str, Weave] = field(default_factory=dict)
    threads: dict[str, dict[str, Thread]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def resolve_config(
    location: ConfigLocation,
    *,
    runtime_params: dict[str, Any] | None = None,
    project_root: ConfigLocation | None = None,
    fabric_context: dict[str, Any] | None = None,
    require_typed_extension: bool = False,
) -> ResolvedConfig:
    """Resolve one config file to a dict ready for hydration.

    Parses and validates the file, resolves params, variables, and child
    references, applies the inheritance cascade, expands ``foreach`` macros,
    checks the name, sets ``qualified_key``, and resolves relative file-Source
    paths. Execution-scope warnings are collected before the cascade.

    Args:
        location: The config file.
        runtime_params: Runtime parameter overrides.
        project_root: Root for child references and Source paths. Defaults to
            the file's directory.
        fabric_context: Optional Fabric environment values for ``${fabric.x}``.
        require_typed_extension: Reject files without a ``.thread``, ``.weave``,
            or ``.loom`` extension instead of detecting the type from content.

    Returns:
        The resolved config.

    Raises:
        ConfigError: Unsupported extension or a name/filename mismatch.
        VariableResolutionError: A variable has no value and no default.
    """
    raw = parse_yaml(location)
    version = extract_config_version(raw)

    ext_type = detect_config_type_from_extension(location)
    if ext_type is None and require_typed_extension:
        raise ConfigError(
            f"Unsupported extension '{location.suffix}'. Expected .thread, .weave, or .loom",
            file_path=str(location),
        )
    config_type = ext_type if ext_type is not None else detect_config_type(raw)

    validate_config_version(version, config_type)
    validated = validate_schema(raw, config_type)
    config_dict = validated.model_dump(exclude_unset=True)

    declared_param_specs = config_dict.get("params")
    resolved_declared_params = resolve_declared_params(
        declared_param_specs,
        runtime_params,
        file_path=str(location),
    )
    # Type validation only — required-missing already raised above.
    validate_params(declared_param_specs, resolved_declared_params)

    try:
        resolved = resolve_config_variables(
            config_dict,
            config_type,
            runtime_params,
            fabric_context=fabric_context,
            entry_params=resolved_declared_params or None,
        )
    except VariableResolutionError as exc:
        if config_type == "thread" and "param." in str(exc):
            raise VariableResolutionError(
                f"{exc}. This thread may be a template designed for "
                f"use with 'as' and 'params' on a ThreadEntry in a weave.",
                config_key=getattr(exc, "config_key", None),
            ) from exc
        raise

    root = project_root if project_root is not None else location.parent
    data = resolve_references(resolved, config_type, root, runtime_params)

    # Execution-scope warnings come from the PRE-cascade dicts: after the
    # cascade an authored execution block is indistinguishable from one
    # injected by defaults.execution.
    warnings = (
        collect_execution_warnings(config_type, data) if config_type in COMPONENT_TYPES else []
    )
    _apply_cascade(config_type, data)

    if config_type == "thread" and isinstance(data.get("steps"), list):
        data["steps"] = expand_foreach(data["steps"])

    if ext_type is not None:
        declared_name = data.get("name", "")
        if declared_name and declared_name != location.stem:
            raise ConfigError(
                f"Declared name '{declared_name}' does not match filename stem '{location.stem}'",
                file_path=str(location),
            )
        if not declared_name:
            data["name"] = location.stem
        data["qualified_key"] = compute_qualified_key(location, root)
    elif not data.get("name"):
        data["name"] = location.stem

    if config_type == "thread":
        resolve_source_paths(data, root)

    return ResolvedConfig(
        location=location,
        project_root=root,
        config_type=config_type,
        config_name=data["name"],
        data=data,
        warnings=warnings,
    )


def hydrate_resolved(resolved: ResolvedConfig) -> HydratedConfig:
    """Hydrate a resolved thread, weave, or loom and the children it pulls in.

    Hydration consumes ``resolved``: child dicts in ``resolved.data`` are
    prepared in place, so hydrate each resolved config once.

    Args:
        resolved: The output of :func:`resolve_config` for a component config.

    Returns:
        The hydrated model with its weaves, threads, and load warnings.

    Raises:
        ModelValidationError: If the config or any child fails model validation.
    """
    model = hydrate_model(resolved.data, resolved.config_type, resolved.location)
    root = resolved.project_root
    weaves, threads = hydrate_children(
        resolved.config_type,
        resolved.data,
        resolved.location,
        resolved.config_name,
        prepare=lambda thread_dict: resolve_source_paths(thread_dict, root),
    )

    warnings = list(resolved.warnings)
    # Trace composition needs hydrated models (model_fields_set): a weave's
    # explicit trace: true under a traceless loom is a no-op.
    if isinstance(model, Loom):
        warnings.extend(
            collect_trace_composition_warnings(
                model.execution,
                {name: w.execution for name, w in weaves.items()},
            )
        )

    return HydratedConfig(
        model=model,
        config_type=resolved.config_type,
        config_name=resolved.config_name,
        weaves=weaves,
        threads=threads,
        warnings=warnings,
    )


def load_resolved_config(
    location: ConfigLocation,
    *,
    runtime_params: dict[str, Any] | None = None,
    project_root: ConfigLocation | None = None,
    fabric_context: dict[str, Any] | None = None,
) -> HydratedConfig:
    """Load a typed thread, weave, or loom file with its resolved children.

    The engine-facing entry point: ``Context`` calls it with its project root
    and Fabric context. Errors name the file they were found in. Warnings are
    returned, not logged.

    Args:
        location: The config file (``.thread``, ``.weave``, or ``.loom``).
        runtime_params: Runtime parameter overrides.
        project_root: Root for child references and Source paths. Defaults to
            the file's directory.
        fabric_context: Optional Fabric environment values for ``${fabric.x}``.

    Returns:
        The hydrated config with its weaves, threads, and load warnings.
    """
    with attribute_errors(location):
        return hydrate_resolved(
            resolve_config(
                location,
                runtime_params=runtime_params,
                project_root=project_root,
                fabric_context=fabric_context,
                require_typed_extension=True,
            )
        )


def load_resolved(
    path: str | Path | ConfigLocation,
    runtime_params: dict[str, Any] | None = None,
    project_root: Path | ConfigLocation | None = None,
) -> Any:  # weevr.result.LoadedConfig; importing it here would close an import cycle
    """Load a thread, weave, or loom with its resolved child threads and weaves.

    Runs the same load as :func:`load_config` and ``Context.load`` — params,
    variables, references, the inheritance cascade, ``foreach`` expansion,
    and model validation of every child — without Spark. Use it to inspect or
    plan a config: the result exposes the hydrated model, each weave's
    threads, the load warnings, and the execution plan.

    Only ``.thread``, ``.weave``, and ``.loom`` files are accepted.
    ``${fabric.x}`` references need a default here, since no Fabric context
    is available.

    Args:
        path: The config file. Local paths are wrapped automatically; remote
            URIs must be supplied as a :class:`RemoteConfigLocation`.
        runtime_params: Optional runtime parameter overrides.
        project_root: The ``.weevr`` project directory. Child references,
            relative file-Source paths, and ``qualified_key`` resolve against
            it; defaults to the file's directory.

    Returns:
        A :class:`~weevr.result.LoadedConfig` with ``model``,
        ``resolved_threads``, ``resolved_weaves``, ``warnings``, and
        ``execution_plan``. Warnings are also
        logged on the ``weevr.config`` logger.

    Raises:
        ConfigError: Unsupported extension or a name/filename mismatch, plus
            every error :func:`load_config` raises.
    """
    # Imported here, not at module level: weevr.result reaches the engine
    # wheel's weevr.engine package, which needs weevr.config fully imported.
    from weevr.result import LoadedConfig

    if project_root is None or isinstance(project_root, ConfigLocation):
        root = project_root
    else:
        root = LocalConfigLocation(Path(project_root))
    hydrated = load_resolved_config(
        make_location(path), runtime_params=runtime_params, project_root=root
    )
    for warning in hydrated.warnings:
        logger.warning(warning)
    return LoadedConfig.from_hydrated(hydrated)


def _apply_cascade(config_type: str, data: dict[str, Any]) -> None:
    """Cascade loom and weave defaults into each child thread, in place."""
    if config_type == "loom" and "_resolved_weaves" in data:
        loom_defaults = data.get("defaults")
        loom_audit_templates = data.get("audit_templates")
        loom_connections = data.get("connections")
        for weave in data["_resolved_weaves"]:
            if "_resolved_threads" in weave:
                weave_defaults = weave.get("defaults")
                weave_audit_templates = weave.get("audit_templates")
                weave_connections = weave.get("connections")
                for i, thread in enumerate(weave["_resolved_threads"]):
                    weave["_resolved_threads"][i] = apply_inheritance(
                        loom_defaults,
                        weave_defaults,
                        thread,
                        loom_audit_templates=loom_audit_templates,
                        weave_audit_templates=weave_audit_templates,
                        loom_connections=loom_connections,
                        weave_connections=weave_connections,
                    )

    elif config_type == "weave" and "_resolved_threads" in data:
        weave_defaults = data.get("defaults")
        weave_audit_templates = data.get("audit_templates")
        weave_connections = data.get("connections")
        for i, thread in enumerate(data["_resolved_threads"]):
            data["_resolved_threads"][i] = apply_inheritance(
                None,
                weave_defaults,
                thread,
                weave_audit_templates=weave_audit_templates,
                weave_connections=weave_connections,
            )


def collect_execution_warnings(config_type: str, resolved: dict[str, Any]) -> list[str]:
    """Gather thread-scope execution warnings from pre-cascade dicts."""
    defaults_by_scope: list[tuple[str, dict[str, Any] | None]] = []
    pre_cascade_threads: list[dict[str, Any]] = []

    if config_type == "loom":
        loom_label = f"loom '{resolved.get('name') or '(unnamed)'}'"
        defaults_by_scope.append((loom_label, resolved.get("defaults")))
        for i, weave in enumerate(resolved.get("_resolved_weaves", [])):
            weave_label = f"weave '{weave.get('name') or f'#{i}'}'"
            defaults_by_scope.append((weave_label, weave.get("defaults")))
            pre_cascade_threads.extend(weave.get("_resolved_threads", []))
    elif config_type == "weave":
        weave_label = f"weave '{resolved.get('name') or '(unnamed)'}'"
        defaults_by_scope.append((weave_label, resolved.get("defaults")))
        pre_cascade_threads.extend(resolved.get("_resolved_threads", []))
    else:  # standalone thread
        pre_cascade_threads.append(resolved)

    return collect_execution_scope_warnings(defaults_by_scope, pre_cascade_threads)


def compute_qualified_key(file_path: ConfigLocation, project_root: ConfigLocation) -> str:
    """Return ``file_path`` expressed relative to ``project_root``.

    Falls back to the absolute string when the file lies outside the
    project root.
    """
    if isinstance(file_path, LocalConfigLocation) and isinstance(project_root, LocalConfigLocation):
        try:
            rel = file_path.path.resolve().relative_to(project_root.path.resolve())
            return str(rel)
        except (OSError, ValueError):
            return str(file_path)
    if isinstance(file_path, RemoteConfigLocation) and isinstance(
        project_root, RemoteConfigLocation
    ):
        file_str = str(file_path)
        root_str = str(project_root).rstrip("/")
        if file_str.startswith(root_str + "/"):
            return file_str[len(root_str) + 1 :]
        return file_str
    return str(file_path)


def resolve_source_paths(
    thread_dict: dict[str, Any],
    project_root: ConfigLocation | Path | str,
) -> None:
    """Resolve relative source file paths against the project root.

    Modifies the thread config dict in place, prepending the project
    root to any relative ``path`` values in file-based sources (csv,
    json, parquet, excel).

    Args:
        thread_dict: Thread configuration dict (pre-hydration).
        project_root: Resolved project root. Accepts a
            :class:`ConfigLocation`, :class:`pathlib.Path`, or string
            for backward compatibility with internal callers.
    """
    # Normalize legacy Path/str inputs into ConfigLocation form so the
    # join logic only has one branch. Strings that look like URIs are
    # left as plain strings — only the local Path branch needs the FUSE
    # post-processing below.
    if isinstance(project_root, ConfigLocation):
        location: ConfigLocation | None = project_root
        legacy_str_root: str | None = None
    elif isinstance(project_root, Path):
        location = LocalConfigLocation(project_root)
        legacy_str_root = None
    else:
        # Plain string — preserve the original abfss-style concat path
        # for callers that still hand in a raw URI string.
        location = None
        legacy_str_root = project_root

    sources = thread_dict.get("sources")
    if not sources or not isinstance(sources, dict):
        return
    for source_cfg in sources.values():
        if not isinstance(source_cfg, dict):
            continue
        path = source_cfg.get("path")
        if path is None:
            continue
        # Skip already-absolute paths and URIs
        if os.path.isabs(path) or "://" in path:
            continue

        if location is not None:
            joined = location.join(path)
            resolved = str(joined)
            # Fabric mount paths (/lakehouse/default/...) are valid for
            # Python file I/O but not for Spark reads which bypass the
            # FUSE mount. Strip the mount prefix so Spark gets a
            # lakehouse-relative path (e.g. "Files/project.weevr/...").
            if isinstance(location, LocalConfigLocation) and resolved.startswith(
                "/lakehouse/default/"
            ):
                resolved = resolved[len("/lakehouse/default/") :]
            source_cfg["path"] = resolved
        else:
            assert legacy_str_root is not None
            source_cfg["path"] = f"{legacy_str_root.rstrip('/')}/{path}"
