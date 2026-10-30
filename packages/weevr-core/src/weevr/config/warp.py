"""Warp discovery, effective warp computation, and drift baseline resolution."""

from __future__ import annotations

import logging
from pathlib import Path

from weevr.config.locations import ConfigLocation, LocalConfigLocation
from weevr.config.parser import parse_yaml
from weevr.config.validation import validate_schema
from weevr.errors import ConfigError
from weevr.model.target import Target
from weevr.model.warp import EffectiveWarp, WarpColumn, WarpConfig

logger = logging.getLogger(__name__)


def resolve_warp(
    warp_ref: str | bool | None,
    thread_dir: ConfigLocation | Path,
    config_root: ConfigLocation | Path,
    target_alias: str | None = None,
) -> WarpConfig | None:
    """Resolve a warp config from a reference, auto-discovery, or opt-out.

    Args:
        warp_ref: Explicit warp name/path (str), opt-out (False), or
            auto-discover (None).
        thread_dir: Directory containing the thread config file.
        config_root: Root directory for the config project.
        target_alias: Target alias used for auto-discovery. The last
            dot-separated segment is used as the table name.

    Returns:
        Resolved WarpConfig, or None if opted out or not found.

    Raises:
        ConfigError: If an explicit reference cannot be resolved.
    """
    found = find_warp(warp_ref, thread_dir, config_root, target_alias)
    return found[0] if found is not None else None


def find_warp(
    warp_ref: str | bool | None,
    thread_dir: ConfigLocation | Path,
    config_root: ConfigLocation | Path,
    target_alias: str | None = None,
) -> tuple[WarpConfig, ConfigLocation] | None:
    """Resolve a warp and return it with the location of its file.

    Same rules as :func:`resolve_warp`.

    Args:
        warp_ref: Explicit warp name/path (str), opt-out (False), or
            auto-discover (None).
        thread_dir: Directory containing the thread config file.
        config_root: Root directory for the config project.
        target_alias: Target alias used for auto-discovery.

    Returns:
        ``(warp, file location)``, or None if opted out or not found.

    Raises:
        ConfigError: If an explicit reference cannot be resolved.
    """
    thread_loc = _as_location(thread_dir)
    root_loc = _as_location(config_root)

    if warp_ref is False:
        return None

    if isinstance(warp_ref, str):
        return _resolve_named(warp_ref, thread_loc, root_loc)

    # Auto-discovery: try to find a .warp file matching the target
    if target_alias is not None:
        table_name = _derive_table_name(target_alias)
        return _auto_discover(table_name, thread_loc, root_loc)

    return None


def discover_warp(
    target: Target,
    thread_dir: ConfigLocation | Path,
    config_root: ConfigLocation | Path,
) -> tuple[WarpConfig, ConfigLocation] | None:
    """Find a warp named after a target, for a thread with no ``target.warp``.

    Looks for ``<target name>.warp`` in the thread's directory, then the
    project root (see :func:`target_warp_name`).

    Args:
        target: The thread's target.
        thread_dir: Directory containing the thread config file.
        config_root: Root directory for the config project.

    Returns:
        ``(warp, file location)``, or None when the target has no usable name
        or no matching file exists.
    """
    name = target_warp_name(target)
    if name is None:
        return None
    return _auto_discover(name, _as_location(thread_dir), _as_location(config_root))


def target_warp_name(target: Target) -> str | None:
    """Return the warp file stem a target maps to.

    The alias's last dot segment, else the path's last segment, else the
    connection target's ``table``.

    Args:
        target: The thread's target.

    Returns:
        The name, or None when the target has none of these.
    """
    if target.alias:
        return _derive_table_name(target.alias)
    if target.path:
        segment = target.path.rstrip("/").rsplit("/", maxsplit=1)[-1]
        return segment or None
    return target.table


def _as_location(loc: ConfigLocation | Path) -> ConfigLocation:
    """Wrap a local path; pass a ConfigLocation through."""
    return loc if isinstance(loc, ConfigLocation) else LocalConfigLocation(Path(loc))


def _resolve_named(
    name: str, thread_dir: ConfigLocation, config_root: ConfigLocation
) -> tuple[WarpConfig, ConfigLocation]:
    """Resolve an explicitly named warp reference.

    Search order:
    1. thread_dir / {name}.warp (or {name} if already has extension)
    2. config_root / {name}.warp (or {name})
    3. config_root / {name} (for paths with directory separators)

    Args:
        name: Warp file name or relative path.
        thread_dir: Thread's directory.
        config_root: Config project root.

    Returns:
        Parsed WarpConfig and its file location.

    Raises:
        ConfigError: If the named warp cannot be found.
    """
    has_extension = name.endswith(".warp")
    filename = name if has_extension else f"{name}.warp"
    has_separator = "/" in name or "\\" in name

    candidates: list[ConfigLocation] = []

    if not has_separator:
        # Try thread directory first
        candidates.append(thread_dir.join(filename))
    # Then config root
    candidates.append(config_root.join(filename))

    for candidate in candidates:
        if candidate.exists():
            return _load_warp(candidate), candidate

    raise ConfigError(
        f"Warp file not found: '{name}'. Searched: " + ", ".join(str(c) for c in candidates),
    )


def _auto_discover(
    table_name: str, thread_dir: ConfigLocation, config_root: ConfigLocation
) -> tuple[WarpConfig, ConfigLocation] | None:
    """Auto-discover a .warp file matching the table name.

    Search order:
    1. thread_dir / {table_name}.warp
    2. config_root / {table_name}.warp

    Args:
        table_name: Derived table name to match.
        thread_dir: Thread's directory.
        config_root: Config project root.

    Returns:
        Parsed WarpConfig and its file location if found, None otherwise.
    """
    candidates = [
        thread_dir.join(f"{table_name}.warp"),
        config_root.join(f"{table_name}.warp"),
    ]
    for candidate in candidates:
        if candidate.exists():
            logger.debug("Auto-discovered warp: %s", candidate)
            return _load_warp(candidate), candidate
    return None


def _derive_table_name(target_alias: str) -> str:
    """Derive table name from a target alias.

    For dotted aliases like 'gold.dim_customer', extracts the last
    segment ('dim_customer'). For simple aliases, returns as-is.

    Args:
        target_alias: Target alias string.

    Returns:
        Derived table name.
    """
    if "." in target_alias:
        return target_alias.rsplit(".", maxsplit=1)[1]
    return target_alias


def _load_warp(location: ConfigLocation) -> WarpConfig:
    """Load and validate a warp config from a file location.

    Args:
        location: Location of the .warp file.

    Returns:
        Validated WarpConfig instance.
    """
    raw = parse_yaml(location)
    result = validate_schema(raw, "warp")
    if not isinstance(result, WarpConfig):
        raise ConfigError(f"Expected WarpConfig from '{location}', got {type(result).__name__}")
    return result


def build_effective_warp(
    warp_columns: list[WarpColumn],
    pipeline_columns: list[str],
    engine_columns: list[str],
) -> EffectiveWarp:
    """Build the effective warp from declared, pipeline, and engine columns.

    Computes which warp columns are "warp-only" (declared in the warp
    but absent from the pipeline output) and which engine columns are
    truly new (not already declared in the warp).

    Args:
        warp_columns: Columns declared in the warp.
        pipeline_columns: Column names present in the pipeline output.
        engine_columns: Engine-managed column names (audit, SCD, etc.).

    Returns:
        EffectiveWarp with declared, warp_only, and engine lists.
    """
    pipeline_set = set(pipeline_columns)
    warp_names = {col.name for col in warp_columns}

    warp_only = [col for col in warp_columns if col.name not in pipeline_set]

    # Engine columns that aren't already in the warp
    effective_engine = [c for c in engine_columns if c not in warp_names]

    return EffectiveWarp(
        declared=list(warp_columns),
        warp_only=warp_only,
        engine=effective_engine,
    )


def get_drift_baseline(
    warp: WarpConfig | None = None,
    spark: object | None = None,
    target_path: str | None = None,
) -> list[str] | None:
    """Get the drift baseline column list.

    When a warp exists, uses warp column names as the baseline.
    Without a warp, attempts to read the existing Delta table schema.
    Returns None if no baseline can be established.

    Args:
        warp: Resolved warp config, if any.
        spark: Spark session for table schema reads (optional).
        target_path: Target table path for schema reads (optional).

    Returns:
        List of baseline column names, or None.
    """
    if warp is not None:
        return [col.name for col in warp.columns]

    if spark is not None and target_path is not None:
        try:
            # Read existing table schema as fallback baseline
            df = spark.read.format("delta").load(target_path)  # type: ignore[union-attr]
            return [field.name for field in df.schema.fields]  # type: ignore[union-attr]
        except Exception:
            logger.warning(
                "Could not read table schema for drift baseline: %s",
                target_path,
                exc_info=True,
            )
            return None

    return None
