"""Warp discovery, effective warp computation, and drift baseline resolution."""

from __future__ import annotations

import logging
from pathlib import Path

from weevr.config.parser import parse_yaml
from weevr.config.validation import validate_schema
from weevr.errors import ConfigError
from weevr.model.warp import EffectiveWarp, WarpColumn, WarpConfig

logger = logging.getLogger(__name__)


def resolve_warp(
    warp_ref: str | bool | None,
    thread_dir: Path,
    config_root: Path,
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
    if warp_ref is False:
        return None

    if isinstance(warp_ref, str):
        return _resolve_named(warp_ref, thread_dir, config_root)

    # Auto-discovery: try to find a .warp file matching the target
    if target_alias is not None:
        table_name = _derive_table_name(target_alias)
        return _auto_discover(table_name, thread_dir, config_root)

    return None


def _resolve_named(name: str, thread_dir: Path, config_root: Path) -> WarpConfig:
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
        Parsed WarpConfig.

    Raises:
        ConfigError: If the named warp cannot be found.
    """
    has_extension = name.endswith(".warp")
    filename = name if has_extension else f"{name}.warp"
    has_separator = "/" in name or "\\" in name

    candidates: list[Path] = []

    if not has_separator:
        # Try thread directory first
        candidates.append(thread_dir / filename)
    # Then config root
    candidates.append(config_root / filename)

    for candidate in candidates:
        if candidate.exists():
            return _load_warp(candidate)

    raise ConfigError(
        f"Warp file not found: '{name}'. Searched: " + ", ".join(str(c) for c in candidates),
    )


def _auto_discover(table_name: str, thread_dir: Path, config_root: Path) -> WarpConfig | None:
    """Auto-discover a .warp file matching the table name.

    Search order:
    1. thread_dir / {table_name}.warp
    2. config_root / {table_name}.warp

    Args:
        table_name: Derived table name to match.
        thread_dir: Thread's directory.
        config_root: Config project root.

    Returns:
        Parsed WarpConfig if found, None otherwise.
    """
    candidates = [
        thread_dir / f"{table_name}.warp",
        config_root / f"{table_name}.warp",
    ]
    for candidate in candidates:
        if candidate.exists():
            logger.debug("Auto-discovered warp: %s", candidate)
            return _load_warp(candidate)
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


def _load_warp(path: Path) -> WarpConfig:
    """Load and validate a warp config from a file path.

    Args:
        path: Absolute path to the .warp file.

    Returns:
        Validated WarpConfig instance.
    """
    from weevr.config.locations import LocalConfigLocation

    location = LocalConfigLocation(path)
    raw = parse_yaml(location)
    result = validate_schema(raw, "warp")
    if not isinstance(result, WarpConfig):
        raise ConfigError(f"Expected WarpConfig from '{path}', got {type(result).__name__}")
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
