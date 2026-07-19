"""OneLake path construction utilities."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from weevr.config.onelake import build_onelake_path, resolve_connection_path
from weevr.model.connection import OneLakeConnection

__all__ = [
    "OneLakeConnection",
    "build_onelake_path",
    "resolve_connection_path",
    "resolve_fuse_path",
]

_ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"
_FUSE_PREFIX = "/lakehouse/"


def resolve_fuse_path(path: str, spark: SparkSession) -> str:
    """Translate a FUSE mount path to its abfss:// equivalent.

    Paths starting with ``/lakehouse/`` are assumed to be FUSE-mounted
    OneLake paths and are rewritten using workspace and lakehouse identifiers
    read from the active Spark session configuration.

    If the path does not start with ``/lakehouse/``, it is returned unchanged.
    If the required ``trident.*`` Spark configuration keys are absent, the
    original path is returned as-is so callers can handle the fallback.

    Args:
        path: The path to resolve.
        spark: Active ``SparkSession`` used to read ``trident.*`` config keys.

    Returns:
        An ``abfss://`` URI when the path is a FUSE mount and the required
        Spark configuration is present; otherwise the original path.
    """
    if not path.startswith(_FUSE_PREFIX):
        return path

    workspace_id: str | None = spark.conf.get("trident.workspace.id", None)
    lakehouse_id: str | None = spark.conf.get("trident.lakehouse.id", None)

    if not workspace_id or not lakehouse_id:
        return path

    # Strip the /lakehouse/default/ prefix, keeping everything after it.
    # The FUSE layout is /lakehouse/default/Tables/... so we drop the mount
    # root and the "default" segment to obtain a Tables-relative suffix.
    remainder = path[len(_FUSE_PREFIX) :]
    # Drop the first path component (typically "default") which is the
    # FUSE mount name, not part of the OneLake namespace.
    parts = remainder.split("/", 1)
    suffix = parts[1] if len(parts) > 1 else ""

    return f"abfss://{workspace_id}@{_ONELAKE_HOST}/{lakehouse_id}/{suffix}"
