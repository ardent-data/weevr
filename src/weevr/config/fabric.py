"""Fabric runtime context provider."""

from pyspark.sql import SparkSession


def build_fabric_context(spark: SparkSession) -> dict[str, str | None]:
    """Extract ${fabric.*} variables from spark.conf.

    Reads trident.* properties from spark.conf to provide
    Fabric runtime context for variable resolution.

    Returns dict with keys: fabric.workspace_id, fabric.lakehouse_id,
    fabric.workspace_name. Values are None if the property is not set.
    """
    return {
        "fabric.workspace_id": _safe_get(spark, "trident.workspace.id"),
        "fabric.lakehouse_id": _safe_get(spark, "trident.lakehouse.id"),
        "fabric.workspace_name": _safe_get(spark, "trident.workspace.name"),
    }


def _safe_get(spark: SparkSession, key: str) -> str | None:
    """Read a spark.conf property, returning None if not set."""
    try:
        value = spark.conf.get(key)
        return value if value else None
    except Exception:
        return None
