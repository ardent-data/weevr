"""Shared Delta Lake utilities used across readers, writers, and state stores."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def is_table_alias(path: str) -> bool:
    """Return True if *path* looks like a table alias (e.g. ``schema.table``).

    Table aliases are dot-separated identifiers resolved by the Spark metastore.
    File paths contain slashes or URI schemes (``://``).
    """
    return "://" not in path and "/" not in path


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Read a Delta table from a file path or table alias.

    Args:
        spark: Active SparkSession.
        path: Table alias (e.g. ``staging.customers``) or file path.

    Returns:
        DataFrame with the table contents.
    """
    if is_table_alias(path):
        return spark.read.format("delta").table(path)
    return spark.read.format("delta").load(path)


def probe_delta_table_exists(spark: SparkSession, path: str) -> bool:
    """Raw existence probe — raises when the probe itself fails.

    Callers that must distinguish "confirmed absent" from "could not
    ask" (the write path, where the two answers route to different
    branches) use this directly; everyone else goes through
    :func:`delta_table_exists`, which folds failures into False.

    The alias branch asks the catalog rather than resolving the table
    through a reader, so no Delta log is read and no job is launched.
    ``tableExists`` answers "any table exists" — not "a Delta-readable
    table exists" — which is equivalent on Fabric, where lakehouse
    tables are Delta by platform contract. If parity evidence ever
    demands distinguishing non-Delta catalog entries, the fallback is a
    provider check (catalog provider field or ``DESCRIBE DETAIL``).

    Args:
        spark: Active SparkSession.
        path: Table alias or file path.
    """
    if is_table_alias(path):
        return spark.catalog.tableExists(path)
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, path)


def delta_table_exists(spark: SparkSession, path: str) -> bool:
    """Return True if a Delta table exists at the given path or alias.

    A probe failure (metastore or storage error — not a clean "absent"
    answer) is logged with its cause and reported as False. Callers for
    whom that conflation is unsafe use :func:`probe_delta_table_exists`.

    Args:
        spark: Active SparkSession.
        path: Table alias or file path.
    """
    try:
        return probe_delta_table_exists(spark, path)
    except Exception as exc:
        logger.warning("Existence probe failed for '%s'; treating as absent: %s", path, exc)
        return False


def resolve_delta_table(spark: SparkSession, path: str) -> Any:
    """Resolve a DeltaTable from a table alias or file path.

    Args:
        spark: Active SparkSession.
        path: Table alias or file path.

    Returns:
        A ``DeltaTable`` instance.
    """
    from delta.tables import DeltaTable

    if is_table_alias(path):
        return DeltaTable.forName(spark, path)
    return DeltaTable.forPath(spark, path)
