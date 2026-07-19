"""Canonical target identity — one key per physical table.

Cache registration and consumption must agree on what "the same target"
means across the three declaration forms (alias, path, connection+table).
This resolver normalizes each form to one canonical string:

- ``connection`` + ``table`` → the abfss:// path the connection resolves
  to (the same resolution the readers and the executor perform inline
  today).
- metastore alias (``schema.table``) → the table's storage location from
  the catalog — a driver-side metadata query, no Spark jobs. Falls back
  to a namespaced lowercased-alias key (``alias:…``) when the location
  cannot be read; alias-form producers and consumers still meet on that
  fallback key, and it can never collide with a location-derived key.
- filesystem path → FUSE-translated, trailing-slash-normalized.
  Normalization is deliberately shallow — equivalent hand-authored
  spellings (double slashes, ``..`` segments, case variants on
  case-insensitive filesystems) are distinct keys; a mismatch costs a
  cache miss, never a wrong hit.

Consumers treat any resolution failure as "no identity": a cache miss,
never an error.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from weevr.config.paths import resolve_connection_path, resolve_fuse_path
from weevr.delta import is_table_alias
from weevr.model.connection import OneLakeConnection

logger = logging.getLogger(__name__)


def _normalize_path(path: str, spark: SparkSession) -> str:
    return resolve_fuse_path(path, spark).rstrip("/")


def _alias_location(alias: str, spark: SparkSession) -> str | None:
    """Storage location of a metastore table, or None when unreadable.

    Uses the parameterized DeltaTable API rather than interpolated SQL —
    the alias is config-supplied text and must never be spliced into a
    statement.
    """
    try:
        from delta.tables import DeltaTable

        rows = DeltaTable.forName(spark, alias).detail().select("location").collect()
        if rows and rows[0][0]:
            return str(rows[0][0])
    except Exception:
        logger.debug("Could not resolve location for alias '%s'", alias, exc_info=True)
    return None


def resolve_target_identity(
    spark: SparkSession,
    *,
    alias: str | None = None,
    path: str | None = None,
    connection: str | None = None,
    table: str | None = None,
    schema_override: str | None = None,
    connections: dict[str, OneLakeConnection] | None = None,
) -> str | None:
    """Canonical identity for a target reference, or None when unresolvable.

    Exactly one declaration form is expected (the model validators enforce
    this upstream); precedence mirrors the executor's resolution order.
    """
    if connection:
        try:
            location = resolve_connection_path(connection, table, schema_override, connections)
        except ValueError:
            return None
        return _normalize_path(location, spark)
    ref = alias or path
    if not ref:
        return None
    if is_table_alias(ref):
        location = _alias_location(ref, spark)
        # The fallback lives in its own key namespace: an unresolved alias
        # must never collide with a location-derived key (two DIFFERENT
        # tables whose aliases differ only by case would otherwise merge)
        return _normalize_path(location, spark) if location else f"alias:{ref.lower()}"
    return _normalize_path(ref, spark)
