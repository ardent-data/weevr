"""OneLake path construction — pure string building, Spark-free.

Lives in the core wheel so both the runtime (readers, executor, target
identity) and plan-time analysis (the planner's target/source indexes)
resolve connection+table declarations to the same physical path. The
engine wheel re-exports these from ``weevr.config.paths`` for
compatibility.
"""

from __future__ import annotations

from weevr.model.connection import OneLakeConnection

_ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"


def build_onelake_path(
    connection: OneLakeConnection,
    schema: str | None,
    table: str,
) -> str:
    """Build an abfss:// path from connection properties.

    Args:
        connection: OneLake connection declaration with workspace and lakehouse
            identifiers.
        schema: Schema name to use. When provided, takes precedence over
            ``connection.default_schema``.
        table: Table name to append to the path.

    Returns:
        A fully-qualified ``abfss://`` URI for the given table.
    """
    effective_schema = schema or connection.default_schema
    base = f"abfss://{connection.workspace}@{_ONELAKE_HOST}/{connection.lakehouse}/Tables"
    if effective_schema:
        return f"{base}/{effective_schema}/{table}"
    return f"{base}/{table}"


def resolve_connection_path(
    connection_name: str,
    table: str | None,
    schema_override: str | None,
    connections: dict[str, OneLakeConnection] | None,
) -> str:
    """Resolve a connection+table declaration to its abfss:// path.

    The one shared implementation of connection resolution — the executor
    (targets), the readers (sources), the target-identity resolver, and
    the planner's dependency analysis all call it instead of duplicating
    the lookup-and-build sequence.

    Raises:
        ValueError: If the connection is undeclared or ``table`` is absent.
            Callers wrap this in their own error types with call-site
            context (thread name, source alias).
    """
    if not connections or connection_name not in connections:
        raise ValueError(f"undefined connection '{connection_name}'")
    if table is None:
        raise ValueError(f"connection '{connection_name}' requires 'table'")
    return build_onelake_path(connections[connection_name], schema_override, table)
