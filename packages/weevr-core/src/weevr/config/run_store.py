"""Run Store locations — where a project's run outputs live. Spark-free.

A project's run outputs (execution history, validation results, quarantine
and run files) live in its Run Store, separate from its definition. This
module computes *where*: pure string and path building, no reads, no writes
and no existence checks. Writers take the result and handle failures (an
unwritable store is a warning at write time, not a load error).

Base — the folder that holds the store's ``Tables/`` and ``Files/`` areas,
chosen in this order:

1. ``run_store.connection`` (from the ``Context`` override, else from
   ``weevr.yaml``) names a :class:`~weevr.model.connection.OneLakeConnection`;
   the base is that connection's lakehouse,
   ``abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>``.
   The connection's ``default_schema`` plays no part: the layout picks the
   schema. A redirected store is always treated as a lakehouse, never as a
   Fabric item's own OneLake.
2. A project root under the default lakehouse's ``/lakehouse/default/Files/``
   → the default lakehouse, ``/lakehouse/default``.
3. A project root ``abfss://<ws>@<host>/<item>/Files/…`` on a OneLake host
   (``*.fabric.microsoft.com``) → ``abfss://<ws>@<host>/<item>``. The path
   doesn't say whether ``<item>`` is a lakehouse or a Fabric item; the
   caller says so with ``fet_item``.
4. Any other local path → ``<root>/.weevr/run_store`` (made absolute), a
   local tooling space written as Delta by path.
5. No root (an in-memory definition) or any other remote URI → no base:
   :func:`resolve_run_store` returns ``None`` and the caller warns that
   persistence is skipped.

Layout — the same relative names under every base:

- ``per_project`` (default): tables at ``Tables/weevr_<name>/<table>``, run
  files under ``Files/weevr_<name>/runs``; in a Fabric item's own OneLake,
  ``Tables/<table>`` and ``Files/runs``.
- ``shared``: tables at ``Tables/<schema>/<table>``, run files under
  ``Files/<schema>/runs/<name>``.
- ``shared`` uses ``run_store.schema``, default ``weevr``; under
  ``per_project`` a ``run_store.schema`` replaces ``weevr_<name>`` (also in
  a Fabric item, which otherwise has no schema).
- A lakehouse without schemas (``schema_enabled=False``) takes the prefix
  form: ``Tables/<schema>_<table>``. Fabric creates schema-enabled
  lakehouses by default, so ``schema_enabled`` defaults to ``True``.
- Each run writes its files under ``<run_files>/<utc-ts>_<run_id>/``; the
  writer adds that folder.
- Every row a writer adds carries the project's ``id``, exposed as
  :attr:`RunStoreLocation.project_id`.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Literal
from uuid import UUID

from weevr.config.locations import ConfigLocation, LocalConfigLocation, RemoteConfigLocation
from weevr.diagnostics import diagnostic
from weevr.errors.exceptions import ConfigLocationError
from weevr.model.connection import OneLakeConnection
from weevr.model.project import Project, RunStore

RUN_STORE_TABLES: tuple[str, ...] = ("execution_history", "validation_results", "quarantine")
"""The Run Store's reserved table names."""

SHARED_SCHEMA = "weevr"
"""The schema of a ``shared`` Run Store that names none."""

DEFAULT_LAKEHOUSE = "/lakehouse/default"
"""Where Fabric mounts a notebook's default lakehouse."""

_DEFAULT_LAKEHOUSE_FILES = f"{DEFAULT_LAKEHOUSE}/Files/"
_ONELAKE_HOST_SUFFIX = ".fabric.microsoft.com"
_ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"
_LOCAL_STORE = ".weevr/run_store"

Layout = Literal["per_project", "shared"]


@dataclass(frozen=True)
class TableLocation:
    """Where one Run Store table lives.

    Attributes:
        name: The table's reserved name (one of :data:`RUN_STORE_TABLES`).
        path: The Delta table path, ``<base>/Tables/<schema>/<name>`` (or the
            prefix or Fabric-item form). Always set; writers can write by path.
        identifier: The catalog name in the session's default lakehouse
            (``<schema>.<name>``, or ``<schema>_<name>`` without schemas), set
            only when the store *is* the default lakehouse; ``None`` otherwise.
    """

    name: str
    path: str
    identifier: str | None


@dataclass(frozen=True)
class RunStoreLocation:
    """Where a project's run outputs live.

    Attributes:
        base: The folder holding the store's ``Tables/`` and ``Files/``.
        layout: ``per_project`` or ``shared``.
        schema: The schema (or table-name prefix) the tables sit in; ``None``
            for a Fabric item's own per-project tables.
        schema_enabled: Whether tables take the schema form (``True``) or the
            prefix form.
        project_id: The project's id, which every written row carries.
        project_name: The project's name.
        tables: Each reserved table's location, by name (read-only).
        run_files: The folder run files go under; each run adds
            ``<utc-ts>_<run_id>/``.
    """

    base: str
    layout: Layout
    schema: str | None
    schema_enabled: bool
    project_id: UUID
    project_name: str
    tables: Mapping[str, TableLocation]
    run_files: str


def resolve_run_store(
    project: Project,
    root: ConfigLocation | None,
    override: RunStore | None = None,
    *,
    connections: Mapping[str, OneLakeConnection] | None = None,
    fet_item: bool = False,
    schema_enabled: bool = True,
) -> RunStoreLocation | None:
    """Return where ``project``'s run outputs live; the rules are in the module docstring.

    Args:
        project: The project document (``weevr.yaml``).
        root: The project root, or ``None`` for an in-memory definition.
        override: Run-store settings that replace ``project.run_store``
            whole (the ``Context(run_store=…)`` argument).
        connections: The connection registry ``run_store.connection`` is
            looked up in.
        fet_item: The root's OneLake item is a Fabric item holding the
            project, not a lakehouse: per-project tables and run files sit
            directly under its ``Tables/`` and ``Files/``. Ignored when a
            connection redirects the store.
        schema_enabled: The store's lakehouse has schemas; ``False`` gives the
            prefix form.

    Returns:
        The store's locations, or ``None`` when there is no base (no root and
        no connection, or a root that is neither local nor a OneLake item).

    Raises:
        ConfigLocationError: ``run_store.connection`` names no connection in
            ``connections`` (``R201``).
    """
    settings = override if override is not None else project.run_store or RunStore()
    if settings.connection is not None:
        base = _connection_base(settings.connection, connections)
        fet_item = False
    else:
        base = _container_base(root)
    if base is None:
        return None

    name = project.name
    if settings.layout == "shared":
        schema: str | None = settings.schema_ or SHARED_SCHEMA
        run_files = f"{base}/Files/{schema}/runs/{name}"
    else:
        default = None if fet_item else f"weevr_{name}"
        schema = settings.schema_ or default
        run_files = f"{base}/Files/{schema}/runs" if schema else f"{base}/Files/runs"

    catalog = base == DEFAULT_LAKEHOUSE
    tables = {
        table: _table(base, schema, table, schema_enabled=schema_enabled, catalog=catalog)
        for table in RUN_STORE_TABLES
    }
    return RunStoreLocation(
        base=base,
        layout=settings.layout,
        schema=schema,
        schema_enabled=schema_enabled,
        project_id=project.id,
        project_name=name,
        tables=MappingProxyType(tables),
        run_files=run_files,
    )


def _table(
    base: str, schema: str | None, table: str, *, schema_enabled: bool, catalog: bool
) -> TableLocation:
    """One table's location: schema form, prefix form, or bare (Fabric item)."""
    if schema is None:
        relative, identifier = table, table
    elif schema_enabled:
        relative, identifier = f"{schema}/{table}", f"{schema}.{table}"
    else:
        relative = identifier = f"{schema}_{table}"
    return TableLocation(
        name=table,
        path=f"{base}/Tables/{relative}",
        identifier=identifier if catalog else None,
    )


def _connection_base(name: str, connections: Mapping[str, OneLakeConnection] | None) -> str:
    """The lakehouse folder of the connection ``name``, or ``R201`` when undeclared."""
    if not connections or name not in connections:
        raise ConfigLocationError.from_diagnostic(
            diagnostic(
                "R201",
                f"run_store names undefined connection '{name}'",
                expected=sorted(connections or {}),
            )
        )
    connection = connections[name]
    return f"abfss://{connection.workspace}@{_ONELAKE_HOST}/{connection.lakehouse}"


def _container_base(root: ConfigLocation | None) -> str | None:
    """The store base the project's container implies, or ``None`` when it implies none."""
    if root is None:
        return None
    if isinstance(root, LocalConfigLocation):
        path = Path(os.path.normpath(root.path.absolute()))
        if f"{path}/".startswith(_DEFAULT_LAKEHOUSE_FILES):
            return DEFAULT_LAKEHOUSE
        return f"{path}/{_LOCAL_STORE}"
    if isinstance(root, RemoteConfigLocation):
        return _onelake_item(root.uri)
    return None


def _onelake_item(uri: str) -> str | None:
    """``abfss://<ws>@<host>/<item>`` for a URI under that item's ``Files/``, else ``None``."""
    scheme, sep, rest = uri.partition("://")
    if not sep or scheme.lower() != "abfss":
        return None
    authority, _, path = rest.partition("/")
    _, at, host = authority.partition("@")
    if not at or not host.lower().endswith(_ONELAKE_HOST_SUFFIX):
        return None
    segments = path.split("/")
    if len(segments) < 2 or not segments[0] or segments[1] != "Files":
        return None
    return f"{scheme}://{authority}/{segments[0]}"
