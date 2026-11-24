"""Run Store locations: where a project's run outputs live, for both layouts."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from weevr.config.locations import ConfigLocation, LocalConfigLocation, RemoteConfigLocation
from weevr.config.run_store import (
    RUN_STORE_TABLES,
    RunStoreLocation,
    TableLocation,
    resolve_run_store,
)
from weevr.errors.exceptions import ConfigLocationError
from weevr.model.connection import OneLakeConnection
from weevr.model.project import Project, RunStore

PROJECT_ID = "8f9d2c4e-1b3a-4c5d-9e6f-7a8b9c0d1e2f"
HOST = "onelake.dfs.fabric.microsoft.com"
LAKE = f"abfss://ws@{HOST}/lake"
CENTRAL = f"abfss://ops-ws@{HOST}/central"


def _project(run_store: dict[str, Any] | None = None, name: str = "sales") -> Project:
    data: dict[str, Any] = {"config_version": "2.0", "id": PROJECT_ID, "name": name}
    if run_store is not None:
        data["run_store"] = run_store
    return Project.model_validate(data)


def _remote(uri: str) -> RemoteConfigLocation:
    return RemoteConfigLocation(uri, MagicMock())


def _local(path: str) -> LocalConfigLocation:
    return LocalConfigLocation(Path(path))


CONNECTIONS = {
    "central_lake": OneLakeConnection(type="onelake", workspace="ops-ws", lakehouse="central"),
}


def _paths(store: RunStoreLocation) -> dict[str, str]:
    return {name: table.path for name, table in store.tables.items()}


def _identifiers(store: RunStoreLocation) -> dict[str, str | None]:
    return {name: table.identifier for name, table in store.tables.items()}


def _tables(prefix: str) -> dict[str, str]:
    return {name: f"{prefix}{name}" for name in RUN_STORE_TABLES}


# ---------------------------------------------------------------------------
# Base inference x layout
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("root", "run_store", "kwargs", "base", "schema", "tables", "run_files"),
    [
        pytest.param(
            _local("/work/sales"),
            None,
            {},
            "/work/sales/.weevr/run_store",
            "weevr_sales",
            _tables("/work/sales/.weevr/run_store/Tables/weevr_sales/"),
            "/work/sales/.weevr/run_store/Files/weevr_sales/runs",
            id="local-folder-per-project",
        ),
        pytest.param(
            _local("/lakehouse/default/Files/sales.weevr"),
            None,
            {},
            "/lakehouse/default",
            "weevr_sales",
            _tables("/lakehouse/default/Tables/weevr_sales/"),
            "/lakehouse/default/Files/weevr_sales/runs",
            id="default-lakehouse-per-project",
        ),
        pytest.param(
            _remote(f"{LAKE}/Files/projects/sales.weevr"),
            None,
            {},
            LAKE,
            "weevr_sales",
            _tables(f"{LAKE}/Tables/weevr_sales/"),
            f"{LAKE}/Files/weevr_sales/runs",
            id="abfss-lakehouse-per-project",
        ),
        pytest.param(
            _remote(f"{LAKE}/Files/sales.weevr"),
            None,
            {"fet_item": True},
            LAKE,
            None,
            _tables(f"{LAKE}/Tables/"),
            f"{LAKE}/Files/runs",
            id="fet-item-per-project",
        ),
        pytest.param(
            _remote(f"{LAKE}/Files/sales.weevr"),
            None,
            {"schema_enabled": False},
            LAKE,
            "weevr_sales",
            _tables(f"{LAKE}/Tables/weevr_sales_"),
            f"{LAKE}/Files/weevr_sales/runs",
            id="non-schema-lakehouse-prefix-form",
        ),
        pytest.param(
            _remote(f"{LAKE}/Files/sales.weevr"),
            {"schema": "sales_runs"},
            {},
            LAKE,
            "sales_runs",
            _tables(f"{LAKE}/Tables/sales_runs/"),
            f"{LAKE}/Files/sales_runs/runs",
            id="per-project-schema-override",
        ),
        pytest.param(
            _remote(f"{LAKE}/Files/sales.weevr"),
            {"schema": "sales_runs"},
            {"fet_item": True},
            LAKE,
            "sales_runs",
            _tables(f"{LAKE}/Tables/sales_runs/"),
            f"{LAKE}/Files/sales_runs/runs",
            id="fet-item-per-project-schema-override",
        ),
        pytest.param(
            _remote(f"{LAKE}/Files/sales.weevr"),
            {"layout": "shared"},
            {},
            LAKE,
            "weevr",
            _tables(f"{LAKE}/Tables/weevr/"),
            f"{LAKE}/Files/weevr/runs/sales",
            id="shared-default-schema",
        ),
        pytest.param(
            _remote(f"{LAKE}/Files/sales.weevr"),
            {"layout": "shared", "schema": "weevr_ops"},
            {},
            LAKE,
            "weevr_ops",
            _tables(f"{LAKE}/Tables/weevr_ops/"),
            f"{LAKE}/Files/weevr_ops/runs/sales",
            id="shared-named-schema",
        ),
        pytest.param(
            _remote(f"{LAKE}/Files/sales.weevr"),
            {"layout": "shared"},
            {"schema_enabled": False},
            LAKE,
            "weevr",
            _tables(f"{LAKE}/Tables/weevr_"),
            f"{LAKE}/Files/weevr/runs/sales",
            id="shared-non-schema-prefix-form",
        ),
        pytest.param(
            _remote(f"{LAKE}/Files/sales.weevr"),
            {"layout": "shared"},
            {"fet_item": True},
            LAKE,
            "weevr",
            _tables(f"{LAKE}/Tables/weevr/"),
            f"{LAKE}/Files/weevr/runs/sales",
            id="fet-item-shared",
        ),
        pytest.param(
            _local("/work/sales"),
            {"connection": "central_lake", "layout": "shared", "schema": "weevr_ops"},
            {"connections": CONNECTIONS},
            CENTRAL,
            "weevr_ops",
            _tables(f"{CENTRAL}/Tables/weevr_ops/"),
            f"{CENTRAL}/Files/weevr_ops/runs/sales",
            id="connection-redirect-shared",
        ),
        pytest.param(
            _remote(f"{LAKE}/Files/sales.weevr"),
            {"connection": "central_lake"},
            {"connections": CONNECTIONS, "fet_item": True},
            CENTRAL,
            "weevr_sales",
            _tables(f"{CENTRAL}/Tables/weevr_sales/"),
            f"{CENTRAL}/Files/weevr_sales/runs",
            id="fet-item-redirected-to-lakehouse-per-project",
        ),
        pytest.param(
            None,
            {"connection": "central_lake"},
            {"connections": CONNECTIONS},
            CENTRAL,
            "weevr_sales",
            _tables(f"{CENTRAL}/Tables/weevr_sales/"),
            f"{CENTRAL}/Files/weevr_sales/runs",
            id="in-memory-with-connection",
        ),
    ],
)
def test_locations(
    root: ConfigLocation | None,
    run_store: dict[str, Any] | None,
    kwargs: dict[str, Any],
    base: str,
    schema: str | None,
    tables: dict[str, str],
    run_files: str,
) -> None:
    store = resolve_run_store(_project(run_store), root, **kwargs)
    assert store is not None
    assert store.base == base
    assert store.schema == schema
    assert _paths(store) == tables
    assert store.run_files == run_files
    assert store.project_id == UUID(PROJECT_ID)
    assert store.project_name == "sales"
    assert store.layout == (run_store or {}).get("layout", "per_project")


def test_every_table_is_named() -> None:
    store = resolve_run_store(_project(), _local("/work/sales"))
    assert store is not None
    assert set(store.tables) == {"execution_history", "validation_results", "quarantine"}
    assert all(isinstance(t, TableLocation) for t in store.tables.values())
    assert {n: t.name for n, t in store.tables.items()} == {n: n for n in RUN_STORE_TABLES}


# ---------------------------------------------------------------------------
# Catalog identifiers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("run_store", "schema_enabled", "expected"),
    [
        (None, True, "weevr_sales.{t}"),
        (None, False, "weevr_sales_{t}"),
        ({"layout": "shared"}, True, "weevr.{t}"),
        ({"layout": "shared", "schema": "ops"}, False, "ops_{t}"),
    ],
)
def test_default_lakehouse_has_catalog_identifiers(
    run_store: dict[str, Any] | None, schema_enabled: bool, expected: str
) -> None:
    store = resolve_run_store(
        _project(run_store),
        _local("/lakehouse/default/Files/sales.weevr"),
        schema_enabled=schema_enabled,
    )
    assert store is not None
    assert _identifiers(store) == {t: expected.format(t=t) for t in RUN_STORE_TABLES}


@pytest.mark.parametrize(
    ("root", "run_store", "kwargs"),
    [
        (_local("/work/sales"), None, {}),
        (_remote(f"{LAKE}/Files/sales.weevr"), None, {}),
        (_remote(f"{LAKE}/Files/sales.weevr"), None, {"fet_item": True}),
        (
            _local("/lakehouse/default/Files/sales.weevr"),
            {"connection": "central_lake"},
            {"connections": CONNECTIONS},
        ),
    ],
)
def test_path_only_stores_have_no_identifier(
    root: ConfigLocation, run_store: dict[str, Any] | None, kwargs: dict[str, Any]
) -> None:
    store = resolve_run_store(_project(run_store), root, **kwargs)
    assert store is not None
    assert set(_identifiers(store).values()) == {None}


# ---------------------------------------------------------------------------
# No base: in-memory or unrecognized roots
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "root",
    [
        None,
        _remote("abfss://container@account.dfs.core.windows.net/item/Files/sales"),
        _remote(f"abfss://ws@{HOST}/lake/Tables/sales"),
        _remote(f"abfss://ws@{HOST}/lake"),
        _remote("s3a://bucket/sales"),
    ],
    ids=["in-memory", "adls-not-onelake", "not-under-files", "item-root", "other-scheme"],
)
def test_no_base_returns_none(root: ConfigLocation | None) -> None:
    assert resolve_run_store(_project(), root) is None


def test_regional_onelake_host_is_recognized() -> None:
    uri = "abfss://ws@westus-onelake.dfs.fabric.microsoft.com/lake/Files/sales.weevr"
    store = resolve_run_store(_project(), _remote(uri))
    assert store is not None
    assert store.base == "abfss://ws@westus-onelake.dfs.fabric.microsoft.com/lake"


def test_relative_local_root_is_made_absolute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    store = resolve_run_store(_project(), _local("sales"))
    assert store is not None
    assert store.base == f"{tmp_path}/sales/.weevr/run_store"


def test_dot_dot_out_of_the_default_lakehouse_is_not_the_default_lakehouse() -> None:
    store = resolve_run_store(_project(), _local("/lakehouse/default/Files/../x"))
    assert store is not None
    assert store.base == "/lakehouse/default/x/.weevr/run_store"


# ---------------------------------------------------------------------------
# Override precedence and connections
# ---------------------------------------------------------------------------


def test_override_replaces_weevr_yaml_run_store() -> None:
    project = _project({"layout": "shared", "schema": "weevr_ops"})
    store = resolve_run_store(
        project,
        _remote(f"{LAKE}/Files/sales.weevr"),
        override=RunStore(connection="central_lake"),
        connections=CONNECTIONS,
    )
    assert store is not None
    assert store.base == CENTRAL
    assert store.layout == "per_project"
    assert store.schema == "weevr_sales"


def test_override_gives_in_memory_project_a_base() -> None:
    store = resolve_run_store(
        _project(), None, override=RunStore(connection="central_lake"), connections=CONNECTIONS
    )
    assert store is not None
    assert store.base == CENTRAL


def test_override_without_connection_keeps_container_base() -> None:
    store = resolve_run_store(_project(), _local("/work/sales"), override=RunStore(layout="shared"))
    assert store is not None
    assert store.base == "/work/sales/.weevr/run_store"
    assert store.schema == "weevr"


@pytest.mark.parametrize("connections", [None, {}, CONNECTIONS])
def test_unknown_connection_is_r201(connections: dict[str, OneLakeConnection] | None) -> None:
    with pytest.raises(ConfigLocationError) as exc_info:
        resolve_run_store(
            _project({"connection": "missing"}),
            _local("/work/sales"),
            connections=connections,
        )
    [diag] = exc_info.value.diagnostics
    assert diag.code == "R201"
    assert list(diag.expected or ()) == sorted(connections or {})


def test_connection_default_schema_does_not_move_the_store() -> None:
    connections = {
        "central_lake": OneLakeConnection(
            type="onelake", workspace="ops-ws", lakehouse="central", default_schema="dbo"
        )
    }
    store = resolve_run_store(
        _project({"connection": "central_lake"}), None, connections=connections
    )
    assert store is not None
    assert store.schema == "weevr_sales"


def test_result_is_frozen() -> None:
    store = resolve_run_store(_project(), _local("/work/sales"))
    assert store is not None
    with pytest.raises(AttributeError):
        store.base = "x"  # type: ignore[misc]
    with pytest.raises(TypeError):
        store.tables["quarantine"] = store.tables["quarantine"]  # type: ignore[index]


def test_run_store_api_is_exported_from_weevr_config() -> None:
    from weevr import config

    exported = {
        "RUN_STORE_TABLES": RUN_STORE_TABLES,
        "RunStoreLocation": RunStoreLocation,
        "TableLocation": TableLocation,
        "resolve_run_store": resolve_run_store,
    }
    for name, value in exported.items():
        assert name in config.__all__
        assert getattr(config, name) is value
