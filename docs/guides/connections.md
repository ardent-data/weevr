# Connections

Connections let you declare a Fabric lakehouse once by name and reference it
from sources, targets, and exports throughout a thread. Instead of embedding
workspace and lakehouse GUIDs — or hardcoded `abfss://` paths — in every
source and target block, you define the connection in one place and refer to
it by a short name.

## When to use connections

| Pattern | Use this |
|---------|----------|
| Single-lakehouse thread, tables registered in the metastore | `alias:` |
| Single-lakehouse thread, raw OneLake path | `path:` |
| Reading from or writing to a different workspace or lakehouse | `connections:` |
| Multiple threads sharing the same remote lakehouse | `connections:` at weave or loom level |
| Portability across environments without changing GUIDs | `connections:` with `${fabric.*}` |

## Connection block structure

Connections are declared in a `connections:` map at the thread, weave, or
loom level. Each key is a logical name; the value is a connection object.

```yaml
connections:
  staging:
    type: onelake
    workspace: "a1b2c3d4-0000-0000-0000-111111111111"
    lakehouse: "e5f6a7b8-0000-0000-0000-222222222222"
    default_schema: staging
```

### Connection fields

| Field | Required | Description |
|-------|----------|-------------|
| `type` | Yes | Connection type. Only `onelake` is supported in v1. |
| `workspace` | Yes | OneLake workspace GUID or `${fabric.workspace_id}`. |
| `lakehouse` | Yes | OneLake lakehouse GUID or `${fabric.lakehouse_id}`. |
| `default_schema` | No | Default schema for tables in this connection. |

Once declared, a connection is referenced from a source or target using
`connection: <name>` together with `table: <table_name>`.

## Cross-lakehouse example

A common pattern is reading from a raw lakehouse and writing to a curated one
in a different workspace.

```yaml
connections:
  raw_lh:
    type: onelake
    workspace: "aaaaaaaa-0000-0000-0000-000000000001"
    lakehouse: "bbbbbbbb-0000-0000-0000-000000000002"
    default_schema: raw

  curated_lh:
    type: onelake
    workspace: "cccccccc-0000-0000-0000-000000000003"
    lakehouse: "dddddddd-0000-0000-0000-000000000004"
    default_schema: curated

sources:
  orders:
    connection: raw_lh
    table: orders

  customers:
    connection: raw_lh
    table: customers

target:
  connection: curated_lh
  table: fact_orders
```

The engine constructs the `abfss://` path for each source and target at
execution time from the workspace, lakehouse, schema, and table name.

## Auto-detection with `${fabric.*}`

When all lakehouses are within the current Fabric session's workspace, you
can use `${fabric.workspace_id}` and `${fabric.lakehouse_id}` rather than
hardcoding GUIDs. The engine reads these values from the active Spark
session at execution time.

```yaml
connections:
  current:
    type: onelake
    workspace: "${fabric.workspace_id}"
    lakehouse: "${fabric.lakehouse_id}"

sources:
  orders:
    connection: current
    table: raw_orders

target:
  connection: current
  table: fact_orders
```

This pattern makes threads portable: the same YAML file runs correctly in
development, test, and production workspaces without modification.

Available `${fabric.*}` variables:

| Variable | Description |
|----------|-------------|
| `${fabric.workspace_id}` | GUID of the current Fabric workspace |
| `${fabric.lakehouse_id}` | GUID of the active lakehouse in the session |
| `${fabric.workspace_name}` | Display name of the current workspace |

## Schema overrides

`default_schema` on a connection applies to all tables unless overridden.
Use `schema:` on a source or target to use a different schema for that
specific table.

```yaml
connections:
  archive:
    type: onelake
    workspace: "aaaaaaaa-0000-0000-0000-000000000001"
    lakehouse: "bbbbbbbb-0000-0000-0000-000000000002"
    default_schema: current_year

sources:
  transactions:
    connection: archive
    table: transactions         # resolves to current_year.transactions

  historical:
    connection: archive
    schema: prior_year          # overrides default_schema
    table: transactions         # resolves to prior_year.transactions
```

The `schema:` field on a source or target always takes precedence over
`default_schema` on the connection.

## Source, target, and export references

Connections work the same way in sources, the primary target, and exports.

**Source:**

```yaml
sources:
  products:
    connection: staging
    table: raw_products
```

**Target:**

```yaml
target:
  connection: curated_lh
  table: dim_products
```

**Export (delta type only):**

```yaml
exports:
  - name: compliance_copy
    type: delta
    connection: archive
    table: fact_orders_archive
```

!!! note "Export connections"
    The `connection` field on exports is only valid when `type: delta`.
    Parquet, CSV, JSON, and ORC exports use `path:` instead.

## Cascade and override

Connections cascade from loom to weave to thread. Define shared connections
at the loom level to make them available everywhere; override at the thread
level when a specific thread needs a different endpoint.

```yaml
# loom.loom
connections:
  shared_raw:
    type: onelake
    workspace: "${fabric.workspace_id}"
    lakehouse: "bbbbbbbb-0000-0000-0000-000000000002"
```

```yaml
# my_thread.thread — override shared_raw with a different lakehouse
connections:
  shared_raw:
    type: onelake
    workspace: "${fabric.workspace_id}"
    lakehouse: "eeeeeeee-0000-0000-0000-000000000005"
    default_schema: overridden
```

The most specific level always wins. A thread-level connection with the
same name as a loom-level connection replaces it entirely for that thread.

## Migration from manual `abfss://` paths

If you have existing threads using hardcoded `abfss://` paths in `source.path`
or `target.path`, you can migrate to connections gradually.

**Before:**

```yaml
sources:
  orders:
    type: delta
    path: >-
      abfss://aaaaaaaa-0000-0000-0000-000000000001@onelake.dfs.fabric.microsoft.com/
      bbbbbbbb-0000-0000-0000-000000000002/Tables/raw/orders

target:
  path: >-
    abfss://aaaaaaaa-0000-0000-0000-000000000001@onelake.dfs.fabric.microsoft.com/
    bbbbbbbb-0000-0000-0000-000000000002/Tables/curated/fact_orders
```

**After:**

```yaml
connections:
  lh:
    type: onelake
    workspace: "aaaaaaaa-0000-0000-0000-000000000001"
    lakehouse: "bbbbbbbb-0000-0000-0000-000000000002"

sources:
  orders:
    connection: lh
    schema: raw
    table: orders

target:
  connection: lh
  schema: curated
  table: fact_orders
```

Connections are also more maintainable when the same lakehouse is referenced
across multiple threads: update the GUID in one place rather than in every
file.

## Column sets via connections

Named column sets defined at the loom or weave level can also resolve their
mapping table through a connection. This is the right pattern when the
mapping data lives in a reference lakehouse that is **not** the attached
notebook lakehouse — for example, a portable loom that runs against
arbitrary workspaces and reads its column dictionary from a centralized
reference lakehouse.

```yaml
connections:
  ref:
    type: onelake
    workspace: "aaaaaaaa-0000-0000-0000-000000000001"
    lakehouse: "cccccccc-0000-0000-0000-000000000003"

column_sets:
  sap_dictionary:
    source:
      type: delta
      connection: ref
      schema: dictionary
      table: sap_column_renames
      from_column: raw_name
      to_column: friendly_name
      filter: "system = 'SAP'"
```

The `connection + table` form is mutually exclusive with the `alias`
form — the validator rejects any `ColumnSetSource` that sets both.
When `connection` is set, `table` is required and `path` is rejected;
`schema` is optional and selects the schema within the connection's
lakehouse. Use `connection` whenever the mapping table is not
guaranteed to exist in the active Spark catalog at execution time.
The `alias` form continues to work for column sets backed by tables
in the attached lakehouse.
