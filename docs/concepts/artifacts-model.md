# Artifacts Model

weevr runs inside Microsoft Fabric and writes to OneLake. This page explains
how threads map to physical storage artifacts.

## OneLake and lakehouses

Microsoft Fabric organizes data into **lakehouses**, each backed by a
OneLake storage account. A lakehouse contains two top-level areas:

- **Tables** -- Delta Lake tables, queryable through Spark and SQL
- **Files** -- Unstructured files (CSV, Parquet, JSON, etc.)

weevr reads from and writes to both areas. The most common pattern is
Delta-to-Delta: reading from a source lakehouse table and writing to a
target lakehouse table.

## Delta Lake as the storage format

All table targets in weevr are **Delta Lake tables**. Delta provides:

- ACID transactions (atomic writes, no partial state on failure)
- Schema enforcement and evolution
- Time travel for auditing and rollback
- Change Data Feed for downstream CDC consumers

weevr relies on these properties for its write mode guarantees. Overwrite
is atomic, merge is transactional, and failed writes leave the target
unchanged. See [Idempotency](idempotency.md) for details.

## Source path resolution

Sources are declared with a type and an abstract reference that resolves
to a physical location through variable injection:

```yaml
sources:
  customers:
    type: delta
    alias: raw.customers       # resolved via params to a full path
  orders_file:
    type: csv
    path: ${env.files_path}/orders/
```

Delta sources use an **alias** that resolves to a four-part name
(`workspace.lakehouse.schema.table`) or to a physical OneLake path.
File sources use a **path** that resolves to a OneLake file location.

!!! tip "Environment-agnostic config"

    By using `${env.*}` variables for lakehouse and path references,
    the same thread configuration works across dev, staging, and prod.
    Only the parameter files differ between environments.

## Target path resolution

Targets follow the same resolution pattern as sources:

```yaml
target:
  alias: silver.dim_customer
```

The alias resolves through variable injection to a concrete Delta table
location. weevr handles table creation (when configured) and all DML
operations against the resolved path.

## How threads map to tables

The mapping is direct: **one thread writes to one Delta table**. The thread
name typically mirrors the target table name, though this is a convention
rather than a requirement.

```text
Thread: dim_customer  →  Delta table: silver.dim_customer
Thread: fact_orders   →  Delta table: silver.fact_orders
```

If the same target data needs to exist in multiple locations (cross-lakehouse
replication, file exports), use mirror outputs on the thread rather than
creating duplicate threads.

## Next steps

- [Thread, Weave, Loom](thread-weave-loom.md) -- The object model hierarchy
- [Execution Modes](execution-modes.md) -- How data is written to targets
- [YAML Schema: Thread](../reference/yaml-schema/thread.md) -- Full source
  and target configuration reference
