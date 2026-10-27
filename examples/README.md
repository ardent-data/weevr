# weevr Examples

Example configurations demonstrating weevr's declarative pipeline model.

Each file uses a typed extension (`.thread`, `.weave`, `.loom`, `.warp`) to indicate
its config type, and is named after the config it declares (the file stem must match
the `name`). They are not runnable without a Spark/Fabric environment, but CI loads
every example through the real config loader (`tests/weevr/test_examples.py`), so each
one is a valid, current config.

## Examples

| File | Description |
|------|-------------|
| `dim_customer.thread` | Minimal thread: read a Delta source, select columns, write to Delta. |
| `fact_order_summary.thread` | Thread with multiple pipeline steps: filter, derive, select, cast. |
| `dim_product.thread` | Thread that merges into a Delta target using match keys. |
| `fact_transactions.thread` | Thread with watermark-based incremental loading. |
| `dim_calendar.thread` | Generated date dimension with calendar attributes. |
| `dim_store.thread` | Thread with data quality validations and post-write assertions. |
| `stg_orders_with_exports.thread` | Thread with secondary exports: Parquet archive and CSV extract. |
| `dim_contacts.thread` | concat, map, fill_null type_defaults, and format in a single pipeline. |
| `fact_sales.thread` | FK resolution: single, compound BK, SCD2 current flag, batch mode, sentinel assertions. |
| `warp-auto.thread` | Warp auto-generation: the engine writes and evolves a `.warp` from the output. |
| `warp-enforcement.thread` | Warp enforcement against `dim_customer.warp` with strict drift handling. |
| `dim_customer.warp` | A warp (schema contract) for the customer dimension. |
| `customer_pipeline.weave` | Weave orchestrating three inline threads with explicit dependencies. |
| `orders_pipeline.weave` | Weave with pre/post quality gates, SQL variables, and log messages. |
| `enrichment_pipeline.weave` | Weave with materialized (broadcast) and on-demand lookups used by its threads. |
| `sales_lakehouse.loom` | Complete loom with two inline weaves, typed params, defaults, and execution settings. |

Weaves and the loom define their children inline so each example stands alone; in a
project, children usually live in their own files and are listed with `ref:`.

## Fabcon 2026 Demos

The `fabcon/` directory contains a progressive demo sequence for the Fabcon 2026
session. Six demos build from a single thread to a full loom, covering CSV
ingestion, validations, merge with soft delete, incremental watermarks, DAG
orchestration, and multi-weave deployment. See [`fabcon/README.md`](fabcon/README.md)
for the full walkthrough.

## Configuration Hierarchy

weevr uses a layered configuration model:

- **Thread** -- Smallest unit: sources, pipeline steps, and a target.
- **Weave** -- Collection of threads with a dependency DAG.
- **Loom** -- Deployment unit containing weaves with shared defaults.

Configuration cascades from loom to weave to thread (most specific wins).
