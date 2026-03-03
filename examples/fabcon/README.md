# Fabcon 2026 — Live Demo Configs

Progressive demo sequence for the Fabcon 2026 session: *"Stop Writing Notebooks,
Start Declaring Intent: Configuration-Driven Data Engineering with weevr."*

Each demo builds on the previous one, walking the audience through a realistic
adoption path — from a single thread to a full deployment loom.

## Demo Sequence

| Demo | Config | What It Shows |
|------|--------|---------------|
| 1 | `stg_customers.thread` | Read CSV, filter, derive, select/cast, write to Delta |
| 2 | `stg_customers_validated.thread` | Pre-write validations, quarantine routing, post-write assertions |
| 3a | `dim_product.thread` | Merge with soft delete, xxhash64 surrogate key |
| 3b | `fact_transactions.thread` | Incremental watermark loading (zero-config persistence) |
| 4 | `customer_pipeline.weave` | 5-thread DAG with narrow lookups, hooks, variables, quality gates |
| 5 | `daily.loom` | Multi-weave loom with config inheritance, typed params, conditional execution |

## Features Showcased

| Feature | Introduced | Details |
|---------|-----------|---------|
| Source reading (CSV, Delta) | Demo 1 | `sources:` block with type-specific options |
| Transform pipeline | Demo 1 | `steps:` — filter, derive, select, cast |
| Dedup | Demo 4 (pipeline) | `dedup:` step with keys and order_by |
| Validations & quarantine | Demo 2 | Pre-write rules, post-write assertions |
| Merge with soft delete | Demo 3a | update/insert/soft_delete via `write.mode: merge` |
| Incremental watermark | Demo 3b | Zero-config state persistence |
| Surrogate keys (xxhash64) | Demo 3a, 4 | `keys.surrogate_key` with algorithm selection |
| Change detection hash | Demo 4 (dim_customer) | `keys.change_detection` for row-level change tracking |
| Narrow lookups | Demo 4 | `key`/`values` projection, `unique_key` validation |
| Hooks & variables | Demo 4 | `pre_steps`/`post_steps` with quality gates, SQL, logging |
| Multi-thread DAG | Demo 4 | Parallel execution groups with dependency ordering |
| Naming normalization | Demo 5 | `naming.columns: snake_case` in loom defaults |
| Config inheritance | Demo 5 | Loom defaults cascade to weaves and threads |
| Typed parameters | Demo 5 | `params:` with type, required, default |
| Conditional execution | Demo 5 | `condition.when:` for weave-level gating |

## Source Data

| File | Description |
|------|-------------|
| `data/customers.csv` | 10 customer rows — 2 inactive, 2 with null emails (for quarantine demo) |
| `data/products.csv` | 12 products across Electronics, Books, Home, and Office categories |
| `data/products_v2.csv` | 13 products — price change (101), 2 new rows (113, 114), 1 removed (112) |
| `data/orders.csv` | 18 orders referencing customers (1–10) and products (101–112) |
| `data/transactions.csv` | 21 transactions — includes 3 bad rows (amount <= 0) for filter demo |
| `data/transactions_v2.csv` | 4 new transactions (March 2026) for incremental append demo |

CSV files can be uploaded to the Lakehouse `Files` area and registered as Delta tables
for Demos 3–5. The `orders.csv` and `transactions.csv` files reference valid customer
and product IDs from the other source files.

## File Layout

```
fabcon/
├── data/
│   ├── customers.csv                  # Source CSV for Demos 1–2
│   ├── products.csv                   # Product catalog (12 rows)
│   ├── products_v2.csv                # Product catalog v2 (13 rows, for Demo 3a re-run)
│   ├── orders.csv                     # Order records (18 rows)
│   ├── transactions.csv              # Transactions with timestamps (21 rows)
│   └── transactions_v2.csv           # Transactions v2 (4 rows, for Demo 3b re-run)
│
├── demo_setup.py                      # Data staging utilities for Fabric notebooks
│
├── stg_customers.thread               # Demo 1: Basic thread
├── stg_customers_validated.thread     # Demo 2: Validations & quarantine
├── dim_product.thread                 # Demo 3a: Merge with soft delete + surrogate key
├── fact_transactions.thread           # Demo 3b: Incremental watermark
│
├── customer_pipeline.weave            # Demo 4: 5-thread DAG with narrow lookups & hooks
│
├── daily.loom                         # Demo 5: Full loom
├── dimensions.weave                   # Dimensions weave (used by loom)
├── facts.weave                        # Facts weave (used by loom)
│
├── dimensions/                        # Thread files for pipeline weaves
│   ├── stg_customers.thread
│   ├── stg_products.thread
│   ├── dim_customer.thread
│   └── dim_product.thread
│
└── facts/                             # Thread files for facts weave
    └── fact_orders.thread
```

## Fabric Environment Setup

1. Create a Fabric workspace with a Lakehouse attached
2. Install weevr: `pip install weevr`
3. Upload the `fabcon/` directory to the Lakehouse `Files` area as `fabcon.weevr`
4. Paste `demo_setup.py` into a notebook cell and run `stage_all(spark)` to create
   all source Delta tables

## Running the Demos

```python
from weevr import Context

# Point to the project folder in the Lakehouse
ctx = Context(spark, "fabcon")

# Demo 1 — Basic thread
result = ctx.run("stg_customers.thread")
result.summary()

# Demo 2 — Validations
result = ctx.run("stg_customers_validated.thread")
result.summary()

# Demo 3a — Merge (first run: initial load)
result = ctx.run("dim_product.thread")
result.summary()

# Demo 3a — Merge (second run: load v2 data, then re-run)
#   load_products_v2(spark)
result = ctx.run("dim_product.thread")
result.summary()

# Demo 3b — Incremental (first run)
result = ctx.run("fact_transactions.thread")
result.summary()

# Demo 3b — Incremental (second run: append v2 transactions, then re-run)
#   append_transactions(spark)
result = ctx.run("fact_transactions.thread")
result.summary()

# Demo 4 — Plan first, then execute
result = ctx.run("customer_pipeline.weave", mode="plan")
result.summary()
result = ctx.run("customer_pipeline.weave")
result.summary()

# Demo 5 — Full loom with params
ctx = Context(spark, "fabcon", params={"lakehouse_path": "/lakehouse/default"})
result = ctx.run("daily.loom")
result.summary()
```

## Hybrid Demo Strategy

- Attempt all demos live — `preview` and `validate` modes make dry runs safe
- Pre-record backups for each segment (OBS scene switch or embedded video)
- If Fabric is slow: skip live execution, show pre-recorded result, narrate live
