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
| 3a | `dim_product.thread` | Merge with soft delete (update, insert, soft-delete in 3 YAML keys) |
| 3b | `fact_transactions.thread` | Incremental watermark loading (zero-config persistence) |
| 4 | `customer_pipeline.weave` | 5-thread DAG with parallel execution groups and broadcast lookups |
| 5 | `daily.loom` | Multi-weave loom with config inheritance, typed params, conditional execution |

## Source Data

| File | Description |
|------|-------------|
| `data/customers.csv` | 10 customer rows — 2 inactive, 2 with null emails (for quarantine demo) |
| `data/products.csv` | 12 products across Electronics, Books, Home, and Office categories |
| `data/orders.csv` | 18 orders referencing customers (1–10) and products (101–112) |
| `data/transactions.csv` | 21 transactions — includes 3 bad rows (amount <= 0) for filter demo |

CSV files can be uploaded to the Lakehouse `Files` area and registered as Delta tables
for Demos 3–5. The `orders.csv` and `transactions.csv` files reference valid customer
and product IDs from the other source files.

## File Layout

```
fabcon/
├── data/
│   ├── customers.csv                  # Source CSV for Demos 1–2
│   ├── products.csv                   # Product catalog (12 rows)
│   ├── orders.csv                     # Order records (18 rows)
│   └── transactions.csv              # Transactions with timestamps (21 rows)
│
├── stg_customers.thread               # Demo 1: Basic thread
├── stg_customers_validated.thread     # Demo 2: Validations & quarantine
├── dim_product.thread                 # Demo 3a: Merge with soft delete
├── fact_transactions.thread           # Demo 3b: Incremental watermark
│
├── customer_pipeline.weave            # Demo 4: 5-thread DAG
│
├── daily.loom                         # Demo 5: Full loom
├── dimensions.weave                   # Dimensions weave (used by loom)
├── facts.weave                        # Facts weave (used by loom)
│
├── dimensions/                        # Thread files for dimensions weave
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
4. Pre-stage source data (CSV in Files, Delta tables in Tables)

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

# Demo 3a — Merge
result = ctx.run("dim_product.thread")
result.summary()

# Demo 3b — Incremental (run twice)
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
