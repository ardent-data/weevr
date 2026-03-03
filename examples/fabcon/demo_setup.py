"""Fabcon 2026 demo data setup utilities.

Paste this into a Fabric notebook cell to stage source data as Delta tables
before running the weevr demo configs.

Usage::

    # Initial setup — load all base CSVs into Delta tables
    stage_all(spark)

    # Demo 3a second run — overwrite products with v2 data
    load_products_v2(spark)

    # Demo 3b second run — append new transactions
    append_transactions(spark)

    # Full reset — drop everything and re-stage
    reset(spark)
"""

# Adjust this path to match where the fabcon data directory is uploaded.
DATA_DIR = "Files/fabcon.weevr/data"


def _load_csv(spark, csv_name, table_name, mode="overwrite"):
    """Read a CSV file and write it as a Delta table.

    Args:
        spark: Active SparkSession.
        csv_name: CSV filename (relative to DATA_DIR).
        table_name: Target Delta table name.
        mode: Write mode — 'overwrite' or 'append'.
    """
    path = f"{DATA_DIR}/{csv_name}"
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    df.write.format("delta").mode(mode).saveAsTable(table_name)
    count = spark.table(table_name).count()
    print(f"  {table_name}: {count} rows ({mode})")


def stage_all(spark):
    """Load all base CSVs into Delta tables for the demo sequence.

    Creates the following tables:
      - raw.customers (from customers.csv)
      - raw.orders (from orders.csv)
      - raw.transactions (from transactions.csv)
      - staging.products (from products.csv) — used by Demo 3a
      - raw.products (from products.csv) — used by pipeline dims

    Args:
        spark: Active SparkSession.
    """
    print("Staging base data...")
    _load_csv(spark, "customers.csv", "raw.customers")
    _load_csv(spark, "orders.csv", "raw.orders")
    _load_csv(spark, "transactions.csv", "raw.transactions")
    _load_csv(spark, "products.csv", "staging.products")
    _load_csv(spark, "products.csv", "raw.products")
    print("Done — all base tables staged.")


def load_products_v2(spark):
    """Overwrite staging.products with v2 data for Demo 3a second run.

    Changes vs v1:
      - Product 101: price 29.99 -> 34.99 (triggers update)
      - Products 113, 114: new rows (triggers insert)
      - Product 112: absent (triggers soft_delete)

    Args:
        spark: Active SparkSession.
    """
    print("Loading products v2...")
    _load_csv(spark, "products_v2.csv", "staging.products")
    print("Done — run dim_product.thread to see merge in action.")


def append_transactions(spark):
    """Append v2 transactions for Demo 3b second run.

    Adds 4 new rows with March 2026 timestamps (after v1 max of 2026-02-20).
    All amounts are positive, so all rows pass the filter step.

    Args:
        spark: Active SparkSession.
    """
    print("Appending transactions v2...")
    _load_csv(spark, "transactions_v2.csv", "raw.transactions", mode="append")
    print("Done — run fact_transactions.thread to see incremental load.")


def reset(spark):
    """Drop all demo tables and re-stage from base CSVs.

    Args:
        spark: Active SparkSession.
    """
    tables = [
        "raw.customers",
        "raw.orders",
        "raw.transactions",
        "raw.products",
        "staging.products",
        "staging.stg_customers",
        "staging.stg_products",
        "silver.dim_customer",
        "silver.dim_product",
        "silver.fact_orders",
        "silver.fact_transactions",
    ]
    print("Resetting demo tables...")
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")  # noqa: S608
        print(f"  Dropped {table}")
    print()
    stage_all(spark)
