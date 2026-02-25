# Your First Loom

This tutorial walks you through building a complete weevr pipeline from scratch.
By the end, you will have a working loom that reads CSV data, applies
transformations, and writes the result to a Delta table.

## Prerequisites

- **Python 3.11** installed (weevr targets Fabric Runtime 1.3)
- **Microsoft Fabric workspace** with a Lakehouse, or a local Spark installation
  for development
- Basic familiarity with YAML syntax

## Install weevr

```bash
pip install weevr
```

In a Fabric environment, install the package into your Spark environment or
add it to your notebook's inline install cell:

```python
%pip install weevr
```

## Project structure

Create a project directory with the conventional layout that weevr expects:

```
my-project/
  threads/
    staging/
      stg_customers.yaml
  weaves/
    staging.yaml
  looms/
    daily.yaml
  data/
    customers.csv
```

The `threads/`, `weaves/`, and `looms/` directories mirror the three layers of
weevr's execution hierarchy. Each config file lives in its conventional location
so the engine can resolve references automatically.

## Step 1 -- Create source data

Place a CSV file at `data/customers.csv` with sample records:

```csv
customer_id,first_name,last_name,email,status,created_date
1001,Alice,Morgan,alice.morgan@example.com,active,2024-01-15
1002,Bob,Chen,bob.chen@example.com,active,2024-02-20
1003,Carol,Santos,,inactive,2024-03-10
1004,Dave,Okafor,dave.okafor@example.com,active,2024-04-05
```

## Step 2 -- Define a thread

A **thread** is the smallest unit of work: read sources, apply transforms, write
to a single target.

Create `threads/staging/stg_customers.yaml`:

```yaml
config_version: "1.0"

sources:
  raw_customers:
    type: csv
    path: data/customers.csv
    options:
      header: "true"
      inferSchema: "true"

steps:
  - filter:
      expr: "status = 'active'"
  - derive:
      columns:
        full_name: "concat(first_name, ' ', last_name)"
  - select:
      columns:
        - customer_id
        - full_name
        - email
        - created_date
  - cast:
      columns:
        customer_id: "int"
        created_date: "date"

target:
  path: Tables/stg_customers

write:
  mode: overwrite
```

This thread reads the CSV, filters to active customers, derives a `full_name`
column, selects and casts the final columns, then overwrites the Delta target.

!!! tip "Step ordering matters"
    Steps execute top-to-bottom. Filter early to reduce the data volume before
    downstream transforms run.

## Step 3 -- Define a weave

A **weave** groups related threads and resolves their execution order via a
dependency DAG. Even a single-thread weave is valid.

Create `weaves/staging.yaml`:

```yaml
config_version: "1.0"

threads:
  - staging.stg_customers
```

The thread reference `staging.stg_customers` resolves to
`threads/staging/stg_customers.yaml` using dot-separated namespace conventions.

## Step 4 -- Define a loom

A **loom** is the deployable execution unit. It sequences one or more weaves.

Create `looms/daily.yaml`:

```yaml
config_version: "1.0"

weaves:
  - staging
```

The weave reference `staging` resolves to `weaves/staging.yaml`.

## Step 5 -- Run the loom

Open a notebook or Python script and execute the loom through the weevr Python
API:

```python
from weevr import Context

# In Fabric, `spark` is available automatically.
# For local development, create a SparkSession:
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.master("local[*]").getOrCreate()

ctx = Context(spark)
result = ctx.run("looms/daily.yaml")

print(result.status)
print(result.summary())
```

!!! info "Working directory"
    The paths in your config files resolve relative to the current working
    directory. Run your script from the project root (`my-project/`) so that
    `data/customers.csv` and the `threads/`, `weaves/`, `looms/` directories
    are all reachable.

## Step 6 -- Verify the output

Check the result object returned by `ctx.run()`:

```python
# Overall status: "success", "failure", or "partial"
assert result.status == "success"

# Human-readable execution summary
print(result.summary())
# Status: success
# Scope:  loom:daily
# Rows:   3 written
# Time:   1.2s

# Structured telemetry data
print(result.telemetry)
```

You can also read the Delta table directly to confirm the data landed correctly:

```python
df = spark.read.format("delta").load("Tables/stg_customers")
df.show()
# +----------+-----------+-----------------------+-----------+
# |customer_id| full_name|                  email|created_date|
# +----------+-----------+-----------------------+-----------+
# |      1001|Alice Mo...|alice.morgan@example...|  2024-01-15|
# |      1002|  Bob Chen| bob.chen@example.com  |  2024-02-20|
# |      1004|Dave Oka...| dave.okafor@example...|  2024-04-05|
# +----------+-----------+-----------------------+-----------+
```

Three rows are written because the filter removed the inactive customer (Carol).

## Execution modes

Besides `execute` (the default), weevr supports three additional modes that are
useful during development:

```python
# Validate config without touching data
result = ctx.run("looms/daily.yaml", mode="validate")

# Show the execution plan (DAG order) without running
result = ctx.run("looms/daily.yaml", mode="plan")

# Run transforms against sampled data, no writes
result = ctx.run("looms/daily.yaml", mode="preview")
```

## Next steps

You now have a working end-to-end pipeline. From here you can:

- [Add more threads](../how-to/add-a-thread.md) with joins, aggregations, and
  lookups
- [Add validation rules](../how-to/add-validation-rules.md) to enforce data
  quality
- [Configure caching](../how-to/cache-a-lookup.md) for lookup tables shared
  across threads
- [Run from a Fabric pipeline](../how-to/run-from-fabric-pipeline.md) for
  scheduled production execution
- Explore [execution modes](../concepts/execution-modes.md) for validate, plan,
  and preview workflows
