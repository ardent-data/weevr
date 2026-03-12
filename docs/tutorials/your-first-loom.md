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

Create a project directory with a `.weevr` extension. Config files use typed
extensions (`.thread`, `.weave`, `.loom`) and can be organized in any directory
layout within the project:

```text
my-project.weevr/
  staging/
    stg_customers.thread
  staging.weave
  daily.loom
  data/
    customers.csv
```

The `.weevr` directory is the project root. Config types are identified by
file extension, so you can organize files in any folder structure that suits
your team.

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

Create `staging/stg_customers.thread`:

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

Create `staging.weave`:

```yaml
config_version: "1.0"

threads:
  - ref: staging/stg_customers.thread
```

The thread reference `staging/stg_customers.thread` is a path relative to the
project root, with the typed extension identifying the config type.

## Step 4 -- Define a loom

A **loom** is the deployable execution unit. It sequences one or more weaves.

Create `daily.loom`:

```yaml
config_version: "1.0"

weaves:
  - ref: staging.weave
```

The weave reference `staging.weave` resolves to the `staging.weave` file
in the project root.

## Step 5 -- Run the loom

Open a notebook or Python script and execute the loom through the weevr Python
API:

```python
from weevr import Context

# In Fabric, `spark` is available automatically.
# For local development, create a SparkSession:
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.master("local[*]").getOrCreate()

ctx = Context(spark, "my-project.weevr")
result = ctx.run("daily.loom")

print(result.status)
print(result.summary())
```

!!! info "Project resolution"
    The `project` argument tells weevr where your `.weevr` directory lives.
    All config paths resolve relative to the project root. In Fabric, the
    project directory sits in the Lakehouse Files section.

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
result = ctx.run("daily.loom", mode="validate")

# Show the execution plan (DAG order) without running
result = ctx.run("daily.loom", mode="plan")
print(result.summary())      # compact execution order with cache markers
print(result.explain())      # detailed breakdown: dependencies, cache targets, thread detail

# Run transforms against sampled data, no writes
result = ctx.run("daily.loom", mode="preview")
```

!!! tip "Notebook display"
    In notebooks, plan mode results render automatically as styled HTML
    with an embedded DAG diagram. Just evaluate `result` in a cell.

## Which execution mode should I use?

```d2
direction: down

start: What does your pipeline need? {
  style.font-size: 16
}

q1: Replace all data\neach run?
q2: Append new rows?
q3: Update existing +\ninsert new?
q4: Track changes\nover time?
q5: Process only\nnew data?

overwrite: full + overwrite {
  style.fill: "#E3F2FD"
  style.font-size: 14
}
append: incremental + append {
  style.fill: "#E8F5E9"
  style.font-size: 14
}
merge: full + merge {
  style.fill: "#FFF3E0"
  style.font-size: 14
}
cdc: cdc + merge {
  style.fill: "#F3E5F5"
  style.font-size: 14
}
watermark: incremental_watermark\n+ merge {
  style.fill: "#E0F2F1"
  style.font-size: 14
}

start -> q1: yes
start -> q2
start -> q3
q1 -> overwrite
q2 -> append
q3 -> q4
q3 -> q5
q3 -> merge: simple upsert
q4 -> cdc
q5 -> watermark
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
