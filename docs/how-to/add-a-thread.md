# Add a Thread

**Goal:** Define a thread YAML that reads from one or more sources, applies
transformation steps, and writes to a Delta target. Then wire it into a weave
and verify the output.

```d2
direction: right

read: Read Sources {
  style.fill: "#E3F2FD"
}
transform: Transform Steps {
  style.fill: "#E8F5E9"
}
validate: Validate Rules {
  style.fill: "#FFF3E0"
}
map: Map Columns {
  style.fill: "#F3E5F5"
}
write: Write Target {
  style.fill: "#E0F2F1"
}
assert: Assert {
  style.fill: "#FCE4EC"
}

read -> transform: DataFrame
transform -> validate: shaped data
validate -> map: clean rows
map -> write: final columns
write -> assert: post-write

quarantine: Quarantine {
  style.fill: "#FFEBEE"
  style.stroke-dash: 3
}
validate -> quarantine: failed rows {style.stroke-dash: 3}
```

## Prerequisites

- A working weevr project with the [conventional directory structure](../tutorials/your-first-loom.md#project-structure)
- At least one weave to add the thread to
- Access to the source data (Delta table, CSV, Parquet, or other Spark-readable
  format)

## Step 1 -- Define sources

Declare each data source your thread needs. The first source listed becomes the
primary DataFrame that flows through the pipeline.

```yaml
config_version: "1.0"

sources:
  orders:
    type: csv
    path: data/orders.csv
    options:
      header: "true"
      delimiter: ","
  customers:
    type: delta
    alias: bronze.customers
```

Source types include `delta`, `csv`, `json`, `parquet`, and `excel`. Delta
sources use `alias` for table resolution; file sources use `path`.

## Step 2 -- Define transformation steps

Steps execute top-to-bottom. Each step has a single key indicating the
operation type:

```yaml
steps:
  - filter:
      expr: "order_total > 0"
  - join:
      source: customers
      type: left
      on:
        - customer_id
  - derive:
      columns:
        order_year: "year(order_date)"
        customer_name: "concat(first_name, ' ', last_name)"
  - rename:
      columns:
        order_total: total_amount
  - cast:
      columns:
        total_amount: "decimal(10,2)"
        order_year: "int"
  - select:
      columns:
        - order_id
        - customer_id
        - customer_name
        - total_amount
        - order_year
```

Available step types include `filter`, `derive`, `join`, `select`,
`drop`, `rename`, `cast`, `dedup`, `sort`, `union`, `aggregate`,
`window`, `pivot`, `unpivot`, `case_when`, `fill_null`, `coalesce`,
`string_ops`, `date_ops`, `concat`, `map`, `format`, and `resolve`.
See the [thread schema reference](../reference/yaml-schema/thread.md)
for the full list.

## Step 3 -- Define the target

Set the write destination. Use `mapping_mode: auto` (the default) to let
matching column names flow through automatically:

```yaml
target:
  path: Tables/fact_orders
  mapping_mode: auto
```

For explicit control over every column, use `mapping_mode: explicit` with a
`columns` map.

## Step 4 -- Configure write behavior

Specify how data lands in the target:

```yaml
write:
  mode: overwrite
```

Available modes: `overwrite` (replace all data), `append` (add rows), and
`merge` (upsert). Merge mode requires `match_keys` and supports `on_match`,
`on_no_match_target`, and `on_no_match_source` options.

## Step 5 -- Add the thread to a weave

Reference your new thread in an existing or new weave config. Thread references
use paths relative to the project root, with typed extensions:

```yaml
# orders.weave
config_version: "1.0"

threads:
  - ref: orders/fact_orders.thread
```

This resolves to the `orders/fact_orders.thread` file in the project directory.

## Step 6 -- Run and verify

Execute the weave and check the result:

```python
from weevr import Context

ctx = Context(spark, "my-project.weevr")
result = ctx.run("orders.weave")

assert result.status == "success"
print(result.summary())
```

Use `mode="preview"` to test transforms against sampled data without writing:

```python
result = ctx.run("orders.weave", mode="preview")
result.preview_data["fact_orders"].show()
```

## Putting it together

Combine the sections above into a single file at
`orders/fact_orders.thread`. The complete structure is:
`config_version` at the top, then `sources`, `steps`, `target`, and `write`.
