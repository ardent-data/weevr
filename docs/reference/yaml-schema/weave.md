# Weave YAML Schema

A weave groups a collection of threads into a dependency graph. It defines
which threads run, their execution order, optional shared defaults, and
runtime settings.

---

## Top-level keys

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `config_version` | `string` | yes | -- | Schema version identifier (e.g. `"1.0"`) |
| `name` | `string` | no | `""` | Human-readable weave name |
| `threads` | `list[ThreadEntry or string]` | yes | -- | Thread references. Strings are shorthand for `{ name: "<value>" }` (local name references). Use `ref` for external file references. |
| `lookups` | `dict[string, Lookup]` | no | `null` | Named lookup sources shared across threads in this weave |
| `column_sets` | `dict[string, ColumnSet]` | no | `null` | Named column sets for bulk rename from external dictionaries. Shared across threads in this weave. |
| `variables` | `dict[string, VariableSpec]` | no | `null` | Weave-scoped typed variables, settable by hook steps |
| `pre_steps` | `list[HookStep]` | no | `null` | Hook steps executed before any thread runs |
| `post_steps` | `list[HookStep]` | no | `null` | Hook steps executed after all threads complete |
| `defaults` | `dict[string, any]` | no | `null` | Default values cascaded into every thread in this weave. `audit_columns` and `exports` use additive merge (see [Exports guide](../../guides/exports.md)). |
| `params` | `dict[string, ParamSpec]` | no | `null` | Typed parameter declarations scoped to this weave |
| `execution` | `ExecutionConfig` | no | `null` | Runtime settings (logging, tracing) cascaded to threads |
| `naming` | `NamingConfig` | no | `null` | Naming normalization cascaded to threads |

---

## threads (ThreadEntry)

Each entry in the `threads` list is either a plain string (shorthand) or a
`ThreadEntry` object. Use `ref` to reference an external `.thread` file, or
`name` for inline thread definitions within the weave.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `ref` | `string` | no | `null` | Path to an external `.thread` file, relative to the project root. Mutually exclusive with inline `name`. |
| `name` | `string` | no | `""` | Thread name. Required for inline definitions; derived from filename stem when using `ref`. |
| `dependencies` | `list[string]` | no | `null` | Explicit upstream thread names. Merged with auto-inferred dependencies from source/target path matching. |
| `condition` | `ConditionSpec` | no | `null` | Conditional execution gate |

### threads.condition (ConditionSpec)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `when` | `string` | yes | -- | Condition expression. Supports `${param.x}` references, `table_exists()`, `table_empty()`, `row_count()`, and boolean operators. |

---

## lookups

Named lookup data sources shared across threads. Each key is a lookup name
that threads can reference via `source.lookup` instead of declaring a direct
source type.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `source` | `Source` | yes | -- | Source definition for the lookup data |
| `materialize` | `bool` | no | `false` | Pre-read and cache/broadcast the data before thread execution |
| `strategy` | `string` | no | `"cache"` | Materialization strategy: `"broadcast"` (Spark broadcast join hints) or `"cache"` (DataFrame caching). Only meaningful when `materialize` is true. |
| `key` | `list[string]` | no | `null` | Column(s) used for matching (join key). When set with `values`, only these columns plus `values` columns are retained. |
| `values` | `list[string]` | no | `null` | Payload column(s) to retrieve. Requires `key` to be set. Must not overlap with `key`. |
| `filter` | `string` | no | `null` | SQL WHERE expression applied to the source before projection |
| `unique_key` | `bool` | no | `false` | Validate that `key` columns form a unique key after filtering and projection |
| `on_failure` | `string` | no | `"abort"` | Behavior when `unique_key` check finds duplicates: `"abort"` or `"warn"`. Only valid when `unique_key` is true. |

```yaml
lookups:
  dim_product:
    source:
      type: delta
      alias: silver.dim_product
    materialize: true
    strategy: cache
    key: [product_id]
    values: [product_name, category]
    unique_key: true

  exchange_rates:
    source:
      type: delta
      alias: ref.exchange_rates
    filter: "effective_date = current_date()"
    materialize: true
    strategy: broadcast
```

Threads reference lookups via the `lookup` field on a source:

```yaml
# In a thread's sources block
sources:
  products:
    lookup: dim_product   # resolved from the weave's lookups map
```

---

## column_sets

Named column sets define externally-sourced column mappings for bulk
rename operations. Each key is a column set name that rename steps can
reference via `column_set: <name>`.

Column sets can be sourced from a Delta table, a YAML file, or a
runtime parameter. A column set must have exactly one of `source` or
`param` — not both.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `source` | `ColumnSetSource` | no | `null` | External source for the column mapping (Delta or YAML) |
| `param` | `string` | no | `null` | Runtime parameter name containing a `dict[str, str]` mapping. Mutually exclusive with `source`. |
| `on_unmapped` | `string` | no | `"pass_through"` | Behavior when a DataFrame column has no rename entry: `"pass_through"` or `"error"` |
| `on_extra` | `string` | no | `"ignore"` | Behavior when a mapping key references a column not in the DataFrame: `"ignore"`, `"warn"`, or `"error"` |
| `on_failure` | `string` | no | `"abort"` | Behavior when the source cannot be read or returns no rows: `"abort"`, `"warn"`, or `"skip"` |

### column_sets.source (ColumnSetSource)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `type` | `string` | yes | -- | Source type: `"delta"` or `"yaml"` |
| `alias` | `string` | no | `null` | Lakehouse table alias (for Delta sources) |
| `path` | `string` | no | `null` | File path (for Delta path or YAML sources) |
| `from_column` | `string` | no | `"source_name"` | Column containing the original (raw) column names |
| `to_column` | `string` | no | `"target_name"` | Column containing the target (friendly) column names |
| `filter` | `string` | no | `null` | SQL WHERE expression applied before collecting mappings |

```yaml
column_sets:
  sap_dictionary:
    source:
      type: delta
      alias: ref.column_dictionary
      from_column: raw_name
      to_column: friendly_name
      filter: "system = 'SAP'"
    on_unmapped: pass_through
    on_extra: ignore
    on_failure: abort

  hr_mappings:
    source:
      type: yaml
      path: mappings/hr_columns.yaml

  runtime_overrides:
    param: column_overrides
```

Rename steps reference column sets by name:

```yaml
steps:
  - rename:
      column_set: sap_dictionary
      columns:
        MANUAL_COL: manual_override  # static wins
```

---

## pre_steps / post_steps (HookStep)

Hook steps that run before or after thread execution within a weave.
`pre_steps` run before any thread starts; `post_steps` run after all
threads complete.

Each hook step is a discriminated union with three types, identified by
the `type` field.

### Common fields

All hook step types share these fields:

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `type` | `string` | yes | -- | Step type: `"quality_gate"`, `"sql_statement"`, or `"log_message"` |
| `name` | `string` | no | `null` | Optional name for telemetry and diagnostics |
| `on_failure` | `string` | no | phase default | Failure behavior: `"abort"` or `"warn"`. Defaults to `"abort"` for pre_steps and `"warn"` for post_steps. |

### quality_gate

Runs a predefined quality check against a data source or target.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `check` | `string` | yes | -- | Check type: `"source_freshness"`, `"row_count_delta"`, `"row_count"`, `"table_exists"`, `"expression"` |
| `source` | `string` | conditional | `null` | Table alias. Required for `source_freshness` and `table_exists`. |
| `max_age` | `string` | conditional | `null` | Duration string (e.g. `"24h"`). Required for `source_freshness`. |
| `target` | `string` | conditional | `null` | Table alias. Required for `row_count_delta` and `row_count`. |
| `max_decrease_pct` | `float` | no | `null` | Max allowed decrease percentage for `row_count_delta` |
| `max_increase_pct` | `float` | no | `null` | Max allowed increase percentage for `row_count_delta` |
| `min_delta` | `int` | no | `null` | Minimum absolute row change for `row_count_delta` |
| `max_delta` | `int` | no | `null` | Maximum absolute row change for `row_count_delta` |
| `min_count` | `int` | conditional | `null` | Minimum row count for `row_count`. At least one of `min_count` or `max_count` required. |
| `max_count` | `int` | conditional | `null` | Maximum row count for `row_count` |
| `sql` | `string` | conditional | `null` | Spark SQL boolean expression for `expression` check. Required for `expression`. |
| `message` | `string` | no | `null` | Failure message for `expression` check diagnostics |

### sql_statement

Executes an arbitrary Spark SQL statement. Optionally captures the scalar
result into a weave-scoped variable.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `sql` | `string` | yes | -- | Spark SQL statement to execute |
| `set_var` | `string` | no | `null` | Variable name to capture the scalar result into. The variable must be declared in `variables`. |

### log_message

Emits a log message. Supports `${var.name}` placeholders resolved from
weave variables.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `message` | `string` | yes | -- | Message template. Supports `${var.name}` placeholders. |
| `level` | `string` | no | `"info"` | Log level: `"info"`, `"warn"`, `"error"` |

```yaml
pre_steps:
  - type: quality_gate
    name: check_source_freshness
    check: source_freshness
    source: raw.orders
    max_age: "24h"

  - type: sql_statement
    name: capture_baseline
    sql: "SELECT count(*) FROM silver.fact_orders"
    set_var: baseline_count

post_steps:
  - type: quality_gate
    name: check_row_count
    check: row_count
    target: silver.fact_orders
    min_count: 1

  - type: log_message
    message: "Pipeline complete. Baseline was ${var.baseline_count} rows."
    level: info
```

---

## variables (VariableSpec)

Typed scalar variables scoped to the weave. Variables can be set by
`sql_statement` hook steps via `set_var` and referenced in downstream
config as `${var.name}`.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `type` | `string` | yes | -- | Scalar type: `"string"`, `"int"`, `"long"`, `"float"`, `"double"`, `"boolean"`, `"timestamp"`, `"date"` |
| `default` | `string\|int\|float\|bool` | no | `null` | Default value used when no hook step sets the variable |

```yaml
variables:
  baseline_count:
    type: int
    default: 0
  run_label:
    type: string
    default: "scheduled"
```

---

## execution (ExecutionConfig)

Cascades to all threads within the weave. Thread-level settings override these.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `log_level` | `string` | no | `"standard"` | Logging verbosity: `"minimal"`, `"standard"`, `"verbose"`, `"debug"` |
| `trace` | `bool` | no | `true` | Collect execution spans for telemetry |

---

## naming (NamingConfig)

Cascades to all threads within the weave. Thread-level and target-level naming
settings override these.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | `NamingPattern` | no | `null` | Column naming pattern |
| `tables` | `NamingPattern` | no | `null` | Table naming pattern |
| `exclude` | `list[string]` | no | `[]` | Names or glob patterns excluded from normalization |
| `on_collision` | `string` | no | `"error"` | Behavior when normalization produces duplicate names: `"error"` or `"suffix"` (appends `_2`, `_3`, etc.) |
| `reserved_words` | `ReservedWordConfig` | no | `null` | Reserved word protection for output column names |

Supported patterns: `snake_case`, `camelCase`, `PascalCase`,
`UPPER_SNAKE_CASE`, `Title_Snake_Case`, `Title Case`, `lowercase`,
`UPPERCASE`, `kebab-case`, `none`.

!!! warning "kebab-case requires backtick-quoting"
    Column names with hyphens must be backtick-quoted in Spark SQL
    expressions (e.g., `` `my-column` ``). A validation warning is
    emitted when `kebab-case` is selected.

### naming.reserved_words (ReservedWordConfig)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `strategy` | `string` | no | `"quote"` | How to handle reserved words: `"prefix"`, `"quote"` (no-op — Spark backtick-quotes automatically), or `"error"` |
| `prefix` | `string` | no | `"_"` | Prefix prepended to reserved words when `strategy` is `"prefix"` |
| `extend` | `list[string]` | no | `[]` | Additional words to treat as reserved |
| `exclude` | `list[string]` | no | `[]` | Words to remove from the default reserved list |

---

## Complete example

```yaml
config_version: "1.0"
name: sales_pipeline

lookups:
  dim_product:
    source:
      type: delta
      alias: silver.dim_product
    materialize: true
    strategy: cache
    key: [product_id]
    values: [product_name, category]
    unique_key: true

column_sets:
  sap_dictionary:
    source:
      type: delta
      alias: ref.column_dictionary
      from_column: raw_name
      to_column: friendly_name

variables:
  baseline_count:
    type: int
    default: 0

pre_steps:
  - type: quality_gate
    name: check_orders_freshness
    check: source_freshness
    source: raw.orders
    max_age: "24h"
  - type: sql_statement
    name: capture_baseline
    sql: "SELECT count(*) FROM curated.order_summary"
    set_var: baseline_count

threads:
  - ref: staging/load_orders.thread
  - ref: staging/load_customers.thread
  - ref: curated/build_order_summary.thread
    dependencies: [load_orders, load_customers]
  - ref: curated/refresh_snapshot.thread
    dependencies: [build_order_summary]
    condition:
      when: "table_exists('curated.order_summary')"

post_steps:
  - type: quality_gate
    name: verify_row_count
    check: row_count
    target: curated.order_summary
    min_count: 1
  - type: log_message
    message: "Pipeline complete. Baseline was ${var.baseline_count} rows."
    level: info

defaults:
  write:
    mode: merge
  exports:
    - name: audit_archive
      type: parquet
      path: /archive/${thread.name}/${run.timestamp}/

params:
  run_date:
    name: run_date
    type: date
    required: true
    description: "Pipeline execution date"

execution:
  log_level: verbose
  trace: true

naming:
  columns: snake_case
  on_collision: suffix
  reserved_words:
    preset: [ansi, dax]
    strategy: prefix
    prefix: "_"
```

### Shorthand thread syntax

Thread entries can be plain strings for inline definitions that need no
overrides:

```yaml
threads:
  - build_snapshot
  - refresh_index
```

This is equivalent to:

```yaml
threads:
  - name: build_snapshot
  - name: refresh_index
```

For external file references, use `ref` explicitly:

```yaml
threads:
  - ref: staging/load_orders.thread
  - ref: staging/load_customers.thread
```
