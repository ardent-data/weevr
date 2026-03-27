# Thread YAML Schema

A thread is the smallest unit of work in weevr. It defines one or more data
sources, an ordered sequence of transformation steps, and a single output
target. This page documents every key accepted inside a thread YAML file.

---

## Top-level keys

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `config_version` | `string` | yes | -- | Schema version identifier (e.g. `"1"`) |
| `name` | `string` | no | `""` | Human-readable thread name. Typically set by the weave that references this thread. |
| `sources` | `dict[string, Source]` | yes | -- | Named data sources keyed by alias |
| `steps` | `list[Step]` | no | `[]` | Ordered transformation pipeline |
| `target` | `Target` | yes | -- | Output destination |
| `write` | `WriteConfig` | no | `null` | Write mode and merge settings |
| `keys` | `KeyConfig` | no | `null` | Business key, surrogate key, and change detection |
| `validations` | `list[ValidationRule]` | no | `null` | Pre-write data quality rules |
| `assertions` | `list[Assertion]` | no | `null` | Post-execution assertions on the target |
| `load` | `LoadConfig` | no | `null` | Incremental load and watermark settings |
| `tags` | `list[string]` | no | `null` | Free-form tags for filtering and organization |
| `params` | `dict[string, ParamSpec]` | no | `null` | Typed parameter declarations |
| `defaults` | `dict[string, any]` | no | `null` | Default values inherited by nested structures |
| `failure` | `FailureConfig` | no | `null` | Per-thread failure handling policy |
| `execution` | `ExecutionConfig` | no | `null` | Runtime settings (logging, tracing) |
| `cache` | `bool` | no | `null` | Whether to cache the final DataFrame before writing |
| `exports` | `list[Export]` | no | `null` | Secondary output destinations. See [Exports guide](../../guides/exports.md). |
| `lookups` | `dict[string, Lookup]` | no | `null` | Thread-level lookup definitions. Merged with weave-level lookups (thread wins on name collision). |
| `column_sets` | `dict[string, ColumnSet]` | no | `null` | Thread-level column set definitions for rename steps. Merged with weave-level column sets (thread wins). |
| `variables` | `dict[string, VariableSpec]` | no | `null` | Thread-level variable declarations. Resolved before `pre_steps` execution. |
| `audit_templates` | `dict[string, dict[string, string]]` | no | `null` | Named audit column templates available to targets in this thread. Merged with parent-level definitions (thread wins on name collision). |
| `pre_steps` | `list[HookStep]` | no | `null` | Hook steps to run before thread core execution. Not cascaded from weave — each level runs its own list. |
| `post_steps` | `list[HookStep]` | no | `null` | Hook steps to run after thread core execution. Not cascaded from weave. |

---

## sources

Each key in `sources` is a logical alias used to reference the source in
subsequent steps (e.g. in join, union). The value is a `Source` object.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `type` | `string` | conditional | -- | Source type: `"delta"`, `"csv"`, `"json"`, `"parquet"`, `"excel"`. Required when `lookup` is not set. |
| `lookup` | `string` | conditional | `null` | Weave-level lookup name. Mutually exclusive with `type`. When set, the source is resolved from the weave's `lookups` map at execution time. |
| `alias` | `string` | no | `null` | Lakehouse table alias. Required when `type` is `"delta"`. |
| `path` | `string` | no | `null` | File path. Required for file-based types (`csv`, `json`, `parquet`, `excel`). |
| `options` | `dict[string, any]` | no | `{}` | Reader options passed to Spark (e.g. `header`, `delimiter`) |
| `dedup` | `DedupConfig` | no | `null` | Deduplication applied immediately after reading |

### sources.dedup

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `keys` | `list[string]` | yes | -- | Columns used for deduplication grouping |
| `order_by` | `string` | no | `null` | Sort expression to determine which row to keep |

```yaml
sources:
  orders:
    type: delta
    alias: raw_orders
    dedup:
      keys: [order_id]
      order_by: "updated_at DESC"
  regions:
    type: csv
    path: /mnt/data/regions.csv
    options:
      header: "true"
      delimiter: ","
  products:
    lookup: dim_product   # resolved from the weave's lookups map
```

---

## steps

An ordered list of transformation steps. Each step is a single-key object
where the key identifies the step type. weevr supports 19 step types.

### filter

Filter rows using a Spark SQL expression.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `expr` | `SparkExpr` | yes | -- | Boolean Spark SQL expression |

```yaml
- filter:
    expr: "amount > 0 AND status != 'cancelled'"
```

### derive

Create one or more new columns from Spark SQL expressions.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | `dict[string, SparkExpr]` | yes | -- | Map of output column name to expression |

```yaml
- derive:
    columns:
      total_amount: "quantity * unit_price"
      order_year: "year(order_date)"
```

### join

Join the current DataFrame with another named source.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `source` | `string` | yes | -- | Name of the source to join with |
| `type` | `string` | no | `"inner"` | Join type: `inner`, `left`, `right`, `full`, `cross`, `semi`, `anti` |
| `on` | `list[string or JoinKeyPair]` | yes | -- | Join keys. Strings are treated as same-name keys on both sides. |
| `null_safe` | `bool` | no | `true` | Use null-safe equality for join conditions |

A `JoinKeyPair` has `left` and `right` fields for asymmetric key names.

```yaml
- join:
    source: regions
    type: left
    on:
      - region_id
      - { left: src_code, right: region_code }
```

### select

Keep only the listed columns.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | `list[string]` | yes | -- | Column names to retain |

```yaml
- select:
    columns: [order_id, customer_id, total_amount, region_name]
```

### drop

Remove columns from the DataFrame.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | `list[string]` | yes | -- | Column names to drop |

```yaml
- drop:
    columns: [temp_flag, _raw_payload]
```

### rename

Rename columns. Supports both static mappings and named column set
references. When both `column_set` and `columns` are specified,
static entries in `columns` override matching column set entries.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | `dict[string, string]` | no | `{}` | Map of old name to new name |
| `column_set` | `string` | no | `null` | Name of a column set defined at weave or loom level |

```yaml
# Static rename only
- rename:
    columns:
      cust_id: customer_id
      amt: amount

# Column set with static overrides
- rename:
    column_set: sap_dictionary
    columns:
      MANUAL_COL: manual_override  # static wins
```

### cast

Change column data types.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | `dict[string, string]` | yes | -- | Map of column name to Spark data type |

```yaml
- cast:
    columns:
      amount: decimal(18,2)
      order_date: date
```

### dedup

Deduplicate rows by key columns.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `keys` | `list[string]` | yes | -- | Columns to group by for deduplication |
| `order_by` | `string` | no | `null` | Sort expression to determine row ordering |
| `keep` | `string` | no | `"last"` | Which row to keep: `"first"` or `"last"` |

```yaml
- dedup:
    keys: [order_id]
    order_by: "updated_at"
    keep: last
```

### sort

Sort rows.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | `list[string]` | yes | -- | Columns to sort by |
| `ascending` | `bool` | no | `true` | Sort direction |

```yaml
- sort:
    columns: [region_name, order_date]
    ascending: false
```

### union

Union the current DataFrame with one or more other sources.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `sources` | `list[string]` | yes | -- | Source aliases to union with |
| `mode` | `string` | no | `"by_name"` | Union mode: `"by_name"` or `"by_position"` |
| `allow_missing` | `bool` | no | `false` | Allow missing columns (filled with nulls) in `by_name` mode |

```yaml
- union:
    sources: [archived_orders, pending_orders]
    mode: by_name
    allow_missing: true
```

### aggregate

Group and aggregate rows.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `group_by` | `list[string]` | no | `null` | Grouping columns. Omit for whole-DataFrame aggregation. |
| `measures` | `dict[string, SparkExpr]` | yes | -- | Map of output alias to aggregate expression. Must not be empty. |

```yaml
- aggregate:
    group_by: [region, product_category]
    measures:
      total_revenue: "sum(amount)"
      order_count: "count(*)"
      avg_price: "avg(unit_price)"
```

### window

Apply window functions over a partition specification.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `functions` | `dict[string, SparkExpr]` | yes | -- | Map of output column to window expression. Must not be empty. |
| `partition_by` | `list[string]` | yes | -- | Partition columns. Must not be empty. |
| `order_by` | `list[string]` | no | `null` | Ordering within each partition |
| `frame` | `WindowFrame` | no | `null` | Frame specification |

A `WindowFrame` object has:

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `type` | `string` | yes | -- | `"rows"` or `"range"` |
| `start` | `int` | yes | -- | Frame start offset (use negative for preceding) |
| `end` | `int` | yes | -- | Frame end offset. Must be >= `start`. |

```yaml
- window:
    functions:
      row_num: "row_number()"
      running_total: "sum(amount)"
    partition_by: [customer_id]
    order_by: [order_date]
    frame:
      type: rows
      start: -2
      end: 0
```

### pivot

Pivot rows into columns.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `group_by` | `list[string]` | yes | -- | Grouping columns |
| `pivot_column` | `string` | yes | -- | Column whose distinct values become output columns |
| `values` | `list[string\|int\|float\|bool]` | yes | -- | Explicit pivot values. Must not be empty. |
| `aggregate` | `SparkExpr` | yes | -- | Aggregate expression applied per cell |

```yaml
- pivot:
    group_by: [customer_id]
    pivot_column: quarter
    values: ["Q1", "Q2", "Q3", "Q4"]
    aggregate: "sum(revenue)"
```

### unpivot

Unpivot columns into rows.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | `list[string]` | yes | -- | Columns to unpivot. Must not be empty. |
| `name_column` | `string` | yes | -- | Name for the output column containing original column names |
| `value_column` | `string` | yes | -- | Name for the output column containing values. Must differ from `name_column`. |

```yaml
- unpivot:
    columns: [jan_sales, feb_sales, mar_sales]
    name_column: month
    value_column: sales_amount
```

### case_when

Create a column with conditional logic (SQL CASE WHEN equivalent).

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `column` | `string` | yes | -- | Output column name |
| `cases` | `list[CaseWhenBranch]` | yes | -- | Ordered list of when/then pairs. Must not be empty. |
| `otherwise` | `SparkExpr` | no | `null` | Default value when no condition matches |

Each `CaseWhenBranch` has:

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `when` | `SparkExpr` | yes | -- | Boolean condition |
| `then` | `SparkExpr` | yes | -- | Value when condition is true |

```yaml
- case_when:
    column: customer_tier
    cases:
      - when: "lifetime_value > 10000"
        then: "'platinum'"
      - when: "lifetime_value > 5000"
        then: "'gold'"
      - when: "lifetime_value > 1000"
        then: "'silver'"
    otherwise: "'bronze'"
```

### fill_null

Replace null values in specified columns.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | `dict[string, any]` | yes | -- | Map of column name to fill value. Must not be empty. |

```yaml
- fill_null:
    columns:
      discount: 0
      notes: "N/A"
      is_active: true
```

### coalesce

Return the first non-null value from an ordered list of source columns.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | `dict[string, list[string]]` | yes | -- | Map of output column to ordered list of source columns. Both the map and each source list must not be empty. |

```yaml
- coalesce:
    columns:
      email: [work_email, personal_email, backup_email]
      phone: [mobile, home_phone]
```

### string_ops

Apply a Spark SQL expression template across selected string columns.
The `{col}` placeholder is replaced with each column name.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | `list[string]` | yes | -- | Columns to transform |
| `expr` | `string` | yes | -- | Expression template containing `{col}` placeholder |
| `on_empty` | `string` | no | `"warn"` | Behavior when column list is empty: `"warn"` or `"error"` |

```yaml
- string_ops:
    columns: [first_name, last_name, city]
    expr: "trim(upper({col}))"
```

### date_ops

Apply a Spark SQL expression template across selected date or timestamp
columns. The `{col}` placeholder is replaced with each column name.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | `list[string]` | yes | -- | Columns to transform |
| `expr` | `string` | yes | -- | Expression template containing `{col}` placeholder |
| `on_empty` | `string` | no | `"warn"` | Behavior when column list is empty: `"warn"` or `"error"` |

```yaml
- date_ops:
    columns: [created_at, updated_at]
    expr: "to_date({col})"
```

---

## target

Defines where the thread writes its output.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `alias` | `string` | no | `null` | Lakehouse table alias for Delta targets |
| `path` | `string` | no | `null` | File path for file-based targets |
| `mapping_mode` | `string` | no | `"auto"` | Column mapping: `"auto"` (pass-through) or `"explicit"` (only mapped columns) |
| `columns` | `dict[string, ColumnMapping]` | no | `null` | Per-column mapping specifications |
| `partition_by` | `list[string]` | no | `null` | Partition columns for the output table |
| `audit_columns` | `dict[string, string]` | no | `null` | Audit column definitions as name-expression pairs |
| `audit_template` | `string or list[string]` | no | `null` | Named template(s) to apply. Resolved from `audit_templates` at any level or from built-in presets (`fabric`, `minimal`). See [Audit Templates guide](../../guides/audit-templates.md). |
| `audit_template_inherit` | `bool` | no | `true` | When `false`, suppresses any `audit_template` inherited from parent levels. Direct `audit_template` on this target still applies. |
| `audit_columns_exclude` | `list[string]` | no | `null` | Column names or glob patterns to exclude from the resolved template set. |
| `naming` | `NamingConfig` | no | `null` | Column and table naming normalization |

### target.columns (ColumnMapping)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `expr` | `SparkExpr` | no | `null` | Spark SQL expression for the column value. Mutually exclusive with `drop`. |
| `type` | `string` | no | `null` | Target data type (cast applied on write) |
| `default` | `any` | no | `null` | Default value when the source column is null |
| `drop` | `bool` | no | `false` | Drop this column from output. Mutually exclusive with `expr`. |

### target.naming (NamingConfig)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | `NamingPattern` | no | `null` | Pattern for column names |
| `tables` | `NamingPattern` | no | `null` | Pattern for table names |
| `exclude` | `list[string]` | no | `[]` | Column names or glob patterns to exclude from normalization |
| `on_collision` | `string` | no | `"error"` | Behavior when normalization produces duplicate names: `"error"` or `"suffix"` |
| `reserved_words` | `ReservedWordConfig` | no | `null` | Reserved word protection. See [Weave schema: naming](../yaml-schema/weave.md#naming-namingconfig) for details. |

Supported patterns: `snake_case`, `camelCase`, `PascalCase`,
`UPPER_SNAKE_CASE`, `Title_Snake_Case`, `Title Case`, `lowercase`,
`UPPERCASE`, `kebab-case`, `none`.

```yaml
target:
  alias: curated_orders
  partition_by: [order_year]
  mapping_mode: auto
  columns:
    total_amount:
      type: "decimal(18,2)"
    _internal_flag:
      drop: true
  naming:
    columns: snake_case
```

---

## write

Controls how data is written to the target.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `mode` | `string` | no | `"overwrite"` | Write mode: `"overwrite"`, `"append"`, `"merge"` |
| `match_keys` | `list[string]` | no | `null` | Match keys for merge mode. Required when `mode` is `"merge"`. |
| `on_match` | `string` | no | `"update"` | Merge behavior when a match is found: `"update"` or `"ignore"` |
| `on_no_match_target` | `string` | no | `"insert"` | Merge behavior for new source rows: `"insert"` or `"ignore"` |
| `on_no_match_source` | `string` | no | `"ignore"` | Merge behavior for missing source rows: `"delete"`, `"soft_delete"`, `"ignore"` |
| `soft_delete_column` | `string` | no | `null` | Column to flag soft deletes. Required when `on_no_match_source` is `"soft_delete"`. |
| `soft_delete_value` | `bool` | no | `true` | Value written to the soft delete column |

```yaml
write:
  mode: merge
  match_keys: [order_id]
  on_match: update
  on_no_match_target: insert
  on_no_match_source: soft_delete
  soft_delete_column: is_deleted
  soft_delete_value: true
```

---

## keys

Key management for business keys, surrogate keys, and change detection hashes.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `business_key` | `list[string]` | no | `null` | Columns forming the natural business key |
| `surrogate_key` | `SurrogateKeyConfig` | no | `null` | Surrogate key generation settings |
| `change_detection` | `ChangeDetectionConfig` | no | `null` | Change detection hash settings |

### keys.surrogate_key

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | `string` | yes | -- | Output column name for the surrogate key |
| `algorithm` | `string` | no | `"sha256"` | Hash algorithm: `"xxhash64"`, `"sha1"`, `"sha256"`, `"sha384"`, `"sha512"`, `"md5"`, `"crc32"`, `"murmur3"` |
| `output` | `string` | no | `"native"` | Output type for integer-returning algorithms (xxhash64, crc32, murmur3). `"native"` preserves the algorithm's return type; `"string"` casts to StringType. Has no effect on sha*/md5. |

!!! note "Algorithm choice"
    `crc32` has a small 32-bit output space with higher collision risk.
    `murmur3` (Spark's `hash()`) may produce different results across
    Spark major versions. Neither is recommended for high-cardinality
    surrogate key use cases.

### keys.change_detection

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | `string` | yes | -- | Output column name for the change hash |
| `columns` | `list[string]` | yes | -- | Columns included in the hash |
| `algorithm` | `string` | no | `"md5"` | Hash algorithm: `"xxhash64"`, `"sha1"`, `"sha256"`, `"sha384"`, `"sha512"`, `"md5"`, `"crc32"`, `"murmur3"` |
| `output` | `string` | no | `"native"` | Output type for integer-returning algorithms. `"native"` preserves the algorithm's return type; `"string"` casts to StringType. |

```yaml
keys:
  business_key: [order_id]
  surrogate_key:
    name: sk_order
    algorithm: sha256
  change_detection:
    name: change_hash
    columns: [status, amount, updated_at]
    algorithm: md5
```

---

## load

Incremental load configuration with watermark tracking.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `mode` | `string` | no | `"full"` | Load mode: `"full"`, `"incremental_watermark"`, `"incremental_parameter"`, `"cdc"` |
| `watermark_column` | `string` | no | `null` | Column for watermark comparison. Required for `incremental_watermark`. Must not be set for `cdc`. |
| `watermark_type` | `string` | no | `null` | Data type of the watermark column: `"timestamp"`, `"date"`, `"int"`, `"long"` |
| `watermark_inclusive` | `bool` | no | `false` | Include rows equal to the last watermark value (use with merge/overwrite for idempotency) |
| `watermark_store` | `WatermarkStoreConfig` | no | `null` | Watermark persistence backend |
| `cdc` | `CdcConfig` | no | `null` | CDC configuration. Required when `mode` is `"cdc"`. |

### load.watermark_store

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `type` | `string` | no | `"table_properties"` | Store type: `"table_properties"` (zero-config) or `"metadata_table"` |
| `table_path` | `string` | no | `null` | Path to metadata table. Required for `"metadata_table"` type. |

### load.cdc

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `preset` | `string` | no | `null` | Preset name: `"delta_cdf"`. Mutually exclusive with `operation_column`. |
| `operation_column` | `string` | no | `null` | Column containing operation type. Mutually exclusive with `preset`. |
| `insert_value` | `string` | no | `null` | Value indicating an insert operation |
| `update_value` | `string` | no | `null` | Value indicating an update operation |
| `delete_value` | `string` | no | `null` | Value indicating a delete operation |
| `on_delete` | `string` | no | `"hard_delete"` | Delete behavior: `"hard_delete"` or `"soft_delete"` |

Either `preset` or `operation_column` must be set (but not both). When using
explicit mapping, at least `insert_value` or `update_value` is required.

```yaml
load:
  mode: incremental_watermark
  watermark_column: updated_at
  watermark_type: timestamp
  watermark_store:
    type: table_properties
```

```yaml
load:
  mode: cdc
  cdc:
    preset: delta_cdf
```

---

## validations

Pre-write data quality rules. Each rule is evaluated as a Spark SQL boolean
expression against the DataFrame before it is written.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | `string` | yes | -- | Human-readable rule name |
| `rule` | `SparkExpr` | yes | -- | Spark SQL boolean expression |
| `severity` | `string` | no | `"error"` | Severity level: `"info"`, `"warn"`, `"error"`, `"fatal"` |

```yaml
validations:
  - name: positive_amount
    rule: "amount > 0"
    severity: error
  - name: valid_status
    rule: "status IN ('active', 'inactive', 'pending')"
    severity: warn
```

---

## assertions

Post-execution assertions evaluated against the target dataset after writing.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `type` | `string` | yes | -- | Assertion type: `"row_count"`, `"column_not_null"`, `"unique"`, `"expression"` |
| `severity` | `string` | no | `"warn"` | Severity level: `"info"`, `"warn"`, `"error"`, `"fatal"` |
| `columns` | `list[string]` | no | `null` | Columns for `column_not_null` and `unique` assertions |
| `min` | `int` | no | `null` | Minimum value for `row_count` assertions |
| `max` | `int` | no | `null` | Maximum value for `row_count` assertions |
| `expression` | `SparkExpr` | no | `null` | Spark SQL expression for `expression` assertions |

```yaml
assertions:
  - type: row_count
    min: 1
    severity: error
  - type: column_not_null
    columns: [order_id, customer_id]
    severity: fatal
  - type: unique
    columns: [order_id]
  - type: expression
    expression: "count(CASE WHEN amount < 0 THEN 1 END) = 0"
    severity: warn
```

---

## failure

Per-thread failure handling policy. Controls what happens to remaining threads
in a weave when this thread fails.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `on_failure` | `string` | no | `"abort_weave"` | Policy: `"abort_weave"`, `"skip_downstream"`, `"continue"` |

- `abort_weave` -- Stop all remaining threads in the weave immediately.
- `skip_downstream` -- Skip only threads that depend on the failed thread.
- `continue` -- Continue executing unrelated threads.

```yaml
failure:
  on_failure: skip_downstream
```

---

## execution

Runtime execution settings. These cascade from loom to weave to thread, with
the most specific level winning.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `log_level` | `string` | no | `"standard"` | Logging verbosity: `"minimal"`, `"standard"`, `"verbose"`, `"debug"` |
| `trace` | `bool` | no | `true` | Collect execution spans for telemetry |

```yaml
execution:
  log_level: verbose
  trace: true
```

---

## params

Typed parameter declarations. Parameters can be referenced in expressions using
`${param.name}` syntax.

Each entry maps a parameter name to a `ParamSpec`:

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | `string` | yes | -- | Parameter name |
| `type` | `string` | yes | -- | Data type: `"string"`, `"int"`, `"float"`, `"bool"`, `"date"`, `"timestamp"`, `"list[string]"` |
| `required` | `bool` | no | `true` | Whether the parameter must be supplied at runtime |
| `default` | `any` | no | `null` | Default value when not supplied |
| `description` | `string` | no | `""` | Human-readable description |

```yaml
params:
  start_date:
    name: start_date
    type: date
    required: true
    description: "Start of the reporting window"
  region_filter:
    name: region_filter
    type: string
    required: false
    default: "ALL"
```

---

## lookups

Thread-level lookup definitions. The structure is identical to
[weave-level lookups](weave.md#lookups). When a thread and its parent weave
define a lookup with the same name, the thread-level definition wins.

```yaml
lookups:
  dim_region:
    source:
      type: delta
      alias: ref.dim_region
    key: [region_id]
    values: [region_name, country_code]
    materialize: true
    strategy: broadcast
```

---

## column_sets

Thread-level column set definitions for rename steps. The structure is
identical to [weave-level column_sets](weave.md#column_sets). When the same
name is defined at both thread and weave level, the thread-level definition
wins.

```yaml
column_sets:
  local_overrides:
    source:
      type: yaml
      path: mappings/thread_overrides.yaml
    on_unmapped: pass_through
```

---

## variables

Thread-level typed variable declarations. Variables can be set by
`sql_statement` hook steps via `set_var` and referenced in downstream config
as `${var.name}`. The structure is identical to
[weave-level variables](weave.md#variables-variablespec).

```yaml
variables:
  row_count:
    type: int
    default: 0
```

---

## pre_steps / post_steps

Hook steps that run before or after the thread's core execution. The
structure is identical to
[weave-level pre_steps / post_steps](weave.md#pre_steps-post_steps-hookstep).

Thread-level hook lists are **not** cascaded from the parent weave. Each
level runs its own list independently. `pre_steps` run before any source is
read; `post_steps` run after write, assertions, and exports.

```yaml
pre_steps:
  - type: quality_gate
    name: check_source_freshness
    check: source_freshness
    source: raw.orders
    max_age: "4h"

post_steps:
  - type: log_message
    message: "Thread complete. Rows written: ${var.row_count}."
    level: info
```

---

## Complete example

```yaml
config_version: "1"

sources:
  orders:
    type: delta
    alias: raw_orders
    dedup:
      keys: [order_id]
      order_by: "updated_at DESC"
  customers:
    type: delta
    alias: dim_customers

steps:
  - filter:
      expr: "status != 'cancelled'"
  - join:
      source: customers
      type: left
      on: [customer_id]
  - derive:
      columns:
        total_amount: "quantity * unit_price"
        order_year: "year(order_date)"
  - select:
      columns:
        - order_id
        - customer_id
        - customer_name
        - total_amount
        - order_year
        - updated_at

target:
  alias: curated_orders
  partition_by: [order_year]
  naming:
    columns: snake_case

write:
  mode: merge
  match_keys: [order_id]
  on_match: update
  on_no_match_target: insert

keys:
  business_key: [order_id]
  surrogate_key:
    name: sk_order
  change_detection:
    name: change_hash
    columns: [total_amount, customer_name]

load:
  mode: incremental_watermark
  watermark_column: updated_at
  watermark_type: timestamp

validations:
  - name: positive_total
    rule: "total_amount >= 0"
    severity: error

assertions:
  - type: row_count
    min: 1
    severity: error
  - type: unique
    columns: [order_id]

failure:
  on_failure: skip_downstream

execution:
  log_level: standard
  trace: true

tags: [orders, curated]
```
