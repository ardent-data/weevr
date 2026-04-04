# Thread YAML Schema

A thread is the smallest unit of work in weevr. It defines one or more data
sources, an ordered sequence of transformation steps, and a single output
target. This page documents every key accepted inside a thread YAML file.

Threads can be used as **parameterized templates** via `as` and `params` on
[ThreadEntry](weave.md#threads-threadentry) in a weave. See the
[Thread Templates guide](../../guides/thread-templates.md) for usage.

---

## Top-level keys

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `config_version` | `string` | yes | -- | Schema version identifier (e.g. `"1.0"`) |
| `name` | `string` | no | `""` | Human-readable thread name. Typically set by the weave that references this thread. |
| `sources` | `dict[string, Source]` | yes | -- | Named data sources keyed by alias. Must contain at least one entry. |
| `with` | `dict[string, SubPipeline]` | no | `null` | Named sub-pipelines (CTEs) resolved before the main steps pipeline. |
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
| `audit_templates` | `dict[string, AuditTemplate]` | no | `null` | Named audit column templates available to targets in this thread. Merged with parent-level definitions (thread wins on name collision). A flat `dict[string, string]` shorthand is also accepted. |
| `pre_steps` | `list[HookStep]` | no | `null` | Hook steps to run before thread core execution. Not cascaded from weave — each level runs its own list. |
| `post_steps` | `list[HookStep]` | no | `null` | Hook steps to run after thread core execution. Not cascaded from weave. |
| `connections` | `dict[string, OneLakeConnection]` | no | `null` | Named OneLake connection declarations. Merged with weave/loom-level connections (thread wins on name collision). See [Connections guide](../../guides/connections.md). |

---

## sources

Each key in `sources` is a logical alias used to reference the source in
subsequent steps (e.g. in join, union). The value is a `Source` object.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `type` | `string` | conditional | -- | Source type: `"delta"`, `"csv"`, `"json"`, `"parquet"`, `"excel"`, `"date_sequence"`, `"int_sequence"`. Required when neither `lookup` nor `connection` is set. |
| `lookup` | `string` | conditional | `null` | Lookup name. Mutually exclusive with `type`. Resolved from the nearest `lookups` map (thread, weave, or loom level) at execution time. |
| `alias` | `string` | conditional | `null` | Lakehouse table alias. Required when `type` is `"delta"` without a `connection`. Mutually exclusive with `connection`. |
| `path` | `string` | conditional | `null` | File path. Required for file-based types (`csv`, `json`, `parquet`, `excel`). |
| `options` | `dict[string, any]` | no | `{}` | Reader options passed to Spark (e.g. `header`, `delimiter`) |
| `dedup` | `DedupConfig` | no | `null` | Deduplication applied immediately after reading |
| `connection` | `string` | conditional | `null` | Name of a connection declared in `connections:`. Requires `table`. Mutually exclusive with `alias`. |
| `schema` | `string` | no | `null` | Schema override within the connection's lakehouse. Overrides `connection.default_schema`. |
| `table` | `string` | conditional | `null` | Table name within the connection's lakehouse. Required when `connection` is set. |
| `start` | `string` \| `int` | conditional | -- | Range start (inclusive). Required for generated types (`date_sequence`, `int_sequence`). |
| `end` | `string` \| `int` | conditional | -- | Range end (inclusive). Required for generated types (`date_sequence`, `int_sequence`). |
| `column` | `string` | conditional | -- | Output column name. Required for generated types (`date_sequence`, `int_sequence`). |
| `step` | `string` \| `int` | no | `"day"` / `1` | Step interval. For `date_sequence`: `"day"`, `"week"`, `"month"`, or `"year"`. For `int_sequence`: a positive integer. |

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

### Generated Sources

Generated sources produce synthetic data at runtime — no lakehouse read or file
path is needed. They are useful for building calendar spines, surrogate key
ranges, or any sequence-based reference table.

Two generated types are available: `date_sequence` and `int_sequence`. Both
produce a single-column DataFrame covering a closed range `[start, end]`.

**Mutual exclusivity:** generated types (`date_sequence`, `int_sequence`) are
incompatible with `alias`, `path`, `options`, `dedup`, `connection`, and
`lookup`. Setting any of those keys alongside a generated type raises a
validation error.

#### `date_sequence`

Produces a single `DateType` column containing one row per calendar interval
from `start` to `end` (both inclusive).

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `type` | `string` | yes | -- | Must be `"date_sequence"` |
| `start` | `string` | yes | -- | Start date, inclusive. ISO-8601 format (`"YYYY-MM-DD"`). |
| `end` | `string` | yes | -- | End date, inclusive. ISO-8601 format (`"YYYY-MM-DD"`). |
| `column` | `string` | yes | -- | Name of the output column. |
| `step` | `string` | no | `"day"` | Step interval: `"day"`, `"week"`, `"month"`, or `"year"`. |

```yaml
sources:
  calendar:
    type: date_sequence
    start: "2024-01-01"
    end: "2024-12-31"
    column: date
    step: day
```

#### `int_sequence`

Produces a single `LongType` column containing one row per integer from `start`
to `end` (both inclusive).

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `type` | `string` | yes | -- | Must be `"int_sequence"` |
| `start` | `int` | yes | -- | Start value, inclusive. |
| `end` | `int` | yes | -- | End value, inclusive. |
| `column` | `string` | yes | -- | Name of the output column. |
| `step` | `int` | no | `1` | Increment between values. Must be a positive integer. |

```yaml
sources:
  sequence:
    type: int_sequence
    start: 1
    end: 1000
    column: id
    step: 1
```

---

## with

Named sub-pipelines that produce intermediate DataFrames before the main
`steps` pipeline runs. Each entry defines a CTE-like named DataFrame that
downstream steps (and other CTEs) can reference as a source.

Each key in `with` is a logical CTE name. The value is a `SubPipeline`
object:

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `from` | `string` | yes | -- | Source alias or earlier CTE name to start the sub-pipeline from |
| `steps` | `list[Step]` | yes | -- | Ordered pipeline steps applied to the source DataFrame |

**Semantics:**

- Declaration order matters. A CTE may only reference sources or CTEs
  declared before it — forward references are not allowed.
- CTE names share the source namespace. A CTE name must not collide with
  any key in `sources`.
- CTEs can chain: a later CTE's `from:` may name an earlier CTE.
- Unused CTEs (not referenced by any join, union, or the main `steps`
  pipeline) produce a validation warning at parse time.

```yaml
sources:
  invoices:
    type: delta
    alias: raw.invoices
  invoice_lines:
    type: delta
    alias: raw.invoice_lines
  plants:
    type: delta
    alias: raw.plants

with:
  # Pre-aggregate invoice lines before joining to invoice header
  agg_lines:
    from: invoice_lines
    steps:
      - filter:
          expr: "line_status = 'POSTED'"
      - aggregate:
          group_by: [invoice_id]
          measures:
            net_amount: "sum(net_value)"
            line_count: "count(*)"

  # Enrich plants with region mapping
  plants_enriched:
    from: plants
    steps:
      - filter:
          expr: "plant_active = true"
      - derive:
          columns:
            region_code: "substring(plant_id, 1, 2)"

steps:
  - join:
      source: agg_lines        # reference the CTE above
      type: left
      on: [invoice_id]
  - join:
      source: plants_enriched  # reference the second CTE
      type: left
      on: [plant_id]
```

---

## steps

An ordered list of transformation steps. Each step is a single-key object
where the key identifies the step type. weevr supports 23 step types.
In addition, [`foreach`](#foreach) macro blocks are accepted and expanded
before model hydration.

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
| `alias` | `string` | no | `null` | DataFrame alias applied to the right-side source before joining. Resolves column name ambiguity via Spark `.alias()`. |
| `filter` | `string` | no | `null` | Spark SQL predicate applied to the right-side DataFrame before the join. Reduces shuffle size for selective joins. |
| `include` | `list[string]` | no | `null` | Glob patterns controlling which right-side columns survive into the result. Applied after duplicate key column removal. |
| `exclude` | `list[string]` | no | `null` | Glob patterns for right-side columns to drop from the result. Applied after `include`. |
| `rename` | `dict[string, string]` | no | `null` | Rename right-side columns after join. Map of old name to new name. Applied after `prefix`. |
| `prefix` | `string` | no | `null` | String prepended to every surviving right-side column name. Applied before `rename`. |

A `JoinKeyPair` has `left` and `right` fields for asymmetric key names.

**Column processing order** (right-side columns only):

1. Duplicate join key columns are dropped automatically
2. `include` glob patterns are applied — only matching columns are kept
3. `exclude` glob patterns are applied — matching columns are removed
4. `prefix` is prepended to all surviving column names
5. `rename` mappings are applied last

```yaml
- join:
    source: regions
    type: left
    on:
      - region_id
      - { left: src_code, right: region_code }

# With column control
- join:
    source: sap_plants
    type: left
    on: [plant_id]
    filter: "plant_status = 'ACTIVE'"
    include: ["plant_*", "region_code"]
    exclude: ["plant_internal_*"]
    prefix: "src_"
    rename:
      src_plant_name: plant_name
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

Replace null values in specified columns. Supports explicit
column-to-value mapping and schema-driven type defaults.
Both modes may appear in the same step — type defaults apply
first, then explicit columns override.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | `dict[string, any]` | cond. | -- | Map of column name to fill value |
| `mode` | `string` | cond. | -- | Set to `"type_defaults"` for schema-driven fills |
| `code` | `string` | cond. | -- | Semantic code: `"unknown"`, `"not_applicable"`, or `"invalid"`. Required when `mode` is set. |
| `include` | `list[string]` | no | all | Glob patterns restricting which columns are filled |
| `exclude` | `list[string]` | no | none | Glob patterns excluding columns from fill |
| `overrides` | `dict[string, any]` | no | -- | Per-column overrides for type-based defaults |
| `where` | `string` | no | -- | Spark SQL predicate for conditional fill |

At least one of `columns` or `mode` must be set.

**Explicit columns:**

```yaml
- fill_null:
    columns:
      discount: 0
      notes: "N/A"
      is_active: true
```

**Type-aware mode:**

```yaml
- fill_null:
    mode: type_defaults
    code: unknown
    exclude: [id, "*_id"]
    overrides:
      region: "Unspecified"
```

**Type-default mappings:**

| Spark type | unknown | not_applicable | invalid |
|-----------|---------|----------------|---------|
| String | `"Unknown"` | `"Not Applicable"` | `"Invalid"` |
| Boolean | `false` | `false` | `false` |
| Integer/Long | `0` | `0` | `0` |
| Float/Double | `0.0` | `0.0` | `0.0` |
| Decimal | `0` | `0` | `0` |
| Date | `1970-01-01` | `1970-01-01` | `1970-01-01` |
| Timestamp | epoch | epoch | epoch |

**Composable (both modes):**

```yaml
- fill_null:
    mode: type_defaults
    code: unknown
    exclude: [id]
    columns:
      special_flag: -1
```

**Conditional fill:**

```yaml
- fill_null:
    mode: type_defaults
    code: unknown
    where: "status = 'UNKNOWN'"
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

### concat

Concatenate multiple columns into a single string column.
Null handling, separator, and trimming are configurable.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `target` | `string` | yes | -- | Output column name |
| `columns` | `list[string]` | yes | -- | Ordered source columns. Must not be empty. |
| `separator` | `string` | no | `""` | String inserted between column values |
| `null_mode` | `string` | no | `"skip"` | How nulls are handled: `"skip"`, `"empty"`, or `"literal"` |
| `null_literal` | `string` | no | `"<NULL>"` | Replacement text when `null_mode` is `"literal"` |
| `trim` | `boolean` | no | `false` | Trim whitespace from inputs and result |
| `collapse_separators` | `boolean` | no | `true` | Collapse adjacent separators. Only applies when `null_mode` is `"skip"`. |

Non-string columns are auto-cast to string before concatenation.
When all input columns are null or blank, the result is `NULL`.

```yaml
- concat:
    target: full_address
    columns: [street, city, state, zip]
    separator: ", "
    null_mode: skip
    trim: true
```

### map

Map discrete values in a column using a lookup dictionary.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `column` | `string` | yes | -- | Source column to map |
| `target` | `string` | no | source column | Output column name |
| `values` | `dict[string, any]` | yes | -- | Value mapping dictionary. Must not be empty. |
| `default` | `any` | no | `null` | Value for unmapped inputs. Mutually exclusive with `unmapped: null\|validate`. |
| `on_null` | `any` | no | `null` | Value when source is null. Falls back to `default` if not set. |
| `unmapped` | `string` | no | `"keep"` | Behavior for unmapped values: `"keep"`, `"null"`, or `"validate"` |
| `case_sensitive` | `boolean` | no | `true` | Whether value matching is case-sensitive |

Null handling cascade: `on_null` (if set) then `default`
(if set) then null passthrough.

```yaml
- map:
    column: status_code
    target: status_label
    values:
      A: Active
      I: Inactive
      C: Closed
    default: Unknown
```

### format

Format columns using pattern, number, or date rules.
Multiple columns can be formatted in a single step.
Each key is a target column name with a format specification.

**Format spec keys:**

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `source` | `string` | no | target name | Source column (in-place when omitted) |
| `pattern` | `string` | cond. | -- | String pattern with `{pos:len}` placeholders (1-indexed) |
| `number` | `string` | cond. | -- | Java DecimalFormat pattern |
| `date` | `string` | cond. | -- | Java SimpleDateFormat pattern |
| `on_short` | `string` | no | `"null"` | Behavior when source is shorter than pattern: `"null"` or `"partial"` |
| `strict_types` | `boolean` | no | `false` | Reject type mismatches instead of auto-casting |

Exactly one of `pattern`, `number`, or `date` must be set
per column.

```yaml
- format:
    phone:
      pattern: "({1:3}){4:3}-{7:4}"
      source: raw_phone
    amount_display:
      number: "#,##0.00"
      source: amount
    event_date:
      date: "yyyy-MM-dd"
```

### resolve

Resolve foreign keys by joining to a named lookup dimension.
Assigns sentinel values for invalid (null/blank) and unknown
(no match) business keys. Supports single FK, compound BK,
SCD2 narrowing, batch mode, and include columns.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | `string` | yes* | -- | Output FK column name |
| `lookup` | `string` | yes* | -- | Named lookup reference |
| `match` | `string`, `list`, or `dict` | yes* | -- | BK column mapping (sugar: string/list/dict) |
| `pk` | `string` | yes* | -- | Surrogate key column in lookup |
| `on_invalid` | `int` | no | `-4` | Sentinel for incomplete BK |
| `on_unknown` | `int` | no | `-1` | Sentinel for no match |
| `on_duplicate` | `string` | no | `"warn"` | `"error"`, `"warn"`, `"first"` |
| `on_failure` | `string` | no | `"abort"` | `"abort"`, `"warn"` |
| `normalize` | `string` | no | `null` | `"trim_lower"`, `"trim_upper"`, `"trim"`, `"none"` |
| `drop_source_columns` | `bool` | no | `false` | Drop source columns after resolve |
| `include` | `list` or `dict` | no | `null` | Extra columns from lookup. See syntax below. |
| `include_prefix` | `string` | no | `null` | Prefix for included columns |
| `effective` | `EffectiveConfig` | no | `null` | SCD2 narrowing (date range or current flag) |
| `where` | `string` | no | `null` | SQL predicate filter on lookup |
| `batch` | `list[ResolveBatchItem]` | no | `null` | Batch FK specs (mutually exclusive with name/lookup/match) |

*Required in single mode. In batch mode, these fields are optional
at the top level and serve as shared defaults merged into each batch
item.

**`include` syntax:**

`include` accepts three forms:

- **String list** — column names to carry over from the lookup as-is:

  ```yaml
  include: [region_name, country_code]
  ```

- **Dict** — map of lookup column to output alias (rename on include):

  ```yaml
  include:
    region_name: region
    country_code: iso_country
  ```

- **Object list** — each entry is a `{column, as}` pair for explicit
  per-column renaming in a list form:

  ```yaml
  include:
    - { column: region_name, as: region }
    - { column: country_code, as: iso_country }
  ```

`include_prefix` applies after all three forms and prepends a string
to every included column name. `include_prefix` and dict/object rename
are mutually exclusive per column.

See the [Resolve Step guide](../../guides/resolve.md) for
detailed examples of all modes and features.

```yaml
- resolve:
    name: plant_id
    lookup: dim_plant
    match: plant_code
    pk: id
    on_invalid: -4
    on_unknown: -1

# With include columns
- resolve:
    name: customer_id
    lookup: dim_customer
    match: customer_code
    pk: sk_customer
    include:
      - { column: customer_name, as: cust_name }
      - { column: customer_tier, as: tier }
```

### foreach

A macro block that expands into repeated step sequences before model
hydration. Not a step type itself — expanded by the config preprocessor.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `values` | `list` | yes | -- | Items to iterate over |
| `as` | `string` | yes | -- | Variable name bound to each item during expansion |
| `steps` | `list[Step]` | yes | -- | Step template repeated for each value |

```yaml
steps:
  - foreach:
      values: [revenue, cost, margin]
      as: col
      steps:
        - derive:
            name: "${col}_rounded"
            expr: "round(${col}, 2)"
```

---

## target

Defines where the thread writes its output.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `alias` | `string` | cond. | `null` | Lakehouse table alias for Delta targets. Mutually exclusive with `connection`. |
| `path` | `string` | cond. | `null` | File path for file-based targets. |
| `connection` | `string` | cond. | `null` | Name of a connection declared in `connections:`. Requires `table`. Mutually exclusive with `alias`. |
| `schema` | `string` | no | `null` | Schema override within the connection's lakehouse. Overrides `connection.default_schema`. |
| `table` | `string` | cond. | `null` | Table name within the connection's lakehouse. Required when `connection` is set. |
| `mapping_mode` | `string` | no | `"auto"` | Column mapping: `"auto"` or `"explicit"` |
| `columns` | `dict[string, ColumnMapping]` | no | `null` | Per-column mapping specifications |
| `partition_by` | `list[string]` | no | `null` | Partition columns for the output table |
| `audit_columns` | `dict[string, string]` | no | `null` | Audit column definitions as name-expression pairs |
| `audit_template` | `string or list[string]` | no | `null` | Named template(s) to apply. Resolved from `audit_templates` at any level or from built-in presets (`fabric`, `minimal`). See [Audit Templates guide](../../guides/audit-templates.md). |
| `audit_template_inherit` | `bool` | no | `true` | When `false`, suppresses any `audit_template` inherited from parent levels. Direct `audit_template` on this target still applies. |
| `audit_columns_exclude` | `list[string]` | no | `null` | Column names or glob patterns to exclude from the resolved template set. |
| `naming` | `NamingConfig` | no | `null` | Column and table naming normalization |
| `dimension` | `DimensionConfig` | no | `null` | Dimension target mode with composable SCD flags. Mutually exclusive with `fact`. See [Dimension Modeling guide](../../guides/dimension-modeling.md). |
| `fact` | `FactConfig` | no | `null` | Fact target mode with FK validation. Mutually exclusive with `dimension`. See [Fact Tables guide](../../guides/fact-tables.md). |
| `seed` | `SeedConfig` | no | `null` | Seed rows inserted on first write or when table is empty. |
| `warp` | `string \| false \| null` | no | `null` | Warp schema contract reference. A string names an explicit `.warp` file; `false` opts out of auto-discovery; `null` enables auto-discovery. |
| `warp_mode` | `"auto" \| null` | no | `null` | Warp generation mode. `"auto"` writes/updates the `.warp` file after each successful run. |
| `warp_init` | `bool` | no | `false` | When `true` and a warp exists, create the Delta table from the effective warp before the first pipeline run. |
| `warp_enforcement` | `"warn" \| "enforce" \| "off"` | no | `"warn"` | Warp contract enforcement mode. `"enforce"` fails the thread on violations; `"warn"` logs findings; `"off"` skips validation. |
| `schema_drift` | `"lenient" \| "strict" \| "adaptive"` | no | `"lenient"` | Schema drift handling mode. `"lenient"` passes extra columns through; `"strict"` applies `on_drift` action; `"adaptive"` passes through and extends auto-generated warps. |
| `on_drift` | `"error" \| "warn" \| "ignore"` | no | `"warn"` | Severity action for strict drift mode. `"error"` aborts the thread; `"warn"` drops extra columns with a warning; `"ignore"` drops silently. |

At least one of `alias`, `path`, or `connection` + `table` is required.

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

### target.dimension (DimensionConfig)

Composable dimension target with behavioral flags replacing
traditional SCD type labels. See the
[Dimension Modeling guide](../../guides/dimension-modeling.md)
for detailed examples.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `business_key` | `list[string]` | yes | -- | Natural key columns |
| `surrogate_key` | `DimensionSurrogateKeyConfig` | yes | -- | SK generation (see below) |
| `track_history` | `bool` | no | `false` | Enable SCD versioning (close-and-insert) |
| `change_detection` | `dict[string, GroupConfig]` | no | auto | Named change detection groups. Defaults to a single auto group (`on_change: version` if `track_history`, else `overwrite`). |
| `previous_columns` | `dict[string, string]` | no | `null` | Map of output → source for prior-value tracking |
| `additional_keys` | `dict[string, KeyConfig]` | no | `null` | Secondary hash key definitions |
| `columns` | `ScdColumnConfig` | no | defaults | SCD column names: `valid_from` (`_valid_from`), `valid_to` (`_valid_to`), `is_current` (`_is_current`) |
| `dates` | `ScdDateConfig` | no | defaults | SCD boundary dates: `min` (`1970-01-01`), `max` (`9999-12-31`) |
| `seed_system_members` | `bool` | no | `false` | Insert Kimball sentinel rows on first write |
| `system_members` | `list[SystemMember]` | no | defaults | Custom sentinel rows (sk, code, label). Defaults: -1/unknown, -2/not_applicable |
| `label_column` | `string` | no | `null` | Column for system member labels |
| `history_filter` | `bool` | no | `true` | Filter target reads to `is_current = true` |

**dimension.surrogate_key:**

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | `string` | yes | -- | Output column name |
| `algorithm` | `string` | no | `"sha256"` | Hash algorithm |
| `columns` | `list[string]` | yes | -- | Source columns to hash |
| `output` | `string` | no | `"native"` | Output type: `"native"` or `"string"` |

**dimension.change_detection group:**

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | `string` | no | dict key | Output hash column name |
| `algorithm` | `string` | no | `"sha256"` | Hash algorithm |
| `columns` | `list[string]` or `"auto"` | yes | -- | Columns to hash. `"auto"` selects remaining data columns. |
| `on_change` | `string` | yes | -- | `"version"`, `"overwrite"`, or `"static"` |
| `output` | `string` | no | `"native"` | Output type |

!!! note "write/keys interaction"
    When `dimension:` is present, `write.match_keys`,
    `write.on_match`, `keys.business_key`,
    `keys.surrogate_key`, and `keys.change_detection` are
    forbidden — the engine derives these from the dimension
    block. `write.on_no_match_source` and
    `write.on_no_match_target` remain available as overrides.

```yaml
target:
  path: Tables/dim_customer
  dimension:
    business_key: [customer_id]
    surrogate_key:
      name: _sk_customer
      columns: [customer_id]
    track_history: true
    seed_system_members: true
```

### target.fact (FactConfig)

Fact target mode providing FK column validation and sentinel
value documentation. See the
[Fact Tables guide](../../guides/fact-tables.md).

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `foreign_keys` | `list[string]` | yes | -- | FK column names to validate exist in output |
| `sentinel_values` | `SentinelValueConfig` | no | defaults | Sentinel value conventions |

**fact.sentinel_values:**

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `invalid` | `int` | no | `-4` | Sentinel for incomplete business keys |
| `missing` | `int` | no | `-1` | Sentinel for unmatched lookups |

```yaml
target:
  path: Tables/fact_orders
  fact:
    foreign_keys: [sk_customer, sk_product, sk_date]
    sentinel_values:
      invalid: -4
      missing: -1
```

### target.seed (SeedConfig)

Seed rows inserted into the target on first write or when the
table is empty.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `on` | `string` | no | `"first_write"` | Trigger: `"first_write"` (table absent) or `"empty"` (table absent or zero rows) |
| `rows` | `list[dict]` | yes | -- | Row data as column-value dicts |

```yaml
target:
  seed:
    on: first_write
    rows:
      - id: 1
        name: Default Category
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
| `soft_delete_value` | `bool` | no | `true` | Value written to the soft delete column when a row is soft-deleted. |
| `soft_delete_active_value` | `bool` | no | `null` | Value written to the soft delete column for active rows (matched updates and new inserts). When omitted, those rows receive `null`. |

```yaml
write:
  mode: merge
  match_keys: [order_id]
  on_match: update
  on_no_match_target: insert
  on_no_match_source: soft_delete
  soft_delete_column: is_deleted
  soft_delete_value: true
  soft_delete_active_value: false
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

!!! info "keys vs dimension surrogate key"
    `keys.surrogate_key` uses `SurrogateKeyConfig` (hash-based, no
    `columns` field). `target.dimension.surrogate_key` uses
    `DimensionSurrogateKeyConfig` (requires `columns` for SK
    generation from business key columns). These are separate
    types with different fields.

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
| `type` | `string` | yes | -- | Assertion type: `"row_count"`, `"column_not_null"`, `"unique"`, `"expression"`, `"fk_sentinel_rate"` |
| `severity` | `string` | no | `"warn"` | Severity level: `"info"`, `"warn"`, `"error"`, `"fatal"` |
| `columns` | `list[string]` | no | `null` | Columns for `column_not_null`, `unique`, and `fk_sentinel_rate` assertions |
| `min` | `int` | no | `null` | Minimum value for `row_count` assertions |
| `max` | `int` | no | `null` | Maximum value for `row_count` assertions |
| `expression` | `SparkExpr` | no | `null` | Spark SQL expression for `expression` assertions |
| `column` | `string` | cond. | `null` | Single column for `fk_sentinel_rate` (mutually exclusive with `columns`) |
| `sentinel` | `int` or `string` | cond. | `null` | Single sentinel value for `fk_sentinel_rate` (mutually exclusive with `sentinels`) |
| `sentinels` | `dict` | cond. | `null` | Named sentinel groups for `fk_sentinel_rate` (dict-of-int or dict-of-dict) |
| `max_rate` | `float` | no | `null` | Maximum sentinel rate threshold for `fk_sentinel_rate` |
| `message` | `string` | no | `null` | Custom failure message for `fk_sentinel_rate` |

For `fk_sentinel_rate`: at least one of `column` or `columns` is
required, and at least one of `sentinel` or `sentinels` is required.

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
  - type: fk_sentinel_rate
    column: plant_id
    sentinel: -4
    max_rate: 0.05
    message: "plant FK invalid rate exceeded"
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

## connections

Named OneLake connection declarations. Connections identify a Fabric workspace
and lakehouse by GUID or variable reference. Sources, targets, and exports can
then refer to a connection by name instead of embedding raw GUIDs or
`abfss://` paths inline.

Connections cascade from loom to weave to thread. When the same name is defined
at multiple levels, the most-specific level wins.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `type` | `string` | yes | -- | Connection type. Currently only `"onelake"` is supported. |
| `workspace` | `string` | yes | -- | OneLake workspace GUID or `${fabric.workspace_id}` variable. |
| `lakehouse` | `string` | yes | -- | OneLake lakehouse GUID or `${fabric.lakehouse_id}` variable. |
| `default_schema` | `string` | no | `null` | Default schema for tables in this connection. Can be overridden per-source or per-target with `schema:`. |

`${fabric.workspace_id}`, `${fabric.lakehouse_id}`, and
`${fabric.workspace_name}` are injected at runtime from the
active Fabric session. Using these variables makes a connection
portable across environments without changing GUIDs.

See the [Connections guide](../../guides/connections.md) for cross-lakehouse
examples, schema overrides, and migration patterns.

```yaml
connections:
  raw:
    type: onelake
    workspace: "${fabric.workspace_id}"
    lakehouse: "${fabric.lakehouse_id}"
    default_schema: raw

  archive:
    type: onelake
    workspace: "a1b2c3d4-0000-0000-0000-111111111111"
    lakehouse: "e5f6a7b8-0000-0000-0000-222222222222"

sources:
  orders:
    connection: raw
    table: orders

  archived_orders:
    connection: archive
    schema: history
    table: orders

target:
  connection: raw
  table: fact_orders
```

---

## Complete example

```yaml
config_version: "1.0"

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
