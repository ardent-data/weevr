# Configuration Keys

This page provides a consolidated reference of every configuration field
across all weevr models. Fields are grouped by the model they belong to.

For YAML syntax and complete examples, see the schema reference pages:
[Thread](yaml-schema/thread.md) | [Weave](yaml-schema/weave.md) | [Loom](yaml-schema/loom.md)

```d2
direction: down

data_flow: Data Flow {
  Thread: Thread {
    shape: class
    config_version: str
    sources: "dict[str, Source]"
    "steps": "list[Step]"
    target: Target
  }
  Source: Source {
    shape: class
    type: str
    alias: str
    path: str
    lookup: str
  }
  Target: Target {
    shape: class
    alias: str
    path: str
    mapping_mode: str
  }

  Thread -> Source: "1..*" {style.stroke: "#1976D2"}
  Thread -> Target: "1..1" {style.stroke: "#1976D2"}
}

orchestration: Orchestration {
  Weave: Weave {
    shape: class
    threads: "list[ThreadEntry]"
    lookups: "dict[str, Lookup]"
    pre_steps: "list[HookStep]"
    post_steps: "list[HookStep]"
    variables: "dict[str, VariableSpec]"
    column_sets: "dict[str, ColumnSet]"
  }
  Loom: Loom {
    shape: class
    weaves: "list[WeaveEntry]"
    defaults: dict
    column_sets: "dict[str, ColumnSet]"
  }
  Loom -> Weave: "1..*" {style.stroke: "#7B1FA2"}
  Weave -> data_flow.Thread: "1..*" {style.stroke: "#7B1FA2"}
}

behavior: Behavior {
  WriteConfig: WriteConfig {
    shape: class
    mode: str
    match_keys: "list[str]"
  }
  LoadConfig: LoadConfig {
    shape: class
    mode: str
    watermark_column: str
  }
  KeyConfig: KeyConfig {
    shape: class
    business_key: "list[str]"
    surrogate_key: config
    change_detection: config
  }
  ValidationRule: ValidationRule {
    shape: class
    name: str
    rule: expr
    severity: str
  }
  FailureConfig: FailureConfig {
    shape: class
    on_failure: str
  }
  Export: Export {
    shape: class
    name: str
    type: str
    path: str
    on_failure: str
  }
}

data_flow.Thread -> behavior.WriteConfig: "0..1" {style.stroke: "#388E3C"}
data_flow.Thread -> behavior.LoadConfig: "0..1" {style.stroke: "#388E3C"}
data_flow.Thread -> behavior.KeyConfig: "0..1" {style.stroke: "#388E3C"}
data_flow.Thread -> behavior.ValidationRule: "0..*" {style.stroke: "#388E3C"}
data_flow.Thread -> behavior.FailureConfig: "0..1" {style.stroke: "#388E3C"}
data_flow.Thread -> behavior.Export: "0..*" {style.stroke: "#388E3C"}
```

---

## Thread

The top-level configuration unit for a single data pipeline.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `config_version` | `str` | *required* | Schema version identifier |
| `name` | `str` | `""` | Thread name |
| `sources` | `dict[str, Source]` | *required* | Named data sources |
| `steps` | `list[Step]` | `[]` | Ordered transformation pipeline |
| `target` | `Target` | *required* | Output destination |
| `write` | `WriteConfig` | `None` | Write mode and merge settings |
| `keys` | `KeyConfig` | `None` | Key management configuration |
| `validations` | `list[ValidationRule]` | `None` | Pre-write data quality rules |
| `assertions` | `list[Assertion]` | `None` | Post-execution assertions |
| `load` | `LoadConfig` | `None` | Incremental load settings |
| `tags` | `list[str]` | `None` | Free-form tags |
| `params` | `dict[str, ParamSpec]` | `None` | Parameter declarations |
| `defaults` | `dict[str, Any]` | `None` | Inherited defaults |
| `failure` | `FailureConfig` | `None` | Failure handling policy |
| `execution` | `ExecutionConfig` | `None` | Runtime settings |
| `cache` | `bool` | `None` | Cache DataFrame before writing |
| `exports` | `list[Export]` | `None` | Secondary output destinations |

---

## Export

A named secondary output destination for thread data. Exports write the
same post-mapping, audit-injected DataFrame as the primary target.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | *required* | Unique identifier (valid Python identifier) |
| `description` | `str` | `None` | Human-readable label shown in `explain()` |
| `type` | `str` | *required* | Output format: `delta`, `parquet`, `csv`, `json`, `orc` |
| `path` | `str` | `None` | OneLake path. Required when `alias` is not set. Supports context variables. |
| `alias` | `str` | `None` | Metastore alias. Delta type only, mutually exclusive with `path`. |
| `mode` | `str` | `"overwrite"` | Write mode (only `overwrite` in v1) |
| `partition_by` | `list[str]` | `None` | Partition columns (independent of primary target) |
| `on_failure` | `str` | `"warn"` | `abort` fails the thread; `warn` logs and continues |
| `enabled` | `bool` | `True` | Set to `false` to suppress an inherited export |
| `options` | `dict[str, str]` | `None` | Format-specific Spark DataFrameWriter options |

**Related:** [Exports guide](../guides/exports.md)

---

## Source

A data source declaration referenced by alias within a thread. A source
is either a direct data reference (with `type`) or a lookup reference
(with `lookup`). These are mutually exclusive.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `str` | conditional | Source type: `delta`, `csv`, `json`, `parquet`, `excel`. Required when `lookup` is not set. |
| `lookup` | `str` | `None` | Weave-level lookup name. Mutually exclusive with `type`. |
| `alias` | `str` | `None` | Table alias (required for `delta`) |
| `path` | `str` | `None` | File path (required for file-based types) |
| `options` | `dict[str, Any]` | `{}` | Spark reader options |
| `dedup` | `DedupConfig` | `None` | Post-read deduplication |

### DedupConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `keys` | `list[str]` | *required* | Deduplication grouping columns |
| `order_by` | `str` | `None` | Sort expression for row selection |

---

## Target

Write destination with column mapping and partitioning.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `alias` | `str` | `None` | Lakehouse table alias |
| `path` | `str` | `None` | File path for file-based targets |
| `mapping_mode` | `"auto" \| "explicit"` | `"auto"` | Column mapping strategy |
| `columns` | `dict[str, ColumnMapping]` | `None` | Per-column mapping |
| `partition_by` | `list[str]` | `None` | Partition columns |
| `audit_columns` | `dict[str, str]` | `None` | Audit column definitions as name-expression pairs |
| `naming` | `NamingConfig` | `None` | Naming normalization |

### ColumnMapping

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `expr` | `SparkExpr` | `None` | Expression for column value (mutually exclusive with `drop`) |
| `type` | `str` | `None` | Target data type for casting |
| `default` | `Any` | `None` | Default when source is null |
| `drop` | `bool` | `False` | Drop column from output (mutually exclusive with `expr`) |

---

## WriteConfig

Write mode and merge behavior for the target.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mode` | `"overwrite" \| "append" \| "merge"` | `"overwrite"` | Write strategy |
| `match_keys` | `list[str]` | `None` | Merge match keys (required for `merge` mode) |
| `on_match` | `"update" \| "ignore"` | `"update"` | Action when source row matches target |
| `on_no_match_target` | `"insert" \| "ignore"` | `"insert"` | Action for new source rows |
| `on_no_match_source` | `"delete" \| "soft_delete" \| "ignore"` | `"ignore"` | Action for missing source rows |
| `soft_delete_column` | `str` | `None` | Column for soft delete flag (required for `soft_delete`) |
| `soft_delete_value` | `bool` | `True` | Value written to the soft delete column |

---

## LoadConfig

Incremental load mode and watermark tracking.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mode` | `"full" \| "incremental_watermark" \| "incremental_parameter" \| "cdc"` | `"full"` | Load strategy |
| `watermark_column` | `str` | `None` | Column for watermark comparison |
| `watermark_type` | `"timestamp" \| "date" \| "int" \| "long"` | `None` | Watermark column data type |
| `watermark_inclusive` | `bool` | `False` | Include rows equal to last watermark |
| `watermark_store` | `WatermarkStoreConfig` | `None` | Watermark persistence backend |
| `cdc` | `CdcConfig` | `None` | CDC configuration |

### WatermarkStoreConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `"table_properties" \| "metadata_table"` | `"table_properties"` | Storage backend |
| `table_path` | `str` | `None` | Path for metadata table (required for `metadata_table`) |

### CdcConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `preset` | `"delta_cdf"` | `None` | CDC preset (mutually exclusive with `operation_column`) |
| `operation_column` | `str` | `None` | Operation type column (mutually exclusive with `preset`) |
| `insert_value` | `str` | `None` | Value indicating insert |
| `update_value` | `str` | `None` | Value indicating update |
| `delete_value` | `str` | `None` | Value indicating delete |
| `on_delete` | `"hard_delete" \| "soft_delete"` | `"hard_delete"` | Delete handling strategy |

---

## KeyConfig

Business key, surrogate key, and change detection settings.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `business_key` | `list[str]` | `None` | Natural business key columns |
| `surrogate_key` | `SurrogateKeyConfig` | `None` | Surrogate key generation |
| `change_detection` | `ChangeDetectionConfig` | `None` | Change detection hash |

### SurrogateKeyConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | *required* | Output column name |
| `algorithm` | `"xxhash64" \| "sha1" \| "sha256" \| "sha384" \| "sha512" \| "md5" \| "crc32" \| "murmur3"` | `"sha256"` | Hash algorithm |
| `output` | `"native" \| "string"` | `"native"` | Output type for integer-returning algorithms (xxhash64, crc32, murmur3). `native` preserves the return type; `string` casts to StringType. |

### ChangeDetectionConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | *required* | Output column name |
| `columns` | `list[str]` | *required* | Columns included in the hash |
| `algorithm` | `"xxhash64" \| "sha1" \| "sha256" \| "sha384" \| "sha512" \| "md5" \| "crc32" \| "murmur3"` | `"md5"` | Hash algorithm |
| `output` | `"native" \| "string"` | `"native"` | Output type for integer-returning algorithms. `native` preserves the return type; `string` casts to StringType. |

---

## ValidationRule

Pre-write data quality rules.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | *required* | Rule name |
| `rule` | `SparkExpr` | *required* | Spark SQL boolean expression |
| `severity` | `"info" \| "warn" \| "error" \| "fatal"` | `"error"` | Failure severity |

---

## Assertion

Post-execution assertions on the target dataset.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `"row_count" \| "column_not_null" \| "unique" \| "expression"` | *required* | Assertion type |
| `severity` | `"info" \| "warn" \| "error" \| "fatal"` | `"warn"` | Failure severity |
| `columns` | `list[str]` | `None` | Columns for `column_not_null` and `unique` |
| `min` | `int` | `None` | Minimum for `row_count` |
| `max` | `int` | `None` | Maximum for `row_count` |
| `expression` | `SparkExpr` | `None` | Expression for `expression` type |

---

## ExecutionConfig

Runtime settings that cascade through loom/weave/thread.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `log_level` | `"minimal" \| "standard" \| "verbose" \| "debug"` | `"standard"` | Logging verbosity |
| `trace` | `bool` | `True` | Collect execution spans |

---

## FailureConfig

Per-thread failure handling policy.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `on_failure` | `"abort_weave" \| "skip_downstream" \| "continue"` | `"abort_weave"` | Failure policy |

---

## ParamSpec

Typed parameter declaration.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | *required* | Parameter name |
| `type` | `str` | *required* | Data type: `string`, `int`, `float`, `bool`, `date`, `timestamp`, `list[string]` |
| `required` | `bool` | `True` | Whether the parameter must be supplied |
| `default` | `Any` | `None` | Default value |
| `description` | `str` | `""` | Human-readable description |

---

## NamingConfig

Column and table naming normalization. Cascades through
loom/weave/thread/target.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `columns` | `NamingPattern` | `None` | Column naming pattern |
| `tables` | `NamingPattern` | `None` | Table naming pattern |
| `exclude` | `list[str]` | `[]` | Names or patterns excluded from normalization |
| `on_collision` | `Literal["suffix", "error"]` | `"error"` | Duplicate name handling after normalization |
| `reserved_words` | `ReservedWordConfig` | `None` | Reserved word protection config |

Supported `NamingPattern` values: `snake_case`, `camelCase`,
`PascalCase`, `UPPER_SNAKE_CASE`, `Title_Snake_Case`, `Title Case`,
`lowercase`, `UPPERCASE`, `kebab-case`, `none`.

### ReservedWordConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | `Literal["prefix", "quote", "error"]` | `"quote"` | How to handle reserved words |
| `prefix` | `str` | `"_"` | Prefix when `strategy` is `"prefix"` |
| `preset` | `str \| list[str]` | `None` | Built-in word list presets |
| `extend` | `list[str]` | `[]` | Extra words to treat as reserved |
| `exclude` | `list[str]` | `[]` | Words to remove from check |

#### Presets

The `preset` field selects one or more built-in reserved word
lists. When omitted, the ANSI SQL list is used as the default.
Specifying any preset replaces that default — to include ANSI
words alongside other presets, list `ansi` explicitly.

A single string is accepted as shorthand for a one-element list.

| Preset | Description |
|--------|-------------|
| `ansi` | ANSI SQL reserved keywords (~80 words) |
| `dax` | DAX reserved words for Power BI semantic models |
| `m` | M language (Power Query) reserved words |
| `powerbi` | Convenience alias — expands to `dax` + `m` |
| `tsql` | T-SQL reserved keywords for SQL endpoints |

Each preset is self-contained: `preset: [tsql]` alone gives
full T-SQL protection without needing to add `ansi`.

```yaml
# Default (no preset = ANSI):
reserved_words:
  strategy: prefix

# Power BI convenience alias (= dax + m):
reserved_words:
  preset: powerbi
  strategy: prefix

# Multi-engine (ANSI + DAX + T-SQL):
reserved_words:
  preset: [ansi, dax, tsql]
  strategy: prefix
  extend: [fiscal_year]
  exclude: [by]
```

`extend` and `exclude` compose on top of the resolved preset
union, adding or removing individual words.

---

## Weave

A collection of threads with shared lookups, hooks, and defaults.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `config_version` | `str` | *required* | Schema version identifier |
| `name` | `str` | `""` | Weave name |
| `threads` | `list[ThreadEntry]` | *required* | Thread references |
| `lookups` | `dict[str, Lookup]` | `None` | Named lookup sources shared across threads |
| `variables` | `dict[str, VariableSpec]` | `None` | Weave-scoped typed variables |
| `pre_steps` | `list[HookStep]` | `None` | Hook steps run before threads |
| `post_steps` | `list[HookStep]` | `None` | Hook steps run after threads |
| `defaults` | `dict[str, Any]` | `None` | Default values cascaded to threads |
| `params` | `dict[str, ParamSpec]` | `None` | Parameter declarations |
| `execution` | `ExecutionConfig` | `None` | Runtime settings cascaded to threads |
| `naming` | `NamingConfig` | `None` | Naming normalization cascaded to threads |
| `column_sets` | `dict[str, ColumnSet]` | `None` | Named column sets for bulk rename |

### ThreadEntry

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | `""` | Thread name (required for inline definitions) |
| `ref` | `str` | `None` | Path to an external `.thread` file |
| `dependencies` | `list[str]` | `None` | Explicit upstream thread names |
| `condition` | `ConditionSpec` | `None` | Conditional execution gate |

### ConditionSpec

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `when` | `str` | *required* | Condition expression. Supports `${param.x}` references, `table_exists()`, `table_empty()`, `row_count()`, and boolean operators. |

---

## Loom

Deployment unit grouping one or more weaves.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `config_version` | `str` | *required* | Schema version identifier |
| `name` | `str` | `""` | Loom name |
| `weaves` | `list[WeaveEntry]` | *required* | Weave references |
| `defaults` | `dict[str, Any]` | `None` | Default values cascaded to weaves and threads |
| `params` | `dict[str, ParamSpec]` | `None` | Parameter declarations |
| `execution` | `ExecutionConfig` | `None` | Runtime settings cascaded down |
| `naming` | `NamingConfig` | `None` | Naming normalization cascaded down |
| `column_sets` | `dict[str, ColumnSet]` | `None` | Named column sets cascaded to weaves |

### WeaveEntry

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | `""` | Weave name (required for inline definitions) |
| `ref` | `str` | `None` | Path to an external `.weave` file |
| `condition` | `ConditionSpec` | `None` | Conditional execution gate |

---

## ColumnSet

Named column mapping sourced from Delta, YAML, or runtime parameters.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `source` | `ColumnSetSource` | `None` | External source definition |
| `param` | `str` | `None` | Runtime parameter name (mutually exclusive with `source`) |
| `on_unmapped` | `Literal["pass_through", "error"]` | `"pass_through"` | Behavior for unmapped DataFrame columns |
| `on_extra` | `Literal["ignore", "warn", "error"]` | `"ignore"` | Behavior for extra mapping keys |
| `on_failure` | `Literal["abort", "warn", "skip"]` | `"abort"` | Behavior on source read failure |

### ColumnSetSource

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `Literal["delta", "yaml"]` | *required* | Source type |
| `alias` | `str` | `None` | Lakehouse table alias (Delta) |
| `path` | `str` | `None` | File path (Delta path or YAML file) |
| `from_column` | `str` | `"source_name"` | Column containing raw names |
| `to_column` | `str` | `"target_name"` | Column containing target names |
| `filter` | `str` | `None` | SQL WHERE expression |

---

## Lookup

A weave-level named data definition referenced by threads via `source.lookup`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `source` | `Source` | *required* | Source definition for the lookup data |
| `materialize` | `bool` | `False` | Pre-read and cache/broadcast before thread execution |
| `strategy` | `"broadcast" \| "cache"` | `"cache"` | Materialization strategy |
| `key` | `list[str]` | `None` | Join key columns for narrow projection |
| `values` | `list[str]` | `None` | Payload columns to retain (requires `key`) |
| `filter` | `str` | `None` | SQL WHERE expression applied before projection |
| `unique_key` | `bool` | `False` | Validate key uniqueness after filtering |
| `on_failure` | `"abort" \| "warn"` | `"abort"` | Behavior on duplicate keys (only valid when `unique_key` is true) |

---

## HookStep

Discriminated union of hook step types, dispatched on the `type` field.
Used in weave `pre_steps` and `post_steps`.

### QualityGateStep

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `"quality_gate"` | *required* | Step type discriminator |
| `name` | `str` | `None` | Step name for telemetry |
| `on_failure` | `"abort" \| "warn"` | phase default | Failure behavior |
| `check` | `str` | *required* | Check type: `source_freshness`, `row_count_delta`, `row_count`, `table_exists`, `expression` |
| `source` | `str` | `None` | Table alias (for `source_freshness`, `table_exists`) |
| `max_age` | `str` | `None` | Duration string (for `source_freshness`) |
| `target` | `str` | `None` | Table alias (for `row_count_delta`, `row_count`) |
| `max_decrease_pct` | `float` | `None` | Max decrease % (for `row_count_delta`) |
| `max_increase_pct` | `float` | `None` | Max increase % (for `row_count_delta`) |
| `min_delta` | `int` | `None` | Min absolute row change (for `row_count_delta`) |
| `max_delta` | `int` | `None` | Max absolute row change (for `row_count_delta`) |
| `min_count` | `int` | `None` | Min row count (for `row_count`) |
| `max_count` | `int` | `None` | Max row count (for `row_count`) |
| `sql` | `str` | `None` | SQL expression (for `expression`) |
| `message` | `str` | `None` | Failure message (for `expression`) |

### SqlStatementStep

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `"sql_statement"` | *required* | Step type discriminator |
| `name` | `str` | `None` | Step name for telemetry |
| `on_failure` | `"abort" \| "warn"` | phase default | Failure behavior |
| `sql` | `str` | *required* | Spark SQL statement to execute |
| `set_var` | `str` | `None` | Variable name to capture scalar result into |

### LogMessageStep

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `"log_message"` | *required* | Step type discriminator |
| `name` | `str` | `None` | Step name for telemetry |
| `on_failure` | `"abort" \| "warn"` | phase default | Failure behavior |
| `message` | `str` | *required* | Message template (supports `${var.name}`) |
| `level` | `"info" \| "warn" \| "error"` | `"info"` | Log level |

---

## VariableSpec

Weave-scoped variable declaration. Set by hook steps via `set_var`,
referenced as `${var.name}` in config expressions.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `str` | *required* | Scalar type: `string`, `int`, `long`, `float`, `double`, `boolean`, `timestamp`, `date` |
| `default` | `str \| int \| float \| bool` | `None` | Default value when no hook sets the variable |
