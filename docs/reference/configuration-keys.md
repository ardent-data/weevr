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
| `lookups` | `dict[str, Lookup]` | `None` | Thread-level lookup definitions |
| `column_sets` | `dict[str, ColumnSet]` | `None` | Thread-level column set definitions |
| `variables` | `dict[str, VariableSpec]` | `None` | Thread-level variable declarations |
| `pre_steps` | `list[HookStep]` | `None` | Hook steps run before thread execution |
| `post_steps` | `list[HookStep]` | `None` | Hook steps run after thread execution |
| `audit_templates` | `dict[str, dict[str, str]]` | `None` | Named audit column templates available to all targets |
| `connections` | `dict[str, OneLakeConnection]` | `None` | Named connection definitions |
| `with` | `dict[str, SubPipeline]` | `None` | Named sub-pipelines (CTEs) resolved before main steps |

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

## AuditTemplate

A named set of audit columns defined under `audit_templates` at loom, weave,
or thread level. Each template is a `dict[str, str]` mapping column names to
Spark SQL expressions.

```yaml
audit_templates:
  lineage:
    _source_system: "${param.source_system}"
    _load_ts: "current_timestamp()"
    _run_id: "${param.run_id}"
```

Templates are referenced by name in `target.audit_template`. The built-in
presets `fabric` and `minimal` do not require a declaration.

**Related:** [Audit Templates guide](../guides/audit-templates.md)

---

## Source

A data source declaration referenced by alias within a thread.
A source is a direct data reference (with `type`), a lookup
reference (with `lookup`), a connection-based reference (with
`connection` + `table`), or a generated sequence (with `type`
set to `date_sequence` or `int_sequence`).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `str` | conditional | Source type: `delta`, `csv`, `json`, `parquet`, `excel`, `date_sequence`, `int_sequence`. Required when neither `lookup` nor `connection` is set. |
| `lookup` | `str` | `None` | Lookup name (thread-, weave-, or loom-level). Mutually exclusive with `type`. |
| `alias` | `str` | `None` | Table alias (required for `delta` without `connection`). Mutually exclusive with `connection`. |
| `path` | `str` | `None` | File path (required for file-based types) |
| `options` | `dict[str, Any]` | `{}` | Spark reader options |
| `dedup` | `DedupConfig` | `None` | Post-read deduplication |
| `connection` | `str` | `None` | Named connection reference. Requires `table`. |
| `schema` | `str` | `None` | Schema override within the connection's lakehouse |
| `table` | `str` | `None` | Table name (required when `connection` is set) |
| `start` | `str \| int` | `None` | Range start, inclusive (required for generated types) |
| `end` | `str \| int` | `None` | Range end, inclusive (required for generated types) |
| `column` | `str` | `None` | Output column name (required for generated types) |
| `step` | `str \| int` | `None` | Step interval: `day`/`week`/`month`/`year` for dates, positive int for `int_sequence` |

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
| `connection` | `str` | `None` | Named connection reference. Requires `table`. Mutually exclusive with `alias`. |
| `schema` | `str` | `None` | Schema override within the connection's lakehouse |
| `table` | `str` | `None` | Table name within the connection's lakehouse. Requires `connection`. |
| `mapping_mode` | `"auto" \| "explicit"` | `"auto"` | Column mapping strategy |
| `columns` | `dict[str, ColumnMapping]` | `None` | Per-column mapping |
| `partition_by` | `list[str]` | `None` | Partition columns |
| `audit_columns` | `dict[str, str]` | `None` | Audit column definitions as name-expression pairs |
| `audit_template` | `str \| list[str]` | `None` | Template name(s) to apply. Accepts built-in presets (`fabric`, `minimal`) or user-defined names. |
| `audit_template_inherit` | `bool` | `True` | When `False`, suppresses inherited `audit_template` from parent levels |
| `audit_columns_exclude` | `list[str]` | `None` | Column names or glob patterns excluded from the resolved template set |
| `naming` | `NamingConfig` | `None` | Naming normalization |
| `warp` | `str \| false \| null` | `null` | Warp reference, opt-out, or auto-discover |
| `warp_mode` | `"auto" \| null` | `null` | Auto-generate warp after write |
| `warp_init` | `bool` | `false` | Pre-initialize table from warp |
| `warp_enforcement` | `"warn" \| "enforce" \| "off"` | `"warn"` | Warp contract enforcement mode |
| `schema_drift` | `"lenient" \| "strict" \| "adaptive"` | `"lenient"` | Schema drift handling mode |
| `on_drift` | `"error" \| "warn" \| "ignore"` | `"warn"` | Severity for strict drift mode |
| `dimension` | `DimensionConfig` | `None` | SCD Type 2 behavior. Mutually exclusive with `fact`. See [thread schema: target.dimension](yaml-schema/thread.md#targetdimension-dimensionconfig). |
| `fact` | `FactConfig` | `None` | Fact table FK validation. Mutually exclusive with `dimension`. See [thread schema: target.fact](yaml-schema/thread.md#targetfact-factconfig). |
| `seed` | `SeedConfig` | `None` | Static seed rows inserted on initial load. See [thread schema: target.seed](yaml-schema/thread.md#targetseed-seedconfig). |

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
| `soft_delete_value` | `bool` | `True` | Value written to the soft delete column when a row is soft-deleted |
| `soft_delete_active_value` | `bool` | `None` | Value written to the soft delete column for active (non-deleted) rows |

---

## LoadConfig

Incremental load mode and watermark tracking.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mode` | `"full" \| "incremental_watermark" \| "incremental_parameter" \| "cdc"` | `"full"` | Load strategy |
| `watermark_column` | `str` | `None` | Column for watermark comparison. Required for `incremental_watermark`; optional for `cdc` mode when `cdc.operation_column` is set (rejected when `cdc.preset` is `"delta_cdf"`). |
| `watermark_type` | `"timestamp" \| "date" \| "int" \| "long"` | `None` | Watermark column data type. Required whenever `watermark_column` is set. |
| `watermark_inclusive` | `bool` | `False` | Include rows equal to last watermark |
| `watermark_store` | `WatermarkStoreConfig` | `None` | Watermark persistence backend |
| `cdc` | `CdcConfig` | `None` | CDC configuration |

Cross-field rules enforced at parse time:

- `mode == "incremental_watermark"` requires `watermark_column`
- `mode == "cdc"` requires `cdc` config
- `mode == "cdc"` with `cdc.preset == "delta_cdf"` rejects `watermark_column`
  (CDF uses commit-version tracking automatically)
- `mode == "cdc"` with `cdc.operation_column` (generic CDC) may compose
  with `watermark_column` to narrow the read window for append-only CDC
  history tables; `watermark_type` is required whenever `watermark_column`
  is set in cdc mode

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
| `type` | `"row_count" \| "column_not_null" \| "unique" \| "expression" \| "fk_sentinel_rate"` | *required* | Assertion type |
| `severity` | `"info" \| "warn" \| "error" \| "fatal"` | `"warn"` | Failure severity |
| `columns` | `list[str]` | `None` | Columns for `column_not_null` and `unique` |
| `column` | `str` | `None` | FK column for `fk_sentinel_rate` |
| `min` | `int` | `None` | Minimum for `row_count` |
| `max` | `int` | `None` | Maximum for `row_count` |
| `expression` | `SparkExpr` | `None` | Expression for `expression` type |
| `sentinel` | `Any` | `None` | Single sentinel value for `fk_sentinel_rate` |
| `sentinels` | `list[Any]` | `None` | Multiple sentinel values for `fk_sentinel_rate` |
| `max_rate` | `float` | `None` | Maximum acceptable sentinel rate (0.0-1.0) for `fk_sentinel_rate` |
| `message` | `str` | `None` | Custom failure message for `fk_sentinel_rate` |

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
| `strategy` | `Literal[7 values]` | `"quote"` | How to handle reserved words |
| `prefix` | `str` | `"_"` | Prefix when strategy is `prefix` |
| `suffix` | `str` | `"_col"` | Suffix when strategy is `suffix` |
| `rename_map` | `dict[str, str]` | `None` | Explicit collision map for `rename` |
| `fallback` | `Literal[6 values]` | `"quote"` | Fallback for unmapped `rename` collisions |
| `preset` | `str \| list` | `None` | Built-in word list presets |
| `extend` | `list[str]` | `[]` | Extra words to treat as reserved |
| `exclude` | `list[str]` | `[]` | Words to remove from check |

#### Strategies

| Strategy | Behavior |
|----------|----------|
| `quote` | Keep name as-is; Spark backtick-quotes in SQL |
| `prefix` | Prepend `prefix` to colliding names |
| `suffix` | Append `suffix` to colliding names |
| `error` | Raise `ConfigError` listing all collisions |
| `rename` | Apply `rename_map`; unmapped use `fallback` |
| `revert` | Discard the rename, keep pre-normalization name |
| `drop` | Remove colliding columns (not valid for tables) |

The `drop` strategy is not valid for table names — neither as a
direct `strategy` nor as a `fallback`. Both raise `ConfigError`
at runtime in `normalize_table_name`.

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

# Suffix — append string to collisions:
reserved_words:
  strategy: suffix
  suffix: "_col"       # order -> order_col

# Explicit rename map with fallback:
reserved_words:
  strategy: rename
  rename_map:
    order: order_name
    select: select_col
  fallback: error      # unmapped collisions fail

# Revert — keep original name on collision:
reserved_words:
  strategy: revert

# Drop — remove colliding columns:
reserved_words:
  strategy: drop

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
| `audit_templates` | `dict[str, dict[str, str]]` | `None` | Named audit column templates cascaded to threads |
| `connections` | `dict[str, OneLakeConnection]` | `None` | Named connection definitions cascaded to threads |

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
| `lookups` | `dict[str, Lookup]` | `None` | Loom-level lookup definitions cascaded to weaves |
| `variables` | `dict[str, VariableSpec]` | `None` | Loom-level variable declarations |
| `pre_steps` | `list[HookStep]` | `None` | Hook steps run before any weave executes |
| `post_steps` | `list[HookStep]` | `None` | Hook steps run after all weaves complete |
| `audit_templates` | `dict[str, dict[str, str]]` | `None` | Named audit column templates cascaded to weaves and threads |
| `connections` | `dict[str, OneLakeConnection]` | `None` | Named connection definitions cascaded to weaves and threads |

### WeaveEntry

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | `""` | Weave name (required for inline definitions) |
| `ref` | `str` | `None` | Path to an external `.weave` file |
| `condition` | `ConditionSpec` | `None` | Conditional execution gate |

---

## OneLakeConnection

Named connection declaration for Fabric OneLake sources and targets.
Declared in the `connections:` block at loom, weave, or thread level
and cascaded through the standard merge rules.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `Literal["onelake"]` | *required* | Connection type identifier. Only `onelake` is supported. |
| `workspace` | `str` | *required* | OneLake workspace GUID or `${param.*}` variable reference |
| `lakehouse` | `str` | *required* | OneLake lakehouse GUID or `${param.*}` variable reference |
| `default_schema` | `str` | `None` | Default schema applied to tables in this connection when none is set |

`workspace` and `lakehouse` both support `${param.*}` and
`${variable.*}` substitution so one declaration can serve multiple
environments.

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
