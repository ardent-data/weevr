# Configuration Keys

This page provides a consolidated reference of every configuration field
across all weevr models. Fields are grouped by the model they belong to.

For YAML syntax and complete examples, see the schema reference pages:
[Thread](yaml-schema/thread.md) | [Weave](yaml-schema/weave.md) | [Loom](yaml-schema/loom.md)

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

---

## Source

A data source declaration referenced by alias within a thread.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `str` | *required* | Source type: `delta`, `csv`, `json`, `parquet`, `excel` |
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
| `audit_template` | `str` | `None` | Audit column template name |
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
| `soft_delete_value` | `str` | `"true"` | Value written to the soft delete column |

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
| `algorithm` | `"sha256" \| "md5"` | `"sha256"` | Hash algorithm |

### ChangeDetectionConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | *required* | Output column name |
| `columns` | `list[str]` | *required* | Columns included in the hash |
| `algorithm` | `"md5" \| "sha256"` | `"md5"` | Hash algorithm |

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

Column and table naming normalization. Cascades through loom/weave/thread/target.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `columns` | `NamingPattern` | `None` | Column naming pattern |
| `tables` | `NamingPattern` | `None` | Table naming pattern |
| `exclude` | `list[str]` | `[]` | Names or patterns excluded from normalization |

Supported `NamingPattern` values: `snake_case`, `camelCase`, `PascalCase`,
`UPPER_SNAKE_CASE`, `Title_Snake_Case`, `Title Case`, `lowercase`, `UPPERCASE`,
`none`.
