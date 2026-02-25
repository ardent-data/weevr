# Weave YAML Schema

A weave groups a collection of threads into a dependency graph. It defines
which threads run, their execution order, optional shared defaults, and
runtime settings.

---

## Top-level keys

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `config_version` | `string` | yes | -- | Schema version identifier (e.g. `"1"`) |
| `name` | `string` | no | `""` | Human-readable weave name |
| `threads` | `list[ThreadEntry or string]` | yes | -- | Thread references. Strings are treated as `{ name: "<value>" }`. |
| `defaults` | `dict[string, any]` | no | `null` | Default values cascaded into every thread in this weave |
| `params` | `dict[string, ParamSpec]` | no | `null` | Typed parameter declarations scoped to this weave |
| `execution` | `ExecutionConfig` | no | `null` | Runtime settings (logging, tracing) cascaded to threads |
| `naming` | `NamingConfig` | no | `null` | Naming normalization cascaded to threads |

---

## threads (ThreadEntry)

Each entry in the `threads` list is either a plain string (shorthand for a
thread name with no overrides) or a `ThreadEntry` object.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | `string` | yes | -- | Thread name matching a thread config file |
| `dependencies` | `list[string]` | no | `null` | Explicit upstream thread names. Merged with auto-inferred dependencies from source/target path matching. |
| `condition` | `ConditionSpec` | no | `null` | Conditional execution gate |

### threads.condition (ConditionSpec)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `when` | `string` | yes | -- | Condition expression. Supports `${param.x}` references, `table_exists()`, `table_empty()`, `row_count()`, and boolean operators. |

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

Supported patterns: `snake_case`, `camelCase`, `PascalCase`, `UPPER_SNAKE_CASE`,
`Title_Snake_Case`, `Title Case`, `lowercase`, `UPPERCASE`, `none`.

---

## Complete example

```yaml
config_version: "1"
name: sales_pipeline

threads:
  - name: load_orders
  - name: load_customers
  - name: build_order_summary
    dependencies: [load_orders, load_customers]
  - name: refresh_snapshot
    dependencies: [build_order_summary]
    condition:
      when: "table_exists('curated.order_summary')"

defaults:
  write:
    mode: merge

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
```

### Shorthand thread syntax

Thread entries can be plain strings when no overrides are needed:

```yaml
threads:
  - load_orders
  - load_customers
  - build_order_summary
```

This is equivalent to:

```yaml
threads:
  - name: load_orders
  - name: load_customers
  - name: build_order_summary
```
