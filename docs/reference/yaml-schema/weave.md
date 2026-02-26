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
| `threads` | `list[ThreadEntry or string]` | yes | -- | Thread references. Strings are shorthand for `{ name: "<value>" }` (inline definitions). Use `ref` for external file references. |
| `defaults` | `dict[string, any]` | no | `null` | Default values cascaded into every thread in this weave |
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
  - ref: staging/load_orders.thread
  - ref: staging/load_customers.thread
  - ref: curated/build_order_summary.thread
    dependencies: [load_orders, load_customers]
  - ref: curated/refresh_snapshot.thread
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
