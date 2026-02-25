# Loom YAML Schema

A loom is a deployment unit that groups weaves into an ordered execution
sequence. It defines which weaves run, optional shared defaults, and
runtime settings that cascade down through weaves to threads.

---

## Top-level keys

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `config_version` | `string` | yes | -- | Schema version identifier (e.g. `"1"`) |
| `name` | `string` | no | `""` | Human-readable loom name |
| `weaves` | `list[WeaveEntry or string]` | yes | -- | Weave references. Strings are treated as `{ name: "<value>" }`. |
| `defaults` | `dict[string, any]` | no | `null` | Default values cascaded into every weave and thread |
| `params` | `dict[string, ParamSpec]` | no | `null` | Typed parameter declarations scoped to this loom |
| `execution` | `ExecutionConfig` | no | `null` | Runtime settings cascaded to weaves and threads |
| `naming` | `NamingConfig` | no | `null` | Naming normalization cascaded to weaves and threads |

---

## weaves (WeaveEntry)

Each entry in the `weaves` list is either a plain string or a `WeaveEntry`
object.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | `string` | yes | -- | Weave name matching a weave config file |
| `condition` | `ConditionSpec` | no | `null` | Conditional execution gate |

### weaves.condition (ConditionSpec)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `when` | `string` | yes | -- | Condition expression. Supports `${param.x}` references, `table_exists()`, `table_empty()`, `row_count()`, and boolean operators. |

---

## Configuration cascade

Defaults and execution settings flow downward through the hierarchy:

```
Loom defaults
  --> Weave defaults (override loom)
    --> Thread config (override weave)
```

The most specific level always wins.

---

## Complete example

```yaml
config_version: "1"
name: daily_etl

weaves:
  - name: ingest_raw
  - name: build_curated
    condition:
      when: "${param.run_curated} == 'true'"
  - name: publish_marts

defaults:
  execution:
    log_level: standard
  write:
    mode: overwrite

params:
  run_curated:
    name: run_curated
    type: bool
    required: false
    default: true
    description: "Whether to run the curated layer"

execution:
  log_level: standard
  trace: true

naming:
  columns: snake_case
  tables: snake_case
```

### Shorthand weave syntax

Weave entries can be plain strings when no condition is needed:

```yaml
weaves:
  - ingest_raw
  - build_curated
  - publish_marts
```
