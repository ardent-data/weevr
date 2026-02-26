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
| `weaves` | `list[WeaveEntry or string]` | yes | -- | Weave references. Strings are shorthand for `{ name: "<value>" }` (inline definitions). Use `ref` for external file references. |
| `defaults` | `dict[string, any]` | no | `null` | Default values cascaded into every weave and thread |
| `params` | `dict[string, ParamSpec]` | no | `null` | Typed parameter declarations scoped to this loom |
| `execution` | `ExecutionConfig` | no | `null` | Runtime settings cascaded to weaves and threads |
| `naming` | `NamingConfig` | no | `null` | Naming normalization cascaded to weaves and threads |

---

## weaves (WeaveEntry)

Each entry in the `weaves` list is either a plain string (shorthand) or a
`WeaveEntry` object. Use `ref` to reference an external `.weave` file, or
`name` for inline weave definitions within the loom.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `ref` | `string` | no | `null` | Path to an external `.weave` file, relative to the project root. Mutually exclusive with inline `name`. |
| `name` | `string` | no | `""` | Weave name. Required for inline definitions; derived from filename stem when using `ref`. |
| `condition` | `ConditionSpec` | no | `null` | Conditional execution gate |

### weaves.condition (ConditionSpec)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `when` | `string` | yes | -- | Condition expression. Supports `${param.x}` references, `table_exists()`, `table_empty()`, `row_count()`, and boolean operators. |

---

## Configuration cascade

Defaults and execution settings flow downward through the hierarchy:

```text
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
  - ref: ingest_raw.weave
  - ref: build_curated.weave
    condition:
      when: "${param.run_curated} == 'true'"
  - ref: publish_marts.weave

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

Weave entries can be plain strings for inline definitions that need no
condition:

```yaml
weaves:
  - ingest_raw
  - build_curated
  - publish_marts
```

This is equivalent to:

```yaml
weaves:
  - name: ingest_raw
  - name: build_curated
  - name: publish_marts
```

For external file references, use `ref` explicitly:

```yaml
weaves:
  - ref: ingest_raw.weave
  - ref: build_curated.weave
  - ref: publish_marts.weave
```
