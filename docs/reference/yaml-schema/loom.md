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
| `defaults` | `dict[string, any]` | no | `null` | Default values cascaded into every weave and thread. `audit_columns` and `exports` use additive merge (see [Exports guide](../../guides/exports.md)). |
| `params` | `dict[string, ParamSpec]` | no | `null` | Typed parameter declarations scoped to this loom |
| `execution` | `ExecutionConfig` | no | `null` | Runtime settings cascaded to weaves and threads |
| `naming` | `NamingConfig` | no | `null` | Naming normalization cascaded to weaves and threads |
| `column_sets` | `dict[string, ColumnSet]` | no | `null` | Named column sets cascaded to weaves. Weave-level definitions with the same name override loom-level definitions. See [Weave schema: column_sets](weave.md#column_sets) for field details. |
| `lookups` | `dict[string, Lookup]` | no | `null` | Loom-level lookup definitions. Merged with weave-level lookups (weave wins on name collision). See [Weave schema: lookups](weave.md#lookups) for field details. |
| `variables` | `dict[string, VariableSpec]` | no | `null` | Loom-level variable declarations. Resolved before `pre_steps` execution. |
| `pre_steps` | `list[HookStep]` | no | `null` | Hook steps to run before any weave executes. See [Weave schema: pre_steps / post_steps](weave.md#pre_steps-post_steps-hookstep) for field details. |
| `post_steps` | `list[HookStep]` | no | `null` | Hook steps to run after all weaves complete. |

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

## lookups

Loom-level lookup definitions shared across all weaves. The structure is
identical to [weave-level lookups](weave.md#lookups). When a weave defines
a lookup with the same name, the weave-level definition wins.

```yaml
lookups:
  dim_calendar:
    source:
      type: delta
      alias: ref.dim_calendar
    key: [date_key]
    values: [fiscal_year, fiscal_quarter]
    materialize: true
    strategy: broadcast
```

---

## variables

Loom-level typed variable declarations. Variables can be set by
`sql_statement` hook steps via `set_var` and referenced in downstream config
as `${var.name}`. The structure is identical to
[weave-level variables](weave.md#variables-variablespec).

```yaml
variables:
  pipeline_start:
    type: timestamp
    default: null
```

---

## pre_steps / post_steps

Hook steps that run before any weave executes (`pre_steps`) or after all
weaves complete (`post_steps`). The structure is identical to
[weave-level pre_steps / post_steps](weave.md#pre_steps-post_steps-hookstep).

These lists are not cascaded to child weaves. Each level runs its own list.

```yaml
pre_steps:
  - type: quality_gate
    name: check_upstream_freshness
    check: source_freshness
    source: raw.events
    max_age: "1h"

post_steps:
  - type: log_message
    message: "Loom complete."
    level: info
```

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
