# Thread Templates

Thread templates let you define a thread config once and
instantiate it multiple times in a weave with different
parameters. This is useful when the same transformation
logic applies to many tables that differ only in source,
target, or a few configuration values.

## How it works

A **template** is a regular `.thread` file that uses
`${param.x}` references instead of hard-coded values.

A **ThreadEntry** in a weave references the template with
`ref`, assigns a unique alias with `as`, and pins
values for it via `params`:

```yaml
# silver.weave
kind: weave
config_version: "2.0"
threads:
  - ref: silver/sap_table.thread
    as: sap_adr6
    params:
      table_name: ADR6
      schema: raw

  - ref: silver/sap_table.thread
    as: sap_adr2
    params:
      table_name: ADR2
      schema: raw
```

The template thread uses `${param.x}` to reference
injected values:

```yaml
# silver/sap_table.thread
kind: thread
config_version: "2.0"
sources:
  sap_data:
    type: delta
    alias: ${param.schema}.${param.table_name}
steps:
  - filter:
      expr: "mandt = '100'"
target:
  alias: silver.${param.table_name}
```

Each entry produces an independent thread with its own
name, telemetry span, and watermark state.

## Effective name resolution

The thread's effective name follows this priority:

1. `as` alias (if set)
2. `name` field (for inline definitions)
3. Filename stem of `ref` (for external references)

The effective name is used everywhere downstream:
planner DAG, executor spans, telemetry, watermark keys,
and display output.

## Parameter resolution

An entry's `params:` are **pins**: values bound for the
referenced thread that beat runtime params and declared
defaults within it. A name the entry doesn't pin falls
through to the usual order: a value supplied above
(runtime params), then the nearest declared default. See
[Binding precedence](params.md#binding-precedence).

Pins are resolved in two phases:

1. **Expression resolution** -- `${param.x}` expressions
   in ThreadEntry `params` values are resolved in the
   weave's scope: the weave's runtime params, pins from
   the loom entry above it, and declared defaults.
2. **Binding** -- Resolved values are bound in the
   thread's param scope under the `param` namespace.

This means you can compose parameters:

```yaml
threads:
  - ref: silver/sap_table.thread
    as: sap_adr6
    params:
      target_schema: "${param.env}_silver"
      table_name: ADR6
```

Here `${param.env}` resolves from runtime params or the weave's declared
params before `target_schema` is bound in the thread. An entry's
`condition` resolves in the weave's scope too.

Pins apply to `ref:` entries. On an inline thread, `params:`
holds the thread's own declarations (`ParamSpec`s), not
pins.

### Pinning a weave from a loom

A loom's weave entries take `params:` too. A pin there
binds the value for the weave and every thread under it:

```yaml
# nightly.loom
weaves:
  - ref: silver.weave
    name: silver_emea
    params:
      region: emea
  - ref: silver.weave
    name: silver_apac
    params:
      region: apac
```

A pin on a thread entry inside the weave beats the loom's
pin for that thread.

### Registry assets are not retargeted

A connection, lookup, column set, audit template, or
variable declared in the weave resolves its `${param.x}`
references in the weave's scope. A pin on a thread entry
does not change it, and the load warns `P122`. To use the
pinned value, declare the asset again under the same name
in the template thread. See
[Registry assets](params.md#registry-assets-resolve-where-they-are-declared).

## Validation

- **Duplicate ref without alias** -- If the same `ref`
  appears twice without `as`, a `ConfigError` is raised.
- **Duplicate effective name** -- Two entries that resolve
  to the same effective name raise a `ConfigError`.
- **Unused pins** -- A pin nothing in the thread reads
  or declares warns `P123`, naming the entry and file.
  Both pin warnings stay warnings under `strict_params`.

## Telemetry

When a thread is loaded from a template, its `template_ref`
field records the original `.thread` file path. This
appears in:

- Telemetry span attributes (`thread.template_ref`)
- `explain()` output: `sap_adr6 (silver/sap_table.thread)`
- Plan HTML table as a secondary label

## Tips

- `as` is allowed on inline threads and single-use refs
  too -- it simply overrides the thread name.
- Thread-level `params:` declarations (typed parameters)
  are not required for templates, but they document the
  expected interface, type-check and coerce each pinned
  value, and list the template's params in the param
  manifest. A pin on an undeclared name binds; when the
  thread reads it, the reference warns `P120`, and when
  nothing reads it, the pin warns `P123`.
- Running a template standalone (without a weave) fails
  for each `${param.x}` nothing supplies: `P101` for a
  required declared param, `P110` for an undeclared one.
