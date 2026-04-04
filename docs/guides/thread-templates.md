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
`ref`, assigns a unique alias with `as`, and passes
values via `params`:

```yaml
# silver.weave
config_version: "1.0"
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
config_version: "1.0"
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

Parameters are resolved in two phases:

1. **Expression resolution** -- `${param.x}` expressions
   in ThreadEntry `params` values are resolved against
   the parent context (weave defaults, runtime params).
2. **Injection** -- Resolved values are injected into the
   thread's parameter context under the `param` namespace.

This means you can compose parameters:

```yaml
threads:
  - ref: silver/sap_table.thread
    as: sap_adr6
    params:
      target_schema: "${env}_silver"
      table_name: ADR6
```

Here `${env}` resolves from runtime or weave defaults
before `target_schema` is injected into the thread.

## Validation

- **Duplicate ref without alias** -- If the same `ref`
  appears twice without `as`, a `ConfigError` is raised.
- **Duplicate effective name** -- Two entries that resolve
  to the same effective name raise a `ConfigError`.
- **Unused params** -- Entry params not consumed by the
  thread config emit a warning with source attribution.

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
  expected interface and enable IDE validation.
- Running a template standalone (without a weave) raises
  a `VariableResolutionError` for unresolved `${param.x}`
  references, helping identify misconfigured templates.
