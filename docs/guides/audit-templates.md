# Audit Column Templates

Audit templates let you define a named set of audit columns once and apply them
across many threads. They are an alternative to repeating the same
`target.audit_columns` block in every thread file.

## Overview

Inline `target.audit_columns` works well for a single thread. As the number of
threads grows, maintaining consistent audit metadata across every file becomes
error-prone. Audit templates address this by separating the column definitions
from their application points.

Templates are declared under an `audit_templates` key at loom, weave, or
thread level. A thread then references one or more templates by name using
`target.audit_template`. The engine resolves the column set at execution time
and appends the columns after all transformation steps, alongside any inline
`target.audit_columns` entries.

## Built-in presets

weevr ships two presets that can be referenced without any declaration.

### `fabric`

Nine columns for Fabric pipeline runs. All values are sourced from pipeline
and notebook parameters or Spark context.

| Column | Expression |
|--------|-----------|
| `_batch_id` | `${param.batch_id}` |
| `_batch_version` | `${param.batch_version}` |
| `_batch_source` | `${param.batch_source}` |
| `_batch_process_ts` | `current_timestamp()` |
| `_pipeline_id` | `${param.pipeline_id}` |
| `_pipeline_name` | `${param.pipeline_name}` |
| `_workspace_id` | `${param.workspace_id}` |
| `_spark_app_id` | `spark_context().applicationId` |
| `_task_ts` | `current_timestamp()` |

The `param.*` references must be satisfied at runtime. Declare the
corresponding parameters in your loom or weave `params` block, or pass them
from a Fabric pipeline activity.

### `minimal`

Three columns for basic load tracking. Useful for lakehouses where Fabric
pipeline parameters are not available.

| Column | Expression |
|--------|-----------|
| `_weevr_loaded_at` | `current_timestamp()` |
| `_weevr_run_id` | `${param.run_id}` |
| `_weevr_thread` | `${thread.name}` |

## Configuration

### Declaring a custom template

Define templates at loom level to share them across all weaves and threads.
Define at weave level to scope them to that weave. Define at thread level for
thread-local templates.

```yaml
# In a loom file
audit_templates:
  lineage:
    _source_system: "${param.source_system}"
    _load_ts: "current_timestamp()"
    _run_id: "${param.run_id}"
```

### Applying a template to a thread

Reference a template by name in `target.audit_template`:

```yaml
# In a thread file
target:
  alias: curated.orders
  audit_template: lineage
```

### Using a built-in preset

Built-in presets are referenced the same way — no declaration needed:

```yaml
target:
  alias: curated.orders
  audit_template: fabric
```

## Composition: template + inline columns

When a thread specifies both `audit_template` and inline `audit_columns`, the
two sets are merged additively. Inline entries take precedence over same-named
template entries:

```yaml
target:
  alias: curated.orders
  audit_template: minimal
  audit_columns:
    _weevr_run_id: "${param.custom_run_id}"   # overrides minimal preset
    _domain: "'orders'"                        # additional column
```

The resolved set here would be `_weevr_loaded_at` and `_weevr_thread` from the
preset, plus `_weevr_run_id` and `_domain` from the inline block (the inline
`_weevr_run_id` replaces the preset value).

## Multi-template: list syntax

`audit_template` accepts either a single string or a list. When a list is
given, templates are merged left to right — later entries override earlier ones
on name collision. Inline `audit_columns` always win over any template.

```yaml
target:
  alias: curated.orders
  audit_template:
    - lineage
    - compliance
  audit_columns:
    _override: "'manual'"
```

## Inheritance: cascade behavior

`audit_templates` cascades additively through loom → weave → thread, matching
the behavior of other additive keys. Child levels can add new template
definitions and override existing ones by name.

To prevent a thread from inheriting any parent-level `audit_template`
assignment, set `audit_template_inherit: false` on the target:

```yaml
target:
  alias: curated.reference_data
  audit_template_inherit: false
  audit_columns:
    _loaded_at: "current_timestamp()"
```

This suppresses all inherited template references. Any `audit_template` set
directly on this target still applies — only the inherited assignment is
dropped.

## Exclusion: `audit_columns_exclude`

To suppress specific columns from a resolved template without replacing the
whole template, use `target.audit_columns_exclude`. This accepts a list of
exact column names or glob patterns:

```yaml
target:
  alias: curated.orders
  audit_template: fabric
  audit_columns_exclude:
    - _batch_version
    - _pipeline_*
```

The `_pipeline_*` pattern matches `_pipeline_id`, `_pipeline_name`,
and any other columns matching the glob. Exclusions are applied after
the full merge of templates and inline `audit_columns` — all columns
are subject to exclusion regardless of origin.

## Shadow warnings

weevr logs a warning at configuration load time when a user-defined column
name matches a built-in preset column name. This catches accidental shadowing
of preset defaults:

```
WARN [config] audit_columns defines '_batch_id' which shadows the 'fabric'
     preset. The user-defined expression will be used.
```

The user-defined value always takes effect. The warning is informational.

## Migration: `defaults.audit_columns`

Configurations written before v1.8 may use `defaults.target.audit_columns` to
cascade audit columns from the loom or weave level:

```yaml
# Legacy pattern (still supported)
defaults:
  target:
    audit_columns:
      _loaded_at: "current_timestamp()"
      _run_id: "${param.run_id}"
```

This pattern continues to work. However, `audit_templates` offers a cleaner
separation: the column definitions live in a named block rather than being
embedded inside a `defaults.target` path. Consider migrating long-running
configurations to templates when you need to reference the same set from
multiple looms.

Both mechanisms can coexist. `defaults.target.audit_columns` entries are
treated as if they were inline `audit_columns` on each thread's target, and
participate in the same merge order — template columns first, then
`defaults.target.audit_columns`, then thread-level inline `audit_columns`.
