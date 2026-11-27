# Project YAML Schema

A project document is the `weevr.yaml` file at the root of a project. It
identifies the project, can pin the weevr versions it runs on, and says where
its run outputs go. See [Project layout](../project-layout.md) for how the
file marks the project root and how the rest of the tree is organized.

---

## Top-level keys

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `kind` | `string` | yes | -- | Document kind: `project`. Required in the file ([`C206`](../diagnostics.md#c206)); the model defaults it when built in code. |
| `config_version` | `string` | yes | -- | Schema version identifier (e.g. `"2.0"`) |
| `id` | `string` (GUID) | yes | -- | The project's identity. Generated once and never derived from Fabric item IDs. |
| `name` | `string` | yes | -- | Project name, 1 to 100 characters: a letter, then letters, digits or `_`. Also the default run-store schema suffix, so it must be a valid schema name. |
| `description` | `string` | no | `null` | What the project is for |
| `requires_weevr` | `string` | no | `null` | PEP 440 version specifier the installed weevr must satisfy, such as `">=2.0.0a1,<3"`. An unmet specifier, or an installed weevr version that can't be determined, fails the load with [`C208`](../diagnostics.md#c208); a malformed one fails with [`C103`](../diagnostics.md#c103). |
| `run_store` | `RunStore` | no | `null` | Where run outputs live. Omitted means the container's default. |
| `params` | `dict[string, ParamSpec]` | no | `null` | Typed parameter declarations every loom, weave and thread sees. See [Params](../../guides/params.md). |
| `strict_params` | `bool` | no | `null` | Enforce the params contract project-wide: every `${param.x}` reference declared (`P120`) and every runtime param declared or referenced (`P121`). A document's own `strict_params`, or the load's, overrides it. Unset is lenient. |
| `connections` | `dict[string, Connection]` | no | `null` | Named connections every loom, weave and thread sees. |
| `audit_templates` | `dict[string, AuditTemplate]` | no | `null` | Named audit column templates every config sees. |
| `defaults` | `dict[string, any]` | no | `null` | Defaults cascaded into every loom, weave and thread. |
| `lookups` | `dict[string, Lookup]` | no | `null` | Named lookups every config sees. |
| `column_sets` | `dict[string, ColumnSet]` | no | `null` | Named column sets every config sees. |

Unknown keys are rejected. `variables:` is reserved at the project tier: a
`weevr.yaml` that declares it fails the load with
[`C210`](../diagnostics.md#c210). Declare variables in the looms, weaves or
threads that use them.

The project is the outermost tier of every cascade. Each block has the same
shape as in a loom, and a loom, weave or thread declaring the same name
shadows the project's. The
[cascade table](../../guides/config-resolution.md#cascade-by-key) lists how
each key merges.

```text
kind: project
config_version: "2.0"
id: 8f9d2c4e-5b1a-4c7e-9a36-2d4f8e1b7c05
name: sales
description: Sales reporting pipelines
requires_weevr: ">=2.0.0a1,<3"
```

A project that declares params, connections, a default and a lookup:

```text
kind: project
config_version: "2.0"
id: 8f9d2c4e-5b1a-4c7e-9a36-2d4f8e1b7c05
name: sales

strict_params: true

params:
  env:
    name: env
    type: string
    default: dev
  workspace_id:
    name: workspace_id
    type: string
    required: true

connections:
  curated:
    type: onelake
    workspace: "${param.workspace_id}"
    lakehouse: "curated_${param.env}"

defaults:
  target:
    audit_columns:
      _loaded_at: "current_timestamp()"

lookups:
  regions:
    source:
      type: delta
      alias: ref.regions
```

!!! note "Pre-release versions"
    PEP 440 orders `2.0.0a2` below `2`, so `">=2,<3"` rejects every 2.0
    pre-release. Use `">=2.0.0a1,<3"` while weevr 2.0 is in pre-release.

---

## run_store

Optional. Controls where runs write execution history, validation results,
quarantine and run files. See
[Run store](../project-layout.md#run-store) for the tables, the base each
container implies, and the two layouts.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `connection` | `string` | no | `null` | Name of a connection whose lakehouse holds the run store. The name is looked up in the connections of the loaded config first, then in the connections `weevr.yaml` declares; the loaded config wins by name. An unknown name fails with [`R201`](../diagnostics.md#r201). |
| `layout` | `string` | no | `per_project` | `per_project` gives the project its own tables; `shared` writes to one schema that several projects share, each row carrying its `project_id`. |
| `schema` | `string` | no | `null` | `shared`: the shared schema (default `weevr`). `per_project`: replaces the name derived from the project name (`weevr_<name>`). Same character rule as `name`. |

```text
run_store:
  connection: central_lake
  layout: shared
  schema: weevr_ops
```
