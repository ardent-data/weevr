# Config Resolution

This guide explains how weevr transforms raw YAML files into validated,
typed configuration models. The resolution pipeline is a multi-stage process
that parses, validates, resolves, inherits, and hydrates configuration.

## Introduction

When you call `Context.run()`, `Context.load()`, `load_config()`, or
`load_resolved()`, weevr runs the same config resolution pipeline. The
stages are designed to fail fast — syntax errors are caught before variable resolution, schema errors
before reference loading, and semantic constraints after all values are
concrete.

## Key concepts

- **Variable interpolation** — `${var}` placeholders in YAML values are
  replaced with concrete values from runtime parameters or config defaults.
  Default values use the `${var:-default}` syntax.
- **Reference resolution** — `ref:` entries in weave and loom configs load
  child configuration files recursively. Circular references are detected
  and rejected.
- **Inheritance cascade** — Defaults flow downward: loom defaults → weave
  defaults → thread config. The most specific value wins. Replacement is
  whole-value, not deep merge, except for the special cascades described in
  Stage 8 (naming, audit columns, exports, connections, target policies).
- **Typed extensions** — Files use `.thread`, `.weave`, and `.loom` extensions
  to declare their config type unambiguously.
- **Macro expansion** — `foreach` blocks in step lists expand into repeated
  step sequences with variable substitution.

## How it works

The pipeline runs in 10 stages, grouped into three phases:

```d2
direction: right

structure: "Phase 1: Structure" {
  style.fill: "#E3F2FD"
  parse: "1. Parse YAML"
  version: "2. Extract version"
  detect: "3. Detect type"
  schema: "4. Validate schema"
  parse -> version -> detect -> schema
}

resolve: "Phase 2: Resolve" {
  style.fill: "#E8F5E9"
  param_ctx: "5. Build param context"
  resolve_vars: "6. Resolve variables"
  resolve_refs: "7. Resolve references"
  param_ctx -> resolve_vars -> resolve_refs
}

finalize: "Phase 3: Finalize" {
  style.fill: "#FFF3E0"
  inherit: "8. Apply inheritance"
  expand: "9. Expand macros"
  hydrate: "10. Hydrate model"
  inherit -> expand -> hydrate
}

structure -> resolve: "all strings\nconcrete" {style.stroke: "#4A90D9"}
resolve -> finalize: "refs loaded,\nvars resolved" {style.stroke: "#2E7D32"}
```

### Stage 1–4: Parse and validate structure

- **Stage 1 — Parse YAML.** Read the file and produce a raw dictionary. File-not-found
   and YAML syntax errors raise `ConfigParseError`.
- **Stage 2 — Extract version.** Read `config_version` and parse into a `(major, minor)`
   tuple. Unsupported versions raise `ConfigVersionError`.
- **Stage 3 — Detect type.** Determine whether the file is a thread, weave, loom, or
   params file. Typed extensions (`.thread`, `.weave`, `.loom`) are checked
   first; if the extension is `.yaml` or `.yml`, the structure is inspected.
- **Stage 4 — Validate schema.** Run the raw dictionary through a Pydantic model for
   loose structural validation. At this stage, `${var}` placeholders are
   permitted (the schema uses `extra="allow"`), so unresolved variables do not
   cause errors.

### Stage 5–6: Resolve values

- **Stage 5 — Build parameter context.** Merge runtime parameters (highest
   priority) over the `param.*` namespace over config-declared
   defaults over fabric context (lowest priority) into a flat
   context dictionary. The `param.*` namespace is populated from
   either the loom/weave's own declared `params:` block (for
   top-level loads only; child configs loaded via `ref:` do not
   resolve their own `params:` block at this stage) or from a
   `ThreadEntry`'s `params:` (for thread templates) — see
   [Declared `params:` and the `${param.x}` namespace](#declared-params-and-the-paramx-namespace)
   below and [Thread Templates](thread-templates.md). The
   `${fabric.*}` namespace comes from the fabric layer. Dotted
   keys like `env.lakehouse` support nested access.
- **Stage 6 — Resolve variables.** Recursively walk all values in the config and
   resolve `${var}` references from the parameter context. Whole-value
   references return the native type; embedded references coerce to
   string (see [Parameter resolution](#parameter-resolution) below).
   Unresolved variables without defaults raise
   `VariableResolutionError`.

### Stage 7: Load child configs

**Resolve references** — For looms and weaves, load child config files:

- Loom → loads referenced weaves
- Weave → loads referenced threads

Each child is itself run through the full pipeline (recursion). A `visited`
set tracks loaded paths to detect circular references. Entry-level
overrides like `dependencies` and `condition` are preserved alongside the
loaded config.

### Stage 8–9: Inherit and expand

- **Stage 8 — Apply inheritance.** First, each thread's own `write` and
  `keys` blocks are checked against the dimension override rules, before
  any loom or weave defaults are merged in, so inherited defaults never
  trip them. Then, for multi-level configs, cascade defaults:
    - Loom defaults merge into weave defaults
    - Merged defaults merge into each thread config
    - Thread values always win (most specific)

   Naming configuration cascades separately as a whole block rather
   than per-field. Audit columns (`audit_columns`), **exports**, and
   **connections** also cascade separately using **additive merge** —
   each level extends the set, and same-named entries at a lower
   level override the definition from the higher level. Exports
   with `enabled: false` are removed after merge.

   Target policies — `schema_drift`, `on_drift`, and `warp_enforcement`
   under `defaults.target` — cascade per key from loom to weave to the
   thread's target, and the most specific level wins.

- **Stage 9 — Expand macros.** `foreach` blocks in thread step lists are expanded
   into repeated sequences. This applies to a thread loaded directly and
   to every thread a weave or loom references, which expand during
   hydration (Stage 10). Expansion happens after variable resolution so
   that `foreach.values` can reference parameters, including values a
   weave passes in through a thread entry's `params`.

### Stage 10: Hydrate

**Validate name and hydrate model** — The config `name` field is
validated against the filename stem (mismatch raises `ConfigError`;
missing name is injected from the stem), and `qualified_key` is set to the
file's path relative to the project root. For a thread, relative `path`
values on file sources (CSV, JSON, Parquet, Excel) are resolved against
the project root. The fully resolved dictionary
is then validated through the typed Pydantic model (`Thread`, `Weave`,
or `Loom`). Semantic errors at this stage raise `ModelValidationError`.
For a weave or loom, every child it pulls in, whether referenced or
inline, is hydrated and validated the same way, so a broken child fails
the whole load. The error's `file_path` names the child's own file, or
the file containing an inline definition.
Incremental-mode constraints that span `load` and `write` (for example,
CDC requires merge write mode, and watermark loads reject the default
`overwrite`) are not part of the load. They are checked when the thread
starts executing, so `load_config`, `load_resolved`, and
`mode="validate"` do not report them; a violating config fails its run
with an `ExecutionError`. See
[When load rules are checked](../reference/yaml-schema/thread.md#when-load-rules-are-checked).

## Loading without running

`Context` executes; `load_config()` and `load_resolved()` inspect. All three
run the same pipeline for `.thread`, `.weave`, and `.loom` files, so a typed
config loads the same way through each. Two differences: only `load_config()`
also accepts `.yaml` and params files, and only `Context` has a Fabric context,
so the other two need a default for every `${fabric.*}` reference. Beyond that,
the difference is what comes back and what's needed:

| Entry point | Needs Spark | Returns |
|---|---|---|
| `Context.load()` | Yes | `LoadedConfig`, with `${fabric.*}` resolved |
| `load_config()` | No | The `Thread`, `Weave`, or `Loom` model (or a params dict) |
| `load_resolved()` | No | `LoadedConfig`, with each weave's resolved threads |

`load_resolved()` suits tools that check or plan configs outside the engine,
such as a CLI or a CI step. It takes `.thread`, `.weave`, and `.loom` files
and returns the hydrated model, every child thread and weave, the load
warnings, and the execution plan:

```python
from pathlib import Path

from weevr.config import load_resolved

project = Path("project.weevr")
loaded = load_resolved(project / "nightly.loom", project_root=project)

for weave_name, threads in loaded.resolved_threads.items():
    print(weave_name, list(threads))

for warning in loaded.warnings:
    print(warning)

for issue in loaded.validate_plans():
    print(f"{issue.weave_name}: {issue.message}")
```

`validate_plans()` builds every weave's execution plan and returns one
`PlanIssue` per weave that fails — a dependency cycle, two threads writing
the same target without an ordering, or a Lookup problem — instead of
stopping at the first. `execution_plan` raises on the first failure.

Without Spark there is no Fabric context, so a `${fabric.*}` reference needs
a default (`${fabric.workspace_id:-local}`) to load through `load_config()` or
`load_resolved()`. Checks that read data, such as whether a source table
exists, run only in `Context.run(mode="validate")`.

!!! note
    `LoadedConfig.threads` and `.weaves` return the model's own entries
    (`ThreadEntry` / `WeaveEntry`). The resolved models are under
    `resolved_threads` and `resolved_weaves`.

## Module map

| Module | Responsibility |
|--------|----------------|
| `config/parser.py` | YAML parsing, version extraction, typed extension detection |
| `config/resolver.py` | Variable interpolation, reference loading, circular dependency detection |
| `config/inheritance.py` | Three-level cascade: loom → weave → thread |
| `config/validation.py` | Pre-resolution schema validation, post-resolution constraint checks |
| `config/macros.py` | `foreach` block expansion in step lists |
| `config/hydration.py` | Hydrates resolved dicts into typed models, including referenced and inline children |
| `config/attribution.py` | Attaches the source file to config errors raised while loading it |
| `config/audit_templates.py` | Audit template presets, context variables, and template merge helpers |
| `config/onelake.py` | Spark-free OneLake path construction shared by the runtime and the planner |
| `config/duration.py` | Re-exports the duration string parser for backward compatibility |
| `config/fabric.py` | Engine wheel. Builds `${fabric.*}` context from Fabric runtime variables |
| `config/paths.py` | Engine wheel. OneLake path utilities, including translating `/lakehouse/` FUSE mount paths to `abfss://` URIs |
| `config/locations.py` | Filesystem-agnostic location abstraction; routes local paths through pathlib and remote URIs through Hadoop FileSystem |
| `config/warp.py` | Warp schema contract discovery and drift baseline resolution |
| `config/loader.py` | The shared load: runs every stage for one file; `load_resolved()` |
| `config/__init__.py` | `load_config()` and `load_resolved()` — the public entry points |

## Deferred variable namespaces

Some variable namespaces are **not** resolved during config loading. They are
preserved as literal `${...}` placeholders and resolved later at execution time:

| Namespace | Resolved at | Used by |
|-----------|-------------|---------|
| `${var.*}` | Weave execution (VariableContext) | Thread steps, sources |
| `${run.timestamp}` | Thread execution | Audit columns, export paths |
| `${run.id}` | Thread execution | Audit columns, export paths |
| `${thread.name}`, `${thread.qualified_key}`, `${thread.source}`, `${thread.sources}` | Thread execution | Audit columns, export paths |
| `${weave.name}`, `${loom.name}` | Thread execution | Audit columns, export paths |

The `run.*` namespace provides per-execution context: `${run.timestamp}` is an
ISO 8601 UTC timestamp and `${run.id}` is a UUID4, both generated once per
`execute_thread()` call. They are available in audit column expressions and
export path templates.

The hierarchy variables are deferred only when the parameter context doesn't supply
them and no `:-default` is given; an unknown property such as `${thread.nme}` still
fails at load.

## Design decisions

- **Pre-resolution loose schema** — Schema validation runs before variable
  resolution, using `extra="allow"` to tolerate `${...}` placeholders. This
  catches structural errors early without requiring all parameters to be
  available.
- **Whole-value replacement** — Inheritance uses simple replacement, not deep
  merge. A thread-level `write` block replaces the entire inherited `write`
  block, not individual fields within it. This keeps behavior predictable.
  The special cascades in Stage 8 are the deliberate exceptions.
- **Typed extensions** — `.thread`, `.weave`, and `.loom` extensions eliminate
  ambiguity about config type without inspecting file contents.
- **Foreach after resolution** — Macro expansion happens after variable
  resolution so that the `values` list can come from parameters, but before
  model hydration so that expanded steps are fully validated.

## Parameter resolution

### Whole-value resolution

When an entire YAML value is a single `${param}` reference (no surrounding
text), the resolver returns the native Python type of the referenced value.
The result is not cast to string.

```yaml
match_keys: ${pk_columns}
# pk_columns: ["mandt", "color"] → list[str]

enabled: ${flag}
# flag: true → bool

limit: ${max_rows}
# max_rows: 1000 → int
```

A whole-value reference that resolves to `None` returns `null` rather than
the string `"None"`:

```yaml
optional_param: ${value}
# value: null → None
```

### Embedded resolution

When `${param}` appears inside a larger string, standard string interpolation
applies. The resolved value is coerced to string and substituted in place.

```yaml
alias: "SAP.${table_name}"
# table_name: "MARA" → "SAP.MARA"

path: "/mnt/${env}/data/${table_name}"
# env: "prod", table_name: "MARA" → "/mnt/prod/data/MARA"
```

Embedded references that resolve to `None` produce the string `"None"`.
Use whole-value references when you need null pass-through.

### Inline defaults

The `${param:-default}` fallback syntax always returns the default as
a **string**, even in a whole-value position. For example,
`limit: ${max_rows:-100}` returns the string `"100"` when `max_rows`
is missing — not the integer `100`. To use a typed default, supply
the parameter value explicitly at runtime rather than relying on the
inline `:-` fallback.

### Declared `params:` and the `${param.x}` namespace

A loom or weave that declares a `params:` block exposes each declared
name under the `${param.x}` namespace, scoped to that file:

```yaml
# silver.loom
config_version: "1.0"
weaves:
  - ref: bronze_to_silver.weave

params:
  workspace_id:
    name: workspace_id
    type: string
    required: true
  region:
    name: region
    type: string
    default: eastus

connections:
  bronze:
    type: onelake
    workspace: "${param.workspace_id}"   # required → from runtime
    lakehouse: "lh-${param.region}"      # default → "lh-eastus"
```

Per-param resolution precedence:

1. Runtime value supplied via `Context(params=...)` or
   `load_config(runtime_params=...)`.
2. The `default` declared on the `ParamSpec`.
3. `ConfigSchemaError` if the param is `required: true`.

Declared params in a loom or weave are reachable as `${param.x}` only
**within the same file's scope**, and only when the file is loaded at
the top level (via `load_config(...)`, `ctx.load(...)`, or `ctx.run(...)`). Two
related limitations follow from this:

1. **Cross-scope inheritance** — child weaves and threads do not
   automatically inherit a parent loom's declared params; a reference
   to `${param.x}` in a child without a local declaration raises a
   `VariableResolutionError`.
2. **Child-config own-scope** — a weave that declares its own
   `params:` and is loaded as a child via a parent loom's `ref:` does
   not have that `params:` block wired into `${param.x}` either. The
   fix that wires declared `params:` into the `${param.x}` namespace
   applies only to top-level loads, not to child configs resolved
   during reference-loading.

Cross-scope cascade and child-config own-scope wiring are planned
follow-ups; until then, declare the param in each top-level scope that
references it, or use `${var.x}` / `defaults:` for cascading values.

`${param.x}` references inside a thread template instantiated via
`ThreadEntry.params` (the `as:` + `params:` form on a weave's `threads:`
list) are unaffected by this rule — they resolve from the entry's
params block as before.

---

## Further reading

- [Configuration Keys](../reference/configuration-keys.md) — Complete field
  reference for all config models
- [Thread, Weave, Loom](../concepts/thread-weave-loom.md) — The three-level
  hierarchy and inheritance model
