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

- **Variable interpolation** — `${param.x}` and `${fabric.x}` placeholders
  in YAML values are replaced with concrete values from params and the Fabric
  context. Default values use the `${param.x:-default}` syntax. Every
  reference names its namespace; a bare `${x}` fails at load.
- **Param scopes** — Each loom, weave, and thread opens a param scope from
  its parent's. Runtime params bind at the config you load and flow down;
  an entry's `params:` pins values for the config it pulls in; a declared
  `default` fills a name nobody supplied. See the [Params guide](params.md).
- **Reference resolution** — `ref:` entries in weave and loom configs load
  child configuration files recursively. Circular references are detected
  and rejected.
- **Inheritance cascade** — Defaults flow downward: project defaults → loom
  defaults → weave defaults → thread config. The most specific value wins.
  Replacement is whole-value, not deep merge, except for the special cascades
  described in Stage 8 (naming, audit columns, exports, connections, target
  policies). The [cascade table](#cascade-by-key) lists every key.
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
  param_ctx: "5. Open param scope"
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
   loose structural validation. At this stage, `${...}` placeholders are
   permitted (the schema uses `extra="allow"`), so unresolved variables do not
   cause errors.

### Stage 5–6: Resolve values

- **Stage 5 — Open the param scope.** Each config gets the values its
   `${param.x}` references read. The config you load is the run root: its
   scope binds the runtime parameters, then fills each name it declares in
   `params:` and nobody supplied from the declared `default`. A required
   declared param with no value fails with `P101`. Values bound for a
   declared name are checked and coerced against its `type` (`P102`,
   `P103`). The `${fabric.*}` namespace comes from the Fabric context, when
   the load has one. `defaults:` values are not variables. The binding rules
   are in the [Params guide](params.md#binding-precedence).
- **Stage 6 — Resolve variables.** Recursively walk all values in the config and
   resolve `${param.x}` and `${fabric.x}` references. Whole-value
   references return the native type; embedded references render the
   value's text (see [Parameter resolution](#parameter-resolution) below).
   Dotted keys like `${param.env.lakehouse}` read into a mapping value.
   Unresolved variables without defaults raise
   `VariableResolutionError` (`P110`). `${var.*}`, `${run.*}`, and the
   hierarchy variables are left in place for execution (see
   [Deferred variable namespaces](#deferred-variable-namespaces)). An inline
   weave or thread resolves in a scope of its own, opened as in Stage 7.

### Stage 7: Load child configs

**Resolve references** — For looms and weaves, load child config files:

- Loom → loads referenced weaves
- Weave → loads referenced threads

Each child is itself run through the full pipeline (recursion), in a param
scope opened from its parent's. Values supplied above (runtime params, and
pins on entries higher up) flow into the child unchanged; the entry's own
`params:` pins beat them for the child's subtree; and the child's
`params:` declarations add defaults, a child's re-declaration shadowing
the parent's. The entry's pin values and its `condition` resolve in the
parent's scope, where the entry is written. A `visited`
set tracks loaded paths to detect circular references. Each child travels
with the entry that pulled it in (`ref`, `as`, `params`, `dependencies`,
`condition`), the file it is written in, its config as written before
variable substitution, and where each of its param values came from (its
provenance). Nothing is added to the config itself.

### Stage 8–9: Inherit and expand

- **Stage 8 — Apply inheritance.** First, each thread's own `write` and
  `keys` blocks are checked against the dimension override rules, before
  any loom or weave defaults are merged in, so inherited defaults never
  trip them. Then, for multi-level configs, cascade defaults:
    - Project defaults (`weevr.yaml`) merge into loom defaults
    - Loom defaults merge into weave defaults
    - Merged defaults merge into each thread config
    - Thread values always win (most specific)

    A thread loaded on its own still cascades through the project tier.
    It never sees a loom or weave it isn't run through.

    Naming configuration cascades separately as a whole block rather
    than per-field. Audit columns (`audit_columns`), **exports**, and
    **connections** also cascade separately using **additive merge** —
    each level extends the set, and same-named entries at a lower
    level override the definition from the higher level. Exports
    with `enabled: false` are removed after merge.

    Target policies — `schema_drift`, `on_drift`, and `warp_enforcement`
    under `defaults.target` — cascade per key from loom to weave to the
    thread's target, and the most specific level wins.

    Named registries — `lookups`, `column_sets`, and `variables` — merge
    by name from project to loom to weave to thread (the project declares
    no variables): each scope sees the outer
    scopes' entries plus its own, and its own win a name clash. The
    merged registries are computed once, here, and recorded on the
    [document tree](#the-document-tree); plan, preview, validate, and
    execution all read them, so they can't disagree about which
    definition a name means. Merging only decides which definition a
    name resolves to: a lookup still materializes once, in the scope
    that declares it.

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
file's path relative to the project root. Relative `path` values on file
sources (CSV, JSON, Parquet) are resolved against the project root,
both a thread's sources and the source of every lookup, whether the thread,
weave, loom, or project declares it. The `path` of a YAML column set at any level
resolves the same way. The fully resolved dictionary
is then validated through the typed Pydantic model (`Thread`, `Weave`,
or `Loom`). Semantic errors at this stage raise `ModelValidationError`.
For a weave or loom, every child it pulls in, whether referenced or
inline, is hydrated and validated the same way, so a broken child fails
the load. The error's `file_path` names the child's own file, or
the file containing an inline definition.
Finally, incremental-mode constraints that span `load` and `write` (for
example, CDC requires merge write mode, and watermark loads reject the
default `overwrite`) are checked on every thread the config pulls in,
after the cascade, so defaults count. A violation raises a `ConfigError`
naming the thread and the file it is written in, from every entry point
and in every `Context` mode. Advisory findings join the load warnings. See
[When load rules are checked](../reference/yaml-schema/thread.md#when-load-rules-are-checked).

### Every problem, located

A load doesn't stop at the first broken file. A child that fails a stage
is left out of the later stages, and its siblings carry on, so one load
reports every problem in the project in one `ConfigError`. Each problem is
a diagnostic with a stable code (see
[Diagnostic Codes](../reference/diagnostics.md)) and where it was written:
the authored `file`, a JSON `pointer`, and the `line` and `column`.

A value a thread inherits from a weave or loom default isn't written in
the thread's file. Its diagnostic names the thread's file and pointer
with no line, and sets `location_fallback`. The same flag marks a value
that came from a `${…}` reference (the line is the reference's), and a
problem under a step list that `foreach` expanded, where step indexes no
longer match the file, so the diagnostic points at the step list.

A key the removed-key table lists fails with `C111` (removed) or `C112`
(renamed), naming what to write instead.

## Cascade by key

Four tiers contribute to a thread: the **project** (`weevr.yaml`), the
**loom**, the **weave**, and the thread itself. Each tier's `defaults:` block
and its named registries cascade down. The nearest tier wins, and a thread's
own keys beat every tier's `defaults:`.

Values (params) and shape (declarations) travel differently. Params bind per
scope and flow down by name. Declarations such as connections, audit
templates, lookups, column sets, and `defaults:` cascade as written here.

### `defaults:` keys

| Key | Behavior | Lands in |
|---|---|---|
| any other key (for example `write`) | Replace: the nearest tier that sets the key supplies the whole value; no deep merge | the thread's own key |
| `naming` | Replace: the nearest tier's block, used only when the thread's `target` sets no `naming` | `target.naming` |
| `target.audit_columns` | Merge by column name, outermost tier first, then the thread's own; a nearer tier wins a name clash. The thread's `audit_template_inherit: false` also drops every outer tier's inline columns, and `audit_columns_exclude` removes matching columns from the result | `target.audit_columns` |
| `target.audit_template` | Merge: each tier's references resolve to columns and apply outermost first, before that tier's own `audit_columns`; the thread's `audit_template_inherit: false` drops every tier's | `target.audit_columns` |
| `target.schema_drift`, `target.on_drift`, `target.warp_enforcement` | Per-key merge: the nearest tier that sets the key wins; the thread's own `target` beats every tier | the same key on `target` |
| `exports` | Merge by export name: a nearer tier's export of the same name replaces the whole entry; one with `enabled: false` is removed | `exports` |
| other `target.*` keys | Replace: a thread always has its own `target`, which replaces the tier's block, so only the keys above reach it | not inherited |

### Registries

| Registry | Behavior | Lands in |
|---|---|---|
| `connections` | Merge by name: the nearest tier's definition wins | the thread's `connections` |
| `audit_templates` | Merge by name: every tier's definitions are visible to every tier's references, the nearest definition of a name winning | consumed by the audit merge; not kept on the thread |
| `lookups` | Merge by name, the project outermost: inner names shadow outer ones | `effective.lookups` on each document node |
| `column_sets` | As `lookups` | `effective.column_sets` |
| `variables` | As `lookups`, from loom downward; the project tier rejects `variables:` | `effective.variables` |
| `params` | Not cascaded as a registry: each scope's declarations bind by name, a nearer declaration shadowing an outer one (see [Params](params.md)) | the `${param.x}` namespace |

Two behaviors follow from the table:

- The project's connections, audit templates, and `defaults:` apply to every
  loom, weave, and thread in the project, and so does the project's
  `params:` block. A required project param with no default and no value fails
  every load in the project with `P101`, just as a required loom param fails
  its loom. A project declaration counts as use for `P121`.
- A project lookup or column set is the outermost layer of every config's
  effective registries, so the load-time reference checks (`R120`, `R122`,
  `R124`) see it without Spark. At run time it is read only when a running
  thread names it and the name resolves to the project's: once at loom start
  under a loom, at weave level for a standalone weave, and on demand for a
  standalone thread.

Provenance records the tier behind each entry. For each connection, audit
template, lookup, column set, variable, and top-level `defaults` key,
`ResolvedDocument.provenance` holds a `RegistryDeclaration` (exported from
`weevr.config`) giving the declaring tier, file, and pointer. See
[The document tree](#the-document-tree).

## Loading without running

`Context` executes; `load_config()` and `load_resolved()` inspect. All three
run the same pipeline for `.thread`, `.weave`, and `.loom` files, so a typed
config loads the same way through each. Two differences: only `load_config()`
also accepts `.yaml` and params files, and `Context` always has a Fabric
context, read from the Spark session, while the other two have one only when
you pass `fabric_context=`. Beyond that, the difference is what comes back and
what's needed:

| Entry point | Needs Spark | Returns |
|---|---|---|
| `Context.load()` | Yes | `LoadedConfig`, with `${fabric.*}` resolved |
| `load_config()` | No | The `Thread`, `Weave`, or `Loom` model (or a params dict); `${fabric.*}` resolved from `fabric_context=`, else left in place |
| `load_resolved()` | No | `LoadedConfig`, with the resolved config tree; `${fabric.*}` as for `load_config()` |

`load_resolved()` suits tools that check or plan configs outside the engine,
such as a CLI or a CI step. When you load a bare file without `project_root`,
weevr walks up from it for `weevr.yaml`; if none is found, the file's folder
is the root and there is no project. It takes `.thread`, `.weave`, and `.loom` files
and returns the hydrated model, every child thread and weave, the load
warnings (as diagnostics), and the execution plan:

```python
from pathlib import Path

from weevr.config import load_resolved

project = Path("project.weevr")
loaded = load_resolved(project / "nightly.loom", project_root=project)

for weave_name, threads in loaded.document.weave_threads().items():
    print(weave_name, list(threads))

for warning in loaded.diagnostics:
    print(warning)  # C491 thread-execution-ignored: ...

for issue in loaded.validate_plans():
    print(issue.diagnostic)  # D101 circular-dependency: ...
```

`validate_plans()` builds every weave's execution plan and returns one
`PlanIssue` per weave that fails — a dependency cycle, two threads writing
the same target without an ordering, or a Lookup problem — instead of
stopping at the first; each issue's `diagnostic` carries its code.
`execution_plan` raises on the first failure. `LoadedConfig.warnings` is
deprecated: it returns the same strings as before and will be removed in
weevr 3.0.

A source `lookup:` must name a lookup the thread can see. Through a weave or
loom, an undefined name fails the load. A thread file loaded on its own can't
see those parents, so a name it doesn't define itself adds a warning to
`LoadedConfig.diagnostics` instead: the thread is valid in a weave that defines
the lookup, but running it alone fails. A `resolve` step's `lookup` is stricter:
it fails the load even when the thread is loaded on its own.

Without Spark there is no Fabric context unless you pass one. With no
`fabric_context=`, `load_config()` and `load_resolved()` leave each
`${fabric.*}` reference that has no `:-` default in place and warn `P113`
(an error under `strict_params`); pass `fabric_context={"workspace_id": ...}`
to resolve them. See [Fabric values](params.md#fabric-values). Checks that
read data, such as whether a source table exists, run only in
`Context.run(mode="validate")`.

!!! note
    `LoadedConfig.threads` and `.weaves` return the model's own entries
    (`ThreadEntry` / `WeaveEntry`). The resolved models are in the
    document tree, below.

### The document tree

`LoadedConfig.document` is a `ResolvedDocument`: the loaded config and
everything it pulls in, as a frozen tree. Each node has:

| Field | What it holds |
|---|---|
| `model` | The hydrated `Thread`, `Weave`, or `Loom` |
| `kind` | `"thread"`, `"weave"`, or `"loom"` |
| `name` | The effective name (`as`, else `name`, else the file stem) |
| `entry` | An `EntryMeta` for the entry that pulled it in: `ref`, `alias` (`as`), `params`, `dependencies`, `condition`. `None` for the loaded file |
| `provenance` | Where its values came from: `provenance.params` maps each param to its source (`runtime`, `pin`, `inherited`, `default`, or `fallback`), the winning declaration, and the pinning entry. `provenance.connections`, `.audit_templates`, `.lookups`, `.column_sets`, `.variables`, and `.defaults` map each name the node sees (for `defaults`, each top-level key) to a `RegistryDeclaration`: the tier that declares it (`project`, `loom`, `weave`, or `thread`), the file, and the JSON pointer. The nearest tier wins. Names and locations only, never values |
| `children` | A loom's weaves or a weave's threads, by effective name, in entry order |
| `source_ref` | The file it is written in: its own, or the file holding it inline |
| `effective` | The `lookups`, `column_sets`, and `variables` it sees, merged over every enclosing scope, the project's included |
| `declared` | The ones it declares itself |

```python
loom = loaded.document
for weave in loom.children.values():
    for thread in weave.children.values():
        print(thread.name, thread.source_ref, sorted(thread.effective.lookups or {}))
```

`document.weave_threads()` and `document.weaves()` give the thread and
weave models by name.

!!! warning "Deprecated: `resolved_threads` and `resolved_weaves`"
    `LoadedConfig.resolved_threads` and `LoadedConfig.resolved_weaves`
    still return the same data, read from the tree, but each access raises
    a `DeprecationWarning`. Use `document.weave_threads()` and
    `document.weaves()` instead. Both properties are removed in weevr 3.0.

## Module map

| Module | Responsibility |
|--------|----------------|
| `config/parser.py` | YAML parsing, version extraction, typed extension detection |
| `config/resolver.py` | Reference loading, circular dependency detection |
| `config/scoping.py` | Scoped variable resolution: opens each config's param scope and each `ref:` entry's child scope; strictness |
| `config/params.py` | Param scopes and binding precedence, type coercion, and the param warnings (`P120`–`P123`) |
| `config/param_provenance.py` | The provenance records: where each param's value came from, without values |
| `config/interpolation.py` | `${...}` namespaces and substitution; the Fabric context |
| `config/interpolation_errors.py` | The diagnostics a reference fails or is deferred with (`P110`–`P113`), with their hints |
| `config/assets.py` | Registry assets as declared, for the pin check (`P122`) |
| `config/manifest.py` | `param_manifest()`: the params a config takes, read without values |
| `config/manifest_types.py` | `ParamManifest` and `ManifestParam` |
| `config/inheritance.py` | The cascade: project → loom → weave → thread, including the named registry merge (`cascade_registry`) |
| `config/validation.py` | Pre-resolution schema validation; dimension override checks (run during load, before the cascade); lookup and column set reference checks (run during hydration); incremental cross-field checks (run at the end of the load, on every thread) |
| `config/document.py` | The resolved config tree: `ResolvedDocument`, `EntryMeta`, and each node's registries |
| `config/macros.py` | `foreach` block expansion in step lists; rejects `foreach` in `with:` sub-pipelines |
| `config/hydration.py` | Hydrates resolved dicts into typed models, including referenced and inline children |
| `config/attribution.py` | Attaches the source file, and each diagnostic's line, to config errors raised while loading it |
| `config/collect.py` | Collects every child's errors in one load and raises them together |
| `config/audit_templates.py` | Audit template presets, context variables, and template merge helpers |
| `config/onelake.py` | Spark-free OneLake path construction shared by the runtime and the planner |
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

The hierarchy variables are deferred only when no `:-default` is given; with
one, the default is used at load. An unknown property such as `${thread.nme}`
still fails at load (`P112`). `${var.*}` and `${run.*}` references are left in
place whole, unchecked at load.

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

When an entire YAML value is a single `${param.x}` reference (no surrounding
text), the resolver returns the native Python type of the referenced value.
The result is not cast to string.

```yaml
match_keys: ${param.pk_columns}
# pk_columns: ["mandt", "color"] → list[str]

enabled: ${param.flag}
# flag: true → bool

limit: ${param.max_rows}
# max_rows: 1000 → int
```

A whole-value reference that resolves to `None` returns `null` rather than
the string `"None"`:

```yaml
optional_param: ${param.value}
# value: null → None
```

### Embedded resolution

When `${param.x}` appears inside a larger string, the value's text is
substituted in place. A string keeps the text as supplied, whatever its
declared type: `"007"` declared `int` embeds as `007`. A boolean embeds as
`true` or `false`, and any other value as its string form.

```yaml
alias: "SAP.${param.table_name}"
# table_name: "MARA" → "SAP.MARA"

path: "/mnt/${param.env}/data/${param.table_name}"
# env: "prod", table_name: "MARA" → "/mnt/prod/data/MARA"
```

Embedded references that resolve to `None` produce the string `"None"`.
Use whole-value references when you need null pass-through.

### Inline defaults

The `${param.x:-default}` fallback syntax always returns the default as
a **string**, even in a whole-value position. For example,
`limit: ${param.max_rows:-100}` returns the string `"100"` when `max_rows`
is missing — not the integer `100`. To use a typed default, declare the
param with a `type` and a `default` rather than relying on the inline `:-`
fallback.

### Declared `params:` and the `${param.x}` namespace

A loom, weave, or thread that declares a `params:` block exposes each
declared name under the `${param.x}` namespace, in that config and every
config below it:

```yaml
# silver.loom
kind: loom
config_version: "2.0"
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

Declared params cascade: a declaration covers the config it is in and every
weave and thread below it, so a `${param.workspace_id}` in any of the loom's
weaves or threads reads the loom's declaration. A child that declares the same name again
shadows the parent's declaration, and its own `default` wins over the
parent's. Runtime values supplied via `Context(params=...)` or
`load_config(runtime_params=...)` beat every default; a `ref:` entry's
`params:` pins beat runtime values for that entry's subtree.

Per name, a scope's value is the first of: the entry's pin, a value supplied
above, the nearest declared default, the reference's `:-` fallback. A required
param left with none fails with `P101`. The full rules, with the type
coercion table, `strict_params`, and the warnings for undeclared and unused
params, are in the [Params guide](params.md).

---

## Further reading

- [Configuration Keys](../reference/configuration-keys.md) — Complete field
  reference for all config models
- [Thread, Weave, Loom](../concepts/thread-weave-loom.md) — The three-level
  hierarchy and inheritance model
- [Params](params.md) — How params bind, pins, declarations, and the
  namespaces
