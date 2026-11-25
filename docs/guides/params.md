# Params

Params are the named values a config reads as `${param.x}`: an environment
name, a lakehouse, a run date, a table to load. The same thread, weave, and
loom files run in every workspace; only the params change. This guide covers
how a param gets its value, how params differ from the other `${...}`
namespaces, and how to move a v1 config off bare `${x}` references.

## Namespaces at a glance

Every `${...}` reference names its namespace. Two namespaces are bound when
the config loads; the rest are left in place and filled in while the run
executes.

| Kind | References | Bound at | Value comes from | See |
|---|---|---|---|---|
| Param | `${param.x}` | Load | Runtime params, entry pins, declared defaults | This page |
| Environment | `${fabric.workspace_id}`, `${fabric.lakehouse_id}`, `${fabric.workspace_name}` | Load | The Fabric session (`Context`), or `fabric_context=` on the core loaders | [Fabric values](#fabric-values) |
| Run context | `${run.id}`, `${run.timestamp}`, `${thread.name}`, `${thread.qualified_key}`, `${thread.source}`, `${thread.sources}`, `${weave.name}`, `${loom.name}` | Execution | The engine, for each thread it runs | [Deferred variable namespaces](config-resolution.md#deferred-variable-namespaces) |
| Variable | `${var.x}` | Execution | Hook steps (`set_var`) or the variable's `default` | [Weave schema: variables](../reference/yaml-schema/weave.md#variables-variablespec) |

How a load treats each reference:

- `${param.x}` and `${fabric.x}` are replaced with their values. Whole-value
  and embedded references behave as described in
  [Parameter resolution](config-resolution.md#parameter-resolution).
- `${var.x}` and `${run.x}` are left in place for execution. The hierarchy
  names (`${thread.*}`, `${weave.*}`, `${loom.*}`) are left in place too, but
  only the keys in the table exist: `${thread.nme}` fails with `P112` and
  suggests `${thread.name}`. A `:-` fallback on a hierarchy reference is used
  at load instead.
- A bare `${x}` fails with `P111`. The message gives the rewrite, such as
  `${param.x}` or `${fabric.workspace_id}`.
- An unknown namespace (`${parm.x}`), or an unknown Fabric key, fails with
  `P112` and suggests the nearest name.
- `defaults:` values are not variables. A `${x}` that names a `defaults:` key
  fails with `P111`, and the hint suggests declaring `x` as a param with that
  default.

A param's value can be a mapping, and a dotted reference reads into it:
`${param.env.lakehouse}` reads the `lakehouse` key of the `env` param.

## Supplying values

A param gets its value from one of four places:

- **Runtime params** — what the invoker passes: `Context(params=...)` in a
  notebook, or `runtime_params=` on `load_config()` and `load_resolved()`.
- **Entry pins** — the `params:` of the `ref:` entry that pulls a weave into a
  loom, or a thread into a weave. See [Pins](#pins).
- **Declared defaults** — the `default` of a `params:` declaration in the
  config or one above it.
- **Inline fallbacks** — `${param.x:-fallback}` on the reference itself. A
  fallback is always a string, even in a whole-value position.

## Binding precedence

Each config opens a param scope from its parent's: a thread's from its
weave's, a weave's from its loom's. The config you load is the run root,
where the runtime params bind. For each name in a scope, the first of these
that applies wins:

1. **A pin on the entry** that pulled this config in.
2. **A value supplied above** this scope, flowing down unchanged: the runtime
   params at the run root, or a pin on an entry higher up.
3. **The nearest declared default.** A child that declares the name again
   shadows its parent's declaration, so its own default wins over the parent's.
4. **The reference's `:-` fallback**, `${param.x:-fallback}`.
5. **No value:** a declared param that is required and has no default fails
   with `P101` at its declaration. Any other unbound reference fails with
   `P110` where it is written.

Three consequences:

- A supplied value always beats a default. A runtime param overrides the
  default of a declaration anywhere in the tree.
- A pin beats the runtime params within the entry's subtree. A pinned literal
  is not overridable at run time.
- A binding never changes after its scope opens.

```yaml
# silver.weave
params:
  env:
    name: env
    type: string
    default: dev          # used unless the invoker supplies env
threads:
  - ref: orders.thread
    params:
      schema: sales       # pinned: beats a runtime "schema" for this thread
```

```yaml
# orders.thread
params:
  schema:
    name: schema
    type: string
    default: raw          # used by any entry that doesn't pin schema
sources:
  orders:
    type: delta
    alias: ${param.env}_${param.schema}.orders
```

`Context(spark, project, params={"env": "prod", "schema": "x"})` runs the
thread against `prod_sales.orders`: `env` comes from the runtime params, and
the pin keeps `schema` at `sales`. The unused runtime value for `schema` is not
an error, because the thread declares `schema`.

## Pins

A pin is a value on the entry that pulls a config in. Both entry types take
them:

- A loom's weave entry: `weaves: [{ref: bronze.weave, params: {...}}]` pins
  values for the weave and every thread under it.
- A weave's thread entry: `threads: [{ref: orders.thread, params: {...}}]`
  pins values for that thread.

Pins apply to `ref:` entries only. On an inline weave or thread (a `name` plus
a body), `params:` is the inline config's own declarations, a map of
`ParamSpec`s, not pins.

A pin value is resolved in the parent's scope, where the entry is written, so
it can compose other params. An entry's `condition` resolves in the parent's
scope too.

```yaml
threads:
  - ref: silver/sap_table.thread
    as: sap_adr6
    params:
      target_schema: "${param.env}_silver"   # env as the weave sees it
      table_name: ADR6
```

Two warnings flag pins that don't do what they look like:

- `P123` — nothing beneath the entry reads or declares the pinned name. Remove
  the pin, or reference it below the entry.
- `P122` — a registry asset declared above the entry reads the pinned name.
  The asset already resolved in its own scope, so the pin does not change it.
  See [Registry assets](#registry-assets-resolve-where-they-are-declared).

Both stay warnings under `strict_params`.

## Declaring params

A `params:` block on a loom, weave, or thread declares the params that config
and the configs below it read. Each entry is a `ParamSpec` with a `name` and a
`type`, and optionally `required` (default `true`), `default`, and
`description`; see the
[thread schema](../reference/yaml-schema/thread.md#params) for the fields.

```yaml
params:
  run_date:
    name: run_date
    type: date
    required: true
    description: "Business date to load"
  region:
    name: region
    type: string
    default: eastus
```

Declaration is optional: an undeclared `${param.x}` still binds from a runtime
param or a pin. Declaring a param adds:

- type checking and coercion of the value (see [Type coercion](#type-coercion));
- a `P101` error at load when a required param has no value, instead of a
  failure where it is read;
- a default for the config and everything below it;
- an entry in the [param manifest](#inspecting-bindings), so tools and
  pipeline authors can see what a config needs.

A declaration covers the config it is in and every config below it, so a
param declared once on the loom covers all of its weaves and threads.

Two warnings point at missing declarations:

- `P120` — a `${param.x}` reference is bound, but no declaration in its scope
  chain covers it. The hint is a `params:` block you can paste, typed by the
  bound value. A mapping-valued param, such as `env` read as
  `${param.env.lakehouse}`, has no `ParamSpec` type, so it stays undeclared
  and its references warn.
- `P121` — a runtime param is supplied that nothing in the loaded config
  declares or references. Each load checks this on its own, so a
  `Context(params=...)` shared across several loads warns for each key a given
  config doesn't use. Declaring the shared names silences it: a declared param
  counts as used.

### Strict params

`strict_params: true` on a loom, weave, or thread turns `P120` and `P113` (a
deferred Fabric reference) into errors for references in that config and the
configs below it. Each reference follows the nearest setting: a thread can
set `strict_params: false` under a strict weave.

`P121` is judged once per load, against the runtime params, so it follows
only the setting of the config you load: `strict_params: true` on the loaded
loom makes it an error, while the same key on a weave or thread below has no
effect on it. When nothing sets it, a load is lenient.

`Context`, `load_config()`, and `load_resolved()` take a `strict_params=`
argument that overrides every document: `True` makes the whole load strict,
`False` keeps the warnings even where a document sets `strict_params: true`.
`P122` and `P123` stay warnings either way.

```yaml
kind: weave
strict_params: true
```

## Type coercion

Each value bound for a declared name is checked against the declaration's
`type`, whether it came from the runtime params, a pin, or a default. The
check is narrow: strings from a pipeline are converted where the table says
so, and nothing else is guessed.

| `type` | Accepts | Binds as |
|---|---|---|
| `string` | A string | The string. A number is rejected. |
| `int` | An integer, or a base-10 integer string (`"42"`, `"-3"`) | An integer |
| `float` | A finite float, an integer, or a numeric string (`"1.5"`, `"2e3"`) | A float |
| `bool` | A boolean, or `"true"` / `"false"` in any case | A boolean |
| `date` | A `YYYY-MM-DD` string (`2024-1-5` is accepted), or a date | The ISO string, as written |
| `timestamp` | An ISO 8601 string (date-only, space separator, compact and week forms included), or a datetime | The ISO string, as written |
| `list[string]` | A list of strings | The list |

A boolean is never accepted as an `int` or `float`. Dates and timestamps bind
as strings, so `T` and `Z` survive in embedded references.

The coerced value is what a whole-value reference (`field: ${param.x}`)
reads. A reference embedded in a larger string keeps the text as supplied:
`"007"` declared `int` embeds as `007`, and `"TRUE"` declared `bool` as
`TRUE`. A boolean passed from Python or written in YAML embeds as `true` or
`false`.

A value the type rejects fails with `P102`; a malformed date or timestamp
string fails with `P103`. Both errors name the declaring file and point at
`/params/<name>`. When a child declares the name again with another type, the
child's declaration checks the value as supplied.

## Registry assets resolve where they are declared

Connections, lookups, column sets, audit templates, and variables are
registry assets. A `${param.x}` inside one resolves once, in the scope of the
config that declares the asset, with that scope's bindings. A pin on an entry
below does not retarget it, and the load warns `P122`.

To specialize an asset for one entry, declare it again under the same name
below the entry. The lower declaration shadows the upper one and resolves in
its own scope:

```yaml
# conn.weave
params:
  lakehouse:
    name: lakehouse
    type: string
    default: dev_lakehouse
connections:
  main:
    type: onelake
    workspace: ${fabric.workspace_id}
    lakehouse: ${param.lakehouse}   # resolves here: dev_lakehouse
threads:
  - ref: orders.thread
    params:
      lakehouse: prod_lakehouse    # P122: does not change connections.main
```

```yaml
# orders.thread: shadow the connection to use the pinned value
connections:
  main:
    type: onelake
    workspace: ${fabric.workspace_id}
    lakehouse: ${param.lakehouse}   # resolves in the thread: prod_lakehouse
```

## Fabric values

`${fabric.workspace_id}`, `${fabric.lakehouse_id}`, and
`${fabric.workspace_name}` describe where the run is. Runtime params never
supply them: a runtime param named `fabric` is an ordinary param.

- **`Context`** always has a Fabric context. It reads the values from the
  Spark session. A value the session doesn't have fails with `P110` unless the
  reference has a `:-` fallback.
- **`load_config()` and `load_resolved()`** take `fabric_context=`:
    - `None` (the default) means no Fabric context. Each `${fabric.x}` without a
      `:-` fallback is left in place and the load warns `P113`, so a config
      loads offline, in a CLI or a CI step. Under `strict_params`, `P113` is an
      error.
    - A mapping is the context, keyed `workspace_id`, `lakehouse_id`, and
      `workspace_name` (a `fabric.` prefix is accepted). A key it lacks fails
      with `P110` unless the reference has a fallback; a key outside those
      three fails with `P112`.

```python
from weevr.config import load_resolved

loaded = load_resolved(
    "project.weevr/nightly.loom",
    runtime_params={"run_date": "2026-01-31"},
    fabric_context={"workspace_id": "a1b2c3d4-0000-0000-0000-111111111111"},
)
```

## When a reference doesn't resolve

The param diagnostics, all listed in
[Diagnostic Codes](../reference/diagnostics.md):

| Code | Severity | Meaning |
|---|---|---|
| `P101` | Error | A required declared param has no value and no default |
| `P102` | Error | A value does not fit the declared type |
| `P103` | Error | A date or timestamp string is not valid ISO |
| `P110` | Error | A reference has no value and no fallback |
| `P111` | Error | A bare `${x}` names no namespace |
| `P112` | Error | Unknown namespace, hierarchy key, or Fabric key |
| `P113` | Warning (error when strict) | `${fabric.x}` left in place: the load has no Fabric context |
| `P120` | Warning (error when strict) | A bound reference no declaration covers |
| `P121` | Warning (error when strict) | A runtime param nothing declares or references |
| `P122` | Warning | A pin a registry asset above the entry reads |
| `P123` | Warning | A pin nothing beneath the entry reads or declares |

A `P110` error says what was in scope: it lists the params bound or declared
there as `expected`, suggests the nearest of those names when the reference
looks like a typo, and gives a `params:` declaration and a `Context(params=...)` call to
fix it. For a param declared `required: false` with no default, it instead
says to supply a value or add a default, naming the files from the
declaration down to the reference.

## Inspecting bindings

Two read-only views show where values came from and what a config needs.
Neither holds param values.

- **Provenance.** Each node of `LoadedConfig.document` has
  `provenance.params`: for each param, the value's source (`runtime`, `pin`,
  `inherited`, `default`, or `fallback`), the declaration that won (tier,
  file, and pointer), and the pinning entry when the value traces to a pin.
  See [The document tree](config-resolution.md#the-document-tree).
- **Param manifest.** `weevr.config.param_manifest(path)` reads a config and
  everything it pulls in, without values and without Spark, and returns every
  declaration, the names the invoker must supply, and the names used without
  a declaration. `Context.run(mode="validate")` returns the same manifest as
  `RunResult.param_manifest`, even when the load fails.

API details are in the [Config API](../reference/api/config.md) and
[Context & Results](../reference/api/context.md) references.

## Moving off bare references

v1 configs could write `${x}` with no namespace. Rules 6 to 10 of the
[migration rules](../reference/migration-rules.md#6-name-the-namespace-of-a-bare-reference)
give the rewrite for each: a declared param or a `defaults:` key becomes
`${param.x}` (the key moves into `params:`), a Fabric key becomes
`${fabric.x}`, and any other name becomes `${param.x}` and needs a declaration
to stop warning `P120`. The `P111` error on a bare reference suggests the same
rewrite.
