# Config Resolution

This guide explains how weevr transforms raw YAML files into validated,
typed configuration models. The resolution pipeline is a multi-stage process
that parses, validates, resolves, inherits, and hydrates configuration.

## Introduction

When you call `Context.run()` or `Context.load()`, weevr runs the full
config resolution pipeline internally. The stages are designed to fail
fast — syntax errors are caught before variable resolution, schema errors
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
  whole-value, not deep merge.
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

1. **Parse YAML** — Read the file and produce a raw dictionary. File-not-found
   and YAML syntax errors raise `ConfigParseError`.
2. **Extract version** — Read `config_version` and parse into a `(major, minor)`
   tuple. Unsupported versions raise `ConfigVersionError`.
3. **Detect type** — Determine whether the file is a thread, weave, loom, or
   params file. Typed extensions (`.thread`, `.weave`, `.loom`) are checked
   first; if the extension is `.yaml` or `.yml`, the structure is inspected.
4. **Validate schema** — Run the raw dictionary through a Pydantic model for
   loose structural validation. At this stage, `${var}` placeholders are
   permitted (the schema uses `extra="allow"`), so unresolved variables do not
   cause errors.

### Stage 5–6: Resolve values

1. **Build parameter context** — Merge runtime parameters (highest priority)
   over config-declared defaults (lowest priority) into a flat context
   dictionary. Dotted keys like `env.lakehouse` support nested access.
2. **Resolve variables** — Recursively walk all strings in the config and
   replace `${var}` references with values from the parameter context.
   Unresolved variables without defaults raise `VariableResolutionError`.

### Stage 7: Load child configs

**Resolve references** — For looms and weaves, load child config files:

- Loom → loads referenced weaves
- Weave → loads referenced threads

Each child is itself run through the full pipeline (recursion). A `visited`
set tracks loaded paths to detect circular references. Entry-level
overrides like `dependencies` and `condition` are preserved alongside the
loaded config.

### Stage 8–9: Inherit and expand

1. **Apply inheritance** — For multi-level configs, cascade defaults:
     - Loom defaults merge into weave defaults
     - Merged defaults merge into each thread config
     - Thread values always win (most specific)

   Naming configuration cascades separately as a whole block rather than
   per-field. Audit columns (`audit_columns`) and **exports** also cascade
   separately using **additive merge** — each level extends the set, and
   same-named entries at a lower level override the definition from the
   higher level. Exports with `enabled: false` are removed after merge.

2. **Expand macros** — `foreach` blocks in thread step lists are expanded
   into repeated sequences. This happens after variable resolution so that
   `foreach.values` can reference parameters.

### Stage 10: Hydrate

**Hydrate model** — The fully resolved dictionary is validated through
the typed Pydantic model (`Thread`, `Weave`, or `Loom`). Semantic errors
at this stage raise `ModelValidationError`. Incremental-mode constraints
(e.g., CDC requires merge write mode) are checked as post-resolution
cross-cutting validations.

## Module map

| Module | Responsibility |
|--------|----------------|
| `config/parser.py` | YAML parsing, version extraction, typed extension detection |
| `config/resolver.py` | Variable interpolation, reference loading, circular dependency detection |
| `config/inheritance.py` | Three-level cascade: loom → weave → thread |
| `config/validation.py` | Pre-resolution schema validation, post-resolution constraint checks |
| `config/macros.py` | `foreach` block expansion in step lists |
| `config/__init__.py` | `load_config()` — orchestrates the full pipeline |

## Deferred variable namespaces

Some variable namespaces are **not** resolved during config loading. They are
preserved as literal `${...}` placeholders and resolved later at execution time:

| Namespace | Resolved at | Used by |
|-----------|-------------|---------|
| `${var.*}` | Weave execution (VariableContext) | Thread steps, sources |
| `${run.timestamp}` | Thread execution | Audit columns, export paths |
| `${run.id}` | Thread execution | Audit columns, export paths |

The `run.*` namespace provides per-execution context: `${run.timestamp}` is an
ISO 8601 UTC timestamp and `${run.id}` is a UUID4, both generated once per
`execute_thread()` call. They are available in audit column expressions and
export path templates.

## Design decisions

- **Pre-resolution loose schema** — Schema validation runs before variable
  resolution, using `extra="allow"` to tolerate `${...}` placeholders. This
  catches structural errors early without requiring all parameters to be
  available.
- **Whole-value replacement** — Inheritance uses simple replacement, not deep
  merge. A thread-level `write` block replaces the entire inherited `write`
  block, not individual fields within it. This keeps behavior predictable.
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

---

## Further reading

- [Configuration Keys](../reference/configuration-keys.md) — Complete field
  reference for all config models
- [Thread, Weave, Loom](../concepts/thread-weave-loom.md) — The three-level
  hierarchy and inheritance model
