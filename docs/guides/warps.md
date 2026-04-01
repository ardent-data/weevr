# Warps

Warps are typed configuration files (`.warp`) that declare the
intended shape of a target table. They enable contract validation,
pre-initialization, stub columns, and documentation independent of
pipeline execution.

## When to use warps

| Scenario | How warps help |
|----------|----------------|
| Prevent accidental type changes | Enforcement catches mismatches |
| Seed an empty table before first run | Pre-initialization from warp |
| Document the expected schema | Human and machine-readable contract |
| Track schema evolution | Auto-generation captures drift columns |
| Add columns not in the pipeline | Warp-only columns appended at write |

## Warp file structure

A `.warp` file is a YAML document with `config_version: "1.0"` and
a list of column declarations.

```yaml
config_version: "1.0"
description: "Customer dimension schema contract"

columns:
  - name: customer_sk
    type: bigint
    nullable: false
    description: "Surrogate key"
  - name: customer_id
    type: string
    nullable: false
  - name: customer_name
    type: string
  - name: email
    type: string
    default: "unknown"

keys:
  surrogate: customer_sk
  business:
    - customer_id
```

### Column fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | Yes | — | Column name in the target table |
| `type` | Yes | — | Spark SQL type string |
| `nullable` | No | `true` | Whether the column allows nulls |
| `default` | No | `null` | Default value for warp-only append |
| `description` | No | — | Human-readable column description |
| `discovered` | No | `false` | Set by adaptive drift auto-gen |

### Keys section

The `keys` section is documentation metadata. It identifies
surrogate and business keys for downstream tooling and plan
output. Keys are not enforced at runtime.

## Discovery

weevr uses hybrid discovery to find warp files for a thread:

1. **Explicit reference** — set `target.warp` to the warp file
   name (with or without `.warp` extension). Searched in the
   thread directory first, then the config root.
2. **Auto-discovery** — when `target.warp` is not set, weevr
   looks for a `.warp` file matching the target alias (last
   dot segment) in the thread directory and config root.
3. **Opt-out** — set `target.warp: false` to disable discovery.

```yaml
# Explicit reference
target:
  alias: gold.dim_customer
  warp: dim_customer

# Auto-discovery (finds dim_customer.warp automatically)
target:
  alias: gold.dim_customer

# Opt-out
target:
  alias: gold.dim_customer
  warp: false
```

## Enforcement

Warp enforcement validates the pipeline output against the warp
contract. Three modes are available:

| Mode | Behavior |
|------|----------|
| `warn` (default) | Log findings, continue execution |
| `enforce` | Fail the thread on any violation |
| `off` | Skip validation entirely |

Set the mode on the target or cascade from defaults:

```yaml
target:
  alias: gold.dim_customer
  warp_enforcement: enforce

# Or cascade from loom/weave defaults
defaults:
  target:
    warp_enforcement: warn
```

### Findings

Enforcement checks three categories:

- **Missing columns** — warp declares a column not in the output
- **Type mismatches** — output column type differs from warp
- **Nullable violations** — nulls in a non-nullable warp column

All findings are reported together (no short-circuit). In
`enforce` mode, all findings are included in the error.

## Effective warp

The effective warp merges three column sets:

1. **Declared** — columns from the warp file
2. **Warp-only** — warp columns absent from pipeline output
   (appended with default or null)
3. **Engine** — audit, SCD, and soft-delete columns managed by
   the engine

Warp-only columns are appended after enforcement, before write.
Engine columns are excluded from enforcement checks.

## Auto-generation

Set `warp_mode: auto` to write or update the `.warp` file after
each successful run. The generated file includes `auto_generated:
true` as a marker.

```yaml
target:
  alias: gold.dim_customer
  warp_mode: auto
```

When combined with `schema_drift: adaptive`, new pipeline columns
are marked with `discovered: true` in the generated warp.

## Pre-initialization

Set `warp_init: true` to create the Delta table from the
effective warp before the first pipeline run. Requires a warp.

```yaml
target:
  alias: gold.dim_customer
  warp: dim_customer
  warp_init: true
```

Pre-initialization is a no-op if the table already exists.

For dimension targets, SCD columns (effective date, current flag,
etc.) are engine-managed and not known at pre-init time. Declare
them in the warp if using `warp_init` with dimension targets.

## Configuration reference

| Key | Level | Default | Description |
|-----|-------|---------|-------------|
| `target.warp` | Thread | `null` | Warp reference or opt-out |
| `target.warp_mode` | Thread | `null` | `auto` for auto-gen |
| `target.warp_init` | Thread | `false` | Pre-init from warp |
| `target.warp_enforcement` | Cascadable | `warn` | Enforcement mode |
