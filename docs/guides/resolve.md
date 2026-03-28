# Resolve Step — FK Resolution

The `resolve` step encapsulates the complete foreign key
resolution pattern: validate business key completeness, join
to a dimension lookup, assign sentinel values for invalid and
unknown BKs, handle SCD2 point-in-time narrowing, and
optionally include extra columns from the lookup.

## Why resolve?

Without `resolve`, FK resolution requires chaining `join` +
`derive` + `filter` + `coalesce` steps — verbose, error-prone,
and lacking standardized sentinel handling. The `resolve` step
replaces that entire chain with one declarative block.

**Value over manual steps:**

1. Sentinel assignment replaces join + coalesce + case_when
2. BK completeness check (null/blank detection) is automatic
3. Single output column — no cleanup of extra join columns
4. Batch mode — 8 FKs in one block vs 8 join chains
5. Lookup integration — references lookups by name
6. `fk_sentinel_rate` assertion — purpose-built FK quality gate

## Single FK resolve

The simplest form maps a source column to a lookup business key
and produces a surrogate key column:

```yaml
steps:
  - resolve:
      name: plant_id
      lookup: dim_plant
      match: plant_code
      pk: id
```

`match` supports three sugar forms:

```yaml
# String — single column, same name both sides
match: natural_id
# Equivalent to: { natural_id: natural_id }

# List — multiple columns, same names
match: [mandt, plant]
# Equivalent to: { mandt: mandt, plant: plant }

# Dict — explicit mapping (different column names)
match:
  plant_code: natural_id
  region: region_code
```

## Sentinel values

Rows with incomplete or unmatched BKs receive sentinel values
instead of nulls:

| Scenario | Default | Config field |
|----------|---------|--------------|
| Null/blank source column | `-4` | `on_invalid` |
| No match in lookup | `-1` | `on_unknown` |

These align with system member codes from dimension seeding.

```yaml
- resolve:
    name: plant_id
    lookup: dim_plant
    match: plant_code
    pk: id
    on_invalid: -4
    on_unknown: -1
```

## SCD2 resolution — effective block

For slowly changing dimensions, the `effective` block narrows
the lookup to the correct version of each record.

### Current flag

Filter to the active record using a boolean or coded column:

```yaml
- resolve:
    name: product_id
    lookup: dim_product
    match: product_code
    pk: id
    effective:
      current: is_current
```

Custom value (e.g. `"Y"` instead of `true`):

```yaml
effective:
  current:
    column: is_current
    value: "Y"
```

### Date range

Half-open interval `[from, to)` checked against a fact date
column. Null `to` matches as the current record:

```yaml
- resolve:
    name: product_id
    lookup: dim_product
    match: product_code
    pk: id
    effective:
      date_column: order_date
      from: effective_from
      to: effective_to
```

## Where predicate

General SQL filter applied to the lookup before joining:

```yaml
- resolve:
    name: plant_id
    lookup: dim_plant
    match: plant_code
    pk: id
    where: "region = 'EMEA'"
```

`effective` and `where` compose with AND semantics — both
filters apply when present together.

## Include columns

Bring additional columns from the lookup into the fact:

```yaml
- resolve:
    name: plant_id
    lookup: dim_plant
    match: plant_code
    pk: id
    include:
      - description
      - category
    include_prefix: "plant_"
```

`include` supports list (keep names), dict (rename), or
string sugar (single column). `include_prefix` prepends a
string to all included column names.

## Normalization

Normalize both source and lookup BK columns before joining:

```yaml
- resolve:
    name: plant_id
    lookup: dim_plant
    match: plant_code
    pk: id
    normalize: trim_lower
```

Available presets: `trim_lower`, `trim_upper`, `trim`, `none`.

## Dropping source columns

After resolution, the original BK source columns can be dropped:

```yaml
- resolve:
    name: plant_id
    lookup: dim_plant
    match: plant_code
    pk: id
    drop_source_columns: true
```

Defaults to `false`. In batch mode, source columns are dropped
only after all FKs complete — preventing mid-batch failures
when the same source column is used by multiple resolutions.

## Duplicate handling

When multiple lookup rows match a single fact row:

```yaml
on_duplicate: warn    # default — log warning, take first
on_duplicate: error   # raise an exception
on_duplicate: first   # take first silently
```

## Batch mode

Resolve multiple FKs in one step with shared defaults:

```yaml
- resolve:
    pk: id
    on_invalid: -4
    on_unknown: -1
    batch:
      - name: plant_id
        lookup: dim_plant
        match: plant_code
      - name: customer_id
        lookup: dim_customer
        match:
          customer_code: natural_id
        normalize: trim_lower
      - name: product_id
        lookup: dim_product
        match: product_code
        effective:
          current: is_current
```

Shared defaults (`pk`, `on_invalid`, etc.) apply to all items.
Item-level values override the shared default. When
`drop_source_columns: true`, source columns are dropped only
after all FKs complete.

## on_failure handling

Control behavior when a lookup cannot be found (single mode):

```yaml
on_failure: abort   # default — fail the thread
on_failure: warn    # assign on_unknown to all rows, continue
```

In batch mode, a missing lookup always aborts regardless of
`on_failure`.

## fk_sentinel_rate assertion

Post-write assertion that checks FK sentinel rates:

```yaml
assertions:
  # Single column, single sentinel
  - type: fk_sentinel_rate
    column: plant_id
    sentinel: -4
    max_rate: 0.05
    message: "plant FK invalid rate exceeded"

  # Named sentinel groups with per-group rates
  - type: fk_sentinel_rate
    column: plant_id
    sentinels:
      invalid:
        value: -4
        max_rate: 0.02
      unknown:
        value: -1
        max_rate: 0.10
    message: "plant FK quality check"

  # Multiple columns, shared config
  - type: fk_sentinel_rate
    columns: [plant_id, customer_id, product_id]
    sentinels:
      invalid: -4
      unknown: -1
    max_rate: 0.10
```

## Resolution statistics

Each resolve step records per-FK statistics in the step
result metadata:

- `total` — total fact rows processed
- `matched` — rows with a successful FK match
- `unknown` — rows assigned `on_unknown` sentinel
- `invalid` — rows assigned `on_invalid` sentinel
- `duplicates` — rows with multiple matches (before dedup)
- `match_rate` — percentage of matched rows
