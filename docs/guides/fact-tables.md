# Fact Tables

weevr supports fact table target validation through the `fact:`
block. This declares foreign key column metadata and sentinel
value conventions for FK integrity checking.

## Quick Start

```yaml
target:
  path: Tables/fact_orders
  fact:
    foreign_keys:
      - sk_customer
      - sk_product
      - sk_date
```

## FK Column Validation

The engine validates that all declared `foreign_keys` columns
exist in the output DataFrame after transforms. Missing columns
raise an execution error with a clear message.

This catches configuration drift early — if a column is renamed
or dropped upstream, the fact validation fails before any data
is written. The existence check inspects only the schema, so it
adds no compute cost to the run.

## Sentinel Values

Declare sentinel value conventions used by your FK columns:

```yaml
fact:
  foreign_keys: [sk_customer, sk_product]
  sentinel_values:
    invalid: -4
    missing: -1
```

Defaults:

| Code | Default Value | Meaning |
|------|--------------|---------|
| `invalid` | -4 | Business key was incomplete |
| `missing` | -1 | No lookup match found |

These values align with the sentinel defaults used by the
[`resolve` step](resolve.md). The resolve step assigns these
sentinel SKs when FK lookups fail or BKs are incomplete.

### Sentinel Advisory

After a successful write, the engine checks each FK column for
the declared sentinel values and logs a warning for any column
that contains none. A fact whose FKs never carry sentinels
usually means the resolve step is not wired up for that column,
or lookup failures are being silently dropped upstream.

The advisory is:

- **Exact** — every row is considered, not a sample. Columns
  with many distinct values cannot produce false warnings.
- **Post-write** — evaluated against the written Delta table,
  where column statistics make the scan cheap. All FK columns
  are checked in a single pass, so cost does not grow with the
  number of foreign keys.
- **Advisory only** — it never blocks or fails the run. Only
  the pre-write existence check is enforcing.

Because the check reads the written table, it sees the table's
full contents: on `append` or `merge` targets, sentinels present
in historical rows satisfy the check even if the current run
added none. FK columns that are renamed or excluded by target
column mapping are skipped.

## Relationship to Dimension Targets

`dimension:` and `fact:` are mutually exclusive on a single
target. A thread produces either a dimension or a fact table.

Accumulating snapshot patterns use `fact:` combined with
`write.mode: merge` and standard `keys:` configuration.

## Example: Transactional Fact

```yaml
config_version: "1.0"
sources:
  orders:
    path: Tables/stg_orders
steps:
  - derive:
      columns:
        order_amount: "quantity * unit_price"
target:
  path: Tables/fact_orders
  fact:
    foreign_keys:
      - sk_customer
      - sk_product
      - sk_date
    sentinel_values:
      invalid: -4
      missing: -1
write:
  mode: append
```

## Example: Accumulating Snapshot

```yaml
target:
  path: Tables/fact_order_fulfillment
  fact:
    foreign_keys:
      - sk_customer
      - sk_order_status
write:
  mode: merge
  match_keys: [order_id]
keys:
  business_key: [order_id]
  change_detection:
    name: _row_hash
    columns: [status, ship_date, delivery_date]
```
