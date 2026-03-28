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
is written.

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

These values document the FK contract. The `resolve` step
(M114, future) will use them to assign sentinel SKs when
FK lookups fail.

## Relationship to Dimension Targets

`dimension:` and `fact:` are mutually exclusive on a single
target. A thread produces either a dimension or a fact table.

Accumulating snapshot patterns use `fact:` combined with
`write.mode: merge` and standard `keys:` configuration.

## Example: Transactional Fact

```yaml
config_version: "1"
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
