# Dimension Modeling

weevr supports Kimball-style dimension targets with composable
behavioral flags. Instead of selecting an SCD type label, you
declare the specific behaviors you need — versioning, overwrite,
static columns, and previous value tracking.

## Quick Start

A minimal versioned dimension (SCD Type 2 equivalent):

```yaml
target:
  path: Tables/dim_customer
  dimension:
    business_key: [customer_id]
    surrogate_key:
      name: _sk_customer
      columns: [customer_id]
    track_history: true
```

## Composable Behavioral Flags

### track_history

When `true`, the engine maintains row versions with SCD columns:

- `_valid_from` — effective start timestamp
- `_valid_to` — effective end timestamp (`9999-12-31` for current)
- `_is_current` — boolean flag for the active row

When `false` (default), the dimension uses standard merge:
match on business key, update on change, insert new.

### change_detection

Declares which columns to monitor for changes and what
action to take. Supports single-group and named-group forms.

**Single group (auto):**

```yaml
dimension:
  business_key: [customer_id]
  surrogate_key:
    name: _sk
    columns: [customer_id]
  track_history: true
  change_detection:
    all_attrs:
      columns: auto
      on_change: version
```

When `change_detection` is omitted, the engine defaults to a
single auto group with `on_change: version` when
`track_history: true`, or `on_change: overwrite` when
`track_history: false`.

**Named groups (hybrid dimension / Type 6):**

```yaml
dimension:
  business_key: [customer_id]
  surrogate_key:
    name: _sk
    columns: [customer_id]
  track_history: true
  change_detection:
    versioned:
      columns: [name, email]
      on_change: version
    overwrite:
      columns: [tier, status]
      on_change: overwrite
    fixed:
      columns: [created_date]
      on_change: static
```

Each group's `on_change` behavior:

| Value | Behavior |
|-------|----------|
| `version` | Close current row, insert new version |
| `overwrite` | Update in place across all versions |
| `static` | Never update, regardless of source |

### previous_columns

Track the previous value of a column before an update:

```yaml
dimension:
  business_key: [customer_id]
  surrogate_key:
    name: _sk
    columns: [customer_id]
  previous_columns:
    _prev_tier: tier
```

On update, `_prev_tier` receives the old `tier` value before
the new value is written.

## System Member Seeding

Insert Kimball sentinel rows on first write:

```yaml
dimension:
  business_key: [customer_id]
  surrogate_key:
    name: _sk
    columns: [customer_id]
  seed_system_members: true
```

Default members:

| SK | Code | Label |
|----|------|-------|
| -1 | unknown | Unknown |
| -2 | not_applicable | Not Applicable |

Override with custom members:

```yaml
  seed_system_members: true
  system_members:
    - sk: -1
      code: unknown
      label: Unknown
    - sk: -2
      code: not_applicable
      label: Not Applicable
    - sk: -3
      code: error
      label: Error
  label_column: member_label
```

## General Seed Rows

Any target can declare seed rows:

```yaml
target:
  seed:
    on: first_write
    rows:
      - id: 1
        name: Default Category
      - id: 2
        name: Uncategorized
```

Trigger conditions:

- `first_write` — insert when the table does not exist
- `empty` — insert when the table is empty or missing

## Additional Keys

Generate secondary hash columns alongside the primary SK:

```yaml
dimension:
  surrogate_key:
    name: _sk_entity
    columns: [customer_id]
  additional_keys:
    version_key:
      name: _sk_version
      columns: [customer_id, name, email]
```

## SCD Column Customization

Override default column names and date boundaries.

Defaults when omitted:

- `columns.valid_from`: `_valid_from`
- `columns.valid_to`: `_valid_to`
- `columns.is_current`: `_is_current`
- `dates.min`: `1970-01-01` (first version `valid_from`)
- `dates.max`: `9999-12-31` (current row `valid_to`)

```yaml
dimension:
  columns:
    valid_from: effective_start
    valid_to: effective_end
    is_current: is_active
  dates:
    min: "1900-01-01"
    max: "9999-12-31"
```

## Write Overrides

The `write:` block can override non-dimension merge behaviors:

```yaml
write:
  on_no_match_source: soft_delete
  soft_delete_column: _is_deleted
```

Forbidden fields when `dimension:` is present:

- `write.match_keys` — derived from `business_key`
- `write.on_match` — derived from change detection
- `keys.business_key` — declared in `dimension:`
- `keys.surrogate_key` — declared in `dimension:`
- `keys.change_detection` — declared in `dimension:`

## SCD Type Mapping

| Traditional Type | weevr Configuration |
|-----------------|---------------------|
| Type 0 | `on_change: static` |
| Type 1 | `track_history: false` |
| Type 2 | `track_history: true`, `on_change: version` |
| Type 3 | `previous_columns` mapping |
| Type 6 | Named groups with mixed `on_change` values |

## Performance

For large dimensions, the engine filters target reads to
`is_current = true` by default. Disable with:

```yaml
dimension:
  history_filter: false
```

Z-ordering on business key columns is recommended for
dimensions with 100M+ entities.
