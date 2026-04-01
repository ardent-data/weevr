# Sub-Pipelines

The `with:` block lets you define named sub-pipelines that run before the main
thread steps. Each sub-pipeline reads from a source (or another sub-pipeline),
applies its own step chain, and produces a named DataFrame that downstream
steps — including the main pipeline — can reference by name.

This is the weevr equivalent of a SQL CTE: complex transformations stay
modular, reusable, and readable without writing intermediate Delta tables.

## When to use `with:`

Reach for `with:` when:

- A transformation is too complex to express in a single step chain
- Two or more joins need the same filtered or aggregated DataFrame
- A self-join requires two differently-aliased views of the same source
- You want to name intermediate stages clearly for explain output

If you only need a simple join with no shared intermediate, inline the
source in `steps` directly — `with:` is for reuse and composition.

## Basic CTE

The simplest form filters a source before joining it in the main pipeline:

```yaml
sources:
  orders:
    path: Tables/stg_orders

with:
  filtered_orders:
    from: orders
    steps:
      - filter:
          expr: "amount > 100"

steps:
  - join:
      source: filtered_orders
      on: [{left: customer_id, right: customer_id}]
      type: inner
```

`filtered_orders` is available to all subsequent `with:` entries and to the
main `steps:` block. It is never written to storage — it exists only in memory
for the duration of the thread.

## Chaining sub-pipelines

A sub-pipeline can reference another sub-pipeline by name. The engine
resolves dependencies in declaration order, so later entries can build on
earlier ones:

```yaml
with:
  big_orders:
    from: orders
    steps:
      - filter:
          expr: "amount > 1000"

  big_order_totals:
    from: big_orders   # references the sub-pipeline above
    steps:
      - aggregate:
          group_by: [customer_id]
          measures:
            total: "sum(amount)"

steps:
  - join:
      source: big_order_totals
      on: [{left: customer_id, right: customer_id}]
      type: left
```

Chains can be as deep as needed. Keep each entry focused on one concern —
filter in one, aggregate in the next — so that the intent is obvious when
reading the config.

## Source reuse with alias and filter

When you need the same source joined twice with different filters — a common
pattern for ranking or period comparison — declare two sub-pipelines and join
each with a distinct alias:

```yaml
with:
  ranked:
    from: invoices
    steps:
      - derive:
          columns:
            inv_rank: "row_number() OVER (PARTITION BY customer_id ORDER BY amount DESC)"

steps:
  - join:
      source: ranked
      on: [{left: id, right: customer_id}]
      type: left
      filter: "inv_rank = 1"
      alias: r1
      include: [amount]
      prefix: "top1_"

  - join:
      source: ranked
      on: [{left: id, right: customer_id}]
      type: left
      filter: "inv_rank = 2"
      alias: r2
      include: [amount]
      prefix: "top2_"
```

`filter` prunes rows from the joined DataFrame before the join condition
applies. `alias` disambiguates column references when the same source appears
more than once. `prefix` namespaces the included columns so `amount` from
the first join becomes `top1_amount` and from the second becomes `top2_amount`.

## Join column control

Use `include`, `exclude`, `rename`, and `prefix` to control which columns
cross the join boundary and what they are called. This avoids broad `SELECT *`
semantics and keeps the output schema explicit.

### include + rename (dimension attribute pattern)

Bring in a single descriptive column from a hierarchy lookup and rename it
to match the output schema:

```yaml
steps:
  - join:
      source: hierarchy
      on: [{left: group_code, right: code}]
      type: left
      alias: grp
      include: [description]
      rename:
        description: group_desc
```

Only `description` crosses the join. It arrives as `group_desc`. The join
key (`code`) is not included because it duplicates `group_code` already on
the left side.

### exclude

Keep everything from the joined DataFrame except a few columns:

```yaml
  - join:
      source: product_lookup
      on: [{left: product_code, right: code}]
      type: left
      exclude: [code, _load_timestamp]
```

Useful when the joined source is wide and only a handful of technical
columns should be suppressed.

### prefix

Namespace all columns from the join uniformly — useful when joining the
same source multiple times or when column names overlap:

```yaml
  - join:
      source: dim_date
      on: [{left: ship_date, right: date_key}]
      type: left
      prefix: "ship_"
```

`prefix` and `rename` compose: `prefix` applies first, then `rename` maps
the prefixed names. `include` and `exclude` are mutually exclusive — use
one or the other on a given join.

## Self-join with column control

A dimension that encodes a parent–child hierarchy often needs to be joined
twice: once for the member row and once for the parent row. Sub-pipelines
make each alias explicit:

```yaml
sources:
  product_dim:
    path: Tables/dim_product

with:
  product_member:
    from: product_dim
    steps:
      - filter:
          expr: "is_current = true"

  product_parent:
    from: product_dim
    steps:
      - filter:
          expr: "is_current = true"

steps:
  - join:
      source: product_member
      on: [{left: product_code, right: code}]
      type: left
      alias: pm
      include: [description, category_code]
      rename:
        description: product_desc

  - join:
      source: product_parent
      on: [{left: category_code, right: code}]
      type: left
      alias: pp
      include: [description]
      rename:
        description: category_desc
```

Two separate sub-pipelines — both drawn from the same source — each filtered
and aliased independently. The first join resolves the product description;
the second uses the `category_code` brought in by the first to resolve the
parent description.

Without `with:`, this requires either two raw source declarations pointing
at the same table or a single source aliased twice, which becomes ambiguous
when column control is involved.

## Migration from PySpark notebook patterns

A common notebook pattern builds a chain of DataFrames in cells:

```python
# Notebook cell 1
big_orders = orders.filter("amount > 1000")

# Notebook cell 2
totals = big_orders.groupBy("customer_id").agg(sum("amount").alias("total"))

# Notebook cell 3
result = customers.join(totals, "customer_id", "left")
```

The weevr equivalent uses `with:` for the intermediate stages and `steps:`
for the final join:

```yaml
with:
  big_orders:
    from: orders
    steps:
      - filter:
          expr: "amount > 1000"

  totals:
    from: big_orders
    steps:
      - aggregate:
          group_by: [customer_id]
          measures:
            total: "sum(amount)"

steps:
  - join:
      source: totals
      on: [{left: customer_id, right: customer_id}]
      type: left
```

**Key differences to expect:**

| Notebook | weevr |
|----------|-------|
| DataFrame variables | Named sub-pipelines |
| Implicit schema drift | Explicit `include`/`rename` at join |
| Manual `.show()` debugging | `explain()` shows each stage |
| Re-run entire cell chain | Engine resolves only affected stages |

The config expresses the same logical pipeline without imperative plumbing.
Schema changes in source tables surface as config validation errors rather
than runtime type failures mid-notebook.
