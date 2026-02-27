# Add Validation Rules

**Goal:** Define pre-write validation rules on a thread to catch data quality
issues before they reach the target table. Configure severity levels to control
whether bad rows are logged, quarantined, or cause the thread to abort.

## Prerequisites

- An existing thread YAML with sources, steps, and a target
- Understanding of Spark SQL expression syntax (rules are evaluated as boolean
  expressions)

## Severity levels

Each validation rule has a severity that determines what happens when rows fail:

| Severity | Behavior |
|---|---|
| `info` | Logged in telemetry. No effect on data flow. |
| `warn` | Logged with elevated visibility. No effect on data flow. |
| `error` | Failing rows are quarantined to `{target}_quarantine`. Clean rows proceed. |
| `fatal` | Any failure aborts the thread. No data is written. |

```d2
direction: down

eval: Evaluate Rules {
  style.font-size: 18
}

info: info severity {
  style.fill: "#E3F2FD"
  action: Log to telemetry
}
warn: warn severity {
  style.fill: "#FFF3E0"
  action: Log with elevated visibility
}
err: error severity {
  style.fill: "#FFEBEE"
  quarantine: Write to quarantine table
  proceed: Clean rows proceed to target
}
fatal_sev: fatal severity {
  style.fill: "#F8D7DA"
  abort_action: Abort thread — no data written
}

eval -> info: rows pass or fail
eval -> warn: rows pass or fail
eval -> err: rows fail
eval -> fatal_sev: any row fails
```

## Step 1 -- Add validation rules to a thread

Add a `validations` section to your thread YAML. Each rule needs a `name`, a
`rule` expression (Spark SQL boolean), and a `severity`:

```yaml
config_version: "1.0"

sources:
  customers:
    type: delta
    alias: bronze.customers

steps:
  - derive:
      columns:
        full_name: "concat(first_name, ' ', last_name)"

validations:
  - name: email_not_null
    rule: "email IS NOT NULL"
    severity: error
  - name: positive_balance
    rule: "balance >= 0"
    severity: fatal
  - name: phone_length
    rule: "LENGTH(phone) >= 10"
    severity: warn
  - name: valid_status
    rule: "status IN ('active', 'inactive', 'suspended')"
    severity: info

target:
  path: Tables/dim_customers

write:
  mode: overwrite
```

Rules are evaluated in a single pass over the DataFrame. Each row is tagged
with pass/fail for every rule, then routed based on severity.

## Step 2 -- Understand quarantine behavior

When a rule with `error` severity fails, the offending rows are split from the
main DataFrame and written to a quarantine Delta table at
`{target_path}_quarantine`. The quarantine table includes metadata columns:

| Column | Description |
|---|---|
| `__rule_name` | Name of the failed validation rule |
| `__rule_expression` | The Spark SQL expression that was evaluated |
| `__severity` | Severity level of the rule |
| `__quarantine_ts` | Timestamp when the row was quarantined |

All original data columns are preserved alongside these metadata columns, making
it straightforward to inspect and reprocess quarantined records.

!!! warning "Fatal rules abort early"
    If any `fatal`-severity rule has failing rows, the entire thread aborts
    immediately. No data is written to the target or quarantine table. Use
    `fatal` for invariants that indicate corrupted or fundamentally invalid
    source data.

## Step 3 -- Add post-execution assertions

Assertions validate outcomes after data is written. They check properties of
the final target dataset:

```yaml
assertions:
  - type: row_count
    min: 100
    max: 1000000
    severity: warn
  - type: column_not_null
    columns:
      - customer_id
      - email
    severity: error
  - type: unique
    columns:
      - customer_id
    severity: error
```

Available assertion types:

- `row_count` -- Verify the target row count falls within bounds
- `column_not_null` -- Verify specified columns contain no nulls
- `unique` -- Verify specified columns form a unique key
- `expression` -- Evaluate a custom Spark SQL expression

## Step 4 -- Verify results via telemetry

After execution, inspect the `RunResult` to see validation outcomes:

```python
from weevr import Context

ctx = Context(spark, "my-project.weevr")
result = ctx.run("staging/stg_customers.thread")

# Check overall status
print(result.status)  # "success", "failure", or "partial"

# Access per-rule results through telemetry
if result.telemetry:
    for vr in result.telemetry.validation_results:
        print(f"{vr.rule_name}: {vr.rows_passed} passed, {vr.rows_failed} failed")

    for ar in result.telemetry.assertion_results:
        print(f"{ar.assertion_type}: {'PASS' if ar.passed else 'FAIL'} - {ar.details}")
```

The telemetry object contains `rows_quarantined` to indicate how many rows were
diverted to the quarantine table, alongside `rows_written` for the main target.
