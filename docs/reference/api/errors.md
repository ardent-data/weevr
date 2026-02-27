# Errors API

The `weevr.errors` module defines the exception hierarchy used throughout
the framework. All weevr-specific exceptions inherit from a common base
class for consistent error handling.

::: weevr.errors
    options:
      members: true
      show_root_heading: true
      show_source: false

---

## Error Troubleshooting Catalog

This section provides actionable guidance for every exception class in
`weevr.errors`. Each entry explains when the error is raised, common
causes, and how to resolve it.

### Exception hierarchy

```d2
direction: down

WeevError: WeevError {
  ConfigError: ConfigError {
    ConfigParseError
    ConfigSchemaError
    ConfigVersionError
    VariableResolutionError
    ReferenceResolutionError
    InheritanceError
    ModelValidationError
  }
  ExecutionError: ExecutionError {
    SparkError
    StateError
  }
  DataValidationError
}
```

---

### WeevError

Base exception for all weevr errors. Not raised directly — catch this to
handle any weevr-specific exception.

```python
from weevr.errors import WeevError

try:
    result = ctx.run("daily.loom")
except WeevError as e:
    print(f"weevr error: {e}")
    if e.cause:
        print(f"  caused by: {e.cause}")
```

**Related:** All exceptions below inherit from `WeevError`.

---

### ConfigError

Base exception for configuration-related errors. Carries optional
`file_path` and `config_key` context.

**Common causes:**

- Project directory missing `.weevr` extension
- Unsupported file extension (not `.thread`, `.weave`, or `.loom`)
- Declared config name does not match filename stem
- Circular dependency detected in weave thread ordering
- Invalid `foreach` block structure in step list

**Resolution:**

1. Verify the project directory ends with `.weevr`
2. Check that all config files use typed extensions
3. Ensure the `name:` field matches the filename (or omit it)

**Example:**

```yaml
# This file is saved as "orders/fact_orders.thread"
# but declares a different name — triggers ConfigError
name: wrong_name  # should be "fact_orders" or omitted
config_version: "1.0"
sources:
  orders:
    type: delta
    alias: raw.orders
```

**Related:** [Configuration Keys](../configuration-keys.md)

---

### ConfigParseError

Raised when a YAML file cannot be read or parsed.

**Common causes:**

- File not found at the specified path
- Empty or null YAML content
- Invalid YAML syntax (indentation, special characters)
- YAML content is not a dictionary (e.g., a list or scalar)
- Missing required `config_version` field
- `config_version` not in `"major.minor"` format

**Resolution:**

1. Verify the file exists at the expected path
2. Validate YAML syntax with a linter or `yamllint`
3. Ensure the file starts with key-value pairs (a YAML mapping)
4. Add `config_version: "1.0"` as the first field

**Example:**

```yaml
# Invalid — missing config_version
sources:
  orders:
    type: delta
    alias: raw.orders

# Fixed
config_version: "1.0"
sources:
  orders:
    type: delta
    alias: raw.orders
```

**Related:** [YAML Schema: Thread](../yaml-schema/thread.md)

---

### ConfigSchemaError

Raised when the configuration structure does not match the expected schema.

**Common causes:**

- Required fields missing (e.g., `sources` or `target` in a thread)
- Unknown config type passed to validation
- Required parameter not supplied at runtime
- Parameter value does not match declared type (wrong type for int, bool,
  date, etc.)
- Date parameter not in `YYYY-MM-DD` format
- Timestamp parameter not in ISO 8601 format

**Resolution:**

1. Compare your config against the
   [YAML Schema Reference](../yaml-schema/thread.md) for required fields
2. Check that all declared parameters are supplied with correct types
3. Use `YYYY-MM-DD` for date parameters and ISO 8601 for timestamps

**Example:**

```yaml
# Invalid — thread missing required 'target' field
config_version: "1.0"
sources:
  orders:
    type: delta
    alias: raw.orders
steps:
  - filter:
      expr: "status = 'active'"
# Missing: target

# Fixed — add target
target:
  path: Tables/stg_orders
```

**Related:** [Configuration Keys](../configuration-keys.md),
[YAML Schema: Thread](../yaml-schema/thread.md)

---

### ConfigVersionError

Raised when the `config_version` is not supported.

**Common causes:**

- Major version does not match the supported version
- Unknown config type for version validation

**Resolution:**

1. Set `config_version: "1.0"` — this is the only supported version
2. Check that the file extension matches the intended config type

**Example:**

```yaml
# Invalid — unsupported version
config_version: "2.0"
sources:
  orders:
    type: delta
    alias: raw.orders

# Fixed
config_version: "1.0"
```

---

### VariableResolutionError

Raised when a `${variable}` reference cannot be resolved and has no
default value.

**Common causes:**

- Variable referenced in config but not supplied in runtime parameters
- Typo in variable name
- Missing `params:` declaration in parent config

**Resolution:**

1. Supply the missing parameter at runtime:
   `ctx.run("daily.loom", params={"env": "prod"})`
2. Add a default value in the config: `${env:-dev}`
3. Check for typos in the variable name

**Example:**

```yaml
# This will fail if "env" is not supplied at runtime
sources:
  orders:
    type: delta
    alias: ${env}.raw.orders

# Fixed — add a default value
sources:
  orders:
    type: delta
    alias: ${env:-dev}.raw.orders
```

**Related:** [Config Resolution](../../guides/config-resolution.md)

---

### ReferenceResolutionError

Raised when a referenced config file cannot be loaded or when circular
references are detected.

**Common causes:**

- Referenced file does not exist at the resolved path
- Circular reference chain (A references B which references A)
- Incorrect relative path in `ref:` entries

**Resolution:**

1. Verify the referenced file exists relative to the project root
2. Check for circular `ref:` chains between weaves and threads
3. Use typed extensions in references (`.thread`, `.weave`, `.loom`)

**Example:**

```yaml
# Weave referencing a non-existent thread
config_version: "1.0"
threads:
  - ref: orders/fact_orders.thread  # file must exist at this path

# Circular reference — thread A depends on thread B's target,
# and thread B depends on thread A's target
```

**Related:** [Thread, Weave, Loom](../../concepts/thread-weave-loom.md)

---

### InheritanceError

Raised when the configuration inheritance cascade fails. Reserved for
future use — not currently raised in v1.0.

**Intended for:** Failures during loom → weave → thread default merging.

---

### ModelValidationError

Raised when the fully resolved configuration fails to hydrate into a
typed model (Thread, Weave, or Loom).

**Common causes:**

- Semantic validation failure after variable resolution
- Field values that pass schema validation but fail model constraints
- Inconsistent combinations of settings that are only detectable after
  all variables are resolved

**Resolution:**

1. Check the error message for the specific field that failed
2. Verify that resolved variable values produce valid combinations
3. Run with `mode="validate"` to test config without executing

**Example:**

```python
# Validate config without running — catches ModelValidationError early
result = ctx.run("daily.loom", mode="validate")
```

---

### ExecutionError

Base exception for runtime execution failures. Carries optional context:
`thread_name`, `step_index`, `step_type`, and `source_name`.

**Common causes:**

- Source read failure (Delta table not found, file path invalid)
- Pipeline step failure (invalid Spark SQL expression, missing columns)
- Write failure (target path not accessible, schema mismatch)
- Merge mode missing `write_config` or `match_keys`
- Target has no `alias` or `path` configured
- Unsupported source type

**Resolution:**

1. Check the error message for the thread name and step index
2. Verify source tables/files exist and are accessible
3. For merge mode, ensure `write.match_keys` is declared
4. Test expressions in Spark SQL directly to isolate syntax issues

**Example:**

```yaml
# Will fail — merge mode requires match_keys
write:
  mode: merge
  # Missing: match_keys

# Fixed
write:
  mode: merge
  match_keys: [customer_id]
```

**Related:** [Add a Thread](../../how-to/add-a-thread.md),
[Execution Modes](../../concepts/execution-modes.md)

---

### SparkError

Raised for Spark-specific execution failures. Reserved for future use —
not currently raised in v1.0. `ExecutionError` is used for all runtime
failures.

---

### StateError

Raised when watermark state cannot be read from or written to the
configured store. Carries optional `store_type` context.

**Common causes:**

- Target path contains backtick characters (unsupported by table
  properties store)
- Failed to read watermark from table properties or metadata table
- Failed to write watermark after successful execution
- Metadata table path is not accessible

**Resolution:**

1. Remove backticks from target paths
2. Verify the target Delta table exists and has table properties enabled
3. For metadata table store, verify the `table_path` is accessible
4. Check Spark permissions for reading/writing table properties

**Example:**

```yaml
# Will fail with StateError — backticks not allowed
target:
  path: "Tables/`my_table`"

# Fixed
target:
  path: Tables/my_table
```

**Related:** [Idempotency](../../concepts/idempotency.md),
[Execution Modes](../../concepts/execution-modes.md)

---

### DataValidationError

Raised when a validation rule with `fatal` severity has failing rows.
The thread aborts immediately — no data is written.

**Common causes:**

- A `fatal`-severity validation rule has one or more failing rows
- Source data violates an invariant that should never occur

**Resolution:**

1. Inspect the source data for the failing condition
2. Check the validation rule expression for correctness
3. Consider downgrading severity to `error` (quarantine) if the rule
   should allow partial writes

**Example:**

```yaml
validations:
  # This will abort the thread if ANY row has a null customer_id
  - name: customer_id_required
    rule: "customer_id IS NOT NULL"
    severity: fatal

  # Downgrade to error to quarantine bad rows instead of aborting
  - name: customer_id_required
    rule: "customer_id IS NOT NULL"
    severity: error  # bad rows go to quarantine table
```

**Related:** [Add Validation Rules](../../how-to/add-validation-rules.md)
