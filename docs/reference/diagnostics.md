# Diagnostic Codes

Every problem weevr reports carries a stable code: one letter for the
area and three digits. Match on the code, never on the message. Messages
are written for people and may be reworded in any release; a code keeps
its meaning and is never reused.

Each code also has a kebab-case name that reads well in logs. Names may
be sharpened in a minor release; the code does not change.

| Letter | Area |
|--------|------|
| `C` | Config files: parsing, schema, envelope, references, rules |
| `P` | Variables and parameters |
| `R` | Registries: lookups, column sets, connections |
| `D` | Plan and dependencies |
| `S` | Sources, targets and storage |
| `E` | Execution |
| `Q` | Data quality |

Stability: **stable** codes keep their meaning; **experimental** codes
may change before they are marked stable; **deprecated** codes are no
longer emitted from the version shown.

The same list is available as JSON in
[`diagnostics.json`](https://github.com/ardent-data/weevr/blob/main/docs/schema/diagnostics.json).

!!! note "Generated page"

    This page is generated from the code registry by
    `scripts/generate_diagnostics.py`. Do not edit it by hand.

## C0xx — Reading and parsing

### C001

`config-file-not-found` · error · stable

The config file does not exist.

### C002

`config-file-unreadable` · error · stable

The config file exists but could not be read.

### C003

`config-file-not-utf8` · error · stable

The config file is not valid UTF-8 text.

### C004

`empty-config` · error · stable

The config file is empty or holds only null.

### C005

`yaml-syntax-error` · error · stable

The config file is not valid YAML.

### C006

`config-not-a-mapping` · error · stable

The top level of the config is not a mapping.

### C007

`unsupported-extension` · error · stable

The file extension is not one weevr loads (.thread, .weave, .loom, .warp, .yaml, .yml).

## C1xx — Schema shape

### C100

`schema-violation` · error · stable

The config does not match the schema, in a way no more specific code covers.

### C101

`missing-field` · error · stable

A required key is missing.

### C102

`wrong-type` · error · stable

A value has the wrong type: one code for every type (string, number, boolean, list, mapping), and for a value no member of a union accepts.

### C103

`invalid-value` · error · stable

A value is not one of the allowed values.

### C104

`unknown-step-type` · error · stable

A pipeline or hook step names no known step type.

### C105

`value-out-of-range` · error · stable

A value breaks a length, range or pattern limit.

### C110

`unknown-key` · error · experimental

A key the schema does not define. Reserved for strict schemas.

## C2xx — Envelope, kind and version

### C201

`missing-config-version` · error · stable

The config has no config_version.

### C202

`invalid-config-version` · error · stable

config_version is not a 'major.minor' string of integers.

### C203

`unsupported-config-version` · error · stable

config_version names a major version this release does not read.

### C204

`unknown-config-kind` · error · stable

The config's kind could not be determined, or is not one weevr knows.

## C4xx — Config rules

### C400

`invalid-config` · error · stable

A config rule was broken, in a way no more specific code covers.

### C401

`empty-collection` · error · stable

A list, mapping or pattern that must have content is empty.

### C402

`conflicting-options` · error · stable

Two or more options that exclude each other are set together.

### C403

`missing-required-option` · error · stable

An option required by the block's kind, mode or other options is not set.

### C404

`option-not-applicable` · error · stable

An option is set that the block's kind or mode does not use.

### C405

`values-must-differ` · error · stable

Two options must hold different values but are equal.

### C406

`value-out-of-order` · error · stable

A lower bound is greater than its upper bound.

### C407

`invalid-format-pattern` · error · stable

A format pattern contains characters it may not.

### C408

`missing-column-placeholder` · error · stable

An expression lacks its required {col} placeholder.

### C440

`invalid-date-format` · error · stable

A date value is not a valid YYYY-MM-DD date.

### C441

`group-overlap` · error · stable

A column appears in more than one group, or a single-use setting is claimed twice.

### C442

`invalid-name` · error · stable

A name is not a valid identifier.

### C443

`baseline-check-in-pre-steps` · error · stable

A check that compares against a pre-run baseline is placed where no baseline exists.

### C444

`incremental-dedup-order` · error · stable

A deduplication on an incremental read does not keep the latest row by the watermark.

### C445

`name-collision` · error · stable

A name or alias collides with another name in the same scope.

### C446

`forward-reference` · error · stable

A name is used before it is declared.

### C447

`duplicate-column` · error · stable

A column name is declared more than once.

### C448

`undeclared-column-reference` · error · stable

A key or reference names a column that is not declared.

## D1xx — Plan and dependencies

### D101

`circular-dependency` · error · stable

The threads of a weave depend on each other in a cycle.

### D102

`conflicting-target-writers` · error · stable

Several threads write one target without being ordered or condition-gated.

### D103

`undefined-dependency` · error · stable

A thread declares a dependency on a thread the weave does not define.

## R1xx — Lookups and column sets

### R101

`lookup-key-values-overlap` · error · stable

A lookup's key and value columns overlap.
