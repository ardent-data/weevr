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

## C3xx — References and entry forms

### C350

`duplicate-weave-name` · error · stable

Two weave entries in a loom resolve to the same name.

## C4xx — Config rules

### C400

`invalid-config` · error · stable

A config rule was broken, in a way no more specific code covers.

### C480

`cdc-requires-merge` · error · stable

A thread in cdc load mode does not write with mode 'merge'.

### C481

`cdc-requires-watermark-column` · error · stable

A cdc mapping with update or delete values has no load.watermark_column to order same-key changes.

### C482

`incremental-watermark-overwrite` · error · stable

A thread in incremental_watermark load mode would overwrite its target with each incremental slice.

### C483

`incremental-parameter-without-param` · warning · stable

A thread in incremental_parameter load mode references no ${param.*} variable in its steps or sources.

### C484

`watermark-inclusive-append` · warning · stable

An inclusive watermark with write mode 'append' can duplicate rows on re-reads.

### C485

`dimension-derived-write-field` · error · stable

A thread with a dimension target sets a write field the engine derives itself.

### C486

`dimension-write-mode` · error · stable

A thread with a dimension target sets write.mode, but dimension targets always merge.

### C487

`dimension-keys-field` · error · stable

A thread with a dimension target sets a keys field that belongs in the dimension block.

### C490

`scope-defaults-execution-ignored` · warning · stable

A loom or weave declares defaults.execution, which thread-scoped execution does not apply.

### C491

`thread-execution-ignored` · warning · stable

A thread declares an execution block, which is not applied at thread scope.

### C492

`weave-trace-ineffective` · warning · stable

A weave sets trace: true under a loom whose effective trace is false.

## R1xx — Lookups and column sets

### R120

`undefined-resolve-lookup` · error · stable

A resolve step names a lookup that no visible scope defines.

### R121

`undefined-resolve-lookup-ignored` · warning · stable

A resolve step with on_failure: warn names a lookup that no visible scope defines.

### R122

`undefined-source-lookup` · error · stable

A lookup-bound source names a lookup that no visible scope defines.

### R123

`source-lookup-outside-thread` · warning · stable

A thread loaded on its own has a lookup-bound source whose lookup it does not define.

### R124

`undefined-column-set` · error · stable

A rename step names a column set that no visible scope defines.

### R125

`column-set-outside-thread` · warning · stable

A thread loaded on its own has a rename step whose column set it does not define.

## S1xx — Source and target existence

### S101

`source-not-found` · error · stable

A source does not exist at its resolved location.

### S102

`source-probe-failed` · error · stable

A source's existence could not be checked, because probing its location failed.

### S103

`source-location-unresolved` · error · stable

A source's location could not be resolved, so its existence was not checked.
