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

### C205

`name-mismatch` · error · stable

The declared name does not match the file name without its extension.

## C3xx — References and entry forms

### C301

`reference-not-found` · error · stable

A ref names a file that does not exist, or a named weave has no file or inline body.

### C302

`circular-reference` · error · stable

A ref leads back to a file already being loaded.

### C303

`reference-outside-project` · error · stable

A ref or weave name resolves to a path outside the project root.

### C304

`invalid-reference` · error · stable

A ref is not a path string.

### C305

`missing-entry-name` · error · stable

An inline weave or thread entry has no name.

### C306

`duplicate-entry-name` · error · stable

Two thread entries in a weave have the same effective name.

### C307

`missing-entry-alias` · error · stable

A thread ref appears more than once in a weave without 'as' aliases.

### C308

`invalid-foreach` · error · stable

A foreach block is missing a required part or has a part of the wrong shape.

### C309

`foreach-not-supported` · error · stable

A foreach block appears where steps are not expanded (inside a 'with' sub-pipeline).

### C310

`warp-not-found` · error · stable

A named warp file is not in any searched location.

## C4xx — Config rules

### C400

`invalid-config` · error · stable

A config rule was broken, in a way no more specific code covers.

## P1xx — Variables and parameters

### P101

`missing-required-parameter` · error · stable

A required parameter has no runtime value and no default.

### P102

`parameter-type-mismatch` · error · stable

A parameter value is not of the type its declaration names.

### P103

`invalid-parameter-format` · error · stable

A date or timestamp parameter is not in the expected ISO format.

### P110

`unresolved-variable` · error · stable

A ${...} variable has no value and no default.

## R1xx — Lookups and column sets

### R140

`unknown-audit-template` · error · stable

An audit template name is neither user-defined nor a built-in preset.
