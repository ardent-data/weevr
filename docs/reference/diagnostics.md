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
