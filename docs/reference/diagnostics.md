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

The record each problem is reported as is described in the
[Diagnostics API](api/diagnostics.md). The same list is available as JSON in
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

A value has the wrong type: one code for every type (string, number, boolean, list, mapping), and
for a value no member of a union accepts.

### C103

`invalid-value` · error · stable

A value is not allowed: not one of the allowed values, or not in the required form.

### C104

`unknown-step-type` · error · stable

A pipeline or hook step names no known step type.

### C105

`value-out-of-range` · error · stable

A value breaks a length, range or pattern limit.

### C110

`unknown-key` · error · experimental

A key the schema does not define. Reserved for strict schemas.

### C111

`removed-key` · error · stable

A key this version no longer reads; the hint says why it was removed.

### C112

`renamed-key` · error · stable

A key this version reads under a new name; expected names the new key.

## C2xx — Envelope, kind and version

### C201

`missing-config-version` · error · stable

The config has no config_version.

### C202

`invalid-config-version` · error · stable

config_version is not a 'major.minor' string of integers.

### C203

`unsupported-config-version` · error · stable

config_version names a major version this release does not read. A 1.x version is a v1 document; the
hint names the weevr-cli migrate command.

### C204

`unknown-config-kind` · error · stable

The config's kind is not one weevr knows.

### C205

`name-mismatch` · error · stable

The declared name does not match the file name without its extension.

### C206

`missing-kind` · error · stable

The document declares no kind. The hint names the weevr-cli migrate command.

### C207

`kind-extension-mismatch` · error · stable

The declared kind disagrees with the file's typed extension; expected names the kind.

### C208

`engine-version-unsatisfied` · error · stable

The installed weevr is outside the project's requires_weevr specifier, or its version cannot be
determined (no package metadata); the message names the cause.

### C209

`project-root-not-found` · error · stable

The Context's project directory holds no weevr.yaml; expected lists every location probed.

### C210

`reserved-project-block` · error · stable

weevr.yaml declares a project-scope block (connections, params, defaults, audit_templates) that is
reserved and not yet supported.

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

### C311

`absolute-path-reference` · error · stable

A reference is an absolute path or URI where a path relative to the project belongs.

### C312

`unsupported-reference-scheme` · error · stable

A reference names a scheme (scheme://) where a project-relative path belongs.

### C313

`name-only-entry` · error · stable

A weave or thread entry gives a name but carries neither ref: nor an inline body.

### C350

`duplicate-weave-name` · error · stable

Two weave entries in a loom resolve to the same name.

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

A range's start comes after its end, or equals it where the rule needs a strict order.

### C440

`column-in-several-groups` · error · stable

A column is listed in more than one group where each column may belong to only one.

### C441

`incremental-dedup-order` · error · stable

A deduplication on an incremental read does not keep the latest row by the watermark.

### C442

`name-collision` · error · stable

A name or alias collides with another name in the same scope.

### C443

`forward-reference` · error · stable

A name is used before it is declared.

### C444

`duplicate-column` · error · stable

A column name is declared more than once.

### C445

`undeclared-column-reference` · error · stable

A key or reference names a column that is not declared.

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

A thread in incremental_parameter load mode references no ${param.*} variable in its steps or
sources.

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

## D1xx — Plan and dependencies

### D100

`plan-failed` · error · stable

A weave's plan could not be built, in a way no more specific code covers.

### D101

`circular-dependency` · error · stable

The threads of a weave depend on each other in a cycle.

### D102

`conflicting-target-writers` · error · stable

Several threads write one target without being ordered or condition-gated.

### D103

`undefined-dependency` · error · stable

A thread declares a dependency on a thread the weave does not define.

### D104

`weave-not-found` · error · stable

A loom lists a weave the loaded config does not hold.

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

### P111

`bare-variable-reference` · error · stable

A ${...} reference names no namespace: ${x} instead of ${param.x}.

### P112

`unknown-variable-namespace` · error · stable

A ${...} reference starts with an unknown namespace, or names an unknown key of a thread, weave,
loom or fabric namespace.

### P113

`fabric-reference-deferred` · warning · stable

A ${fabric.x} reference is left unresolved because the load has no Fabric context: a core loader
given no fabric_context. An error under strict_params.

### P120

`undeclared-parameter` · warning · stable

A ${param.x} reference is bound, but no params: declaration in its scope chain covers it. An error
under strict_params.

### P121

`unused-supplied-parameter` · warning · stable

A runtime parameter is supplied that nothing in the loaded tree declares or references. An error
under strict_params.

### P122

`pin-ignored-by-upstream-asset` · warning · stable

A weave or thread entry pins a parameter that a registry asset declared above the entry reads; the
asset resolved in its own scope, so the pin does not retarget it. Never an error, even under
strict_params.

### P123

`unconsumed-pin` · warning · stable

A weave or thread entry pins a parameter that nothing beneath the entry reads or declares.

## R1xx — Lookups and column sets

### R101

`lookup-key-values-overlap` · error · stable

A lookup's key and value columns overlap.

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

### R140

`unknown-audit-template` · error · stable

An audit template name is neither user-defined nor a built-in preset.

## R2xx — Connections

### R201

`undefined-connection` · error · stable

A source or target names a connection no scope declares.

## S1xx — Source and target existence

### S101

`source-not-found` · error · stable

A source does not exist at its resolved location.

### S102

`source-probe-failed` · error · stable

A source's existence could not be checked, because probing its location failed.

## Removed and renamed keys

A key listed here fails the load with `C111` (removed) or `C112` (renamed),
naming what to write instead. `*` matches any one key or list item.

| Kind | Key | Replacement or reason | Removed in |
|------|-----|-----------------------|------------|
| thread | `/type` | `/kind` | 2.0 |
| weave | `/type` | `/kind` | 2.0 |
| loom | `/type` | `/kind` | 2.0 |
| params | `/type` | `/kind` | 2.0 |
| warp | `/type` | `/kind` | 2.0 |
