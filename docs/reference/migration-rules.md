# Migration Rules (v1 to v2)

weevr 2.0 does not read v1 documents. Loading one fails with `C203`, and
the error names `weevr migrate`, the weevr-cli command that upgrades a
project. The engine itself ships no migration code. This page is the
specification that command implements: a fixed set of text edits that
turn a v1 project folder into a v2 one.

The rules are deterministic. Given the same folder and the same project
`id`, every implementation produces the same bytes. The weevr repository
proves them against fixture projects (`tests/fixtures/migration/v1/`)
and their expected results (`tests/fixtures/migration/v2/`).

## Scope

A migration runs on one **project folder**, the folder that holds the
project's looms, weaves and threads. That folder becomes the v2 project
root (the folder holding `weevr.yaml`).

Every file under the folder is handled once:

| File | Handling |
|------|----------|
| `.thread`, `.weave`, `.loom`, `.warp` | Migrated when it is a v1 document |
| `.yaml`, `.yml` | Migrated when it is a v1 document and its kind can be detected |
| Anything else (`.csv`, `.md`, `.sql`, ...) | Copied unchanged |
| Files under a folder whose name starts with `.`, and `.platform` files | Copied unchanged |

A file is a **v1 document** when it parses as YAML, its root is a
mapping, and its root `config_version` is a 1.x version: the string
`"1.<minor>"` or the bare number `1.<minor>`. Anything else, including a
file that is not UTF-8 or fails to parse, is copied unchanged.

Extensions match case-insensitively (`.Thread` is a `.thread` file).

The rules edit block-style YAML. A v1 document whose root is not a block
mapping starting at column 0 (flow style, JSON, or an indented root) is
copied unchanged. So is one whose edits would not have exactly the
effect the rules describe, for example because a key the rules edit is
written in quotes (`"config_version": "1.0"`) or a `config_version` sits
somewhere other than the places rule 2 names. A tool checks this by
parsing its output and comparing it with the original plus the intended
changes.

A tool should report every `.thread`, `.weave`, `.loom` or `.warp` file
it did not migrate, since the v2 engine will reject it.

Tools may parse a document to decide which rules apply. The edits
themselves are made on lines of text, never by writing the parsed YAML
back out (see [Edit-preserving output](#edit-preserving-output)).

## Rules

Each rule below names the lines it changes. All rules read the original
document, so the order they are applied in does not change the result.

### 1. Add `kind`

The document's kind comes from its file extension:

| Extension | Kind |
|-----------|------|
| `.thread` | `thread` |
| `.weave` | `weave` |
| `.loom` | `loom` |
| `.warp` | `warp` |

A `.yaml` or `.yml` document has no kind in its name. v1 detected it from
the document's root keys, and migration uses the same order. The first
match wins:

1. A root `type:` whose value is `thread`, `weave`, `loom`, `warp` or
   `params` gives that kind.
2. A root `weaves` key: `loom`.
3. A root `threads` key: `weave`.
4. A root `sources` or `target` key: `thread`.
5. A root `config_version` plus at least one other root key: `params`.

A `.yaml` or `.yml` document that matches none of these was not loadable
in v1 either. It is copied unchanged, and a tool should report it.

**Placement.** The new line is `kind: <kind>`. It goes immediately
before the document's first root key line: the first line that is not
blank, not a comment, not a `%` directive and not the `---` document
marker. Leading comments, blank lines and the `---` marker stay above
it. When that first key is a root `type:` line that rule 3 removes, the
`kind:` line takes its place.

A document that already has a root `kind` key gets no second one.

### 2. Bump `config_version`

A `config_version` whose value is a 1.x version is rewritten to `2.0` in
these places, and only these:

- the root key;
- each inline thread under a weave's root `threads`;
- each inline weave under a loom's root `weaves`, and each inline thread
  under that weave's `threads`.

Text inside a block scalar (`note: |`) is never edited. Only the value
changes:

- `"1.0"` (double-quoted) becomes `"2.0"`.
- `'1.2'` (single-quoted) becomes `'2.0'`.
- `1.0` (a bare number) becomes `"2.0"`. The version must be a string (v1
  rejected a bare number with `C202` too), so the result is always quoted.

Spacing around the colon, and any trailing comment, are kept. A
`config_version` that is not 1.x is left as it is.

### 3. Drop the root `type:`

v2 names the kind with `kind:`, so the root `type:` line is removed when
its value is one of the kind names in rule 1. Only the root key is
removed: a nested `type:` (a source's `type: delta`, a column's
`type: string`) is untouched.

A root `type:` with any other value is kept. In a params document it is
an ordinary parameter, but v2 rejects a root `type` key in every kind,
so a tool should report it for the author to rename.

### 4. Reference weaves by file in looms

In v1 a loom could name a weave without saying where it was. weevr loaded
`<name>.weave` from the project root. v2 needs the file named with
`ref:` (a name with no body fails with `C313`). Two loom entry forms
change, when `<name>.weave` exists at the project root:

- A **plain string entry**, `- dims`, becomes `- ref: dims.weave`.
- A **name-only mapping entry**, one with a `name` key, no `ref`, and no
  keys other than `name` and `condition`, has its `name: facts` line
  rewritten to `ref: facts.weave`. The `condition` lines stay as they are.

```diff
 weaves:
-  - dims
+  - ref: dims.weave
-  - name: facts
+  - ref: facts.weave
     condition:
       when: "table_exists('raw.orders')"
   - ref: weaves/returns.weave
```

The written path is `<name>.weave`, unquoted when the name holds only
letters, digits, `_`, `-`, `.` and `/`, otherwise in double quotes.
The rest of the line (the `-` marker, spacing and any trailing comment)
is kept.

These entries are left unchanged:

- An entry that already has `ref`.
- An inline weave: a mapping with keys other than `name` and `condition`,
  such as `threads`. (v1 loaded a same-named `<name>.weave` file in place
  of an inline body when one existed; v2 never does. A tool should warn
  when an inline weave's name matches a weave file at the project root.)
- An entry whose `<name>.weave` does not exist at the project root. It
  did not load in v1 either; a tool should report it.
- Entries written in flow style (`weaves: [dims]`, `- {name: facts}`).
  A tool should report them.

Thread references in weaves are paths already and need no change. v1
and v2 both resolve `ref:` against the project root.

### 5. Generate `weevr.yaml`

When the project folder has no `weevr.yaml` at its root, one is created
with these four lines, in this order, with `\n` line endings:

```text
kind: project
config_version: "2.0"
id: <id>
name: <name>
```

- `id` is a new random UUID (version 4), written in lowercase with
  hyphens. A tool generates a fresh one for every project it migrates.
- `name` comes from the project folder's name: drop a trailing `.weevr`;
  replace every character other than `A`–`Z`, `a`–`z`, `0`–`9` and `_`
  with `_`; when the result does not start with a letter, prefix
  `project_`; an empty result is `project`; keep at most the first 100
  characters. `sales.weevr` gives `sales`; `2024 sales-eu.weevr` gives
  `project_2024_sales_eu`.

An existing `weevr.yaml` is left as it is.

`requires_weevr` is not written. A tool that writes one before weevr
2.0.0 is final should use `">=2.0.0a1,<3"`: under PEP 440 the
pre-release `2.0.0a2` sorts below `2`, so `">=2,<3"` would reject it.

### 6. Name the namespace of a bare reference

v1 let a document write `${name}` with no namespace. v2 reads only
`${param.x}`, `${fabric.x}`, `${var.x}`, `${run.x}` and the hierarchy
namespaces `${thread.x}`, `${weave.x}` and `${loom.x}`; a bare reference
fails with `P111`. Every bare `${name}` and `${name:-default}` in a
document (a reference whose first segment is none of those namespaces) is
rewritten by the first of rules 7 to 10 that matches. A reference already in
a namespace is not touched.

References are found in every string value of the document: plain,
quoted, and block scalar text. A `#` comment is never edited. The
`:-default` text stays exactly as written through every rewrite, so
`${env:-dev}` becomes `${param.env:-dev}`.

A name is classified in the reference's **scope chain**: the document that
holds it, then the looms and weaves above it, reached by the `ref:` entries
and weave names that rule 4 resolves. A document referenced from more than
one parent takes all of them. The chain is ordered nearest first: the document
itself, then its parents level by level (parents, then their parents), the
documents on one level sorted by project-relative path, each document once, so a
reference cycle ends at the first revisit. The first document in this order
that matches decides. Inline weaves and threads share the chain of the
document that holds them.

Two limits. A reference in a mapping **key** (`${x}: 1`) is not rewritten; a
document holding one is left as v1 and a tool should report it. A reference
nested in another's `:-` default (`${x:-${y}}`) is not supported: the pattern
`${name}` or `${name:-default}` ends at the first `}`, so only the outer
reference is rewritten (`${param.x:-${y}}`) and the inner one stays bare, for
the author to fix.

v1 read a bare name from the runtime parameters first, then from `defaults:`,
then from the Fabric context. The order of rules 7 to 10 keeps that
behavior. A migration has no runtime parameters, so a name only a run could
supply is classified by rule 10 and reported.

### 7. A declared param becomes `${param.x}`

When `x` is declared in a `params:` block of any document in the chain
(or of an inline weave or thread there), `${x}` becomes `${param.x}`.

### 8. A `defaults:` key becomes a param

When `x` is a key of the root `defaults:` block of a document in the chain
and rule 7 did not match, the key is converted to a param declared at the
same tier, and `${x}` becomes `${param.x}`. The nearest document in the chain
that has the key wins, as the v1 cascade did.

The key moves out of `defaults:` and into the same document's `params:`
block as a `ParamSpec`, with the key's value as its `default`:

```diff
-defaults:
-  env: prod
+params:
+  env:
+    name: env
+    type: string
+    default: prod
```

- Only a key a reference names is converted. Other `defaults:` keys stay.
- Placement. When the document has no root `params:`, a new `params:` block
  takes the place of the converted keys' `defaults:` block: it starts at the
  `defaults:` line, with the `defaults:` keys that stay below it. When there is
  a root `params:` block, the new specs follow its last content line.
  Specs are written in the order the keys appear in `defaults:`.
- Indentation. Let `n` be the indent of the `defaults:` entries. A new
  `params:` block puts each name at `n` spaces and its `name`, `type` and
  `default` lines at `2n`. An existing `params:` block keeps its own name
  indent `m`: a new spec there puts the name at `m` and those lines at `m + n`.
  The `name`, `type`, `default` order is fixed.
- When a new `params:` block is created and `defaults:` keys remain, one blank
  line separates them. It is not added when the block is extended, and a
  `defaults:` block left with no keys is removed with its header.
- Every added line takes the line ending of the `defaults:` line (of the last
  `params:` line when the block is extended), or `\n` when that line has none; a
  missing final newline stays missing.
- The key's text and trailing comment are kept as the `default`.
- The `type` follows the value as a YAML 1.1 parser reads it, and the `default` keeps
  the value's text as written: a boolean (`true`, `yes`, `off`) gives `bool`, an
  integer (`500`, `0x1F`, `1_000`) gives `int`, a float (`1.5`, `1.0e+3`) gives
  `float`, a list of strings gives `list[string]`, and everything else, such as
  `1e3` (not a float in YAML 1.1), gives `string`. A date or timestamp written
  without quotes (`2024-01-01`) is quoted in the `default` and typed `string`, as
  v1 substituted its text.
- An embedded reference to a boolean renders as written, `true` or `false`.
  v1 rendered `True` or `False`, so text built from a v1 boolean default
  changes case: with `dry_run: false`, `dry-${dry_run}` gave `dry-False` in v1,
  and its migrated `dry-${param.dry_run}` gives `dry-false`.
- Only a one-line scalar, or a one-line list of strings, in a block `defaults:` is
  converted, and only when the document's root `params:` is absent or a block
  mapping. Any other key, such as a flow mapping, a block list or a null, is not
  converted; a reference to it is classified by rule 10 and reported, and its author
  declares the param.
- Only a document's root `defaults:` is read. The `defaults:` block of an inline
  weave or thread is not converted, and a reference to its keys is classified by
  rule 10 and reported.

### 9. A Fabric key becomes `${fabric.x}`

When `x` is `workspace_id`, `lakehouse_id` or `workspace_name` and rules 7 and
8 did not match, `${x}` becomes `${fabric.x}`. A dotted reference never matches
this rule.

### 10. Everything else becomes `${param.x}`, and is reported

A name that no rule above matches can only have been supplied at run time. It
becomes `${param.x}`, and the migration reports it as a **fallback**: the
reference now needs a `params:` declaration, or the load warns (`P120`). A
fallback is a review signal. It is never an error, and a run that supplies
the value behaves as in v1.

### Dotted names

A dotted bare name, `${a.b.c}`, is classified by its first segment `a`
with rules 7, 8 and 10, and the whole name is prefixed: `${env.lakehouse}` becomes
`${param.env.lakehouse}`. Rule 9 does not apply to a dotted name, since a Fabric
key has no sub-keys. A migration report lists it as a fallback when rule 10 classified
the first segment.

The reference migrator's report lists each reference rewritten, with its rule
(7 to 10, and whether the name was dotted), and marks the fallbacks. It also lists
the documents it left as v1 because the edits would not have had the effect the
rules describe.

The engine's `P111` hint computes the same rewrite for a document that still
holds a bare reference, with two differences. A migration has no run, so its
order is the one above.

- The hint also checks whether the load supplied the name at run time before it
  reads `defaults:`. This only changes the hint's wording, not the namespace it
  suggests.
- For a dotted name whose first segment is not declared, supplied or a `defaults:`
  key, the engine raises `P112` (unknown namespace) and suggests no rewrite. The
  migration writes `${param.a.b}` and reports it as a rule 10 fallback.

### Example

A v1 thread with a root `type:` and its v2 result:

```diff
-type: thread
-config_version: "1.0"
+kind: thread
+config_version: "2.0"

 sources:
   orders:
     type: delta
     alias: raw.orders
```

A thread whose first key is not `config_version` gets `kind` before its
first key, below the leading comments:

```diff
 # Customer dimension.

+kind: thread
 target:
   alias: gold.dim_customer

 config_version: "2.0"  # set when the thread was created
```

## Edit-preserving output

Only lines a rule names change. Every other line is byte-for-byte
identical, so a migration reviews as a small diff:

- Comments, blank lines, key order, indentation and quoting are kept.
- Each line keeps its own line ending (`\n`, `\r\n` or `\r`). An
  inserted `kind:` line takes the ending of the line it is inserted
  before, or `\n` when that line has none.
- A missing final newline stays missing.
- A UTF-8 byte order mark stays at the start of the file.

Files that are not migrated are copied byte-for-byte.

## Idempotence

Running the rules on a migrated project changes nothing. Its documents
have version `2.0`, so none of them is a v1 document, and `weevr.yaml`
already exists. The fixture test checks this by migrating the v2 result
again.
