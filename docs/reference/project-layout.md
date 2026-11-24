# Project Layout

A weevr project is a tree of YAML documents rooted by a `weevr.yaml` file,
plus a run store that holds everything runs produce. This page is the
reference for that layout: what is definition and what is run output, how the
root is found, how documents refer to each other, how a project maps to a
Fabric item, and where run outputs live.

Documents you write are the **definition**. What runs produce is the **run
store**. They are kept apart: the engine never writes to the definition at run
time, and run outputs go only to the run store.

## Envelope

Every document starts with two keys:

```text
kind: thread
config_version: "2.0"
```

| Key | Values |
|-----|--------|
| `kind` | `project`, `loom`, `weave`, `thread`, `warp` or `params`. Loom, weave and thread are executable. |
| `config_version` | A `"major.minor"` string. This release reads major version 2 (write `"2.0"`). |

- `kind` is authoritative. File extensions (`.thread`, `.weave`, `.loom`,
  `.warp`) are convention, but a `kind` that disagrees with a typed extension
  fails the load with [`C207`](diagnostics.md#c207).
- A missing `kind` fails with [`C206`](diagnostics.md#c206); an unknown one
  with [`C204`](diagnostics.md#c204).
- A v1 document (`config_version` 1.x) fails with
  [`C203`](diagnostics.md#c203), and the error names the `weevr migrate`
  command. See [Migration rules](migration-rules.md).
- Each file holds one document.

## The project document

`weevr.yaml` has `kind: project`. It gives the project an `id` and `name`,
can pin the weevr versions it needs (`requires_weevr`), and can configure the
run store. The keys are on the [Project schema](yaml-schema/project.md) page.
The scope blocks `connections`, `params`, `defaults` and `audit_templates` are
reserved and fail with [`C210`](diagnostics.md#c210).

## Project root

The project root is the folder that holds `weevr.yaml`.

- **`Context`** requires it. `Context(project=...)` resolves a directory, and
  that directory must contain `weevr.yaml`. If it doesn't, construction fails
  with [`C209`](diagnostics.md#c209), listing every location probed. A
  `weevr.yaml` that declares another kind fails with
  [`C207`](diagnostics.md#c207).
- **Core loads of a bare file** (`load_config`, `load_resolved` with no root
  given) walk up the parent folders for the nearest `weevr.yaml`. When none is
  found, the file's own folder is the root and the project scope is empty:
  `project` is `None` on the result.
- The `.weevr` folder suffix (`sales.weevr`) is a naming convention for the
  name-based `Context` forms, not a requirement. Fabric Git folders are named
  `<name>.<ItemType>`. Either way the folder is the root only if it holds
  `weevr.yaml`.

A walk-up can find an unrelated `weevr.yaml` in an ancestor folder. Check the
root a load reports when a bare file picks up unexpected project settings.

## References and entries

A reference points to another document in the project:

```text
ref: [scheme://]relative-path
```

- A reference with no scheme is a path **relative to the project root**,
  whatever file it appears in.
- It must stay inside the root. A path that escapes (`../other/x.weave`) fails
  with [`C303`](diagnostics.md#c303).
- An absolute path fails with [`C311`](diagnostics.md#c311).
- The path must end in `.thread`, `.weave`, `.loom` or `.warp`
  ([`C007`](diagnostics.md#c007) otherwise), and the file must exist
  ([`C301`](diagnostics.md#c301)).
- A reference with a scheme (`scheme://...`) fails with
  [`C312`](diagnostics.md#c312). The grammar leaves room for schemes, but
  none is supported in 2.0.

Loom `weaves` entries and weave `threads` entries take one of two forms:

```text
weaves:
  - ref: weaves/silver.weave      # a file
  - name: gold                    # an inline body
    config_version: "2.0"
    threads:
      - ref: threads/gold/revenue.thread
```

A bare `name:` with neither `ref:` nor an inline body fails with
[`C313`](diagnostics.md#c313): write `ref: <name>.weave` for a weave entry or
`ref: <name>.thread` for a thread entry. See the
[Loom](yaml-schema/loom.md) and [Weave](yaml-schema/weave.md) schemas for the
entry keys.

## Standalone execution profile

When you load or run a single document directly, it sees the project scope
(when a `weevr.yaml` was found), its own configuration, and the invocation
params. It does not borrow defaults from a loom that would normally contain
it. A thread that needs its loom's defaults runs through the loom, selecting
threads with `threads=[...]`.

## Definition layout

A typical project folder:

```text
sales.weevr/
  weevr.yaml
  looms/
    nightly.loom
  weaves/
    silver.weave
  threads/
    silver/
      customer.thread
  warps/
    customer.warp
  reference/
    country_codes.csv
  README.md
```

Only `weevr.yaml` is fixed by name. Folder names are convention, because
every reference is written out in the document that uses it.

### Discovery

No document lists the project's documents. Tooling finds them by scanning the
tree. `discover_documents(root)` in `weevr.config` returns each document's
path (relative to the root) and its `kind`, sorted by path:

- Files with a `.thread`, `.weave`, `.loom` or `.warp` extension, and
  `.yaml` / `.yml` files that declare a known `kind`, are documents.
  `weevr.yaml` is listed with kind `project`.
- Other files are inert definition content and are carried untouched.
- A typed-extension file that declares no known `kind` is still listed, with
  kind `None`, so tooling can report it.
- Dot-folders (including `.weevr/`) and `.platform` files are skipped.
- The root must be an existing local folder; anything else fails with
  [`C209`](diagnostics.md#c209).

Adding a file changes only that file's entry.

### Reference data

If it should be reviewed in a pull request, it is definition. Small authored
text files, such as CSV, JSON or YAML code mappings and seed rows, live in the
definition tree (convention: a `reference/` folder). Sources address them with
project-root-relative paths, and they deploy with the project. Git limits
apply (25 MB per file).

Data that differs per environment or is produced elsewhere is an ordinary
source in a lakehouse, or in the run store's `Files/` area seeded per
environment. It is not part of the definition.

## Item mapping

How a project maps onto a Fabric item (packaging itself is a 2.x feature):

- One project is one Fabric item. Its definition parts are the files of the
  tree, one part per file, with paths kept verbatim, as UTF-8 text.
- `weevr.yaml` is the entry part. There is no index part.
- `.platform` is owned by Fabric. It is not a weevr document and discovery
  skips it.
- Parts weevr does not recognize are ignored on load and preserved when the
  item is repacked.
- Documents carry no container data: no item GUIDs, nothing that couples them
  to `.platform`. The project `id` is weevr's own and is never derived from a
  Fabric ID.

The same tree works from a local folder, a Git checkout, a Fabric item
definition and a lakehouse `Files/` folder.

## Collaboration rules

The format keeps merges clean:

- One document per file, at a stable path.
- No document enumerates the others, so two branches that add files don't
  collide on a shared index. Only a concurrent edit of the same lines
  conflicts, and that is a real conflict.
- `id` is written once.

Tools that write definitions (the designer, `weevr migrate`, `weevr init`)
rewrite only the documents that change, keep key order, comments and
formatting, and never write derived, cached or timestamp values into
documents.

A clean merge can still break a project, for example a rename on one branch
against a new reference on another. Run `validate` on the merged tree in pull
request CI.

## Run store

The run store holds what runs produce. It lives per environment, never in the
definition. Its locations are computed without Spark by
`weevr.config.run_store.resolve_run_store`, and a `Context` exposes them as
`Context.run_store` (`None` when nothing implies a base, in which case
persistence is skipped).

!!! note "Writers"
    This release defines the run store's locations and names. The components
    that write execution history, validation results and quarantine adopt
    them in later releases, and own their table schemas.

### Tables and files

| Output | Name |
|--------|------|
| Execution history | `execution_history` |
| Validation results | `validation_results` |
| Quarantine | `quarantine` |
| Run files | `runs/<utc-ts>_<run_id>/` under `Files/` |

These table names are reserved. Every row carries the project's `id` as
`project_id`. Table schemas belong to the components that write them and
evolve additively only: columns are added, never removed or retyped.

### Base

The base is the folder that holds the store's `Tables/` and `Files/` areas.
It is chosen in this order:

| Case | Base |
|------|------|
| `run_store.connection` set (or the `Context(run_store=...)` override) | The lakehouse of that connection. An unknown name fails with [`R201`](diagnostics.md#r201). |
| Project under the default lakehouse's `Files/` | The default lakehouse (`/lakehouse/default`) |
| Project at an `abfss://` OneLake path under an item's `Files/` | That item. |
| Any other local folder or Git checkout | `<root>/.weevr/run_store`, written as Delta by path |
| In-memory definition, or another remote location | None. Pass `Context(run_store=...)` with a `connection`, or persistence is skipped with a warning. |

`Context(run_store=RunStore(...))` replaces the `run_store` block of
`weevr.yaml` as a whole.

`weevr.yaml` can't declare connections yet ([`C210`](diagnostics.md#c210)),
so `Context.run_store` looks `run_store.connection` up in the connections of
the config the `Context` loaded last. Before any load, or when that config
doesn't declare the name, it fails with [`R201`](diagnostics.md#r201).

### Layout

| Output | `per_project` (default) | `shared` |
|--------|-------------------------|----------|
| Tables | `weevr_<name>.<table>` | `<schema>.<table>` (schema defaults to `weevr`) |
| Run files | `Files/weevr_<name>/runs/` | `Files/<schema>/runs/<name>/` |

- In `per_project`, `run_store.schema` replaces `weevr_<name>`.
- A Fabric item's own OneLake can hold `per_project` tables directly under
  `Tables/<table>` and run files under `Files/runs/` (under
  `Tables/<schema>/` and `Files/<schema>/runs/` when `run_store.schema` is
  set). A path alone doesn't say whether an item is a lakehouse, so this form
  applies only when the caller of `resolve_run_store` passes `fet_item=True`;
  `Context.run_store` uses the lakehouse layout above.
- A lakehouse without schemas takes the prefix form: `weevr_<name>_<table>`
  (or `<schema>_<table>` when shared). This applies when the caller of
  `resolve_run_store` passes `schema_enabled=False`; `Context.run_store`
  assumes a schema-enabled lakehouse.
- Names derive from the project `name`. Renaming a project starts a new
  schema.
- Two projects with the same `name` in one lakehouse share a `per_project`
  schema. Their rows stay apart by `project_id`.
- Redirecting to a central lakehouse needs the run identity to have write
  access there. weevr does not grant it.

## See also

- [Project schema](yaml-schema/project.md): the `weevr.yaml` keys
- [Configuration keys](configuration-keys.md)
- [Migration rules](migration-rules.md): upgrading a v1 project
- [Diagnostic codes](diagnostics.md): `C203`, `C204`, `C206` to `C210`,
  `C303`, `C311` to `C313`, `R201`
