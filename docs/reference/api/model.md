# Model API

The `weevr.model` package exports the Pydantic domain models that represent
the configuration structure -- threads, weaves, looms, pipeline steps, sources,
targets, hooks, lookups, exports, variables, failure configuration, and
supporting types. The warp contract models (`.warp` files and the drift
report) are not exported here: see the [Warps guide](../../guides/warps.md)
for the `.warp` format, and
[`SchemaDriftError`](errors.md#schemadrifterror) for the drift report's
fields.

::: weevr.model
    options:
      members: true
      show_root_heading: true
      show_source: false
