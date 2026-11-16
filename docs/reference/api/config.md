# Config API

The `weevr.config` module handles YAML parsing, schema validation, configuration
inheritance, and parameter resolution across the loom/weave/thread hierarchy.
`load_resolved()` and `Context.load()` return the result as a tree of
`ResolvedDocument` nodes (`LoadedConfig.document`); see
[The document tree](../../guides/config-resolution.md#the-document-tree).

::: weevr.config
    options:
      members: true
      show_root_heading: true
      show_source: false
