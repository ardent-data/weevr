# State API

The `weevr.state` module manages watermark persistence for incremental loads.
`resolve_store` returns the `WatermarkStore` for a thread's `load` config:
Delta table properties on the target (the default) or a dedicated metadata
table (`watermark_store.type: metadata_table`). The concrete store classes
are internal; use the `WatermarkStore` interface.

::: weevr.state
    options:
      members: true
      show_root_heading: true
      show_source: false
