# Context API

The `Context` class is the primary entry point for running weevr pipelines.
It manages configuration loading, parameter injection, and orchestrates
thread, weave, and loom execution. The `run()` method returns a `RunResult`,
and `load()` returns a `LoadedConfig` — both documented below.

::: weevr.context
    options:
      members: true
      show_root_heading: true
      show_source: false

---

## Result Types

::: weevr.result
    options:
      members: true
      show_root_heading: true
      show_source: false
