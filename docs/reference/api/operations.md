# Operations API

The `weevr.operations` module implements the concrete readers, transformation
steps, writers, audit column injection, export writers, and quarantine
handling that the engine dispatches to. Each pipeline step type maps to
an operation function.

::: weevr.operations
    options:
      members: true
      show_root_heading: true
      show_source: false
