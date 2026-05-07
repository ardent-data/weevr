# Engine API

The `weevr.engine` module contains the execution planner and executor. It
resolves the thread dependency DAG, schedules execution order, and runs each
thread through its read-transform-write lifecycle. The engine also handles
pre/post weave hooks, lookup pre-materialization, conditional thread
execution, and weave-scoped variable binding.

!!! note "Wheel provenance"

    `weevr.engine` is *federated* across both PyPI distributions.
    `ExecutionPlan` and `build_plan` (along with the duration and
    column-name formatters) ship from **`weevr-core`** and import without
    PySpark — useful for tools that visualize a plan without running it.
    The remaining symbols on this page (executor, runner, cache, display,
    hooks, lookups, conditions, column sets, resources, result, variables)
    ship from **`weevr`** and require Spark at runtime.

::: weevr.engine
    options:
      members: true
      show_root_heading: true
      show_source: false
