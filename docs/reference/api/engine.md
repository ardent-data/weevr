# Engine API

The `weevr.engine` module contains the execution planner and executor. It
resolves the thread dependency DAG, schedules execution order, and runs each
thread through its read-transform-write lifecycle. The engine also handles
pre/post hooks, lookup pre-materialization, conditional execution, and
variable binding at loom, weave, and thread level.

!!! note "Wheel provenance"

    `weevr.engine` is *federated* across both PyPI distributions.
    `ExecutionPlan` and `build_plan` ship from **`weevr-core`** and
    import without PySpark — useful for tools that visualize a plan
    without running it. The other symbols on this page
    (`execute_thread`, `execute_weave`, `execute_loom`, the
    `ThreadResult` / `WeaveResult` / `LoomResult` models, `CacheManager`,
    `DAGDiagram`, and `FlowDiagram`) ship from **`weevr`** and require
    Spark at runtime. Hooks, lookups, conditions, and variables run
    inside the executor and runner and are not part of the public API.

::: weevr.engine
    options:
      members: true
      show_root_heading: true
      show_source: false
