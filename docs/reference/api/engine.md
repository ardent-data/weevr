# Engine API

The `weevr.engine` module contains the execution planner and executor. It
resolves the thread dependency DAG, schedules execution order, and runs each
thread through its read-transform-write lifecycle. The engine also handles
pre/post weave hooks, lookup pre-materialization, conditional thread
execution, and weave-scoped variable binding.

::: weevr.engine
    options:
      members: true
      show_root_heading: true
      show_source: false
