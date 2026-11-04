# Move to Runtime 2.0

**Goal:** Move a weevr workload that runs on Fabric Runtime 1.3 to Runtime 2.0,
test it before production switches over, and keep a way back.

## Prerequisites

- A weevr workload running on Runtime 1.3, in notebooks or Spark Job
  Definitions
- Permission to create and publish Environments in the workspace

## What carries over

Your project, configs, and data move as they are. weevr's YAML, its output, and
its stored state are the same on both runtimes:

- **Configs.** Threads, weaves, and looms need no changes. The same configs
  produce the same tables, keys, and hashes on either runtime.
- **Incremental state.** Watermark and CDC state is stored with your data, and
  both runtimes read and update it the same way. An incremental thread that last
  ran on Runtime 1.3 continues on Runtime 2.0 from where it stopped, reading
  only new rows or changes. The same holds in the other direction.
- **Existing tables.** Runtime 2.0 appends to, merges into, and overwrites
  tables that Runtime 1.3 created without changing their Delta protocol, so
  they stay readable everywhere they were readable before.

Two things do change:

- **New tables get deletion vectors.** Tables that Runtime 2.0 creates have
  deletion vectors enabled, which sets their Delta protocol to reader version 3
  and writer version 7. Runtime 1.3 reads these tables and merges into them, but
  readers outside Fabric that use an older Delta version can't. A protocol
  upgrade can't be undone.
- **Your own Spark SQL runs on Spark 4.1.** Expressions, filters, and SQL that
  you write in YAML run on the new Spark version. Most behave the same, but
  Spark 4 changed some functions and defaults. Step 4 covers what to check.

## Step 1 -- Upgrade weevr while still on Runtime 1.3

weevr 1.21 is the first version that installs on Runtime 2.0, and it also runs
on Runtime 1.3. Upgrade first, on the runtime you already use, so the weevr
upgrade and the runtime change happen separately:

1. In your Runtime 1.3 Environment, set the `weevr` library to version 1.21 or
   later and select **Publish**. If you install with `%pip`, change the pinned
   version instead.
2. Run your workload as usual and confirm that it succeeds.

## Step 2 -- Pin production items to Runtime 1.3

Runtime 2.0 is the default for new workspaces, and changing a workspace's
runtime also moves its existing items the next time they start a session.
To control when production moves, attach each production notebook and Spark Job
Definition to a published Environment set to Runtime 1.3. An item attached to a
1.3 Environment stays on Runtime 1.3, even in a workspace whose default is
Runtime 2.0.

Keep this Environment after the move. It is your way back.

## Step 3 -- Create a Runtime 2.0 Environment

Create a second Environment rather than changing the existing one:

1. In your workspace, select **New item** > **Environment**.
2. Set its runtime to **2.0**.
3. Under **Libraries**, add `weevr` with the same version as in Step 1. Add any
   other libraries your workload uses, checking that each has a version that
   supports Python 3.13.
4. Select **Publish**.

If you install weevr from uploaded wheel files, upload both the `weevr` and
`weevr-core` wheels to the new Environment. If you change the runtime of an
existing Environment instead, republish it so its libraries install for Python
3.13.

## Step 4 -- Test a copy on Runtime 2.0

Run a copy of your workload on the new Environment before production moves:

1. Copy a notebook that runs a loom, attach the copy to the Runtime 2.0
   Environment, and point it at non-production tables. Use
   [variables](../guides/fabric-runtime.md#variable-injection-for-lakehouse-paths)
   to switch the Lakehouse paths.
2. Run the loom and check that `result.status` is `success`.
3. Compare the output tables with the ones that Runtime 1.3 produced.

Pay attention to anything that weevr passes to Spark unchanged:

- **Spark SQL in your configs.** Expressions in steps such as `filter`,
  `derive`, and `aggregate`, validation rules, and `sql_statement` hooks run on
  Spark 4.1. Check them against Microsoft's
  [Runtime 2.0 migration guide](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0-migration-guide)
  and the Spark
  [SQL migration guide](https://spark.apache.org/docs/latest/sql-migration-guide.html).
- **ANSI mode.** Fabric runs with `spark.sql.ansi.enabled` off on both
  runtimes, and weevr doesn't change it. If you turn it on, a cast that fails
  raises an error instead of returning `NULL`.
- **Python code in your notebooks.** Code around the `ctx.run()` call runs on
  Python 3.13, with Runtime 2.0's library versions.

The Native Execution Engine gives the same results whether it is on or off, so
you can test with the setting you plan to use in production.

## Step 5 -- Switch production items

Attach each production notebook and Spark Job Definition to the Runtime 2.0
Environment. Incremental threads continue from their stored state on the next
run, so no full reload is needed.

If a Fabric Pipeline runs your notebooks, nothing in the pipeline changes: the
Notebook activity runs each notebook on the Environment it is attached to.

## Roll back

To go back, attach the items to the Runtime 1.3 Environment again. weevr 1.21
runs there, and incremental threads continue from the state that Runtime 2.0
left. Tables that Runtime 2.0 created keep their deletion vectors, and Runtime
1.3 reads and merges into them.

## See also

- [Fabric Runtime](../guides/fabric-runtime.md#runtime-compatibility) -- Runtime
  compatibility and choosing a runtime
- [Compatibility](../reference/compatibility.md) -- Supported versions
- [Run from Fabric Pipeline](run-from-fabric-pipeline.md) -- Scheduled execution
