# Run from a Fabric Pipeline

**Goal:** Execute a weevr loom from a Microsoft Fabric notebook, handle the
results, and schedule the notebook through a Fabric Data Pipeline for automated
production runs.

```d2
direction: right

pipeline: Fabric Data Pipeline {
  style.fill: "#E3F2FD"

  trigger: Schedule Trigger\n(daily, hourly) {style.fill: "#BBDEFB"}
  params: Pipeline Parameters\nrun_date, batch_id {style.fill: "#BBDEFB"}

  notebook_activity: Notebook Activity {
    style.fill: "#FFF3E0"

    cell1: "from weevr import Context" {style.fill: "#FFE0B2"}
    cell2: "ctx = Context(spark, ...)\nresult = ctx.run('nightly.loom')" {style.fill: "#FFE0B2"}
    cell3: "if result.status == 'failure':\n    raise RuntimeError(...)" {style.fill: "#FFE0B2"}
    cell4: "mssparkutils.notebook.exit(\n    result.status\n)" {style.fill: "#FFE0B2"}

    cell1 -> cell2 -> cell3 -> cell4
  }

  trigger -> params -> notebook_activity
}

lakehouse: Lakehouse {
  style.fill: "#E8F5E9"
  configs: "Files/\n  my-project.weevr/\n    nightly.loom\n    dimensions.weave\n    ..." {style.fill: "#C8E6C9"}
  tables: "Tables/\n  bronze.*/\n  silver.*/" {style.fill: "#C8E6C9"}
}

pipeline.notebook_activity -> lakehouse: "read configs\nwrite targets" {style.stroke: "#2E7D32"}

success: Pipeline Success {style.fill: "#C8E6C9"}
failure: Pipeline Failure {style.fill: "#FFCDD2"}

pipeline.notebook_activity -> success: "exit('success')" {style.stroke: "#2E7D32"}
pipeline.notebook_activity -> failure: "raise RuntimeError" {style.stroke: "#C62828"}
```

## Prerequisites

- A Microsoft Fabric workspace with a Lakehouse attached
- weevr installed in the Fabric Spark environment (or inline via `%pip`)
- A weevr project with config files (threads, weaves, looms) synced to the
  Lakehouse via Fabric Git integration or uploaded manually

## Step 1 -- Create a Fabric notebook

In your Fabric workspace, create a new notebook. Attach it to a Lakehouse that
contains your weevr config files and source data.

Install weevr if it is not already part of the Spark environment:

```python
%pip install weevr
```

## Step 2 -- Import weevr and get the SparkSession

Fabric notebooks provide a pre-configured `spark` variable. Use it directly
with the weevr `Context`:

```python
from weevr import Context

ctx = Context(
    spark,
    "my-project.weevr",
    log_level="standard",
)
```

!!! tip "Environment-specific parameters"
    Use the `params` argument to pass environment-specific values
    at runtime. Variables in your YAML configs resolve from these parameters.

## Step 3 -- Execute the loom

Call `ctx.run()` with the path to your loom config:

```python
result = ctx.run("nightly.loom")
```

The path resolves relative to the notebook's working directory. In Fabric, this
is typically the root of the attached Lakehouse's Files section. Adjust the
path if your config files are in a subdirectory.

## Step 4 -- Handle the result

Inspect the `RunResult` and decide how to surface outcomes:

```python
print(result.summary())

if result.status == "failure":
    # Log details for operational visibility
    for warning in result.warnings:
        print(f"WARNING: {warning}")

    # Optionally raise to signal failure to the pipeline
    raise RuntimeError(f"Loom execution failed: {result.config_name}")
```

For partial failures (some threads succeeded, others failed):

```python
if result.status == "partial":
    print("Partial success -- some threads failed.")
    print(result.summary())
```

## Step 5 -- Pass runtime parameters

Override parameter file values or inject execution-specific values through the
`params` argument:

```python
ctx = Context(
    spark,
    "my-project.weevr",
    params={
        "run_date": "2025-06-15",
        "batch_id": "nightly_20250615",
    },
)

result = ctx.run("nightly.loom")
```

Runtime parameters take highest priority, followed by config defaults.

## Step 6 -- Schedule via a Fabric Data Pipeline

Create a Fabric Data Pipeline to automate execution:

1. In your workspace, create a new **Data Pipeline**.
2. Add a **Notebook activity** to the pipeline canvas.
3. Configure the activity to run your weevr notebook.
4. Set pipeline parameters if you need to pass dynamic values (such as a run
   date) into the notebook. Access them with `mssparkutils.notebook.params`.
5. Add a **Schedule trigger** with your desired frequency (daily, hourly, etc.).

To pass pipeline parameters into weevr:

```python
from weevr import Context

# Read pipeline parameters injected by the Notebook activity
run_date = mssparkutils.notebook.params.get("run_date", "")

ctx = Context(
    spark,
    "my-project.weevr",
    params={"run_date": run_date},
)

result = ctx.run("nightly.loom")

# Signal success or failure to the pipeline
if result.status == "failure":
    raise RuntimeError(f"Loom failed: {result.summary()}")

# Exit cleanly to mark the activity as successful
mssparkutils.notebook.exit(result.status)
```

!!! info "Pipeline error propagation"
    Raising an exception from the notebook causes the pipeline Notebook activity
    to report failure. Downstream pipeline activities can branch on this status
    using success/failure conditions.

## Verify the scheduled run

After the first scheduled execution completes:

1. Check the pipeline run history in Fabric for success/failure status.
2. Open the notebook run output to see `result.summary()` and any warnings.
3. Query the target Delta tables to confirm data landed as expected.
4. If you have a [telemetry sink](implement-custom-telemetry-sink.md),
   verify that telemetry rows were written for the run.
