# Fabric Runtime

This guide covers running weevr on Microsoft Fabric, including environment
setup, Lakehouse integration, and tips for moving between development and
production.

## Runtime compatibility

weevr targets **Fabric Runtime 1.3**, which ships with:

| Component  | Version    |
|------------|------------|
| Python     | 3.11       |
| PySpark    | 3.5.x      |
| Delta Lake | 3.2.x      |

weevr pins `requires-python = ">=3.11,<3.12"` to match. All transforms,
readers, writers, and telemetry primitives are tested against PySpark 3.5 and
Delta 3.2.

!!! note "Runtime version lock"

    Fabric Runtime versions are fixed per workspace capacity. If you upgrade
    to a newer runtime in the future, check the
    [Compatibility](../reference/compatibility.md) page for supported
    version combinations.

## Installing weevr

### Option 1 -- pip install (recommended)

In a Fabric Notebook, install weevr at the top of the first cell:

```python
%pip install weevr
```

This pulls the latest release from PyPI and makes it available for the
duration of the Spark session.

For a persistent installation that survives notebook restarts, add weevr to
your **Fabric Environment** via the workspace portal:

1. Go to **Workspace settings** > **Data Engineering** > **Environment**.
2. Under **Public libraries**, add `weevr` with the desired version pin.
3. Save and restart your Spark session.

### Option 2 -- wheel upload

If your workspace has restricted network access, download the `.whl` file
from PyPI and upload it to a Lakehouse Files area:

```python
%pip install /lakehouse/default/Files/libs/weevr-0.7.4-py3-none-any.whl
```

Replace the version number with the actual wheel filename.

### Option 3 -- inline package in a Spark Job Definition

For production jobs, declare weevr in the Spark Job Definition's
**Referenced libraries** section. This avoids install latency on each run
and ensures reproducible builds.

## Lakehouse integration

Fabric Lakehouses expose Delta tables through `abfss://` paths. weevr reads
and writes these tables using standard Spark DataFrame APIs, so any path
that Spark can resolve works in thread configuration.

### Path formats

Fabric provides several ways to reference Lakehouse tables:

```yaml
# Relative path — resolves within the attached Lakehouse
target:
  path: Tables/dim_customer

# Absolute abfss:// path — cross-Lakehouse or cross-workspace
sources:
  raw_orders:
    type: delta
    path: "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Tables/raw_orders"
```

!!! tip "Use variables for environment portability"

    Hard-coding `abfss://` paths makes configs environment-specific.
    Use YAML variable injection to keep configs portable across
    dev, staging, and production workspaces.

### Variable injection for Lakehouse paths

Define environment-specific paths in a parameter file and reference them
in thread configuration:

**params/prod.yaml**

```yaml
lakehouse_base: "abfss://prod-ws@onelake.dfs.fabric.microsoft.com/prod.Lakehouse"
bronze_base: "${lakehouse_base}/Tables/bronze"
silver_base: "${lakehouse_base}/Tables/silver"
```

**params/dev.yaml**

```yaml
lakehouse_base: "Tables"
bronze_base: "${lakehouse_base}/bronze"
silver_base: "${lakehouse_base}/silver"
```

**threads/staging/stg_customers.yaml**

```yaml
config_version: "1.0"

sources:
  raw_customers:
    type: delta
    path: "${bronze_base}/customers"

target:
  path: "${silver_base}/stg_customers"

write:
  mode: overwrite
```

Pass the parameter file through the `Context`:

```python
from weevr import Context

ctx = Context(spark, param_file="params/prod.yaml")
result = ctx.run("looms/nightly.yaml")
```

This pattern lets the same YAML configs run in any environment by swapping
the parameter file.

## Workspace and capacity considerations

### Workspace-level isolation

Each Fabric workspace maps to its own Lakehouse namespace. Keep weevr
config projects scoped to a single workspace when possible. Cross-workspace
reads require full `abfss://` paths and appropriate access permissions.

### Capacity sizing

weevr executes threads concurrently within a weave using a
`ThreadPoolExecutor`. The number of simultaneous Spark operations depends
on the capacity allocated to your workspace:

- **Small/Medium capacities** -- Limit weave concurrency to avoid Spark
  job queue contention. Use sequential weaves for heavy workloads.
- **Large capacities** -- Weave-level parallelism is effective. Threads
  that are independent in the DAG execute simultaneously.

Cache configuration also affects memory. If you are caching lookup tables
across threads, ensure executor memory is sized accordingly. See
[Cache a Lookup](../how-to/cache-a-lookup.md) for details.

### Spark configuration

Fabric sets Spark configuration at the capacity and workspace level. weevr
does not override Spark settings. If you need to tune shuffle partitions,
executor memory, or other Spark properties, configure them through:

- **Workspace Spark settings** in the Fabric portal
- **Spark session configuration** in notebook `%%configure` cells
- **Spark Job Definition** configuration for production jobs

## Development vs production

### Local development

For local iteration, use a standalone Spark installation:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)
```

Local Delta tables write to the filesystem. Use relative paths in your dev
parameter file so that `Tables/stg_customers` resolves to a local directory.

This approach lets you run the full weevr pipeline -- threads, weaves, and
looms -- without a Fabric workspace. The same YAML configs work in both
environments.

### Production

In production, weevr runs inside a Fabric Notebook or Spark Job Definition
where the `spark` variable is pre-initialized by the runtime:

```python
from weevr import Context

ctx = Context(
    spark,
    param_file="params/prod.yaml",
    log_level="standard",
)
result = ctx.run("looms/nightly.yaml")

assert result.status == "success", result.summary()
```

!!! warning "Do not call `SparkSession.builder` in Fabric"

    Fabric provides a managed `spark` session. Creating a new session
    with `SparkSession.builder` can cause unexpected behavior. Always use
    the pre-existing `spark` variable.

### Running from a Fabric Pipeline

Fabric Pipelines can invoke Notebooks as activities. This is the standard
pattern for scheduled execution:

1. Create a Notebook that calls `ctx.run()`.
2. Add the Notebook as a **Notebook activity** in a Fabric Pipeline.
3. Pass runtime parameters (e.g., `run_date`) through Pipeline parameters
   that map to notebook widgets or cell parameters.

See [Run from Fabric Pipeline](../how-to/run-from-fabric-pipeline.md) for
a step-by-step walkthrough.

## Troubleshooting

### Package not found after install

If `%pip install weevr` succeeds but `from weevr import Context` fails,
restart the Spark session. In-session pip installs require a kernel restart
to take effect in some Fabric configurations.

### Permission denied on abfss:// paths

Ensure your Fabric workspace identity has **Contributor** or **Admin** access
to the target Lakehouse. Cross-workspace reads also require explicit sharing.

### Slow first run

The first execution in a Fabric session incurs Spark startup overhead. This
is a Fabric characteristic, not a weevr issue. Subsequent runs reuse the warm
session and are significantly faster.

## Next steps

- [Your First Loom](../tutorials/your-first-loom.md) -- Build a complete
  pipeline from scratch
- [Observability](observability.md) -- Monitor execution with structured
  telemetry
- [Compatibility](../reference/compatibility.md) -- Supported version
  matrix
