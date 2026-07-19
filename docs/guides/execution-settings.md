# Execution Settings

The `execution:` block controls runtime behavior — logging verbosity,
telemetry collection, and thread parallelism. It is declared at the
**top level** of a loom or weave file:

```yaml
# nightly.loom
execution:
  max_parallel_threads: 3   # capacity-wide default

# facts.weave — inherits the cap, overrides logging only
execution:
  log_level: verbose
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `log_level` | `minimal` \| `standard` \| `verbose` \| `debug` | `standard` | Logging verbosity for execution output |
| `trace` | `bool` | `true` | Whether execution spans are collected for telemetry |
| `max_parallel_threads` | `int >= 1` | unset | Upper bound on concurrently executing threads within a weave's parallel group; unset means unbounded |
| `capture_samples` | `bool` | `false` | Capture 10-row output/quarantine samples at write time for display. Preview always samples regardless. |

## How loom and weave blocks combine

The effective settings for each weave merge **field-level**: a field the
weave explicitly sets wins; otherwise the loom's value applies; otherwise
the default. In the example above, the `facts` weave runs with
`log_level: verbose` *and* the loom's cap of 3 — declaring a
logging-only block does not drop an inherited capacity cap.

This is a deliberate exception to the usual cascade rule where value
blocks replace whole-value. The `execution:` block is the only top-level
block that merges per field.

## Thread parallelism

Threads inside a weave execute in dependency-ordered groups; threads
within a group run concurrently. By default each group's worker pool is
sized to the group, so six independent facts contend for the same Spark
executors at once. `max_parallel_threads` bounds that concurrency:

```yaml
execution:
  max_parallel_threads: 3
```

With a cap of 3, a six-thread group never has more than three threads in
flight. Groups still execute sequentially, and dependency ordering and
failure routing are unchanged. Size the cap to your Fabric capacity — a
good starting point is the number of concurrent Spark jobs your pool
serves without queueing.

When tracing is on, the weave span records the effective cap and the
per-group thread counts, so a capped run is identifiable in `RunResult`
telemetry.

## Log level

Precedence, highest first:

1. An explicit `Context(log_level=...)` argument — a runtime override
   that beats any YAML setting for the whole run.
2. The effective YAML value from the loom/weave `execution:` block.
3. The default, `standard`.

The effective level applies in every mode (`execute`, `validate`,
`plan`, `preview`). During a loom run, a weave whose effective level
differs from the loom's is applied for that weave and restored
afterward — including when the weave fails.

## Tracing

`trace: false` suppresses telemetry at the scope that declares it:

- **Loom scope** — no telemetry is collected for the run;
  `RunResult.telemetry` is `None`.
- **Weave scope** (under a tracing loom) — that weave's nodes are absent
  from the telemetry tree; other weaves are unaffected.
- **Standalone weave run** — the weave's own block gates collection.

Tracing composes one-directionally: a weave can turn tracing *off* under
a tracing loom, but `trace: true` on a weave cannot re-enable it under a
loom whose effective `trace` is false — there is no root collector to
attach to. Config validation warns when a weave declares an ineffective
`trace: true`.

Standalone thread runs always trace.

## Thread-scoped execution is not applied

`execution:` on a *thread* — whether authored in the thread file or
arrived via a `defaults.execution` block — is declared but **not
applied** in v1.x, in any invocation shape. Within a weave the cap is a
group property, and per-thread logging or tracing under parallel
execution would race on global state.

`defaults.execution` is therefore not a recognized declaration site: it
cascades to thread scope, where execution settings do not apply. A run
whose configs declare either form logs a warning at start naming the
unapplied fields and pointing at the top-level block:

```text
WARN: loom 'nightly' declares defaults.execution (log_level) — thread-scoped
execution settings are not applied. Move them to the top-level execution
block on the loom or weave.
```

## Output samples

Sample capture re-executes the working DataFrame's lineage, so it is
off by default. Enable it per loom or weave:

```yaml
execution:
  capture_samples: true
```

When disabled, result rendering shows a hint in place of the sample
table. Preview mode always captures samples — previewing is what it is
for. Like every `execution:` field, thread-scoped declarations are
accepted but not applied in v1.x and produce a run-start warning.
