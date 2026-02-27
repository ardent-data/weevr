# Idempotency

weevr aims for deterministic behavior: the same configuration and inputs
should produce the same result. This page explains how idempotency works
across write modes and how the framework maintains safe rerun semantics.

## Determinism by design

weevr's interpretive execution model is inherently deterministic. The engine
reads YAML, builds an execution plan, and applies Spark DataFrame operations
in a fixed order. There is no randomness, no generated code that varies
between runs, and no implicit state beyond what is explicitly declared.

!!! note "Business data vs. system columns"

    Idempotency applies to **business data**. Audit columns such as
    `_weevr_loaded_at` and `_weevr_run_id` will differ between reruns.
    Two runs with the same inputs produce the same business rows, but
    timestamps and run identifiers reflect the actual execution.

## Idempotency by write mode

```d2
direction: right

overwrite: Overwrite {
  run1: Run 1 {
    source: Source\n3 rows
    target: Target\n3 rows
    source -> target: write
  }
  run2: Run 2 (rerun) {
    source: Source\n3 rows
    target: Target\n3 rows
    source -> target: replace
  }
  result: "Idempotent ✓\nSame 3 rows"
  result.style.font-color: "#2E7D32"
  run1 -> run2: rerun
  run2 -> result
}

append: Append {
  run1: Run 1 {
    source: Source\n3 rows
    target: Target\n3 rows
    source -> target: insert
  }
  run2: Run 2 (rerun) {
    source: Source\n3 rows
    target: Target\n6 rows
    source -> target: insert
  }
  result: "Not idempotent ✗\nDuplicate rows"
  result.style.font-color: "#C62828"
  run1 -> run2: rerun
  run2 -> result
}

merge: Merge {
  run1: Run 1 {
    source: Source\n3 rows
    target: Target\n3 rows
    source -> target: upsert
  }
  run2: Run 2 (rerun) {
    source: Source\n3 rows
    target: Target\n3 rows
    source -> target: match + update
  }
  result: "Idempotent ✓\nSame 3 rows"
  result.style.font-color: "#2E7D32"
  run1 -> run2: rerun
  run2 -> result
}
```

Each write mode has different rerun characteristics:

| Write mode | Idempotent? | Behavior on rerun |
|---|---|---|
| `overwrite` | Yes | Target is fully replaced with the same data |
| `append` | **No** | Duplicate rows are inserted |
| `merge` | Yes | Match keys ensure the same upsert result |

**Overwrite** is naturally idempotent. The target table is replaced
entirely, so rerunning produces identical business data regardless of
how many times the thread executes.

**Append** is not idempotent. Each run inserts rows, and a rerun inserts
them again. To prevent duplicates, pair append with an incremental load
mode that prevents reprocessing the same source window. See
[Execution Modes](execution-modes.md) for load mode details.

**Merge** is idempotent by match key. The merge operation matches source
rows to target rows using the declared `match_keys`. Rerunning with the
same source data produces the same target state because matched rows are
updated to the same values and unmatched rows are inserted identically.

## Watermarks and state

Incremental watermark loads maintain state between runs. After a successful
execution, weevr persists the high-water mark (the maximum value of the
watermark column from the source data that was just processed). On the next
run, only rows beyond that watermark are read.

This creates a natural deduplication boundary: each run processes a
non-overlapping window of source data. Combined with merge or overwrite
write modes, the result is idempotent incremental processing.

!!! tip "Inclusive watermarks"

    By default, the watermark filter uses a strict greater-than comparison,
    so rows at the exact boundary are not re-read. If your source can
    receive late-arriving updates at the boundary value, set
    `watermark_inclusive: true` and pair it with a merge write mode. The
    merge ensures that re-read rows are updated rather than duplicated.

## Merge and match keys

The merge write mode depends on `match_keys` to identify which source rows
correspond to which target rows. Choosing the right match keys is critical
for correct idempotent behavior:

- Match keys should uniquely identify a business entity in the target table.
- If match keys are not unique, the merge may produce unexpected results
  (multiple source rows matching the same target row, or vice versa).
- The `keys.business_key` declaration and the `write.match_keys` declaration
  often use the same columns, but they serve different purposes: business
  keys identify the entity, match keys drive the merge operation.

## Null-safe key handling

Null values in key columns are a common source of silent data quality
issues in Spark. weevr applies **opinionated null-safe defaults** across
all key operations:

- **Surrogate keys** -- Null values in key columns are replaced with
  deterministic sentinel values before hashing. This ensures surrogate
  keys are always non-null and consistent.
- **Join keys** -- Null-safe equality (`<=>`) is used by default for all
  join conditions, preventing silent data loss when key columns contain
  nulls.
- **Change detection hashes** -- Null values are replaced with
  type-appropriate sentinels before hashing, ensuring that a null-to-value
  change is correctly detected.
- **Merge match keys** -- Null-safe comparison ensures that rows with null
  key values are matched correctly rather than silently dropped.

These defaults prevent the most common Spark null-related pitfalls without
requiring authors to add defensive logic to every thread.

## Next steps

- [Execution Modes](execution-modes.md) -- Write and load mode details
- [Thread, Weave, Loom](thread-weave-loom.md) -- The object model
- [YAML Schema: Thread](../reference/yaml-schema/thread.md) -- Full write
  and key configuration reference
