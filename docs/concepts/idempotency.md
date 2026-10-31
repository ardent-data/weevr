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

```d2
direction: right

run1: Run 1 {
  style.fill: "#E3F2FD"
  source1: Source\nall rows\n(no prior watermark)
  target1: Target\n1,000 rows written
  wm1: "watermark = 2024-03-15" {style.fill: "#BBDEFB"}
  source1 -> target1: full read
  target1 -> wm1: persist
}

run2: Run 2 {
  style.fill: "#E8F5E9"
  source2: Source\nmodified_date >\n2024-03-15
  target2: Target\n150 new rows merged
  wm2: "watermark = 2024-04-22" {style.fill: "#C8E6C9"}
  source2 -> target2: incremental read
  target2 -> wm2: persist
}

run3: Run 3 {
  style.fill: "#FFF3E0"
  source3: Source\nmodified_date >\n2024-04-22
  target3: Target\n80 new rows merged
  wm3: "watermark = 2024-05-10" {style.fill: "#FFE0B2"}
  source3 -> target3: incremental read
  target3 -> wm3: persist
}

boundary: {
  style.fill: transparent
  style.stroke: transparent
  note: |md
    Each run processes a **non-overlapping window**.
    Combined with merge, this is idempotent incremental processing.
  |
}

run1 -> run2: next run
run2 -> run3: next run
```

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

### Stored values

Timestamp watermarks are stored as UTC instants in ISO-8601 form with
microseconds, such as `2024-03-15T17:00:00.000000Z`, so a watermark means
the same moment whatever the driver's or the Spark session's time zone.
`timestamp_ntz` columns, which carry no zone, are stored as their wall-clock
value. Date watermarks are stored as `2024-03-15` and integer watermarks as
digits.

Watermarks written by earlier weevr versions (timestamps without the `Z`)
are still read correctly — they are interpreted in the driver's time zone,
which is what they encode — and are rewritten in the new form at the next
run that advances the watermark.

### Deduplication and watermarks

A `dedup` on the primary (first) source must keep the latest row per key by
the watermark column — `order_by: "<watermark_column> DESC"`. The watermark
advances to the newest value read, so a dedup that keeps an older row per
key would drop that key's later changes for good. Any other `order_by`, or
none, fails the config load with the required value.

### Recovery after a failed state save

An incremental append records its new watermark in the Delta commit that
writes the data. If saving the watermark to the state store fails after the
write, the next run finds the newer value in the target's commit history
(the latest 100 commits) and continues from it instead of re-appending the
same rows. A watermark you set in the store after that commit — a manual
reset — takes precedence.

A first run that finds no stored or recorded watermark, but an append target
that already holds rows, fails before writing: re-reading the whole source
would duplicate data. Seed the watermark or truncate the target as
described below. Merge targets are not affected, since a merge does not
duplicate rows.

If that error says the target's commit history can't be read, retry once the
history is readable before you seed or truncate: the history may hold a
recorded watermark that the run can recover.

### Seed or reset a watermark

weevr has no command for setting a watermark; set it directly in the state
store. Use the same value form weevr stores (see [Stored values](#stored-values)).

A stored value that can't be parsed for the `watermark_type` fails the run
with a `StateError` rather than being ignored; seed or reset it to continue.
This includes a string `watermark_column` holding timestamps or dates in a
format Spark can't cast, which fails on the run after its watermark is
stored. For that column, set `load.watermark_format`.

**`metadata_table` store** — upsert a row for the thread into the table at
`watermark_store.table_path`:

```sql
MERGE INTO delta.`<table_path>` AS t
USING (SELECT 'my_thread' AS thread_name, 'modified_date' AS watermark_column,
              'timestamp' AS watermark_type,
              '2024-03-15T00:00:00.000000Z' AS last_value,
              current_timestamp() AS last_updated, CAST(NULL AS STRING) AS run_id) AS s
ON t.thread_name = s.thread_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Use the table alias instead of ``delta.`<table_path>` `` when the store is a
lakehouse table.

**`table_properties` store** (the default) — set the thread's properties on
the target table:

```sql
ALTER TABLE <target> SET TBLPROPERTIES (
  'weevr.watermark.my_thread.last_value' = '2024-03-15T00:00:00.000000Z',
  'weevr.watermark.my_thread.watermark_column' = 'modified_date',
  'weevr.watermark.my_thread.watermark_type' = 'timestamp',
  -- Replace with the current UTC time, for example 2026-10-31T09:30:00+00:00
  'weevr.watermark.my_thread.last_updated' = '<current_utc_time>'
)
```

Set `last_updated` to the current time, not the time in `last_value`: with an
older `last_updated`, a newer value recorded in an earlier commit overrides
the seed. To reload everything into an
append target instead, truncate the target (and remove any stored
watermark) before the run.

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
issues in Spark. weevr applies **opinionated null-safe defaults** to key
hashing, joins, and dimension merges:

| Operation | Standard Spark behavior | weevr behavior |
|---|---|---|
| **Surrogate keys** | `hash(NULL)` produces inconsistent results | Null → sentinel before hashing; keys always non-null |
| **Join keys** | `NULL = NULL` → `NULL` (row dropped) | Null-safe `<=>` equality; null keys participate in joins |
| **Change detection** | `hash(NULL)` masks null↔value changes | Null → `__NULL__` sentinel before hashing |
| **Dimension merge keys** | `NULL = NULL` → no match (row orphaned) | Business keys compared null-safely; null keys match correctly |

These defaults prevent the most common Spark null-related pitfalls without
requiring authors to add defensive logic to every thread.

!!! warning "Standard merge match keys are not null-safe"
    `write.mode: merge` outside a dimension target compares `match_keys`
    with standard `=` equality. A source row whose match key is null never
    matches a target row, so it is inserted again on every run. Fill or
    filter null match keys before the write (for example with `fill_null`
    or `filter`).

## Next steps

- [Execution Modes](execution-modes.md) -- Write and load mode details
- [Thread, Weave, Loom](thread-weave-loom.md) -- The object model
- [YAML Schema: Thread](../reference/yaml-schema/thread.md) -- Full write
  and key configuration reference
