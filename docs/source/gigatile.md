# Giga Tile Design

## Problem

The mega tile architecture (see [megatile.md](megatile.md)) reduces KV reads from O(N tiles) to
3 point gets, but the fetcher still does work on every read: decode batch IR + decode today/yesterday
streaming entries + merge collapsed + scan tail hops + merge streaming + finalize. This compute
happens on every feature request.

## Goal

Push the merge to the write path. Flink produces a **fully finalized feature vector** per entity.
The fetcher does a single point get with zero compute.

```
Current (mega tile, pull-based):
  Fetcher: 3 KV gets → decode → merge batch + streaming → finalize → return

Proposed (giga tile, push-based):
  Flink: on event → merge batch + streaming → finalize → KV write
  Fetcher: 1 KV get → return
```

## Key Insight: Hop-Based Pruning

The `FinalBatchIr` structure stores tail hops as individual hop-aligned partial aggregates:
```
collapsed: aggregate of [alignedCollapsedBoundary, batchEnd)
tailHops[hopIndex]: time-sorted array of [baseIr_0, ..., hopStartTs]
```

`mergeTailHops` selects relevant hops using `queryTail = round(queryTs - windowMillis, hopSize)`.
As `queryTs` advances, `queryTail` advances, and older hops are **excluded** (not subtracted).
This gives us "invertibility for free" — even for non-invertible aggregations like MIN, MAX, FIRST,
LAST. We simply re-run `mergeTailHops` with the current `queryTs` and get the correct answer.

## Architecture

```
Iceberg (batch IR, written daily by GroupByUpload)
    │
    │  Flink reads periodically (connected keyed stream)
    ▼
Flink state per entity:
    batchIr: FinalBatchIr           // latest batch IR (collapsed + tail hops)
    batchEndTs: Long                // when batch was last computed
    runningLargeIr: Array[Any]      // fully merged: batch + streaming for large window cols
    largeTodayIr: Array[Any]        // today's streaming delta (for rotation tracking)
    largeYesterdayIr: Array[Any]    // yesterday's streaming delta
    tiles + cachedSmallWindowIr     // (existing mega tile state for small windows)
    │
    │  On event: update streaming state + running sum → emit finalized vector
    ▼
KV Store: (entity) → finalized feature vector
    │
    │  Single point get
    ▼
Fetcher: read → return
```

## Batch IR Ingestion

Flink reads the batch IR from the **existing GroupByUpload Iceberg table** — the same table
that Spark already writes to. No new tables, no new Spark changes.

```
GroupByUpload (Spark)
  │  Writes partition ds=YYYY-MM-DD to Iceberg upload table
  │  Columns: key_bytes, value_bytes, key_json, value_json, ds
  │  Iceberg commits a new snapshot on write
  │
  ▼
Flink-Iceberg Source (streaming monitor mode)
  │  monitorInterval = 30 min
  │  On startup: full table scan (latest ds partition) → all entities
  │  On new snapshot: incremental read → only new/changed files
  │
  │  Decode key_bytes → entityKey
  │  keyBy(entityKey) → network shuffle to correct task slot
  │
  ▼
CoProcessFunction (same task slot as Kafka events for this entity)
  │  processElement1: Kafka event → onEvent
  │  processElement2: batch IR row → onBatchUpdate
```

```
Stream 1: real events (Kafka)                            → keyBy(entityKey) ──┐
                                                                               ├→ CoProcessFunction
Stream 2: batch IR (Iceberg upload table, monitor mode)  → keyBy(entityKey) ──┘
```

**Why this works without any Spark changes:**
- GroupByUpload already writes `(key_bytes, value_bytes)` to an Iceberg table partitioned by `ds`
- Each write commits an Iceberg snapshot (standard Iceberg behavior)
- Flink's Iceberg source detects new snapshots and reads the new data
- `value_bytes` contains Avro-encoded `FinalBatchIr` — same format the fetcher reads today
- `key_bytes` contains Avro-encoded entity keys — same encoding as the Kafka event keys

**Key superset:** The Iceberg source emits ALL entities from the batch table. Entities that
only exist in batch (no streaming events) get a Flink key slot via the batch stream.
This solves the key superset problem without requiring Kafka events for every entity.

## State Layout

Extends the existing mega tile `TileStore` with batch-side state:

```
// Existing (mega tile, small windows):
tiles: MapState                    // per-tier base IRs for small window columns
cachedSmallWindowIr: ValueState    // sawtooth running sum, corrected on eviction

// Existing (mega tile, large window streaming delta):
largeTodayIr: ValueState           // daily accumulator [todayStart, now)
largeYesterdayIr: ValueState       // daily accumulator [yesterdayStart, todayStart)

// New (giga tile, batch + merged):
batchIr: ValueState                // FinalBatchIr (collapsed + tail hops)
batchEndTs: ValueState             // Long
runningLargeIr: ValueState         // fully merged large window IR (batch + streaming)

// Bookkeeping:
currentDayStart, earliestTileStart // (existing)
```

## Processing Logic

### On Event (hot path)

Same cost as mega tile — O(1) per event for large windows, O(tiers) for small windows.

```
def onEvent(row, eventTs):
  // Small windows: unchanged from mega tile
  updateTiles(row, eventTs)
  updateCachedSmallWindowIr(row)

  // Large windows: update daily accumulator AND running sum
  if eventTs >= todayStart:
    updateLargeWindowColumns(largeTodayIr, row)
    updateLargeWindowColumns(runningLargeIr, row)    // running sum stays current
  else if eventTs >= yesterdayStart:
    updateLargeWindowColumns(largeYesterdayIr, row)
    updateLargeWindowColumns(runningLargeIr, row)    // late event also merged into running sum

  // Emit finalized feature vector
  emit(finalize(pack(cachedSmallWindowIr, runningLargeIr)))
```

The `runningLargeIr` is the fully merged value: batch collapsed + relevant tail hops + all
streaming events. Updated incrementally on each event. Sawtooth at the tail — corrected on eviction.

### On Eviction (periodic, every minTileSize)

Corrects both small window sawtooth and large window tail hop selection.

```
def onEviction(timerTs):
  // Small windows: rebuild from tiles (existing mega tile logic)
  evictStaleTiles(timerTs)
  cachedSmallWindowIr = buildMegaTileIr(tiles, timerTs, todayStart)

  // Large windows: recompute running sum from batch + streaming.
  // queryTs advanced → tail hops may have shifted (oldest hop excluded).
  if batchIr != null:
    runningLargeIr = clone(batchIr.collapsed)
    mergeTailHops(runningLargeIr, queryTs=timerTs, batchEndTs=batchEndTs, batchIr)
  else:
    // No batch yet (new entity, batch hasn't run). Streaming-only.
    runningLargeIr = windowedAgg.init

  for col where !isNoBatch(col):
    runningLargeIr(col) = merge(runningLargeIr(col), largeTodayIr(col))
    if batchEndTs < todayStart:
      runningLargeIr(col) = merge(runningLargeIr(col), largeYesterdayIr(col))

  emit(finalize(pack(cachedSmallWindowIr, runningLargeIr)))
```

### On Batch IR Update (daily, from Iceberg stream)

Stores the new batch IR immediately. Recomputation of `runningLargeIr` is deferred if the
watermark hasn't caught up to the new `batchEnd` (prevents double-counting from overlap
between batch and `largeTodayIr`). The next eviction picks it up.

```
def onBatchUpdate(newBatchIr, newBatchEnd):
  oldBatchEnd = batchEndTs
  if newBatchEnd <= oldBatchEnd: return   // same or older batch, skip

  // Always store the new batch IR — eviction and future events use it.
  batchIr = newBatchIr
  batchEndTs = newBatchEnd

  // Register eviction timer unconditionally — needed for:
  // - batch-only entities (no streaming events to trigger it)
  // - large-windows-only GroupBys (no small window tiles to drive eviction)
  // minEvictionInterval = min(activeTiers) — the smallest hop size across ALL windows.
  // Matches hop granularity so tail hop corrections fire at the right cadence.
  registerEvictionTimer(now + minEvictionInterval)

  if newBatchEnd > currentDayStart:
    if currentDayStart < 0:
      // Uninitialized (no events yet). Safe to set from batch — largeTodayIr is init,
      // no overlap concern. Without this, all startup batch loads would defer.
      currentDayStart = newBatchEnd
      // fall through to recomputation
    else:
      // Real defer: watermark hasn't caught up. largeTodayIr has events that overlap
      // with batch. Wait for advanceWatermark to rotate, then eviction recomputes.
      return

  // Safe: newBatchEnd <= currentDayStart — no overlap between batch and largeTodayIr.

  // Save old running sum for comparison
  oldRunningLargeIr = clone(runningLargeIr)

  // Clear yesterday if batch now covers it
  if newBatchEnd >= currentDayStart:
    largeYesterdayIr = init

  // Recompute running sum: new batch + tail hops + streaming
  runningLargeIr = clone(newBatchIr.collapsed)
  mergeTailHops(runningLargeIr, queryTs=watermark, batchEndTs=newBatchEnd, newBatchIr)
  for col where !isNoBatch(col):
    runningLargeIr(col) = merge(runningLargeIr(col), largeTodayIr(col))
    // Include yesterday if batch doesn't cover it
    if newBatchEnd < currentDayStart and largeYesterdayIr(col) != null:
      runningLargeIr(col) = merge(runningLargeIr(col), largeYesterdayIr(col))

  // Emit only on mismatch — most entities won't change materially
  if !equal(oldRunningLargeIr, runningLargeIr):
    emit(finalize(pack(cachedSmallWindowIr, runningLargeIr)))
```

**Why defer when `newBatchEnd > currentDayStart`?**

`largeTodayIr` covers `[currentDayStart, now)`. If `newBatchEnd > currentDayStart`, batch covers
`[..., newBatchEnd)` which overlaps with `[currentDayStart, newBatchEnd)` in `largeTodayIr`.
We can't subtract the overlap (non-invertible aggregations). We can't clear `largeTodayIr`
(loses post-`batchEnd` events). So we wait for the watermark to advance past `newBatchEnd`,
which rotates `largeTodayIr` via `advanceWatermark`. The next eviction then recomputes cleanly.

In normal operation, this defer never triggers: batch lands at ~6 AM, watermark passed midnight
hours ago, `newBatchEnd = currentDayStart`. The defer is a safety net for the narrow race window
when batch arrives just before the watermark crosses midnight.

### Day Transition (advanceWatermark)

Same as mega tile, plus `runningLargeIr` gets yesterday cleared.

```
def advanceWatermark(watermarkTs):
  wmDay = round(watermarkTs, DayMillis)
  if wmDay > currentDayStart:
    // Rotate yesterday → discard (or keep if single-day hop, clear if multi-day)
    largeYesterdayIr = if (wmDay == currentDayStart + DayMillis) largeTodayIr else init
    largeTodayIr = init
    currentDayStart = wmDay

    // runningLargeIr is NOT reset here — it's a cumulative sum.
    // Yesterday's data is still valid in the running sum.
    // The eviction timer will re-run mergeTailHops with the new queryTs
    // to prune any tail hops that fell off. Between the day transition
    // and the next eviction, the running sum is slightly over-inclusive
    // at the tail (sawtooth).
```

## Emit and KV Key

The emitted value is a **finalized feature vector** — the same format that the fetcher would
return to the ML model. No further processing needed.

```
KV dataset: {GROUP_BY_NAME}_PUSH  (not _STREAMING)
KV key:     entityKeyBytes (plain entity key, no TileKey wrapper, no day suffix)
KV value:   finalized feature vector (Avro encoded output schema)
```

Single entry per entity. Overwritten on every emit. The fetcher reads one key, decodes, returns.

### Why a dedicated `_PUSH` dataset

Push writes use simple key→value semantics (one value per entity, overwritten on every emit).
The existing `_STREAMING` dataset has **time-series semantics** baked into all KV implementations:

| Implementation | `_STREAMING` behavior | Problem for push |
|---|---|---|
| DynamoDB | Composite key (partition + sort key = timestamp) | `GetItem` on plain entity key fails — sort key not specified |
| BigTable | Day-based row key generation for time-series reads | Won't find rows written with plain entity keys |
| Redis | Sorted sets (`zadd`/`zrangebyscore`) | Each timestamp creates a new member — unbounded growth |

The `_PUSH` dataset avoids all of this:

| Implementation | `_PUSH` behavior | Why it works |
|---|---|---|
| DynamoDB | Partition key only (no sort key) | `PutItem` overwrites; `GetItem` returns single value |
| BigTable | Non-time-series; `cellsPerRow(1)` GC | `setCell` overwrites; read returns latest cell |
| Redis | Simple `setex`/`get` | Overwrites on every write; single value per key |
| Cosmos | Batch-style `upsertItem` by key hash | Document ID has no timestamp; upsert overwrites |

The `_PUSH` suffix is chosen intentionally: DynamoDB's `isStreamingTable` check (`dataset.endsWith("_STREAMING")`)
determines whether to add a sort key. `_PUSH` doesn't match, so the table is created as a simple key-value store.

### Planner and upload changes

For `OnlineStrategy.PUSH` GroupBys:

- **Planner**: The `uploadToKVNode` is excluded from the plan — `KVUploadNodeRunner` (bulkPut of entity
  rows) is not scheduled. Flink reads entity batch IRs directly from the Iceberg upload table.
- **GroupByUpload**: Writes `GroupByServingInfo` directly to KV via `kvStore.put()` at the end of the
  upload job. This replaces the `KVUploadNodeRunner` path for the metadata row. The serving info is
  already in memory from `buildServingInfo()` — no extra table read needed.
- **Upload table**: Still receives both entity rows and the serving info row (unchanged). Flink's Iceberg
  source reads entity rows from here.

## Scenario Tables

All windows, tailBuffer = 2d, now = Mar 26 14:00.

### Scenario 1: Batch fresh (batchEnd = Mar 26 00:00, 14h stale)

Streaming covers [batchEnd, now) = [Mar 26 00:00, Mar 26 14:00) = 14h.
`largeTodayIr` covers [Mar 26 00:00, Mar 26 14:00). `largeYesterdayIr` is empty (batch is fresh).

| window | category | runningLargeIr composition | tail hops selected | streaming added |
|--------|----------|----------------------------|--------------------|-----------------|
| 6h | SMALL | — (uses cachedSmallWindowIr) | — | — |
| 1d | SMALL | — | — | — |
| 2d | SMALL | — | — | — |
| 49h | LARGE | collapsed [Mar 24 23:00, Mar 26 00:00) + 1 hop [Mar 24 01:00, Mar 24 23:00) | hops where hopStart >= round(Mar 26 14:00 - 49h) = Mar 24 13:00 | + largeTodayIr [Mar 26 00:00, 14:00) |
| 3d | LARGE | collapsed [Mar 24 00:00, Mar 26 00:00) + 24 hops [Mar 23 00:00, Mar 24 00:00) | hops where hopStart >= round(Mar 26 14:00 - 3d) = Mar 23 14:00 | + largeTodayIr |
| 7d | LARGE | collapsed [Mar 20 00:00, Mar 26 00:00) + 48 hops [Mar 19 00:00, Mar 20 00:00) | hops where hopStart >= round(Mar 26 14:00 - 7d) = Mar 19 14:00 | + largeTodayIr |

**On event at Mar 26 14:00:**
- `runningLargeIr(49h)` = batch portion (collapsed + selected hops) + `largeTodayIr` contribution + new event
- Emit: `finalize(pack(cachedSmallWindowIr, runningLargeIr))` → single KV write

**On eviction at Mar 26 14:05:**
- `queryTs` advanced by 5min → `queryTail` for 49h advances → no hop falls off (1hr hops)
- `queryTail` for 3d advances → no hop falls off
- `runningLargeIr` recomputed from batch hops + streaming. Same value (no tail shift). No-op.

### Scenario 2: Batch stale (batchEnd = Mar 25 00:00, 38h stale)

Streaming covers [batchEnd, now) = [Mar 25 00:00, Mar 26 14:00) = 38h.
`largeYesterdayIr` covers [Mar 25 00:00, Mar 26 00:00). `largeTodayIr` covers [Mar 26 00:00, Mar 26 14:00).
Both are included because `batchEnd < todayStart`.

| window | category | runningLargeIr composition | tail hops selected | streaming added |
|--------|----------|----------------------------|--------------------|-----------------|
| 6h | SMALL | — | — | — |
| 1d | SMALL | — | — | — |
| 2d | SMALL | — | — | — |
| 49h | LARGE | collapsed [Mar 22 23:00, Mar 25 00:00) + hops | hops where hopStart >= Mar 24 13:00 | + largeYesterdayIr + largeTodayIr |
| 3d | LARGE | collapsed [Mar 23 00:00, Mar 25 00:00) + hops | hops where hopStart >= Mar 23 14:00 | + largeYesterdayIr + largeTodayIr |
| 7d | LARGE | collapsed [Mar 19 00:00, Mar 25 00:00) + hops | hops where hopStart >= Mar 19 14:00 | + largeYesterdayIr + largeTodayIr |

**On batch refresh (batchEnd advances Mar 25 → Mar 26 00:00):**
1. Store new batchIr (collapsed now covers [Mar 20 00:00, Mar 26 00:00) for 7d window)
2. Clear `largeYesterdayIr` — batch now covers [Mar 25 00:00, Mar 26 00:00)
3. Recompute `runningLargeIr`:
   - 49h: new collapsed + selected hops + largeTodayIr only (yesterday cleared)
   - 3d: new collapsed + selected hops + largeTodayIr only
4. Compare with old `runningLargeIr`
5. Emit only if mismatch (likely: batch incorporated Mar 25's full data, replacing streaming's accumulation)

### State transitions through a day

```
Time        Event                   runningLargeIr state
─────────── ─────────────────────── ──────────────────────────────────────────────
Mar 26 00:00  advanceWatermark       yesterday→today rotation. Running sum unchanged
              (day transition)       (sawtooth: slightly over-inclusive at tail)

Mar 26 00:05  eviction timer         Recompute from batch hops + streaming.
                                     Tail hops re-selected with queryTs=00:05.
                                     Running sum corrected.

Mar 26 00:05  event arrives          Merge into largeTodayIr + runningLargeIr.
  to 06:00    (continuous)           Emit finalized vector on each event.

Mar 26 06:00  batch refresh          New batchIr loaded from Iceberg.
              (Iceberg snapshot)     Clear largeYesterdayIr.
                                     Recompute running sum.
                                     Emit only if value changed.

Mar 26 06:00  events continue        runningLargeIr updated incrementally.
  to 24:00                           Eviction corrects tail every minTileSize.
```

## Comparison with Mega Tile

| | Mega tile (current) | Giga tile (proposed) |
|---|---|---|
| **KV reads per query** | 3 (today + yesterday + batch) | 1 |
| **Fetcher compute** | decode + merge + finalize | decode only |
| **Flink state** | ~17 KB/entity | ~19 KB/entity (+batchIr ~1-2 KB) |
| **KV writes per event** | 1 (streaming entry) | 1 (finalized vector) |
| **Batch data in Flink** | No | Yes (via Iceberg connected stream) |
| **Cold entity serving** | Fetcher reads batch KV | Flink emits on batch load |
| **Batch correction** | Implicit (fetcher merges latest) | Flink re-merges, emits on mismatch |

## Cost Model

**Per event (hot path):**
- Small windows: ~6 codec ops (unchanged from mega tile)
- Large windows: 1 additional `updateLargeWindowColumns(runningLargeIr, row)` — same column aggregator update as largeTodayIr. Negligible.
- Emit: encode finalized vector instead of windowed IR. Similar cost.

**Per eviction (every minTileSize):**
- Small windows: rebuild from tiles (unchanged)
- Large windows: `clone(collapsed) + mergeTailHops + merge(streaming)` — O(tailHops × columns). For 48 hops × 7 columns = 336 merge ops per eviction. At 5min cadence, that's ~1.1 merges/sec. Negligible.

**Per batch refresh (daily):**
- Recompute `runningLargeIr` for all entities. O(entities × tailHops × columns).
- Mismatch check + conditional emit. Most entities won't emit.
- Spread over the Iceberg scan duration (~minutes). Not bursty.

## Bootstrapping and Startup

### Emit strategy

Flink continues rebuilding state during replay, but PUSH writes are fenced until the connected
watermark is near wall clock. A batch source that is still active can hold that watermark at
`MIN`; source idleness lets it advance but does not prove the initial scan completed. An optional
write cadence can further coalesce hot-key updates after the fence opens.

**Emit triggers:**
- **Event arrival**: update state and publish only when the replay fence is open.
- **Eviction timer**: rebuild expired state and publish a changed or deferred snapshot.
- **Batch IR update**: emit if `runningLargeIr` changed. Single rule covers
  null→value (first batch load), batch correction, tail shift, no-change skip.

```
def onBatchUpdate(newBatchIr, newBatchEnd):
  // ... store batchIr, register timer, defer if overlap (see detailed pseudocode above) ...

  oldRunningLargeIr = clone(runningLargeIr)
  // ... recompute runningLargeIr from batch + hops + streaming ...

  if !equal(oldRunningLargeIr, runningLargeIr):
    emit(finalize(pack(cachedSmallWindowIr, runningLargeIr)))
```

### Readiness

The implementation has no positive initial-Iceberg-scan-complete signal. A near-live connected
watermark proves Kafka catch-up, not batch coverage. Rollout tooling must validate batch input and
serving output separately before routing traffic.

### Kafka replay on cold start

On cold start (no checkpoint), Flink rebuilds batch and streaming state from the configured
sources. Serving writes remain fenced until the connected watermark reaches `Live`.

```
Cold start / first startup:
1. Flink starts with no state
2. Iceberg source scans upload table (latest ds partition)
   → rows populate batch state as they arrive
3. Kafka replay rebuilds retained streaming state
4. Connected watermark reaches `Live`
   → the latest deferred snapshot can publish (subject to optional cadence)
5. Rollout tooling validates batch coverage and serving output before routing traffic
```

### Kafka retention requirement

Kafka retention must cover the actual gap from `batchEnd` through replay plus the required
correctness horizon. For GroupBys with only small windows, it must also cover the maximum
window size.

### Key completeness

**Will all keys be in the KV store after startup?**

Yes, for all temporal entities:
- **Inactive entities** (in batch table, no recent streaming events): Iceberg source
  loads their batch IR; the batch-only vector can publish after the live fence opens.
- **Active entities**: Iceberg and Kafka replay rebuild one deferred snapshot, which can
  publish after the live fence opens.
- **New entities (in Kafka, not in batch)**: first event creates Flink state. Columns that
  require batch history remain unset until a batch row arrives, unless the batch-absent
  fallback below is explicitly enabled.

`gigatile_first_seen_key_grace_millis` enables the batch-absent fallback when set to a
positive value; it is disabled by default. The operator-local grace starts on the first keyed
callback after open or restore. Its deadline is a lower bound: the fallback installs on the next
eligible keyed callback, which may be the next 5m, 1h, or 1d eviction for a sparse key. A missing
row may then be treated as empty history, while a later real batch row remains authoritative.
The grace is an operational assertion, not proof that the initial Iceberg scan completed.
Enabling it therefore requires a configured Iceberg source and fails the job on source
construction or entity-row decode/update errors; the serving-info metadata row is ignored.

To roll back this option, fence serving first, disable it on the current binary, and keep serving
fenced until null corrections are verified in KV and a later checkpoint completes. Only then
roll back the binary or unfence. Correction waits for a live watermark and the affected key's
next eviction callback (5m, 1h, or 1d), so binary rollback alone is not an immediate retraction.

**Note:** Giga tile only applies to `Accuracy.TEMPORAL` GroupBys (with a streaming topic).
`SNAPSHOT` GroupBys bypass Flink — they use the traditional `bulkPut` from Spark to KV.

## Entity behavior

- **Active:** combine batch state with replayed and live events; publish after the live fence opens.
- **Batch-only:** load and publish batch state after the fence opens; no per-key event is required.
- **New:** retain streaming state, but leave batch-backed values unset until a real batch row arrives
  or the explicit grace installs an empty baseline.

Daily state remains keyed by day and is pruned once covered by batch or outside every finite window.

### Batch refresh (onBatchUpdate) edge cases

| Scenario | Guard | Behavior |
|----------|-------|----------|
| `newBatchEnd <= oldBatchEnd` | Early return | Skip (same or older batch) |
| `currentDayStart < 0` (uninitialized, no events) | Init `currentDayStart = newBatchEnd` | Safe: no events → no overlap. Recompute and emit. |
| `newBatchEnd > currentDayStart` (batch ahead of the materialized horizon) | Defer (return) | Store `batchIr` but don't recompute. The next callback retries after the selected horizon advances. |
| `newBatchEnd <= currentDayStart` (normal) | Prune slots with `dayStart < batchEndDay` | Recompute from the new batch plus uncovered daily slots. |
| Stale batch catch-up | Retain slots at or after `batchEndDay` | Uncovered streaming days remain in the recomputed running view. |

### Eviction edge cases

| Scenario | Behavior |
|----------|----------|
| `batchIr = null` (new entity, type C) | Retain streaming state, but keep batch-backed output unset unless the explicit grace is active. |
| `batchIr != null` (types A, B) | Rebuild `runningLargeIr` from batch collapsed/tail hops plus retained daily slots. |
| `earliestTileStart = MaxValue` (no tiles, large-windows-only) | Skip tile eviction. Still recompute `runningLargeIr` from batch hops. |
| Batch-only entity (type B), no `hasSmallWindows` | Timer registered unconditionally by `onBatchUpdate`. Eviction fires at `minEvictionInterval`. |

## Implementation Path

Building on the existing mega tile infrastructure:

1. **Extend TileStore with batch state**
   - Add `getBatchIr/putBatchIr`, `getBatchEndTs/putBatchEndTs`, `getRunningLargeIr/putRunningLargeIr`
   - InMemoryTileStore + FlinkTileStore implementations

2. **Extend MegaTileStreamProcessor → GigaTileStreamProcessor**
   - Add `onBatchUpdate(newBatchIr, newBatchEnd)` method
   - Modify eviction to recompute `runningLargeIr` from batch hops
   - Emit finalized vectors instead of windowed IRs

3. **Add Iceberg connected stream to Flink job**
   - `CoProcessFunction` handling both event stream and batch IR stream
   - Periodic Iceberg snapshot monitoring (every 30 min)

4. **Simplify fetcher path**
   - Single point get on entity key
   - Decode finalized vector → return
   - No merge logic needed

5. **Integration test**
   - Traffic replay: events + batch uploads + queries, time-ordered
   - Compare push-based results with backfilled results via `.diff`

Steps 1-2 are pure aggregator/online changes (testable without Flink/Spark).
Step 3 is Flink wiring. Steps 4-5 are cleanup and validation.
