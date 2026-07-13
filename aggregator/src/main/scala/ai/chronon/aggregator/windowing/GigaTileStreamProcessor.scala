package ai.chronon.aggregator.windowing

import ai.chronon.api.Row
import ai.chronon.api.TsUtils

import scala.collection.mutable

private[chronon] final case class EvictionTimes(timerTs: Long, smallWindowAsOfTs: Long)

/** Pure Scala state manager for the GigaTile streaming pipeline.
  *
  * Flink holds the FinalBatchIr in state (loaded from Iceberg) and one daily large-window
  * IR slot per day with streaming events between batchEndDay and the current materialized day. The
  * runningLargeIr cache is the merged batch + every retained daily slot, refreshed on
  * eviction and on batch update.
  *
  * State layout:
  *   - tiles, cachedSmallWindowIr: small-window sawtooth (per-hop tile state + cached IR)
  *   - dailyLargeIrs: per-day accumulator keyed by day-start. Slots with dayStart < batchEndDay
  *     are pruned when batch advances (their data is now in batch).
  *   - batchIr: FinalBatchIr from Iceberg (collapsed + tail hops)
  *   - batchEndTs: when batch was last computed
  *   - runningLargeIr: fully merged large-window IR (batch + every retained daily slot)
  */
class GigaTileStreamProcessor(
    val megaTileAgg: MegaTileAggregator,
    val store: GigaTileStore,
    // Returns true if two windowed IRs are equal (for batch update mismatch detection).
    // Default: always report mismatch (always emit on batch update).
    val irEqual: (Array[Any], Array[Any]) => Boolean = (_, _) => false,
    // Bound on how stale the batch can get before late events for the oldest covered days
    // are dropped. Keeps daily-IR state from growing unbounded if the Iceberg connected
    // stream stalls. 32 days is well past any realistic batch SLA.
    val maxBatchStalenessDays: Int = 32
) {

  val DayMillis: Long = 24 * 3600 * 1000L
  private val maxStalenessMillis: Long = maxBatchStalenessDays.toLong * DayMillis

  private val windowedAgg = megaTileAgg.windowedAggregator
  private val baseAgg = megaTileAgg.baseAggregator
  private val isNoBatch = megaTileAgg.isNoBatch
  private val columnHopSize = megaTileAgg.columnHopSize
  private val isFiniteLargeWindow: Array[Boolean] = megaTileAgg.windowMappings.zipWithIndex.map { case (mapping, col) =>
    val window = mapping.aggregationPart.window
    !isNoBatch(col) && window != null && window.length > 0 && window.length < Int.MaxValue
  }
  private val hasFiniteLargeWindows = isFiniteLargeWindow.contains(true)

  val smallWindowTiers: Set[Long] = {
    val tiers = mutable.Set.empty[Long]
    var col = 0
    while (col < windowedAgg.length) {
      if (isNoBatch(col)) tiers += columnHopSize(col)
      col += 1
    }
    tiers.toSet
  }

  val hasSmallWindows: Boolean = smallWindowTiers.nonEmpty

  // Eviction cadence: smallest hop across ALL windows (not just small).
  // Large-window-only GroupBys need eviction for tail hop correction.
  val minEvictionInterval: Long = megaTileAgg.activeTiers.min

  val minSmallWindowTileSize: Long = if (smallWindowTiers.nonEmpty) smallWindowTiers.min else minEvictionInterval

  // Default as-of bumped to the next small-window hop boundary so an event at exactly eventTs
  // is included in its own emit (chronon's as-of is exclusive). Production callers should pass
  // smallWindowAsOfTs explicitly — derived from processingTs / watermark — so retained late
  // events don't get their own permissive horizon.
  private def defaultSmallWindowAsOfTs(eventTs: Long): Long =
    TsUtils.round(eventTs, minSmallWindowTileSize) + minSmallWindowTileSize

  // Hop indices only used by small (NO BATCH) windows — stripped from batch IR on load.
  // 5-min tail hops for ≤12h windows are never used by mergeTailHopsForBatchColumns.
  private[windowing] val smallWindowOnlyHopIndices: Set[Int] = {
    val usedByBatch = mutable.Set.empty[Int]
    var col = 0
    while (col < windowedAgg.length) {
      val window = megaTileAgg.windowMappings(col).aggregationPart.window
      if (!isNoBatch(col) && window != null) {
        usedByBatch += megaTileAgg.tailHopIndicesArray(col)
      }
      col += 1
    }
    (0 until megaTileAgg.hopSizesArray.length).filterNot(usedByBatch.contains(_)).toSet
  }

  private def addIfNoOverflow(a: Long, b: Long): Long =
    if (a > Long.MaxValue - b) Long.MaxValue else a + b

  private def subtractIfNoUnderflow(a: Long, b: Long): Long =
    if (a < Long.MinValue + b) Long.MinValue else a - b

  def onEvent(row: Row, eventTs: Long): GigaEmitResult = onEvent(row, eventTs, defaultSmallWindowAsOfTs(eventTs))

  /** smallWindowAsOfTs is the as-of timestamp used to gate cached small-window IR updates.
    * Late retained events still update the underlying base tile (so a future eviction can
    * rebuild correctly), but only events inside each column's effective horizon contribute
    * to the live cached IR that gets emitted.
    */
  def onEvent(row: Row, eventTs: Long, smallWindowAsOfTs: Long): GigaEmitResult =
    onEvent(row, eventTs, largeWindowAsOfTs = smallWindowAsOfTs, smallWindowAsOfTs = smallWindowAsOfTs)

  /** Separate horizons keep the small-window exclusive upper bound from shifting the
    * large-window query time at an exact hop boundary.
    */
  private[chronon] def onEvent(
      row: Row,
      eventTs: Long,
      largeWindowAsOfTs: Long,
      smallWindowAsOfTs: Long
  ): GigaEmitResult = {
    var currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) {
      currentDayStart = TsUtils.round(eventTs, DayMillis)
      store.putCurrentDayStart(currentDayStart)
    }

    var dirty = false

    // --- Small windows: update tiles, then update cached IR only for columns whose
    // tile lies inside the smallWindowAsOfTs horizon. ---
    if (hasSmallWindows) {
      val targetHop = TsUtils.round(smallWindowAsOfTs, minSmallWindowTileSize)
      val cachedHop = store.getCachedSmallWindowAsOfTs
      // An older or delayed callback can arrive after this cache has advanced. Keep the live
      // horizon monotonic instead of rebuilding or gating the event against an older range
      // and resurrecting an expired tile.
      val effectiveSmallWindowAsOfTs = if (cachedHop > targetHop) cachedHop else smallWindowAsOfTs
      // A delayed callback can advance to a new hop before the scheduled eviction runs.
      // Rebuild first so this event cannot publish an expired tile from the previous hop.
      if (store.getEarliestTileStart != Long.MaxValue && cachedHop < targetHop) {
        rebuildCachedSmallWindowIr(smallWindowAsOfTs, currentDayStart)
        dirty = true
      }

      val acceptedTileStarts = mutable.Map.empty[Long, Long]
      val tileStarts = megaTileAgg.tileStartsForEvent(eventTs)
      for ((hopSize, tileStart) <- tileStarts) {
        if (smallWindowTiers.contains(hopSize)) {
          val floor = megaTileAgg.retentionFloor(hopSize, eventTs, currentDayStart)
          val ceiling = currentDayStart + 2 * DayMillis
          if (tileStart >= floor && tileStart < ceiling) {
            val existing = store.getTile(hopSize, tileStart)
            val ir = if (existing != null) existing else baseAgg.init
            baseAgg.update(ir, row)
            store.putTile(hopSize, tileStart, ir)
            val earliest = store.getEarliestTileStart
            if (tileStart < earliest) store.putEarliestTileStart(tileStart)
            acceptedTileStarts(hopSize) = tileStart
          }
        }
      }

      if (acceptedTileStarts.nonEmpty) {
        var cachedIr: Array[Any] = null
        var cachedIrUpdated = false
        var col = 0
        while (col < windowedAgg.length) {
          if (isNoBatch(col)) {
            val hopSize = columnHopSize(col)
            acceptedTileStarts.get(hopSize).foreach { tileStart =>
              val effStart = megaTileAgg.effectiveStart(col, effectiveSmallWindowAsOfTs, currentDayStart)
              // Retained late events: keep the base tile but skip cached-IR update so the live
              // emit reflects only events inside the as-of horizon for this column's window.
              if (tileStart >= effStart && tileStart < effectiveSmallWindowAsOfTs) {
                if (cachedIr == null) cachedIr = store.getCachedSmallWindowIr
                windowedAgg.columnAggregators(col).update(cachedIr, row)
                cachedIrUpdated = true
              }
            }
          }
          col += 1
        }
        if (cachedIrUpdated) {
          store.putCachedSmallWindowIr(cachedIr)
          store.putCachedSmallWindowAsOfTs(math.max(cachedHop, targetHop))
          dirty = true
        }
      }
    }

    // A delayed callback can cross one or more large-window boundaries before its
    // scheduled eviction runs. Track the last full recompute per key so the event path
    // repairs a stale large view once per hop without coupling it to the small horizon.
    if (recomputeRunningLargeIrForEventIfNeeded(largeWindowAsOfTs, currentDayStart)) dirty = true

    // --- Large windows: route to the per-day slot for round(eventTs, DayMillis) ---
    // Accept events as far back as the staleness bound. The daily slot retains the event for
    // every large column, but the incremental running view only updates columns whose daily
    // slot overlaps the monotonic materialized horizon. This prevents an out-of-order event
    // from resurrecting an already-expired shorter window before the next eviction.
    //
    // Daily slot IRs are sized to the BASE aggregator (one column per (op, input)) — not
    // the windowed aggregator. The same SUM(num) value covers every windowed SUM(num, W)
    // column when fanned out at recompute time via baseIrIndices. Saves N× state and
    // update cost when the same input appears in many window sizes.
    val eventDayStart = TsUtils.round(eventTs, DayMillis)
    val oldestAcceptedDay = currentDayStart - maxStalenessMillis
    var droppedStaleEvent = false
    if (eventDayStart >= oldestAcceptedDay) {
      val existing = store.getDailyLargeIr(eventDayStart)
      val dayIr = if (existing != null) existing else baseAgg.init
      baseAgg.update(dayIr, row)
      store.putDailyLargeIr(eventDayStart, dayIr)

      val runningIr = store.getRunningLargeIr
      val lastLargeRecomputeAsOfTs = store.getLastLargeRecomputeAsOfTs
      val effectiveLargeAsOfTs =
        if (lastLargeRecomputeAsOfTs == Long.MinValue) largeWindowAsOfTs
        else math.max(lastLargeRecomputeAsOfTs, largeWindowAsOfTs)
      updateLargeWindowColumns(runningIr, row, eventDayStart, effectiveLargeAsOfTs)
      store.putRunningLargeIr(runningIr)
      dirty = true
    } else {
      droppedStaleEvent = true
    }

    if (dirty) GigaEmitResult(packAndFinalize(), droppedStaleEvent = droppedStaleEvent)
    else GigaEmitResult(null, droppedStaleEvent = droppedStaleEvent)
  }

  /** As-of-driven day transition. Finite day jumps rebuild both views so the next callback
    * cannot combine a corrected small-window cache with stale large-window state.
    */
  def advanceDayAsOf(asOfTs: Long): GigaEmitResult = {
    val currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) return GigaEmitResult(null)
    val asOfDay = TsUtils.round(asOfTs, DayMillis)
    if (asOfDay > currentDayStart) {
      val previousPackedIr = windowedAgg.clone(pack())
      store.putCurrentDayStart(asOfDay)
      // A terminal Long.MaxValue horizon from a bounded watermark is not a serving horizon.
      // Rebuilding there would expire every retained value and publish a synthetic all-null row.
      if (asOfTs != Long.MaxValue) {
        rebuildCachedSmallWindowIr(asOfTs, asOfDay)
        // A future batch row is retained but must not become active until its boundary.
        // Small-window time can still advance while the prior large view remains serving.
        if (!largeRecomputeDeferred(asOfDay)) recomputeRunningLargeIr(asOfTs, asOfDay)
        val packed = pack()
        val packedIsEmpty = isAllNull(packed)
        val previousWasEmpty = isAllNull(previousPackedIr)
        if (!((packedIsEmpty && previousWasEmpty) || irEqual(previousPackedIr, packed))) {
          return GigaEmitResult(windowedAgg.finalize(packed), isEmpty = packedIsEmpty)
        }
      }
    }
    GigaEmitResult(null, isEmpty = isAllNull(pack()))
  }

  // Compatibility wrapper for callers that still pass an event-time watermark directly.
  def advanceWatermark(watermarkTs: Long): GigaEmitResult = advanceDayAsOf(watermarkTs)

  /** Direct eviction/serve-as-of: corrects small window sawtooth and large window tail selection. */
  def onEviction(timerTs: Long): GigaEmitResult =
    onEviction(EvictionTimes(timerTs = timerTs, smallWindowAsOfTs = timerTs))

  private[chronon] def onEviction(evictionTimes: EvictionTimes): GigaEmitResult = {
    val currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) return GigaEmitResult(null)

    val cachedSmallWindowHop = store.getCachedSmallWindowAsOfTs
    val requestedSmallWindowHop = TsUtils.round(evictionTimes.smallWindowAsOfTs, minSmallWindowTileSize)
    val cachedSmallWindowHopUnknown = cachedSmallWindowHop < 0L
    val smallWindowCanAdvance = hasSmallWindows &&
      (cachedSmallWindowHopUnknown || requestedSmallWindowHop >= cachedSmallWindowHop)
    val smallMayChange = smallWindowCanAdvance
    val largeRecomputeIsDeferred = largeRecomputeDeferred(currentDayStart)
    val largeMayChange = !largeRecomputeIsDeferred
    if (!smallMayChange && !largeMayChange) {
      // Do not advance the large recompute marker. A later callback checks the entire gap
      // from the last materialized horizon, including boundaries skipped by delayed timers.
      return GigaEmitResult(null, needsEvictionTimer = largeRecomputeIsDeferred, isEmpty = isAllNull(pack()))
    }

    val previousPackedIr = windowedAgg.clone(pack())

    if (smallMayChange) rebuildCachedSmallWindowIr(evictionTimes.smallWindowAsOfTs, currentDayStart)
    if (largeMayChange) recomputeRunningLargeIr(evictionTimes.timerTs, currentDayStart)

    val packed = pack()
    val packedIsEmpty = isAllNull(packed)
    val previousWasEmpty = isAllNull(previousPackedIr)
    if ((packedIsEmpty && previousWasEmpty) || irEqual(previousPackedIr, packed)) {
      // Nothing changed for this key. Suppress redundant KV writes, including fully-decayed
      // idle entities where the previous and current packed IR are both all-null.
      GigaEmitResult(null, needsEvictionTimer = largeRecomputeIsDeferred, isEmpty = packedIsEmpty)
    } else {
      GigaEmitResult(windowedAgg.finalize(packed),
                     needsEvictionTimer = largeRecomputeIsDeferred,
                     isEmpty = packedIsEmpty)
    }
  }

  /** Serve-as-of helper for the Flink wiring: decay an idle entity's state at the requested
    * as-of timestamp without requiring a new event. Same semantics as onEviction but named
    * for the use case so callers don't get confused.
    */
  def serveAsOf(asOfTs: Long): GigaEmitResult = onEviction(asOfTs)

  /** Global batch-advance hook for entities not present in a new batch row.
    *
    * The Iceberg connected stream emits a BatchIrRow only for entities included in the new
    * batch. Idle entities receive no per-key signal that the global batchEnd has moved, so
    * their batchEndTs in state stays stale forever. This method lets the Flink wiring
    * broadcast a batch boundary advance: it advances batchEndTs (without loading a new
    * batch IR), prunes daily slots covered by the new boundary, and recomputes the running
    * IR so the next emit reflects the new horizon.
    *
    * Caller responsibility: only call this for entities the global batch is known to cover.
    * Calling for an entity whose events are NOT in batch will delete those events from
    * state without batch backfilling them — a real correctness loss.
    */
  def onGlobalBatchAdvance(newBatchEnd: Long, currentWatermark: Long): GigaEmitResult = {
    val oldBatchEnd = store.getBatchEndTs
    if (newBatchEnd <= oldBatchEnd) return GigaEmitResult(null)

    store.putBatchEndTs(newBatchEnd)

    var currentDayStart = store.getCurrentDayStart
    if (currentDayStart < 0) {
      currentDayStart = newBatchEnd
      store.putCurrentDayStart(currentDayStart)
    }

    val batchEndDay = TsUtils.round(newBatchEnd, DayMillis)
    val toRemove = mutable.ArrayBuffer.empty[Long]
    val iter = store.dailyLargeIrIterator
    while (iter.hasNext) {
      val (dayStart, _) = iter.next()
      if (dayStart < batchEndDay) toRemove += dayStart
    }
    toRemove.foreach(store.removeDailyLargeIr)

    recomputeRunningLargeIr(currentWatermark, currentDayStart)
    val packed = pack()
    val packedIsEmpty = isAllNull(packed)
    GigaEmitResult(windowedAgg.finalize(packed), needsEvictionTimer = true, isEmpty = packedIsEmpty)
  }

  /** True when every column in the packed pre-finalize IR is null. Used by the eviction
    * suppress path and surfaced so Flink can stop scheduling further decay callbacks.
    */
  private def isAllNull(packed: Array[Any]): Boolean = {
    var i = 0
    while (i < packed.length) {
      if (packed(i) != null) return false
      i += 1
    }
    true
  }

  /** Process a new batch IR from the Iceberg connected stream.
    *
    * @param newBatchIr  decoded FinalBatchIr from the Iceberg upload table
    * @param newBatchEnd batch upload boundary timestamp (midnight-aligned)
    * @param currentWatermark Flink's current watermark (for tail hop selection as queryTs)
    */
  def onBatchUpdate(newBatchIr: FinalBatchIr, newBatchEnd: Long, currentWatermark: Long): GigaEmitResult =
    onBatchUpdate(newBatchIr, newBatchEnd, largeWindowAsOfTs = currentWatermark, smallWindowAsOfTs = currentWatermark)

  private[chronon] def onBatchUpdate(
      newBatchIr: FinalBatchIr,
      newBatchEnd: Long,
      largeWindowAsOfTs: Long,
      smallWindowAsOfTs: Long
  ): GigaEmitResult = {
    val oldBatchEnd = store.getBatchEndTs
    if (newBatchEnd <= oldBatchEnd) return GigaEmitResult(null)

    val previousPackedIr = windowedAgg.clone(pack())
    val strippedBatchIr = stripSmallWindowHops(newBatchIr)
    store.putBatchIr(strippedBatchIr)
    store.putBatchEndTs(newBatchEnd)

    var currentDayStart = store.getCurrentDayStart

    if (newBatchEnd > currentDayStart) {
      if (currentDayStart < 0) {
        // Uninitialized (no events yet). Daily slots are empty so there's no overlap risk.
        currentDayStart = newBatchEnd
        store.putCurrentDayStart(currentDayStart)
      } else {
        // Defer until the selected large-window as-of day reaches batchEnd. Daily slots between
        // currentDayStart and batchEnd would otherwise overlap with batch and double-count. Event,
        // timer, and day-roll recomputation paths all honor largeRecomputeDeferred.
        return GigaEmitResult(null, needsEvictionTimer = true)
      }
    }

    // Prune daily slots now covered by batch. Batch ends are midnight-aligned, so any slot
    // with dayStart strictly less than batchEndDay is fully inside batch.
    val batchEndDay = TsUtils.round(newBatchEnd, DayMillis)
    val toRemove = mutable.ArrayBuffer.empty[Long]
    val iter = store.dailyLargeIrIterator
    while (iter.hasNext) {
      val (dayStart, _) = iter.next()
      if (dayStart < batchEndDay) toRemove += dayStart
    }
    toRemove.foreach(store.removeDailyLargeIr)

    rebuildCachedSmallWindowIr(smallWindowAsOfTs, currentDayStart)
    recomputeRunningLargeIr(largeWindowAsOfTs, currentDayStart)
    val packed = pack()
    val packedIsEmpty = isAllNull(packed)

    if (!irEqual(previousPackedIr, packed)) {
      GigaEmitResult(windowedAgg.finalize(packed), needsEvictionTimer = true, isEmpty = packedIsEmpty)
    } else {
      GigaEmitResult(null, needsEvictionTimer = true, isEmpty = packedIsEmpty)
    }
  }

  // --- Private helpers ---

  private def recomputeRunningLargeIrForEventIfNeeded(queryTs: Long, currentDayStart: Long): Boolean = {
    if (!hasFiniteLargeWindows || largeRecomputeDeferred(currentDayStart)) return false

    if (largeIrMayChangeSinceLastRecompute(queryTs)) {
      recomputeRunningLargeIr(queryTs, currentDayStart)
      true
    } else {
      false
    }
  }

  private def largeRecomputeDeferred(currentDayStart: Long): Boolean =
    currentDayStart >= 0L && store.getBatchEndTs > currentDayStart

  /** True when a finite large-window start crossed its own tail-hop boundary since the exact
    * prior materialized horizon. Batch ends and daily slots are midnight-aligned, and every
    * tail hop divides a day, so the same boundary covers collapsed, tail, and daily expiry.
    * This stays O(columns) on the event path; iterating retained daily state here would decode
    * every slot for every event until the next real boundary.
    */
  private def largeIrMayChangeSinceLastRecompute(queryTs: Long): Boolean = {
    val previousAsOfTs = store.getLastLargeRecomputeAsOfTs
    if (previousAsOfTs == Long.MinValue) return true
    if (queryTs <= previousAsOfTs) return false

    var col = 0
    while (col < windowedAgg.length) {
      if (isFiniteLargeWindow(col)) {
        val windowMillis = megaTileAgg.columnWindowMillis(col)
        val hopSize = columnHopSize(col)
        val previousStart = TsUtils.round(subtractIfNoUnderflow(previousAsOfTs, windowMillis), hopSize)
        val queryStart = TsUtils.round(subtractIfNoUnderflow(queryTs, windowMillis), hopSize)
        if (queryStart > previousStart) return true
      }
      col += 1
    }

    false
  }

  /** Recompute runningLargeIr from: batch (collapsed + tail hops) + every retained daily slot
    * with dayStart >= batchEndDay. For columns where the entire window has moved past
    * batchEndTs, the collapsed value is stale — zero it out so stale batch data doesn't
    * persist for idle entities.
    */
  private def recomputeRunningLargeIr(queryTs: Long, currentDayStart: Long): Unit = {
    val batchIr = store.getBatchIr
    val batchEndTs = store.getBatchEndTs
    val runningIr = if (batchIr != null) {
      val ir = windowedAgg.clone(batchIr.collapsed)
      megaTileAgg.mergeTailHopsForBatchColumns(ir, queryTs, batchEndTs, batchIr)
      var col = 0
      while (col < windowedAgg.length) {
        val window = megaTileAgg.windowMappings(col).aggregationPart.window
        if (!isNoBatch(col) && window != null && queryTs - megaTileAgg.windowMappings(col).millis >= batchEndTs) {
          ir(col) = null
        }
        col += 1
      }
      ir
    } else {
      windowedAgg.init
    }

    // Slot eviction (state size): drop slots whose [dayStart, dayStart+1d) is entirely older
    // than the largest column window — they cannot contribute to any column.
    // Per-column merge (correctness): for retained slots, fan out from base-IR shape to each
    // windowed column via baseIrIndices, but only for columns whose own window overlaps the
    // slot's date range. Otherwise we'd over-count smaller-window columns by an entire day's
    // worth of out-of-window events.
    val batchEndDay = if (batchEndTs >= 0L) TsUtils.round(batchEndTs, DayMillis) else Long.MinValue
    val maxWindowMillis = megaTileAgg.maxWindowMillis
    val columnWindowMillis = megaTileAgg.columnWindowMillis
    val baseIrIndices = megaTileAgg.baseIrIndicesArray
    val toEvict = mutable.ArrayBuffer.empty[Long]
    val iter = store.dailyLargeIrIterator
    while (iter.hasNext) {
      val (dayStart, dayIr) = iter.next()
      val slotEnd = dayStart + DayMillis
      // Slot entirely outside the largest window — useful to no column. Drop from state.
      if (dayStart < batchEndDay) {
        toEvict += dayStart
      } else if (maxWindowMillis > 0 && maxWindowMillis < Long.MaxValue && slotEnd <= queryTs - maxWindowMillis) {
        toEvict += dayStart
      } else if (dayStart >= batchEndDay && dayIr != null) {
        var col = 0
        while (col < windowedAgg.length) {
          if (!isNoBatch(col)) {
            val baseSlotValue = dayIr(baseIrIndices(col))
            if (baseSlotValue != null) {
              // Per-column window overlap: slot's [dayStart, slotEnd) must intersect the
              // column's window [queryTs - colWindow, queryTs]. Unwindowed columns
              // (colWindow < 0) always include all events ever.
              val colWindow = columnWindowMillis(col)
              val include = colWindow < 0 || slotEnd > queryTs - colWindow
              if (include) {
                runningIr(col) = windowedAgg.columnAggregators(col).merge(runningIr(col), baseSlotValue)
              }
            }
          }
          col += 1
        }
      }
    }
    toEvict.foreach(store.removeDailyLargeIr)

    // Some column aggregators (FIRST, LAST, and similar non-mutating reducers) return one of
    // their merge inputs by reference. Without re-cloning, runningIr columns can end up
    // aliased to cached tail-hop or daily-slot IRs; subsequent in-place onEvent updates would
    // then mutate that shared state and corrupt future serves.
    store.putRunningLargeIr(windowedAgg.clone(runningIr))
    store.putLastLargeRecomputeAsOfTs(queryTs)
  }

  /** Rebuild cachedSmallWindowIr from retained tiles using the supplied as-of horizon.
    * Used for day-roll and hop-boundary correction, and before event publication whenever
    * the cached as-of marker is stale.
    */
  private def rebuildCachedSmallWindowIr(asOfTs: Long, currentDayStart: Long): Unit = {
    if (!hasSmallWindows) return
    val earliest = store.getEarliestTileStart
    if (earliest == Long.MaxValue) return

    // Single-pass classify-and-collect. Flink-backed TileStore decodes values during
    // iteration, so a second full scan would deserialize every retained tile again.
    val staleEntries = mutable.ArrayBuffer.empty[(Long, Long)]
    val tiles: Map[Long, mutable.Map[Long, Array[Any]]] =
      smallWindowTiers.map(hop => hop -> mutable.Map.empty[Long, Array[Any]]).toMap
    var newEarliest = Long.MaxValue
    val iter = store.tileIterator
    while (iter.hasNext) {
      val (hopSize, tileStart, ir) = iter.next()
      if (smallWindowTiers.contains(hopSize)) {
        val floor = megaTileAgg.retentionFloor(hopSize, asOfTs, currentDayStart)
        if (tileStart < floor) {
          staleEntries += ((hopSize, tileStart))
        } else {
          tiles(hopSize)(tileStart) = ir
          if (tileStart < newEarliest) newEarliest = tileStart
        }
      }
    }
    staleEntries.foreach { case (h, t) => store.removeTile(h, t) }
    store.putEarliestTileStart(newEarliest)

    val rebuiltIr = megaTileAgg.buildMegaTileIr(tiles, now = asOfTs, batchEnd = currentDayStart)
    store.putCachedSmallWindowIr(rebuiltIr)
    store.putCachedSmallWindowAsOfTs(TsUtils.round(asOfTs, minSmallWindowTileSize))
  }

  private def updateLargeWindowColumns(
      ir: Array[Any],
      row: Row,
      eventDayStart: Long,
      effectiveAsOfTs: Long
  ): Unit = {
    val slotEnd = addIfNoOverflow(eventDayStart, DayMillis)
    var col = 0
    while (col < windowedAgg.length) {
      if (!isNoBatch(col)) {
        val include =
          !isFiniteLargeWindow(col) ||
            slotEnd > subtractIfNoUnderflow(effectiveAsOfTs, megaTileAgg.columnWindowMillis(col))
        if (include) windowedAgg.columnAggregators(col).update(ir, row)
      }
      col += 1
    }
  }

  private def pack(): Array[Any] = {
    val cachedIr = store.getCachedSmallWindowIr
    val runningIr = store.getRunningLargeIr
    // Until a batch IR has loaded, the running IR for batch-dependent columns is just the
    // streaming-since-startup partial sum — emitting that would write an under-counted row
    // to the PUSH KV that the fetcher then serves indefinitely. Null those columns out so
    // the writer can either suppress the emit or write nulls for batch-dependent fields.
    val batchAvailable = store.getBatchIr != null
    val packed = new Array[Any](windowedAgg.length)
    var col = 0
    while (col < windowedAgg.length) {
      packed(col) =
        if (isNoBatch(col)) cachedIr(col)
        else if (batchAvailable) runningIr(col)
        else null
      col += 1
    }
    packed
  }

  private[chronon] def packAndFinalize(): Array[Any] = windowedAgg.finalize(pack())

  /** Snapshot the current value without moving either aggregation clock. */
  private[chronon] def currentSnapshot: GigaEmitResult = {
    val packed = pack()
    GigaEmitResult(windowedAgg.finalize(packed),
                   needsEvictionTimer = largeRecomputeDeferred(store.getCurrentDayStart),
                   isEmpty = isAllNull(packed))
  }

  /** Strip tail hops that are only used by small windows to reduce state size.
    * 5-min hops for ≤12h windows are never consumed by mergeTailHopsForBatchColumns.
    */
  private[windowing] def stripSmallWindowHops(batchIr: FinalBatchIr): FinalBatchIr = {
    if (smallWindowOnlyHopIndices.isEmpty || batchIr.tailHops == null) return batchIr
    val strippedHops = batchIr.tailHops.clone()
    for (idx <- smallWindowOnlyHopIndices) {
      if (idx < strippedHops.length) {
        strippedHops(idx) = Array.empty
      }
    }
    FinalBatchIr(batchIr.collapsed, strippedHops)
  }
}

case class GigaEmitResult(
    finalizedVector: Array[Any],
    // True when this key must keep its fixed-cadence eviction timer registered. Batch
    // updates start the cadence; deferred future batches keep it live even for empty keys.
    needsEvictionTimer: Boolean = false,
    // True when the packed pre-finalize IR has every column null. Flink can emit the encoded
    // all-null correction and stop decay timers; the current sink does not issue a DELETE.
    isEmpty: Boolean = false,
    // True when this onEvent call dropped its event because eventTs was older than the
    // staleness bound. Surfaced so the Flink wiring can bump a metric / log instead of
    // silently losing data.
    droppedStaleEvent: Boolean = false
)
