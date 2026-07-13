package ai.chronon.aggregator.windowing

import ai.chronon.api.Row
import ai.chronon.api.TsUtils

import scala.collection.mutable

/** Pure Scala state manager for the GigaTile streaming pipeline.
  *
  * Flink holds the FinalBatchIr in state (loaded from Iceberg) and one daily large-window
  * IR slot per day with streaming events between batchEndDay and the watermark day. The
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
  def onEvent(row: Row, eventTs: Long, smallWindowAsOfTs: Long): GigaEmitResult = {
    var currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) {
      currentDayStart = TsUtils.round(eventTs, DayMillis)
      store.putCurrentDayStart(currentDayStart)
    }

    var dirty = false

    // --- Small windows: update tiles, then update cached IR only for columns whose
    // tile lies inside the smallWindowAsOfTs horizon. ---
    if (hasSmallWindows) {
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
              val effStart = megaTileAgg.effectiveStart(col, smallWindowAsOfTs, currentDayStart)
              // Retained late events: keep the base tile but skip cached-IR update so the live
              // emit reflects only events inside the as-of horizon for this column's window.
              if (tileStart >= effStart && tileStart < smallWindowAsOfTs) {
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
          dirty = true
        }
      }
    }

    // --- Large windows: route to the per-day slot for round(eventTs, DayMillis) ---
    // Accept events as far back as the staleness bound. Events whose dayStart is already
    // covered by batch are still accepted for the incremental running IR (so onEvent emits
    // see them immediately), and recomputeRunningLargeIr will drop their daily slot at the
    // next eviction — matching the original "incremental shows it, eviction may drop it"
    // trade-off when the late event was not in the prior batch's source data.
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
      updateLargeWindowColumns(runningIr, row)
      store.putRunningLargeIr(runningIr)
      dirty = true
    } else {
      droppedStaleEvent = true
    }

    if (dirty) GigaEmitResult(packAndFinalize(), droppedStaleEvent = droppedStaleEvent)
    else GigaEmitResult(null, droppedStaleEvent = droppedStaleEvent)
  }

  /** As-of-driven day transition. Updates currentDayStart and rebuilds the small-window
    * cache because each column's effectiveStart shifts with the new as-of horizon. Daily
    * large-IR slots are unaffected — they're keyed by day-start and persist across rollovers
    * until batch covers them.
    */
  def advanceWatermark(watermarkTs: Long): GigaEmitResult = {
    val currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) return GigaEmitResult(null)
    val wmDay = TsUtils.round(watermarkTs, DayMillis)
    if (wmDay > currentDayStart) {
      val isAdjacentRollover = wmDay == currentDayStart + DayMillis
      store.putCurrentDayStart(wmDay)
      // Rebuild only on an adjacent-day transition. Multi-day jumps and end-of-stream
      // (watermark = MAX) push the as-of so far past retained tiles that everything would be
      // marked stale and the cache cleared — emitting a spurious null that would overwrite the
      // PUSH KV row. Leave those cases to the next eviction at a sane timer timestamp.
      if (isAdjacentRollover && hasSmallWindows) {
        val previousPackedIr = windowedAgg.clone(pack())
        rebuildCachedSmallWindowIr(watermarkTs, wmDay)
        val packed = pack()
        val packedIsEmpty = isAllNull(packed)
        val previousWasEmpty = isAllNull(previousPackedIr)
        if (!((packedIsEmpty && previousWasEmpty) || irEqual(previousPackedIr, packed))) {
          return GigaEmitResult(windowedAgg.finalize(packed), isEmpty = packedIsEmpty)
        }
      }
    }
    GigaEmitResult(null)
  }

  /** Direct eviction/serve-as-of: corrects small window sawtooth and large window tail selection. */
  def onEviction(timerTs: Long): GigaEmitResult =
    runEviction(timerTs, force = true)

  /** Scheduled eviction used by Flink timers. Timer cadence stays fixed, but a timer that does
    * not cross a real small or large boundary skips the expensive state rebuilds.
    */
  def onScheduledEviction(timerTs: Long): GigaEmitResult =
    runEviction(timerTs, force = false)

  private def runEviction(timerTs: Long, force: Boolean): GigaEmitResult = {
    val currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) return GigaEmitResult(null)

    val smallMayChange = force || smallWindowMayChangeAt(timerTs, currentDayStart)
    val largeMayChange = force || largeIrMayChangeAt(timerTs)
    if (!smallMayChange && !largeMayChange) {
      return GigaEmitResult(null, isEmpty = isAllNull(pack()))
    }

    val previousPackedIr = windowedAgg.clone(pack())

    if (smallMayChange) rebuildCachedSmallWindowIr(timerTs, currentDayStart)
    if (largeMayChange) recomputeRunningLargeIr(timerTs, currentDayStart)

    val packed = pack()
    val packedIsEmpty = isAllNull(packed)
    val previousWasEmpty = isAllNull(previousPackedIr)
    if ((packedIsEmpty && previousWasEmpty) || irEqual(previousPackedIr, packed)) {
      // Nothing changed for this key. Suppress redundant KV writes, including fully-decayed
      // idle entities where the previous and current packed IR are both all-null.
      GigaEmitResult(null, isEmpty = packedIsEmpty)
    } else {
      GigaEmitResult(windowedAgg.finalize(packed), isEmpty = packedIsEmpty)
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
    * suppress path and surfaced on GigaEmitResult so the Flink writer can DELETE the KV row.
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
  def onBatchUpdate(newBatchIr: FinalBatchIr, newBatchEnd: Long, currentWatermark: Long): GigaEmitResult = {
    val oldBatchEnd = store.getBatchEndTs
    if (newBatchEnd <= oldBatchEnd) return GigaEmitResult(null)

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
        // Defer until watermark advances past batchEnd. Daily slots between currentDayStart
        // and batchEnd would otherwise overlap with batch and double-count.
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

    val oldRunningIr = store.getRunningLargeIr
    recomputeRunningLargeIr(currentWatermark, currentDayStart)

    val newRunningIr = store.getRunningLargeIr

    if (!irEqual(oldRunningIr, newRunningIr)) {
      GigaEmitResult(packAndFinalize(), needsEvictionTimer = true)
    } else {
      GigaEmitResult(null, needsEvictionTimer = true)
    }
  }

  // --- Private helpers ---

  private def previousTimerTs(queryTs: Long): Long =
    if (queryTs <= Long.MinValue + minEvictionInterval) Long.MinValue else queryTs - minEvictionInterval

  private def crossedSincePreviousTimer(queryTs: Long, boundaryTs: Long): Boolean =
    boundaryTs > previousTimerTs(queryTs) && boundaryTs <= queryTs

  private def smallWindowMayChangeAt(queryTs: Long, currentDayStart: Long): Boolean = {
    if (!hasSmallWindows || store.getEarliestTileStart == Long.MaxValue) return false

    var col = 0
    while (col < windowedAgg.length) {
      val windowMillis = megaTileAgg.columnWindowMillis(col)
      if (isNoBatch(col) && windowMillis > 0) {
        val hopSize = columnHopSize(col)
        val effectiveStart = megaTileAgg.effectiveStart(col, queryTs, currentDayStart)
        if (effectiveStart >= Long.MinValue + hopSize) {
          val expiredTileStart = effectiveStart - hopSize
          if (store.getTile(hopSize, expiredTileStart) != null) return true
        }
      }
      col += 1
    }

    false
  }

  private def largeIrMayChangeAt(queryTs: Long): Boolean = {
    val batchIr = store.getBatchIr
    val batchEndTs = store.getBatchEndTs
    val batchEndDay = if (batchEndTs > 0) TsUtils.round(batchEndTs, DayMillis) else Long.MinValue

    if (batchEndDay != Long.MinValue) {
      val iter = store.dailyLargeIrIterator
      while (iter.hasNext) {
        val (dayStart, _) = iter.next()
        if (dayStart < batchEndDay) return true
      }
    }

    if (batchIr != null && batchEndTs > 0L) {
      var col = 0
      while (col < windowedAgg.length) {
        val windowMillis = megaTileAgg.columnWindowMillis(col)
        if (!isNoBatch(col) && windowMillis > 0) {
          val collapsedExpiry = addIfNoOverflow(batchEndTs, windowMillis)
          if (
            batchIr.collapsed != null && col < batchIr.collapsed.length &&
            batchIr.collapsed(col) != null && crossedSincePreviousTimer(queryTs, collapsedExpiry)
          ) {
            return true
          }

          val hopIndex = megaTileAgg.tailHopIndicesArray(col)
          if (batchIr.tailHops != null && hopIndex < batchIr.tailHops.length && batchIr.tailHops(hopIndex) != null) {
            val hopSize = megaTileAgg.hopSizesArray(hopIndex)
            val prevTs = previousTimerTs(queryTs)
            val previousTail =
              if (prevTs == Long.MinValue) Long.MinValue
              else TsUtils.round(subtractIfNoUnderflow(prevTs, windowMillis), hopSize)
            val queryTail = TsUtils.round(subtractIfNoUnderflow(queryTs, windowMillis), hopSize)
            if (queryTail > previousTail) {
              val baseIrIndex = megaTileAgg.baseIrIndicesArray(col)
              val hopIrs = batchIr.tailHops(hopIndex)
              var idx = 0
              while (idx < hopIrs.length) {
                val hopIr = hopIrs(idx)
                if (hopIr != null && baseIrIndex < hopIr.length - 1) {
                  val hopStart = hopIr.last.asInstanceOf[Long]
                  if (hopStart >= previousTail && hopStart < queryTail && hopIr(baseIrIndex) != null) {
                    return true
                  }
                }
                idx += 1
              }
            }
          }
        }
        col += 1
      }
    }

    val columnWindowMillis = megaTileAgg.columnWindowMillis
    val baseIrIndices = megaTileAgg.baseIrIndicesArray
    var col = 0
    while (col < windowedAgg.length) {
      val windowMillis = columnWindowMillis(col)
      if (!isNoBatch(col) && windowMillis > 0 && queryTs >= windowMillis + DayMillis) {
        val dayStart = TsUtils.round(queryTs - windowMillis - DayMillis, DayMillis)
        val expiryTs = addIfNoOverflow(addIfNoOverflow(dayStart, DayMillis), windowMillis)
        if (crossedSincePreviousTimer(queryTs, expiryTs)) {
          val dayIr = store.getDailyLargeIr(dayStart)
          val baseIrIndex = baseIrIndices(col)
          if (dayIr != null && baseIrIndex < dayIr.length && dayIr(baseIrIndex) != null) {
            return true
          }
        }
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
    val batchEndDay = if (batchEndTs > 0) TsUtils.round(batchEndTs, DayMillis) else Long.MinValue
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
      } else if (maxWindowMillis > 0 && slotEnd <= queryTs - maxWindowMillis) {
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
  }

  /** Rebuild cachedSmallWindowIr from retained tiles using the supplied as-of horizon.
    * Called from advanceWatermark on day rollover (effective starts shift) and from
    * onEviction (sawtooth correction at hop boundaries).
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
  }

  private def updateLargeWindowColumns(ir: Array[Any], row: Row): Unit = {
    var col = 0
    while (col < windowedAgg.length) {
      if (!isNoBatch(col)) {
        windowedAgg.columnAggregators(col).update(ir, row)
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

  private[windowing] def packAndFinalize(): Array[Any] = windowedAgg.finalize(pack())

  /** Snapshot the current value without moving either aggregation clock. */
  private[chronon] def currentSnapshot: GigaEmitResult = {
    val packed = pack()
    GigaEmitResult(windowedAgg.finalize(packed), isEmpty = isAllNull(packed))
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
    // True when a non-timer path, such as batch update, wants the next fixed-cadence
    // eviction timer registered.
    needsEvictionTimer: Boolean = false,
    // True when the packed pre-finalize IR has every column null. The Flink wiring uses this
    // to (a) DELETE the PUSH KV row instead of writing a row of nulls and (b) decide whether
    // to keep re-registering decay timers — once a key is fully empty, no further decay is
    // possible until a new event arrives.
    isEmpty: Boolean = false,
    // True when this onEvent call dropped its event because eventTs was older than the
    // staleness bound. Surfaced so the Flink wiring can bump a metric / log instead of
    // silently losing data.
    droppedStaleEvent: Boolean = false
)
