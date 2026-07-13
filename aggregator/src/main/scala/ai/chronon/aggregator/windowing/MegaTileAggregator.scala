package ai.chronon.aggregator.windowing

import ai.chronon.api._
import ai.chronon.api.Extensions.WindowOps

import scala.collection.mutable

class MegaTileAggregator(aggregations: Seq[Aggregation],
                         inputSchema: Seq[(String, DataType)],
                         resolution: Resolution = FiveMinuteResolution,
                         override val tailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis)
    extends SawtoothMutationAggregator(aggregations, inputSchema, resolution, tailBufferMillis) {

  // Per-column hop size: maps windowed column index → hop size in millis
  val columnHopSize: Array[Long] = windowMappings.map { mapping =>
    Option(mapping.aggregationPart.window) match {
      case Some(w) => resolution.calculateTailHop(w)
      case None    => hopSizes.head // unwindowed uses largest hop
    }
  }

  // Distinct hop sizes that have at least one column mapped to them
  val activeTiers: Array[Long] = columnHopSize.distinct.sorted

  // Per-column: is this a NO BATCH column (window <= tailBuffer)?
  val isNoBatch: Array[Boolean] = windowMappings.map { mapping =>
    Option(mapping.aggregationPart.window) match {
      case Some(w) => w.millis <= tailBufferMillis
      case None    => false // unwindowed always needs batch
    }
  }

  def effectiveStart(col: Int, now: Long, batchEnd: Long): Long = {
    val window = windowMappings(col).aggregationPart.window
    if (window == null) {
      // unwindowed: need all streaming data from batchEnd
      batchEnd
    } else if (window.millis <= tailBufferMillis) {
      // NO BATCH: mega tile covers full window, rounded to hop boundary (sawtooth tail)
      val hopSize = columnHopSize(col)
      TsUtils.round(now - window.millis, hopSize)
    } else {
      // BATCH: mega tile covers [batchEnd, now)
      batchEnd
    }
  }

  // Compute tile start timestamps for an event across all active tiers.
  // Returns an Array to avoid Map allocation on every event.
  def tileStartsForEvent(eventTs: Long): Array[(Long, Long)] =
    activeTiers.map(hopSize => (hopSize, TsUtils.round(eventTs, hopSize)))

  // Per-tier retention floor: the earliest tile we must keep
  def retentionFloor(hopSize: Long, now: Long, batchEnd: Long): Long = {
    var minStart = Long.MaxValue
    var col = 0
    while (col < windowedAggregator.length) {
      if (columnHopSize(col) == hopSize) {
        val effStart = effectiveStart(col, now, batchEnd)
        if (effStart < minStart) minStart = effStart
      }
      col += 1
    }
    // one buffer tile before the floor
    minStart - hopSize
  }

  // Build windowed mega tile IR from small tiles
  def buildMegaTileIr(tiles: Map[Long, collection.Map[Long, Array[Any]]], now: Long, batchEnd: Long): Array[Any] = {
    val megaTileIr = windowedAggregator.init
    var col = 0
    while (col < windowedAggregator.length) {
      val hopSize = columnHopSize(col)
      val effStart = effectiveStart(col, now, batchEnd)
      val bucketIdx = baseIrIndices(col)
      val tierTiles = tiles.get(hopSize)

      tierTiles.foreach { tm =>
        tm.foreach { case (tileStart, tileIr) =>
          if (tileStart >= effStart && tileStart < now) {
            megaTileIr(col) = windowedAggregator.columnAggregators(col).merge(megaTileIr(col), tileIr(bucketIdx))
          }
        }
      }
      col += 1
    }
    megaTileIr
  }

  // Merge mega tile with batch IR to produce finalized result
  def serveMegaTile(batchIr: FinalBatchIr, megaTileIr: Array[Any], queryTs: Long, batchEnd: Long): Array[Any] = {
    // Start from batch collapsed (normalized) — same as lambdaAggregateIrTiled
    val resultIr = if (batchIr != null) windowedAggregator.clone(batchIr.collapsed) else windowedAggregator.init
    var col = 0
    while (col < windowedAggregator.length) {
      val window = windowMappings(col).aggregationPart.window
      if (window != null && window.millis <= tailBufferMillis) {
        // NO BATCH: mega tile is self-contained, replace batch value
        resultIr(col) = megaTileIr(col)
      } else if (megaTileIr(col) != null) {
        // BATCH or unwindowed: merge mega tile into batch collapsed
        resultIr(col) = windowedAggregator.columnAggregators(col).merge(resultIr(col), megaTileIr(col))
      }
      col += 1
    }

    // Tail hops ONLY for BATCH columns (window > tailBuffer).
    // NO BATCH columns are fully covered by the mega tile — adding tail hops would double-count.
    if (batchIr != null) {
      mergeTailHopsForBatchColumns(resultIr, queryTs, batchEnd, batchIr)
    }

    windowedAggregator.finalize(resultIr)
  }

  // Expose protected fields from SawtoothAggregator for use by GigaTileStreamProcessor
  val tailHopIndicesArray: Array[Int] = tailHopIndices
  val hopSizesArray: Array[Long] = hopSizes
  val baseIrIndicesArray: Array[Int] = baseIrIndices

  /** Largest retention horizon across all columns, in millis. Used by GigaTile to bound daily-
    * slot retention. Unwindowed columns retain uncovered slots until a later batch includes them,
    * so their horizon is intentionally unbounded.
    */
  val maxWindowMillis: Long = {
    var m: Long = 0L
    var hasUnwindowedColumn = false
    var i = 0
    while (i < windowMappings.length) {
      val w = windowMappings(i).aggregationPart.window
      if (w == null) hasUnwindowedColumn = true
      else if (w.millis > m) m = w.millis
      i += 1
    }
    if (hasUnwindowedColumn) Long.MaxValue else m
  }

  /** Per-windowed-column window in millis. -1 for unwindowed columns (which always include the slot). */
  val columnWindowMillis: Array[Long] = windowMappings.map { mapping =>
    Option(mapping.aggregationPart.window).map(_.millis).getOrElse(-1L)
  }

  // Like mergeTailHops but skips NO BATCH columns (window <= tailBuffer)
  private[windowing] def mergeTailHopsForBatchColumns(ir: Array[Any],
                                                      queryTs: Long,
                                                      batchEndTs: Long,
                                                      batchIr: FinalBatchIr): Array[Any] = {
    var i: Int = 0
    while (i < windowedAggregator.length) {
      val windowMillis = windowMappings(i).millis
      val window = windowMappings(i).aggregationPart.window
      if (window != null && window.millis > tailBufferMillis) {
        val hopIndex = tailHopIndices(i)
        val queryTail = TsUtils.round(queryTs - windowMillis, hopSizes(hopIndex))
        val alignedCollapsed = alignedCollapsedBoundary(batchEndTs - windowMillis, i)
        val hopIrs = batchIr.tailHops(hopIndex)
        val relevantHops = mutable.ArrayBuffer[Any](ir(i))
        var idx: Int = 0
        while (idx < hopIrs.length) {
          val hopIr = hopIrs(idx)
          val hopStart = hopIr.last.asInstanceOf[Long]
          if (alignedCollapsed > hopStart && hopStart >= queryTail) {
            val hopVal = hopIr(baseIrIndices(i))
            if (relevantHops(0) == null) {
              // bulkMerge may mutate its first non-null IR; clone cached tail-hop state on that path.
              relevantHops += windowedAggregator(i).clone(hopVal)
            } else {
              relevantHops += hopVal
            }
          }
          idx += 1
        }
        val merged = windowedAggregator(i).bulkMerge(relevantHops.iterator)
        ir.update(i, merged)
      }
      i += 1
    }
    ir
  }
}
