package ai.chronon.aggregator.windowing

/** Extended TileStore for GigaTile: adds batch IR and per-day large-window IR state.
  *
  * GigaTile holds the FinalBatchIr in Flink state (loaded from Iceberg) and maintains
  * one daily large-window IR slot per day with streaming events. recomputeRunningLargeIr
  * merges batch + every daily slot whose dayStart is at or after batchEndDay.
  */
trait GigaTileStore extends TileStore {
  def getBatchIr: FinalBatchIr
  def putBatchIr(ir: FinalBatchIr): Unit

  def getBatchEndTs: Long
  def putBatchEndTs(ts: Long): Unit

  def getRunningLargeIr: Array[Any]
  def putRunningLargeIr(ir: Array[Any]): Unit

  /** Hop-aligned as-of timestamp represented by the cached small-window IR. */
  def getCachedSmallWindowAsOfTs: Long
  def putCachedSmallWindowAsOfTs(ts: Long): Unit

  /** As-of timestamp used by the last full running-large IR recomputation. */
  def getLastLargeRecomputeAsOfTs: Long
  def putLastLargeRecomputeAsOfTs(ts: Long): Unit

  // Per-day large-window IR state. One slot per day with streaming events between
  // batchEndDay and the current watermark day; pruned on batch advance.
  def getDailyLargeIr(dayStart: Long): Array[Any]
  def putDailyLargeIr(dayStart: Long, ir: Array[Any]): Unit
  def removeDailyLargeIr(dayStart: Long): Unit
  def dailyLargeIrIterator: Iterator[(Long, Array[Any])]
}
