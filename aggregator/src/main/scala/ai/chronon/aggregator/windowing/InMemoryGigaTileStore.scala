package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.row.RowAggregator

import scala.collection.mutable

class InMemoryGigaTileStore(windowedAgg: RowAggregator) extends InMemoryTileStore(windowedAgg) with GigaTileStore {
  private var _batchIr: FinalBatchIr = _
  private var _batchEndTs: Long = -1L
  private var _runningLargeIr: Array[Any] = windowedAgg.init
  private var _cachedSmallWindowAsOfTs: Long = -1L
  private var _lastLargeRecomputeAsOfTs: Long = Long.MinValue
  private val _dailyLargeIrs: mutable.Map[Long, Array[Any]] = mutable.Map.empty

  override def getBatchIr: FinalBatchIr = _batchIr
  override def putBatchIr(ir: FinalBatchIr): Unit = _batchIr = ir

  override def getBatchEndTs: Long = _batchEndTs
  override def putBatchEndTs(ts: Long): Unit = _batchEndTs = ts

  override def getRunningLargeIr: Array[Any] = _runningLargeIr
  override def putRunningLargeIr(ir: Array[Any]): Unit = _runningLargeIr = ir

  override def getCachedSmallWindowAsOfTs: Long = _cachedSmallWindowAsOfTs
  override def putCachedSmallWindowAsOfTs(ts: Long): Unit = _cachedSmallWindowAsOfTs = ts

  override def getLastLargeRecomputeAsOfTs: Long = _lastLargeRecomputeAsOfTs
  override def putLastLargeRecomputeAsOfTs(ts: Long): Unit = _lastLargeRecomputeAsOfTs = ts

  override def getDailyLargeIr(dayStart: Long): Array[Any] = _dailyLargeIrs.getOrElse(dayStart, null)
  override def putDailyLargeIr(dayStart: Long, ir: Array[Any]): Unit = _dailyLargeIrs(dayStart) = ir
  override def removeDailyLargeIr(dayStart: Long): Unit = _dailyLargeIrs.remove(dayStart)
  override def dailyLargeIrIterator: Iterator[(Long, Array[Any])] = _dailyLargeIrs.iterator
}
