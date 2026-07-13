package ai.chronon.flink.window

import ai.chronon.aggregator.windowing.{FinalBatchIr, GigaTileStore, GigaTileStreamProcessor, MegaTileAggregator}
import ai.chronon.api.{Constants, DataType, GroupBy, TsUtils}
import ai.chronon.api.ScalaJavaConversions.IteratorOps
import ai.chronon.flink.SparkExpressionEval
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.types.{BatchIrRow, TimestampedTile}
import ai.chronon.online.{GigaTileCodec, MegaTileCodec}
import ai.chronon.online.serde.ArrayRow
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.{TimeDomain, TimerService}
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/** Flink CoProcessFunction for the GigaTile pipeline.
  *
  * Two inputs:
  *   - Stream 1 (processElement1): Kafka events (ProjectedEvent)
  *   - Stream 2 (processElement2): Batch IR rows from Iceberg upload table (BatchIrRow)
  *
  * Delegates all aggregation logic to GigaTileStreamProcessor.
  * Emits finalized feature vectors (not windowed IRs) to the KV store.
  */
class GigaTileProcessFunction(
    groupBy: GroupBy,
    inputSchema: Seq[(String, DataType)],
    enableDebug: Boolean = false
) extends KeyedCoProcessFunction[java.util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile] {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  @transient private var processor: GigaTileStreamProcessor = _
  @transient private var gigaTileCodec: GigaTileCodec = _
  @transient private var megaTileCodec: MegaTileCodec = _
  @transient private var flinkStore: FlinkGigaTileStore = _

  @transient private var eventProcessingErrorCounter: Counter = _
  @transient private var batchUpdateCounter: Counter = _
  @transient private var lastKey: java.util.List[Any] = _

  private val valueColumns: Array[String] = inputSchema.map(_._1).toArray
  private val timeColumnAlias: String = Constants.TimeColumn

  // Flink managed state
  private var tileState: MapState[String, Array[Byte]] = _
  private var megaTileIrState: ValueState[Array[Byte]] = _
  private var dailyLargeIrState: MapState[java.lang.Long, Array[Byte]] = _
  private var currentDayStartState: ValueState[java.lang.Long] = _
  private var earliestTileStartState: ValueState[java.lang.Long] = _
  private var batchIrState: ValueState[Array[Byte]] = _
  private var batchEndTsState: ValueState[java.lang.Long] = _
  private var runningLargeIrState: ValueState[Array[Byte]] = _
  private var nextProcessingEvictionTimerState: ValueState[java.lang.Long] = _
  private var lastEmittedVersionState: ValueState[java.lang.Long] = _
  private var pendingVersionCollisionState: ValueState[java.lang.Long] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)
    eventProcessingErrorCounter = metricsGroup.counter("event_processing_error")
    batchUpdateCounter = metricsGroup.counter("batch_update_count")

    tileState = getRuntimeContext.getMapState(
      new MapStateDescriptor[String, Array[Byte]]("giga-tile-tiles", classOf[String], classOf[Array[Byte]]))
    megaTileIrState =
      getRuntimeContext.getState(new ValueStateDescriptor[Array[Byte]]("giga-tile-ir", classOf[Array[Byte]]))
    dailyLargeIrState = getRuntimeContext.getMapState(
      new MapStateDescriptor[java.lang.Long, Array[Byte]]("giga-tile-large-daily",
                                                          classOf[java.lang.Long],
                                                          classOf[Array[Byte]]))
    currentDayStartState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-day-start", classOf[java.lang.Long]))
    earliestTileStartState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-earliest-tile", classOf[java.lang.Long]))
    batchIrState =
      getRuntimeContext.getState(new ValueStateDescriptor[Array[Byte]]("giga-tile-batch-ir", classOf[Array[Byte]]))
    batchEndTsState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-batch-end", classOf[java.lang.Long]))
    runningLargeIrState =
      getRuntimeContext.getState(new ValueStateDescriptor[Array[Byte]]("giga-tile-running-large", classOf[Array[Byte]]))
    nextProcessingEvictionTimerState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-next-evict-pt-timer", classOf[java.lang.Long]))
    lastEmittedVersionState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-last-emitted-version", classOf[java.lang.Long]))
    pendingVersionCollisionState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-pending-version-collision", classOf[java.lang.Long]))

    initializeTransients()
  }

  private def initializeTransients(): Unit = {
    val inputCols = inputSchema.map { case (name, dt) => (name, dt) }
    val megaTileAgg = new MegaTileAggregator(groupBy.getAggregations.iterator().toScala.toSeq, inputCols)
    gigaTileCodec = new GigaTileCodec(groupBy, inputCols)
    megaTileCodec = new MegaTileCodec(groupBy, inputCols)
    flinkStore = new FlinkGigaTileStore(megaTileAgg, megaTileCodec, gigaTileCodec)

    val irEqual: (Array[Any], Array[Any]) => Boolean = { (a, b) =>
      java.util.Arrays.equals(gigaTileCodec.encodeWindowedIr(a), gigaTileCodec.encodeWindowedIr(b))
    }

    processor = new GigaTileStreamProcessor(megaTileAgg, flinkStore, irEqual)
  }

  private def ensureStateBound(currentKey: java.util.List[Any]): Unit = {
    if (lastKey == null || !lastKey.equals(currentKey)) {
      lastKey = currentKey
      flinkStore.bindFlinkState(
        tileState,
        megaTileIrState,
        dailyLargeIrState,
        currentDayStartState,
        earliestTileStartState,
        batchIrState,
        batchEndTsState,
        runningLargeIrState
      )
    }
  }

  /** Processing-time callbacks can share a millisecond. Coalesce later same-millisecond updates
    * behind a timer so emitted versions are strictly increasing before async sink handoff.
    */
  private def emitOrCoalesceVersionCollision(
      timerService: TimerService,
      finalizedVector: Array[Any],
      currentKey: java.util.List[Any],
      versionMillis: Long,
      startProcessingTimeMillis: Long,
      out: Collector[TimestampedTile]
  ): Unit = {
    if (finalizedVector == null) return

    val lastEmittedVersion = lastEmittedVersionState.value()
    if (
      pendingVersionCollisionState.value() != null ||
      (lastEmittedVersion != null && versionMillis <= lastEmittedVersion.longValue())
    ) {
      scheduleVersionCollisionFlushIfNeeded(timerService, lastEmittedVersion)
    } else {
      try {
        out.collect(
          new TimestampedTile(currentKey,
                              gigaTileCodec.encodeOutput(finalizedVector),
                              versionMillis,
                              startProcessingTimeMillis))
        lastEmittedVersionState.update(versionMillis)
      } catch {
        case e: Exception =>
          // State was already mutated. Retry the current snapshot on a future timer so a
          // transient first-write failure cannot leave the key unpublished.
          scheduleVersionCollisionFlushIfNeeded(timerService, lastEmittedVersion)
          throw e
      }
    }
  }

  private def scheduleVersionCollisionFlushIfNeeded(
      timerService: TimerService,
      lastEmittedVersion: java.lang.Long
  ): Unit = {
    val currentProcessingTime = timerService.currentProcessingTime()
    if (
      pendingVersionCollisionState.value() == null && currentProcessingTime < Long.MaxValue &&
      (lastEmittedVersion == null || lastEmittedVersion.longValue() < Long.MaxValue)
    ) {
      val flushVersion =
        if (lastEmittedVersion == null) currentProcessingTime + 1L
        else math.max(lastEmittedVersion.longValue() + 1L, currentProcessingTime + 1L)
      timerService.registerProcessingTimeTimer(flushVersion)
      pendingVersionCollisionState.update(flushVersion)
    }
  }

  private def isCurrentVersionCollisionTimer(timestamp: Long): Boolean =
    Option(pendingVersionCollisionState.value()).exists(_.longValue() == timestamp)

  private def processingTimerMarkerIsExpired(
      markerTimestamp: Long,
      currentProcessingTime: Long,
      currentTimerCallback: Option[(TimeDomain, Long)]
  ): Boolean =
    currentTimerCallback match {
      case None => markerTimestamp <= currentProcessingTime
      // Flink exposes the jump target as currentProcessingTime while draining every due
      // timer in timestamp order. A later marker is still physically queued; only a marker
      // before this callback can have been consumed without clearing its ownership state.
      case Some((TimeDomain.PROCESSING_TIME, callbackTimestamp)) => markerTimestamp < callbackTimestamp
      case Some((TimeDomain.EVENT_TIME, _))                      => false
    }

  private def isCurrentProcessingTimerCallback(
      timestamp: Long,
      currentTimerCallback: Option[(TimeDomain, Long)]
  ): Boolean =
    currentTimerCallback.contains(TimeDomain.PROCESSING_TIME -> timestamp)

  /** An older binary can consume additive processing-time timers without clearing their
    * keyed ownership markers. Repair expired ownership on the next keyed callback. This
    * cannot repair a completely idle key because a KeyedCoProcessFunction cannot enumerate
    * keyed state during open; rolling back below this timer-aware layer still requires a
    * retained compatible checkpoint or a state migration.
    */
  private def repairExpiredProcessingTimerMarkers(
      timerService: TimerService,
      currentProcessingTime: Long,
      currentTimerCallback: Option[(TimeDomain, Long)]
  ): Unit = {
    val trackedEviction = nextProcessingEvictionTimerState.value()
    if (
      trackedEviction != null && processingTimerMarkerIsExpired(trackedEviction.longValue(),
                                                                currentProcessingTime,
                                                                currentTimerCallback)
    ) {
      nextProcessingEvictionTimestamp(currentProcessingTime) match {
        case Some(nextEviction) =>
          // Register first. If registration fails, leave the stale marker visible so a later
          // callback can retry instead of recording ownership with no physical timer.
          timerService.registerProcessingTimeTimer(nextEviction)
          nextProcessingEvictionTimerState.update(nextEviction)
        case None => nextProcessingEvictionTimerState.clear()
      }
    }

    val trackedCollision = pendingVersionCollisionState.value()
    if (
      trackedCollision != null && processingTimerMarkerIsExpired(trackedCollision.longValue(),
                                                                 currentProcessingTime,
                                                                 currentTimerCallback)
    ) {
      val sharedEviction = Option(nextProcessingEvictionTimerState.value())
        .map(_.longValue())
        .filterNot(processingTimerMarkerIsExpired(_, currentProcessingTime, currentTimerCallback))
      val nextCollision = sharedEviction.orElse {
        val lastEmittedVersion = lastEmittedVersionState.value()
        if (
          currentProcessingTime == Long.MaxValue ||
          (lastEmittedVersion != null && lastEmittedVersion.longValue() == Long.MaxValue)
        ) None
        else {
          val afterLast = if (lastEmittedVersion == null) Long.MinValue else lastEmittedVersion.longValue() + 1L
          Some(math.max(currentProcessingTime + 1L, afterLast))
        }
      }
      nextCollision match {
        case Some(timestamp) =>
          // If the collision joins the callback currently being drained, role discovery below
          // will consume it in this invocation. Re-registering that timestamp would enqueue an
          // unnecessary immediate duplicate after Flink has removed the physical timer.
          if (!isCurrentProcessingTimerCallback(timestamp, currentTimerCallback)) {
            timerService.registerProcessingTimeTimer(timestamp)
          }
          pendingVersionCollisionState.update(timestamp)
        case None => pendingVersionCollisionState.clear()
      }
    }
  }

  private def flushVersionCollision(
      timerService: TimerService,
      currentProcessingTime: Long,
      currentKey: java.util.List[Any],
      out: Collector[TimestampedTile]
  ): Unit = {
    try {
      val pendingVersion = pendingVersionCollisionState.value()
      if (pendingVersion == null) return

      val snapshot = processor.currentSnapshot
      val encoded = gigaTileCodec.encodeOutput(snapshot.finalizedVector)
      out.collect(new TimestampedTile(currentKey, encoded, pendingVersion.longValue(), System.currentTimeMillis()))
      lastEmittedVersionState.update(pendingVersion)
      pendingVersionCollisionState.clear()
    } catch {
      case e: Exception =>
        // The fired timer was the only flush trigger. Re-arm before the outer handler records
        // the failure so a transient encoding/collection error cannot strand the latest value.
        if (currentProcessingTime < Long.MaxValue) {
          val retryVersion = currentProcessingTime + 1L
          timerService.registerProcessingTimeTimer(retryVersion)
          pendingVersionCollisionState.update(retryVersion)
        }
        throw e
    }
  }

  /** Process a Kafka event (stream 1). */
  override def processElement1(
      event: ProjectedEvent,
      ctx: KeyedCoProcessFunction[java.util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile]#Context,
      out: Collector[TimestampedTile]
  ): Unit = {
    try {
      if (processor == null) initializeTransients()

      val element = event.fields
      val tsMills = Try(element(timeColumnAlias).asInstanceOf[Long])
        .getOrElse(element(timeColumnAlias).asInstanceOf[Double].toLong)
      val values: Array[Any] = valueColumns.map(element(_))
      val row = new ArrayRow(values, tsMills)

      ensureStateBound(ctx.getCurrentKey)
      val timerService = ctx.timerService()
      val currentProcessingTime = timerService.currentProcessingTime()
      repairExpiredProcessingTimerMarkers(timerService, currentProcessingTime, None)
      val rolloverResult = processor.advanceWatermark(currentProcessingTime)
      val eventResult = processor.onEvent(row, tsMills, currentProcessingTime)
      val result = if (eventResult.finalizedVector != null) eventResult else rolloverResult

      scheduleProcessingEvictionTimerIfNeeded(timerService, currentProcessingTime)
      emitOrCoalesceVersionCollision(timerService,
                                     result.finalizedVector,
                                     ctx.getCurrentKey,
                                     currentProcessingTime,
                                     event.startProcessingTimeMillis,
                                     out)

    } catch {
      case e: Exception =>
        logger.error(s"Error processing giga tile event for groupBy=${groupBy.getMetaData.getName}", e)
        eventProcessingErrorCounter.inc()
    }
  }

  /** Process a batch IR row from Iceberg (stream 2). */
  override def processElement2(
      batchRow: BatchIrRow,
      ctx: KeyedCoProcessFunction[java.util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile]#Context,
      out: Collector[TimestampedTile]
  ): Unit = {
    try {
      if (processor == null) initializeTransients()

      ensureStateBound(ctx.getCurrentKey)
      val timerService = ctx.timerService()
      val currentProcessingTime = timerService.currentProcessingTime()
      repairExpiredProcessingTimerMarkers(timerService, currentProcessingTime, None)
      val rolloverResult = processor.advanceWatermark(currentProcessingTime)

      val batchIr = gigaTileCodec.decodeBatchIr(batchRow.valueBytes)
      val batchResult = processor.onBatchUpdate(batchIr, batchRow.batchEndTs, currentProcessingTime)
      val result = if (batchResult.finalizedVector != null) batchResult else rolloverResult

      batchUpdateCounter.inc()

      if (batchResult.needsEvictionTimer) {
        scheduleProcessingEvictionTimerIfNeeded(timerService, currentProcessingTime)
      }
      emitOrCoalesceVersionCollision(timerService,
                                     result.finalizedVector,
                                     ctx.getCurrentKey,
                                     currentProcessingTime,
                                     System.currentTimeMillis(),
                                     out)
    } catch {
      case e: Exception =>
        logger.error(s"Error processing batch IR for groupBy=${groupBy.getMetaData.getName}", e)
        eventProcessingErrorCounter.inc()
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedCoProcessFunction[java.util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile]#OnTimerContext,
      out: Collector[TimestampedTile]
  ): Unit = {
    var timerServiceOnFailure: TimerService = null
    var processingTimeOnFailure = Long.MinValue
    var evictionTimerFired = false
    var versionCollisionTimerFired = false
    try {
      if (processor == null) initializeTransients()

      ensureStateBound(ctx.getCurrentKey)
      val timerService = ctx.timerService()
      val currentProcessingTime = timerService.currentProcessingTime()
      timerServiceOnFailure = timerService
      processingTimeOnFailure = currentProcessingTime
      repairExpiredProcessingTimerMarkers(timerService, currentProcessingTime, Some(ctx.timeDomain() -> timestamp))

      // Checkpoints written by the event-time implementation may still contain several
      // event-time eviction timers per key. Let those callbacks migrate the key to one
      // processing-time timer, but do not publish from both timer domains.
      if (ctx.timeDomain() == TimeDomain.EVENT_TIME) {
        scheduleProcessingEvictionTimerIfNeeded(timerService, currentProcessingTime)
        return
      }

      if (ctx.timeDomain() != TimeDomain.PROCESSING_TIME) {
        return
      }

      val isEvictionTimer = isCurrentProcessingEvictionTimer(timestamp)
      val isVersionCollisionTimer = isCurrentVersionCollisionTimer(timestamp)
      evictionTimerFired = isEvictionTimer
      versionCollisionTimerFired = isVersionCollisionTimer
      if (!isEvictionTimer && !isVersionCollisionTimer) return

      var finalizedVector: Array[Any] = null
      if (isEvictionTimer) {
        nextProcessingEvictionTimerState.clear()
        val rolloverResult = processor.advanceWatermark(currentProcessingTime)
        val evictionResult = processor.onScheduledEviction(currentProcessingTime)
        finalizedVector =
          if (evictionResult.finalizedVector != null) evictionResult.finalizedVector else rolloverResult.finalizedVector

        // Re-register before a coincident collision flush: if encoding the flush fails, both
        // the retry and normal eviction cadence remain live.
        if (!evictionResult.isEmpty) {
          scheduleProcessingEvictionTimerIfNeeded(timerService, currentProcessingTime)
        }
      }

      if (isVersionCollisionTimer) {
        // Eviction wins when both timers share a timestamp; publish one post-eviction snapshot.
        flushVersionCollision(timerService, currentProcessingTime, ctx.getCurrentKey, out)
      } else {
        emitOrCoalesceVersionCollision(timerService,
                                       finalizedVector,
                                       ctx.getCurrentKey,
                                       currentProcessingTime,
                                       System.currentTimeMillis(),
                                       out)
      }
    } catch {
      case e: Exception =>
        if (timerServiceOnFailure != null) {
          val trackedEviction = nextProcessingEvictionTimerState.value()
          if (
            evictionTimerFired &&
            (trackedEviction == null || trackedEviction.longValue() <= timestamp)
          ) {
            nextProcessingEvictionTimerState.clear()
            scheduleProcessingEvictionTimerIfNeeded(timerServiceOnFailure, processingTimeOnFailure)
          }
          val pendingCollision = pendingVersionCollisionState.value()
          if (
            versionCollisionTimerFired && pendingCollision != null &&
            pendingCollision.longValue() <= timestamp && processingTimeOnFailure < Long.MaxValue
          ) {
            // A shared callback must preserve eviction-before-publication ordering. If
            // eviction failed, park the collision on the re-armed eviction timer instead
            // of publishing the stale pre-eviction snapshot one millisecond later.
            val retryVersion =
              if (evictionTimerFired) {
                Option(nextProcessingEvictionTimerState.value())
                  .map(_.longValue())
                  .filter(_ > timestamp)
                  .getOrElse(processingTimeOnFailure + 1L)
              } else {
                processingTimeOnFailure + 1L
              }
            timerServiceOnFailure.registerProcessingTimeTimer(retryVersion)
            pendingVersionCollisionState.update(retryVersion)
          }
        }
        logger.error(s"Error in giga tile eviction for groupBy=${groupBy.getMetaData.getName}", e)
        eventProcessingErrorCounter.inc()
    }
  }

  private def isCurrentProcessingEvictionTimer(timestamp: Long): Boolean =
    Option(nextProcessingEvictionTimerState.value()).exists(_.longValue() == timestamp)

  private def nextProcessingEvictionTimestamp(currentProcessingTime: Long): Option[Long] = {
    val interval = processor.minEvictionInterval
    if (currentProcessingTime <= Long.MaxValue - interval) {
      Some(TsUtils.round(currentProcessingTime, interval) + interval)
    } else {
      None
    }
  }

  private def scheduleProcessingEvictionTimerIfNeeded(
      timerService: TimerService,
      currentProcessingTime: Long
  ): Unit = {
    if (nextProcessingEvictionTimerState.value() == null) {
      nextProcessingEvictionTimestamp(currentProcessingTime).foreach { nextEviction =>
        timerService.registerProcessingTimeTimer(nextEviction)
        nextProcessingEvictionTimerState.update(nextEviction)
      }
    }
  }
}

/** TileStore backed by Flink state for GigaTile. Extends the mega tile state with batch IR
  * storage and a per-day large-IR map (keyed by day-start) sized to bridge any retained gap
  * between batchEndDay and the watermark day.
  */
class FlinkGigaTileStore(megaTileAgg: MegaTileAggregator, codec: MegaTileCodec, gigaCodec: GigaTileCodec)
    extends GigaTileStore {
  private val windowedAgg = megaTileAgg.windowedAggregator

  private var tileState: MapState[String, Array[Byte]] = _
  private var megaTileIrState: ValueState[Array[Byte]] = _
  private var dailyLargeIrState: MapState[java.lang.Long, Array[Byte]] = _
  private var currentDayStartState: ValueState[java.lang.Long] = _
  private var earliestTileStartState: ValueState[java.lang.Long] = _
  private var batchIrState: ValueState[Array[Byte]] = _
  private var batchEndTsState: ValueState[java.lang.Long] = _
  private var runningLargeIrState: ValueState[Array[Byte]] = _

  // Decode cache invalidated on key switch. Keeping per-day decoded values in an off-heap
  // mutable map would defeat the idea of per-key Flink state, so we just memoize the values
  // we read inside one event (typically getDailyLargeIr is called once per slot per recompute).
  private var cachedSmallDecoded: Array[Any] = _
  private var cachedSmallValid: Boolean = false
  private var batchIrDecoded: FinalBatchIr = _
  private var batchIrValid: Boolean = false
  private var runningLargeDecoded: Array[Any] = _
  private var runningLargeValid: Boolean = false

  def bindFlinkState(tiles: MapState[String, Array[Byte]],
                     megaTileIr: ValueState[Array[Byte]],
                     dailyLargeIr: MapState[java.lang.Long, Array[Byte]],
                     dayStart: ValueState[java.lang.Long],
                     earliest: ValueState[java.lang.Long],
                     batchIr: ValueState[Array[Byte]],
                     batchEndTs: ValueState[java.lang.Long],
                     runningLarge: ValueState[Array[Byte]]): Unit = {
    tileState = tiles
    megaTileIrState = megaTileIr
    dailyLargeIrState = dailyLargeIr
    currentDayStartState = dayStart
    earliestTileStartState = earliest
    batchIrState = batchIr
    batchEndTsState = batchEndTs
    runningLargeIrState = runningLarge
    cachedSmallValid = false
    batchIrValid = false
    runningLargeValid = false
  }

  private def tileKey(hopSize: Long, tileStart: Long): String = s"$hopSize:$tileStart"

  override def getTile(hopSize: Long, tileStart: Long): Array[Any] = {
    val bytes = tileState.get(tileKey(hopSize, tileStart))
    if (bytes != null) codec.decodeBaseIr(bytes) else null
  }
  override def putTile(hopSize: Long, tileStart: Long, ir: Array[Any]): Unit =
    tileState.put(tileKey(hopSize, tileStart), codec.encodeBaseIr(ir))
  override def removeTile(hopSize: Long, tileStart: Long): Unit =
    tileState.remove(tileKey(hopSize, tileStart))
  override def tileIterator: Iterator[(Long, Long, Array[Any])] = {
    val iter = tileState.iterator()
    new Iterator[(Long, Long, Array[Any])] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): (Long, Long, Array[Any]) = {
        val entry = iter.next()
        val parts = entry.getKey.split(":")
        (parts(0).toLong, parts(1).toLong, codec.decodeBaseIr(entry.getValue))
      }
    }
  }

  private def decodeWindowedIr(state: ValueState[Array[Byte]]): Array[Any] = {
    val bytes = state.value()
    if (bytes != null) codec.decode(bytes) else windowedAgg.init
  }

  override def getCachedSmallWindowIr: Array[Any] = {
    if (!cachedSmallValid) { cachedSmallDecoded = decodeWindowedIr(megaTileIrState); cachedSmallValid = true }
    cachedSmallDecoded
  }
  override def putCachedSmallWindowIr(ir: Array[Any]): Unit = {
    megaTileIrState.update(codec.encode(ir))
    cachedSmallDecoded = ir; cachedSmallValid = true
  }

  // Today/yesterday accessors required by the parent TileStore trait but unused in GigaTile —
  // routing happens through the per-day map below. Stubbed to safe defaults so a misroute is
  // detectable downstream (empty IRs / no-op writes) instead of throwing inside Flink.
  override def getLargeTodayIr: Array[Any] = windowedAgg.init
  override def putLargeTodayIr(ir: Array[Any]): Unit = ()
  override def getLargeYesterdayIr: Array[Any] = windowedAgg.init
  override def putLargeYesterdayIr(ir: Array[Any]): Unit = ()

  // Daily slot IRs are base-aggregator-shaped (one column per (op, input)) — fanned out
  // to per-window columns at recompute time via baseIrIndices.
  override def getDailyLargeIr(dayStart: Long): Array[Any] = {
    val bytes = dailyLargeIrState.get(dayStart)
    if (bytes != null) codec.decodeBaseIr(bytes) else null
  }
  override def putDailyLargeIr(dayStart: Long, ir: Array[Any]): Unit =
    dailyLargeIrState.put(dayStart, codec.encodeBaseIr(ir))
  override def removeDailyLargeIr(dayStart: Long): Unit =
    dailyLargeIrState.remove(dayStart)
  override def dailyLargeIrIterator: Iterator[(Long, Array[Any])] = {
    val iter = dailyLargeIrState.iterator()
    new Iterator[(Long, Array[Any])] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): (Long, Array[Any]) = {
        val entry = iter.next()
        (entry.getKey.longValue(), codec.decodeBaseIr(entry.getValue))
      }
    }
  }

  override def getCurrentDayStart: Long = Option(currentDayStartState.value()).map(_.longValue()).getOrElse(-1L)
  override def putCurrentDayStart(ts: Long): Unit = currentDayStartState.update(ts)

  override def getEarliestTileStart: Long =
    Option(earliestTileStartState.value()).map(_.longValue()).getOrElse(Long.MaxValue)
  override def putEarliestTileStart(ts: Long): Unit = earliestTileStartState.update(ts)

  override def getBatchIr: FinalBatchIr = {
    if (!batchIrValid) {
      val bytes = batchIrState.value()
      batchIrDecoded = if (bytes != null) gigaCodec.decodeBatchIr(bytes) else null
      batchIrValid = true
    }
    batchIrDecoded
  }
  override def putBatchIr(ir: FinalBatchIr): Unit = {
    batchIrState.update(gigaCodec.encodeBatchIr(ir))
    batchIrDecoded = ir; batchIrValid = true
  }

  override def getBatchEndTs: Long = Option(batchEndTsState.value()).map(_.longValue()).getOrElse(-1L)
  override def putBatchEndTs(ts: Long): Unit = batchEndTsState.update(ts)

  override def getRunningLargeIr: Array[Any] = {
    if (!runningLargeValid) {
      runningLargeDecoded = decodeWindowedIr(runningLargeIrState)
      runningLargeValid = true
    }
    runningLargeDecoded
  }
  override def putRunningLargeIr(ir: Array[Any]): Unit = {
    runningLargeIrState.update(codec.encode(ir))
    runningLargeDecoded = ir; runningLargeValid = true
  }
}
