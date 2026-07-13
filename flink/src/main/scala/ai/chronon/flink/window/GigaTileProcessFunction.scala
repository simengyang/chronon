package ai.chronon.flink.window

import ai.chronon.aggregator.windowing.{
  EvictionTimes,
  FinalBatchIr,
  GigaEmitResult,
  GigaTileStore,
  GigaTileStreamProcessor,
  MegaTileAggregator
}
import ai.chronon.api.{Constants, DataType, GroupBy, TsUtils}
import ai.chronon.api.ScalaJavaConversions.IteratorOps
import ai.chronon.flink.{FlinkJob, SparkExpressionEval}
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.types.{BatchIrRow, TimestampedTile}
import ai.chronon.flink.window.ChrononClockMode.{Catchup, Horizons, Live, Mode, NoWatermark}
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
    enableDebug: Boolean = false,
    bufferingOutputTimeMillis: Long = 0L
) extends KeyedCoProcessFunction[java.util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile] {

  require(bufferingOutputTimeMillis >= 0L, "GigaTile output buffer must be non-negative")

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  @transient private var processor: GigaTileStreamProcessor = _
  @transient private var gigaTileCodec: GigaTileCodec = _
  @transient private var megaTileCodec: MegaTileCodec = _
  @transient private var flinkStore: FlinkGigaTileStore = _

  @transient private var eventProcessingErrorCounter: Counter = _
  @transient private var batchUpdateCounter: Counter = _
  @transient private var futureEventDropCounter: Counter = _
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
  private var cachedSmallWindowAsOfTsState: ValueState[java.lang.Long] = _
  private var lastLargeRecomputeAsOfTsState: ValueState[java.lang.Long] = _
  private var lastEmittedVersionState: ValueState[java.lang.Long] = _
  private var pendingVersionCollisionState: ValueState[java.lang.Long] = _
  private var pendingPublicationState: ValueState[java.lang.Boolean] = _
  private var pendingPublicationRetryTimerState: ValueState[java.lang.Long] = _
  private var pendingPublicationActivationTimerState: ValueState[java.lang.Long] = _
  private var bufferedWriteLatestTsState: ValueState[java.lang.Long] = _
  private var nextBufferedWriteTimerState: ValueState[java.lang.Long] = _

  private val publicationRetryDelayMillis = math.max(1L, 2L * FlinkJob.AutoWatermarkInterval)
  private val activationCooldownDelayMillis =
    math.max(publicationRetryDelayMillis, FlinkJob.CatchupWatermarkLagSlackMillis / 2L)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)
    eventProcessingErrorCounter = metricsGroup.counter("event_processing_error")
    batchUpdateCounter = metricsGroup.counter("batch_update_count")
    futureEventDropCounter = metricsGroup.counter("future_event_drop_count")

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
    cachedSmallWindowAsOfTsState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-cached-small-window-as-of-ts", classOf[java.lang.Long]))
    lastLargeRecomputeAsOfTsState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-last-large-recompute-as-of-ts", classOf[java.lang.Long]))
    lastEmittedVersionState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-last-emitted-version", classOf[java.lang.Long]))
    pendingVersionCollisionState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-pending-version-collision", classOf[java.lang.Long]))
    pendingPublicationState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Boolean]("giga-tile-pending-publication", classOf[java.lang.Boolean]))
    pendingPublicationRetryTimerState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-pending-publication-retry-pt-timer", classOf[java.lang.Long]))
    pendingPublicationActivationTimerState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-pending-publication-activation-et-timer",
                                               classOf[java.lang.Long]))
    bufferedWriteLatestTsState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-buffered-write-latest-ts", classOf[java.lang.Long]))
    nextBufferedWriteTimerState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-next-emit-pt-timer", classOf[java.lang.Long]))

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
        runningLargeIrState,
        cachedSmallWindowAsOfTsState,
        lastLargeRecomputeAsOfTsState
      )
    }
  }

  private def currentClockMode(currentProcessingTime: Long, eventTimeWatermark: Long): Mode =
    ChrononClockMode.classify(currentProcessingTime,
                              eventTimeWatermark,
                              FlinkJob.AllowedOutOfOrderness.toMillis,
                              FlinkJob.LiveWatermarkLagToleranceMillis)

  private def bufferingEnabled: Boolean = bufferingOutputTimeMillis > 0L

  private def hasBufferedWrite: Boolean = bufferedWriteLatestTsState.value() != null

  private def repairStaleBufferedWriteTimerIfNeeded(
      timerService: TimerService,
      currentProcessingTime: Long,
      currentTimerCallback: Option[(TimeDomain, Long)]
  ): Unit = {
    val bufferedVersion = bufferedWriteLatestTsState.value()
    val bufferedTimer = nextBufferedWriteTimerState.value()
    if (
      bufferedVersion != null && bufferedTimer != null &&
      processingTimerMarkerIsExpired(bufferedTimer.longValue(), currentProcessingTime, currentTimerCallback)
    ) {
      // An older binary can consume a cadence timer without understanding these keyed
      // markers. Recreate logical/physical ownership on the first later callback.
      pendingPublicationState.update(java.lang.Boolean.TRUE)
      nextBufferedWriteTimestamp(lastKey, currentProcessingTime) match {
        case Some(nextEmit) =>
          // Register the replacement before moving the keyed marker. If registration
          // fails, the restored stale marker remains visible for a later repair attempt.
          timerService.registerProcessingTimeTimer(nextEmit)
          nextBufferedWriteTimerState.update(nextEmit)
        case None =>
          nextBufferedWriteTimerState.clear()
      }
    }
  }

  private def stableBufferedWritePhaseMillis(key: java.util.List[Any]): Long =
    Math.floorMod((groupBy.getMetaData.getName :: key.iterator().toScala.toList).hashCode().toLong,
                  bufferingOutputTimeMillis)

  private def nextBufferedWriteTimestamp(
      key: java.util.List[Any],
      currentProcessingTime: Long
  ): Option[Long] = {
    if (!bufferingEnabled) return None

    val phase = stableBufferedWritePhaseMillis(key)
    val currentPhase = Math.floorMod(currentProcessingTime, bufferingOutputTimeMillis)
    val elapsedSincePhase = Math.floorMod(currentPhase - phase, bufferingOutputTimeMillis)
    val delay =
      if (elapsedSincePhase == 0L) bufferingOutputTimeMillis
      else bufferingOutputTimeMillis - elapsedSincePhase
    if (currentProcessingTime <= Long.MaxValue - delay) Some(currentProcessingTime + delay) else None
  }

  private def scheduleBufferedWriteTimerIfNeeded(
      timerService: TimerService,
      currentProcessingTime: Long
  ): Unit = {
    if (bufferingEnabled && nextBufferedWriteTimerState.value() == null) {
      nextBufferedWriteTimestamp(lastKey, currentProcessingTime).foreach { nextEmit =>
        timerService.registerProcessingTimeTimer(nextEmit)
        nextBufferedWriteTimerState.update(nextEmit)
      }
    }
  }

  private def isCurrentBufferedWriteTimer(timestamp: Long): Boolean =
    Option(nextBufferedWriteTimerState.value()).exists(_.longValue() == timestamp)

  private def bufferLatestWrite(
      timerService: TimerService,
      versionMillis: Long
  ): Unit = {
    val previousVersion = bufferedWriteLatestTsState.value()
    if (previousVersion == null || versionMillis > previousVersion.longValue()) {
      bufferedWriteLatestTsState.update(versionMillis)
    }
    scheduleBufferedWriteTimerIfNeeded(timerService, versionMillis)
  }

  private def isFutureEvent(eventTime: Long, currentProcessingTime: Long): Boolean =
    eventTime > currentProcessingTime

  private def hasPendingPublication: Boolean =
    java.lang.Boolean.TRUE.equals(pendingPublicationState.value()) && pendingVersionCollisionState.value() == null

  private def hasDeferredPublication: Boolean =
    hasPendingPublication &&
      (!hasBufferedWrite || nextBufferedWriteTimerState.value() == null)

  private def schedulePublicationRetryIfNeeded(
      timerService: TimerService,
      currentProcessingTime: Long
  ): Unit =
    schedulePublicationRetryAfterIfNeeded(timerService, currentProcessingTime, publicationRetryDelayMillis)

  private def schedulePublicationRetryAfterIfNeeded(
      timerService: TimerService,
      currentProcessingTime: Long,
      delayMillis: Long
  ): Unit = {
    if (hasDeferredPublication && !hasPublicationWakeupTimer && currentProcessingTime <= Long.MaxValue - delayMillis) {
      val retryTimestamp = currentProcessingTime + delayMillis
      timerService.registerProcessingTimeTimer(retryTimestamp)
      pendingPublicationRetryTimerState.update(retryTimestamp)
    }
  }

  private def schedulePublicationRetryAtIfNeeded(
      timerService: TimerService,
      currentProcessingTime: Long,
      retryTimestamp: Long
  ): Unit = {
    if (hasDeferredPublication && !hasPublicationWakeupTimer && retryTimestamp > currentProcessingTime) {
      timerService.registerProcessingTimeTimer(retryTimestamp)
      pendingPublicationRetryTimerState.update(retryTimestamp)
    }
  }

  private def hasPublicationWakeupTimer: Boolean =
    pendingPublicationRetryTimerState.value() != null || pendingPublicationActivationTimerState.value() != null

  private def nextPublicationActivationWatermark(currentProcessingTime: Long): Option[Long] = {
    val liveWatermarkLagMillis = FlinkJob.LiveWatermarkLagToleranceMillis
    if (currentProcessingTime < Long.MinValue + liveWatermarkLagMillis) None
    else Some(currentProcessingTime - liveWatermarkLagMillis)
  }

  private def firstSafeProcessingTimeAfterFutureWatermark(eventTimeWatermark: Long): Option[Long] = {
    val outOfOrdernessMillis = FlinkJob.AllowedOutOfOrderness.toMillis
    if (eventTimeWatermark > Long.MaxValue - outOfOrdernessMillis) None
    else {
      val lastUnsafeProcessingTime = eventTimeWatermark + outOfOrdernessMillis
      if (lastUnsafeProcessingTime == Long.MaxValue) None else Some(lastUnsafeProcessingTime + 1L)
    }
  }

  private def schedulePublicationActivationIfNeeded(
      timerService: TimerService,
      currentProcessingTime: Long,
      eventTimeWatermark: Long
  ): Unit = {
    if (hasDeferredPublication && !hasPublicationWakeupTimer) {
      nextPublicationActivationWatermark(currentProcessingTime)
        .filter(_ > eventTimeWatermark)
        .foreach { activationWatermark =>
          timerService.registerEventTimeTimer(activationWatermark)
          pendingPublicationActivationTimerState.update(activationWatermark)
        }
    }
  }

  private def scheduleFencedPublicationWakeupIfNeeded(
      timerService: TimerService,
      currentProcessingTime: Long,
      eventTimeWatermark: Long,
      mode: Mode
  ): Unit =
    mode match {
      case Catchup =>
        schedulePublicationActivationIfNeeded(timerService, currentProcessingTime, eventTimeWatermark)
      case NoWatermark if eventTimeWatermark == Long.MinValue =>
        schedulePublicationActivationIfNeeded(timerService, currentProcessingTime, eventTimeWatermark)
      case NoWatermark =>
        // Watermarks never move backward. A future-poisoned watermark becomes safe at one
        // exact wall-clock boundary, so avoid polling every pending key while time catches up.
        firstSafeProcessingTimeAfterFutureWatermark(eventTimeWatermark)
          .foreach { retryTimestamp =>
            schedulePublicationRetryAtIfNeeded(timerService, currentProcessingTime, retryTimestamp)
          }
      case Live => ()
    }

  private def clearPublicationRetryMarker(): Unit =
    pendingPublicationRetryTimerState.clear()

  private def clearExpiredPublicationRetryMarker(
      currentProcessingTime: Long,
      currentTimerCallback: Option[(TimeDomain, Long)]
  ): Boolean = {
    val retryTimestamp = pendingPublicationRetryTimerState.value()
    if (
      retryTimestamp != null && processingTimerMarkerIsExpired(retryTimestamp.longValue(),
                                                               currentProcessingTime,
                                                               currentTimerCallback)
    ) {
      pendingPublicationRetryTimerState.clear()
      true
    } else {
      false
    }
  }

  private def clearPublicationWakeupMarkers(): Unit = {
    pendingPublicationRetryTimerState.clear()
    pendingPublicationActivationTimerState.clear()
  }

  private def isCurrentPublicationRetryTimer(timestamp: Long): Boolean =
    Option(pendingPublicationRetryTimerState.value()).exists(_.longValue() == timestamp)

  private def isCurrentPublicationActivationTimer(timestamp: Long): Boolean =
    Option(pendingPublicationActivationTimerState.value()).exists(_.longValue() == timestamp)

  private def shouldPublish(
      mode: Mode,
      horizons: Horizons,
      currentProcessingTime: Long
  ): Boolean = {
    val currentDayStart = flinkStore.getCurrentDayStart
    ChrononClockMode.allowsPublication(mode) &&
    horizons.largeWindowAsOfMillis == currentProcessingTime &&
    !(currentDayStart >= 0L && flinkStore.getBatchEndTs > currentDayStart)
  }

  private def advanceDayForMode(
      mode: Mode,
      currentProcessingTime: Long,
      eventTimeWatermark: Long
  ): GigaEmitResult =
    ChrononClockMode
      .dayAdvanceAsOfMillis(mode, currentProcessingTime, eventTimeWatermark)
      .map(processor.advanceDayAsOf)
      .getOrElse(GigaEmitResult(null))

  private def preferLatestResult(primary: GigaEmitResult, fallback: GigaEmitResult): GigaEmitResult =
    if (primary.finalizedVector != null || fallback.finalizedVector == null) primary else fallback

  private def rebuildPendingAtCurrentTime(
      mode: Mode,
      horizons: Horizons,
      currentProcessingTime: Long
  ): Option[GigaEmitResult] =
    if (hasDeferredPublication && shouldPublish(mode, horizons, currentProcessingTime)) {
      Some(
        processor.onEviction(
          EvictionTimes(timerTs = horizons.largeWindowAsOfMillis, smallWindowAsOfTs = horizons.smallWindowAsOfMillis)))
    } else {
      None
    }

  private def emitOrBuffer(
      finalizedVector: Array[Any],
      currentKey: java.util.List[Any],
      currentProcessingTime: Long,
      startProcessingTime: Long,
      timerService: TimerService,
      out: Collector[TimestampedTile]
  ): Unit = {
    if (!bufferingEnabled) {
      // A checkpoint may have been written with buffering enabled. The first immediate
      // callback supersedes that buffered snapshot; leave its physical timer in Flink and
      // clear only the logical markers so the restored callback becomes a no-op.
      bufferedWriteLatestTsState.clear()
      nextBufferedWriteTimerState.clear()
      emitOrCoalesceVersionCollision(timerService,
                                     finalizedVector,
                                     currentKey,
                                     currentProcessingTime,
                                     startProcessingTime,
                                     out)
      return
    }

    bufferLatestWrite(timerService, currentProcessingTime)
    // Only the timestamp is buffered; the value is rebuilt from keyed state at the cadence
    // boundary. If no later timestamp can be represented, fall back to the normal path so a
    // value is not stranded at the end of the processing-time range.
    if (nextBufferedWriteTimerState.value() == null) {
      emitOrCoalesceVersionCollision(timerService,
                                     finalizedVector,
                                     currentKey,
                                     currentProcessingTime,
                                     startProcessingTime,
                                     out)
      bufferedWriteLatestTsState.clear()
    }
  }

  private def emitOrDefer(
      mode: Mode,
      horizons: Horizons,
      result: GigaEmitResult,
      currentKey: java.util.List[Any],
      currentProcessingTime: Long,
      startProcessingTime: Long,
      timerService: TimerService,
      out: Collector[TimestampedTile],
      scheduleWatermarkRetry: Boolean
  ): Unit = {
    if (!shouldPublish(mode, horizons, currentProcessingTime)) {
      val publicationPending = result.finalizedVector != null || result.needsEvictionTimer || hasPendingPublication
      if (publicationPending) {
        pendingPublicationState.update(java.lang.Boolean.TRUE)
        if (scheduleWatermarkRetry) schedulePublicationRetryIfNeeded(timerService, currentProcessingTime)
      }
      return
    }

    val finalizedVector =
      if (result.finalizedVector != null) result.finalizedVector
      else if (hasDeferredPublication) processor.currentSnapshot.finalizedVector
      else null

    if (finalizedVector != null) {
      // This tracks ownership through cadence buffering, encoding, and Collector handoff. The
      // downstream async KV result is outside this keyed operator and cannot acknowledge or
      // retry through this state.
      pendingPublicationState.update(java.lang.Boolean.TRUE)
      try {
        emitOrBuffer(finalizedVector, currentKey, currentProcessingTime, startProcessingTime, timerService, out)
        // A buffered value has not reached the collector yet. Keep the legacy pending
        // marker until cadence/collision publication succeeds so an older binary restored
        // from this checkpoint can rebuild it on the normal eviction timer.
        if (!hasBufferedWrite && pendingVersionCollisionState.value() == null) pendingPublicationState.clear()
        // Cadence or collision state owns any deferred write from here.
        clearPublicationWakeupMarkers()
      } catch {
        case e: Exception =>
          logger.error(s"Error emitting giga tile for groupBy=${groupBy.getMetaData.getName}", e)
          eventProcessingErrorCounter.inc()
      }
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

  private def postponeVersionCollision(
      timerService: TimerService,
      currentProcessingTime: Long,
      currentTimerCallback: Option[(TimeDomain, Long)]
  ): Unit = {
    if (pendingVersionCollisionState.value() != null) {
      val trackedEviction = nextProcessingEvictionTimerState.value()
      val retryVersion =
        if (
          trackedEviction != null && !processingTimerMarkerIsExpired(trackedEviction.longValue(),
                                                                     currentProcessingTime,
                                                                     currentTimerCallback)
        ) {
          Some(trackedEviction.longValue())
        } else {
          nextProcessingEvictionTimestamp(currentProcessingTime)
        }
      retryVersion.foreach { version =>
        // This normally shares the tracked eviction timer; registering the same keyed
        // timestamp is idempotent and avoids a separate hot retry loop while fenced.
        timerService.registerProcessingTimeTimer(version)
        pendingVersionCollisionState.update(version)
      }
    }
  }

  private def flushVersionCollision(
      timerService: TimerService,
      currentProcessingTime: Long,
      currentTimerCallback: Option[(TimeDomain, Long)],
      currentKey: java.util.List[Any],
      out: Collector[TimestampedTile]
  ): Unit = {
    try {
      val pendingVersion = pendingVersionCollisionState.value()
      if (pendingVersion == null) return

      val snapshot = processor.currentSnapshot
      val encoded = gigaTileCodec.encodeOutput(snapshot.finalizedVector)
      val bufferedVersion = bufferedWriteLatestTsState.value()
      val outputVersion =
        if (bufferedVersion != null && bufferedVersion.longValue() > pendingVersion.longValue()) bufferedVersion
        else pendingVersion
      out.collect(new TimestampedTile(currentKey, encoded, outputVersion.longValue(), System.currentTimeMillis()))
      lastEmittedVersionState.update(outputVersion)
      pendingVersionCollisionState.clear()
      // The collision snapshot includes every mutation represented by the buffered
      // cadence state. Relinquish that logical timer ownership so its physical callback
      // cannot schedule and publish the same value again.
      bufferedWriteLatestTsState.clear()
      nextBufferedWriteTimerState.clear()
      pendingPublicationState.clear()
      clearPublicationWakeupMarkers()
    } catch {
      case e: Exception =>
        // The fired timer was the only flush trigger. Re-arm before the outer handler records
        // the failure so a transient encoding/collection error cannot strand the latest value.
        scheduleProcessingEvictionTimerIfNeeded(timerService, currentProcessingTime)
        postponeVersionCollision(timerService, currentProcessingTime, currentTimerCallback)
        throw e
    }
  }

  private def parkBufferedWrite(): Unit = {
    nextBufferedWriteTimerState.clear()
    if (hasBufferedWrite) {
      pendingPublicationState.update(java.lang.Boolean.TRUE)
    }
  }

  private def flushBufferedWrite(
      timerService: TimerService,
      currentProcessingTime: Long,
      currentKey: java.util.List[Any],
      out: Collector[TimestampedTile]
  ): Unit = {
    val bufferedVersion = bufferedWriteLatestTsState.value()
    nextBufferedWriteTimerState.clear()
    if (bufferedVersion == null) return

    pendingPublicationState.update(java.lang.Boolean.TRUE)
    try {
      emitOrCoalesceVersionCollision(timerService,
                                     processor.packAndFinalize(),
                                     currentKey,
                                     bufferedVersion.longValue(),
                                     System.currentTimeMillis(),
                                     out)
      bufferedWriteLatestTsState.clear()
      if (pendingVersionCollisionState.value() == null) pendingPublicationState.clear()
      clearPublicationWakeupMarkers()
    } catch {
      case e: Exception =>
        // The fired cadence timer was the only write trigger. Re-arm it before the outer
        // handler records the failure so a transient encoding/collection error cannot strand
        // the latest keyed value.
        scheduleBufferedWriteTimerIfNeeded(timerService, currentProcessingTime)
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
      repairStaleBufferedWriteTimerIfNeeded(timerService, currentProcessingTime, None)
      if (isFutureEvent(tsMills, currentProcessingTime)) {
        futureEventDropCounter.inc()
        return
      }
      clearExpiredPublicationRetryMarker(currentProcessingTime, None)
      val eventTimeWatermark = timerService.currentWatermark()
      val mode = currentClockMode(currentProcessingTime, eventTimeWatermark)
      val horizons = ChrononClockMode.streamEventHorizons(mode,
                                                          tsMills,
                                                          currentProcessingTime,
                                                          eventTimeWatermark,
                                                          processor.minSmallWindowTileSize)

      val rolloverResult =
        if (mode == NoWatermark) processor.advanceDayAsOf(tsMills)
        else advanceDayForMode(mode, currentProcessingTime, eventTimeWatermark)
      val pendingRebuild = rebuildPendingAtCurrentTime(mode, horizons, currentProcessingTime)
      val eventResult = processor.onEvent(row,
                                          tsMills,
                                          largeWindowAsOfTs = horizons.largeWindowAsOfMillis,
                                          smallWindowAsOfTs = horizons.smallWindowAsOfMillis)
      val result = preferLatestResult(eventResult, pendingRebuild.getOrElse(rolloverResult))

      scheduleProcessingEvictionTimerIfNeeded(timerService, currentProcessingTime)
      emitOrDefer(mode,
                  horizons,
                  result,
                  ctx.getCurrentKey,
                  currentProcessingTime,
                  event.startProcessingTimeMillis,
                  timerService,
                  out,
                  scheduleWatermarkRetry = true)

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
      repairStaleBufferedWriteTimerIfNeeded(timerService, currentProcessingTime, None)
      clearExpiredPublicationRetryMarker(currentProcessingTime, None)
      val eventTimeWatermark = timerService.currentWatermark()
      val mode = currentClockMode(currentProcessingTime, eventTimeWatermark)
      val horizons = ChrononClockMode.batchUpdateHorizons(mode,
                                                          batchRow.batchEndTs,
                                                          flinkStore.getCurrentDayStart,
                                                          currentProcessingTime,
                                                          eventTimeWatermark,
                                                          processor.minSmallWindowTileSize)

      val rolloverResult = advanceDayForMode(mode, currentProcessingTime, eventTimeWatermark)
      val pendingRebuild = rebuildPendingAtCurrentTime(mode, horizons, currentProcessingTime)
      val batchIr = gigaTileCodec.decodeBatchIr(batchRow.valueBytes)
      val batchResult = processor.onBatchUpdate(batchIr,
                                                batchRow.batchEndTs,
                                                largeWindowAsOfTs = horizons.largeWindowAsOfMillis,
                                                smallWindowAsOfTs = horizons.smallWindowAsOfMillis)
      val result = preferLatestResult(batchResult, pendingRebuild.getOrElse(rolloverResult))

      batchUpdateCounter.inc()

      emitOrDefer(mode,
                  horizons,
                  result,
                  ctx.getCurrentKey,
                  currentProcessingTime,
                  System.currentTimeMillis(),
                  timerService,
                  out,
                  scheduleWatermarkRetry = true)

      if (batchResult.needsEvictionTimer || hasPendingPublication) {
        scheduleProcessingEvictionTimerIfNeeded(timerService, currentProcessingTime)
      }
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
    var bufferedWriteTimerFired = false
    var versionCollisionTimerFired = false
    var publicationRetryTimerFired = false
    var publicationActivationTimerFired = false
    var expiredPublicationRetryMarkerCleared = false
    try {
      if (processor == null) initializeTransients()

      ensureStateBound(ctx.getCurrentKey)
      val timerService = ctx.timerService()
      val currentProcessingTime = timerService.currentProcessingTime()
      val currentTimerCallback = Some(ctx.timeDomain() -> timestamp)
      timerServiceOnFailure = timerService
      processingTimeOnFailure = currentProcessingTime
      repairExpiredProcessingTimerMarkers(timerService, currentProcessingTime, currentTimerCallback)

      // Checkpoints written by the former event-time implementation may still contain
      // eviction timers. Only the explicitly tracked activation timer may publish from this
      // domain; untracked legacy timers remain migration-only.
      if (ctx.timeDomain() == TimeDomain.EVENT_TIME) {
        val isPublicationActivationTimer = isCurrentPublicationActivationTimer(timestamp)
        publicationActivationTimerFired = isPublicationActivationTimer
        if (isPublicationActivationTimer) pendingPublicationActivationTimerState.clear()
        scheduleProcessingEvictionTimerIfNeeded(timerService, currentProcessingTime)
        if (isPublicationActivationTimer) {
          if (hasDeferredPublication) {
            val eventTimeWatermark = timerService.currentWatermark()
            currentClockMode(currentProcessingTime, eventTimeWatermark) match {
              case Live =>
                val horizons = ChrononClockMode
                  .processingTimerHorizons(Live,
                                           currentProcessingTime,
                                           eventTimeWatermark,
                                           processor.minSmallWindowTileSize)
                  .get
                val rolloverResult = advanceDayForMode(Live, currentProcessingTime, eventTimeWatermark)
                val retryResult = rebuildPendingAtCurrentTime(Live, horizons, currentProcessingTime)
                  .getOrElse(GigaEmitResult(null))
                val result = preferLatestResult(retryResult, rolloverResult)
                emitOrDefer(Live,
                            horizons,
                            result,
                            ctx.getCurrentKey,
                            currentProcessingTime,
                            System.currentTimeMillis(),
                            timerService,
                            out,
                            scheduleWatermarkRetry = false)
              case Catchup =>
                // If callback delivery consumed the Live slack, separate retries from source
                // watermark cadence before installing a new activation boundary.
                schedulePublicationRetryAfterIfNeeded(timerService,
                                                      currentProcessingTime,
                                                      activationCooldownDelayMillis)
              case mode =>
                scheduleFencedPublicationWakeupIfNeeded(timerService, currentProcessingTime, eventTimeWatermark, mode)
            }
          }
        }
        return
      }

      if (ctx.timeDomain() != TimeDomain.PROCESSING_TIME) {
        return
      }

      // One physical Flink timer can represent several logical triggers. Snapshot every role
      // before clearing any marker, then mutate state before publication work.
      val isTrackedEvictionTimer = isCurrentProcessingEvictionTimer(timestamp)
      val isBufferedWriteTimer = isCurrentBufferedWriteTimer(timestamp)
      bufferedWriteTimerFired = isBufferedWriteTimer
      val isVersionCollisionTimer = isCurrentVersionCollisionTimer(timestamp)
      versionCollisionTimerFired = isVersionCollisionTimer
      val isPublicationRetryTimer = isCurrentPublicationRetryTimer(timestamp)
      publicationRetryTimerFired = isPublicationRetryTimer
      val isLegacyEvictionTimer =
        !isTrackedEvictionTimer && !isBufferedWriteTimer && !isVersionCollisionTimer && !isPublicationRetryTimer &&
          isUnmarkedLegacyEvictionTimer(timestamp)
      val isEvictionTimer = isTrackedEvictionTimer || isLegacyEvictionTimer
      evictionTimerFired = isEvictionTimer

      repairStaleBufferedWriteTimerIfNeeded(timerService, currentProcessingTime, currentTimerCallback)
      if (!isPublicationRetryTimer) {
        expiredPublicationRetryMarkerCleared =
          clearExpiredPublicationRetryMarker(currentProcessingTime, currentTimerCallback)
      }

      if (
        !isEvictionTimer && !isBufferedWriteTimer && !isVersionCollisionTimer && !isPublicationRetryTimer &&
        !expiredPublicationRetryMarkerCleared
      ) return

      // The physical retry timer has fired. Clear only its logical role before running all
      // coincident roles, then arm one watermark-driven wake-up if publication remains fenced.
      if (isPublicationRetryTimer) clearPublicationRetryMarker()
      val eventTimeWatermark = timerService.currentWatermark()
      val mode = currentClockMode(currentProcessingTime, eventTimeWatermark)
      val timerHorizons = ChrononClockMode.processingTimerHorizons(mode,
                                                                   currentProcessingTime,
                                                                   eventTimeWatermark,
                                                                   processor.minSmallWindowTileSize)

      var timerResult: Option[GigaEmitResult] = None
      var rebuiltPendingPublication = false
      if (isEvictionTimer) {
        nextProcessingEvictionTimerState.clear()
        val result = mode match {
          case NoWatermark =>
            // The active batch input holds the connected watermark at MIN while it scans.
            // Keep consuming records, but do not age or roll keyed state from wall clock.
            val snapshot = processor.currentSnapshot
            if (!snapshot.isEmpty) pendingPublicationState.update(java.lang.Boolean.TRUE)
            GigaEmitResult(null, needsEvictionTimer = snapshot.needsEvictionTimer, isEmpty = snapshot.isEmpty)
          case _ =>
            val horizons = timerHorizons.get
            val rolloverResult = advanceDayForMode(mode, currentProcessingTime, eventTimeWatermark)
            val pendingRebuild = rebuildPendingAtCurrentTime(mode, horizons, currentProcessingTime)
            rebuiltPendingPublication = pendingRebuild.nonEmpty
            val evictionResult = pendingRebuild.getOrElse(
              processor.onEviction(EvictionTimes(timerTs = horizons.largeWindowAsOfMillis,
                                                 smallWindowAsOfTs = horizons.smallWindowAsOfMillis)))
            preferLatestResult(evictionResult, rolloverResult)
        }
        timerResult = Some(result)

        // Keep normal decay live while state or a deferred publication remains. A coincident
        // collision flush rearms itself on failure, so it does not need a speculative timer.
        if (!result.isEmpty || result.needsEvictionTimer || hasPendingPublication || hasBufferedWrite) {
          scheduleProcessingEvictionTimerIfNeeded(timerService, currentProcessingTime)
        }
      }

      if (isPublicationRetryTimer && !isEvictionTimer && hasDeferredPublication) {
        timerHorizons.foreach { horizons =>
          val pendingRebuild = rebuildPendingAtCurrentTime(mode, horizons, currentProcessingTime)
          rebuiltPendingPublication = pendingRebuild.nonEmpty
          timerResult = Some(pendingRebuild.getOrElse(GigaEmitResult(null)))
        }
      }

      timerResult.foreach { result =>
        timerHorizons.foreach { horizons =>
          emitOrDefer(mode,
                      horizons,
                      result,
                      ctx.getCurrentKey,
                      currentProcessingTime,
                      System.currentTimeMillis(),
                      timerService,
                      out,
                      scheduleWatermarkRetry = false)
        }
      }

      if (hasPendingPublication) {
        scheduleProcessingEvictionTimerIfNeeded(timerService, currentProcessingTime)
      }

      if (isBufferedWriteTimer) {
        timerHorizons match {
          case Some(horizons) if shouldPublish(mode, horizons, currentProcessingTime) =>
            if (!rebuiltPendingPublication && hasDeferredPublication) {
              rebuildPendingAtCurrentTime(mode, horizons, currentProcessingTime).foreach { rebuildResult =>
                emitOrDefer(mode,
                            horizons,
                            rebuildResult,
                            ctx.getCurrentKey,
                            currentProcessingTime,
                            System.currentTimeMillis(),
                            timerService,
                            out,
                            scheduleWatermarkRetry = false)
              }
            }
            flushBufferedWrite(timerService, currentProcessingTime, ctx.getCurrentKey, out)
          case _ =>
            // Park cadence ownership while fenced. The watermark/future-time wakeup is the
            // primary resume path; normal eviction remains a state-decay fallback.
            parkBufferedWrite()
            if (hasPendingPublication) {
              scheduleProcessingEvictionTimerIfNeeded(timerService, currentProcessingTime)
            }
        }
      }

      // Version collision is always last so a shared callback emits only the post-mutation,
      // post-cadence snapshot with a strictly newer version.
      if (isVersionCollisionTimer) {
        timerHorizons match {
          case Some(horizons) if shouldPublish(mode, horizons, currentProcessingTime) =>
            // Eviction runs first when both markers share a timestamp, so this is one
            // post-eviction snapshot with a strictly newer version.
            pendingPublicationState.update(java.lang.Boolean.TRUE)
            flushVersionCollision(timerService, currentProcessingTime, currentTimerCallback, ctx.getCurrentKey, out)
          case _ =>
            pendingPublicationState.update(java.lang.Boolean.TRUE)
            scheduleProcessingEvictionTimerIfNeeded(timerService, currentProcessingTime)
            postponeVersionCollision(timerService, currentProcessingTime, currentTimerCallback)
        }
      }

      if (
        (isPublicationRetryTimer || expiredPublicationRetryMarkerCleared || isBufferedWriteTimer) &&
        hasDeferredPublication
      ) {
        scheduleProcessingEvictionTimerIfNeeded(timerService, currentProcessingTime)
        scheduleFencedPublicationWakeupIfNeeded(timerService, currentProcessingTime, eventTimeWatermark, mode)
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
          val trackedBufferedWrite = nextBufferedWriteTimerState.value()
          if (
            bufferedWriteTimerFired && hasBufferedWrite &&
            (trackedBufferedWrite == null || trackedBufferedWrite.longValue() <= timestamp)
          ) {
            nextBufferedWriteTimerState.clear()
            scheduleBufferedWriteTimerIfNeeded(timerServiceOnFailure, processingTimeOnFailure)
          }
          val pendingCollision = pendingVersionCollisionState.value()
          if (
            versionCollisionTimerFired && pendingCollision != null &&
            pendingCollision.longValue() <= timestamp && processingTimeOnFailure < Long.MaxValue
          ) {
            scheduleProcessingEvictionTimerIfNeeded(timerServiceOnFailure, processingTimeOnFailure)
            postponeVersionCollision(timerServiceOnFailure,
                                     processingTimeOnFailure,
                                     Some(TimeDomain.PROCESSING_TIME -> timestamp))
          }
          if (
            (publicationRetryTimerFired || expiredPublicationRetryMarkerCleared || bufferedWriteTimerFired) &&
            hasDeferredPublication
          ) {
            val eventTimeWatermark = timerServiceOnFailure.currentWatermark()
            currentClockMode(processingTimeOnFailure, eventTimeWatermark) match {
              case Live => schedulePublicationRetryIfNeeded(timerServiceOnFailure, processingTimeOnFailure)
              case mode =>
                scheduleFencedPublicationWakeupIfNeeded(timerServiceOnFailure,
                                                        processingTimeOnFailure,
                                                        eventTimeWatermark,
                                                        mode)
            }
          }
          if (publicationActivationTimerFired && hasDeferredPublication) {
            schedulePublicationRetryAfterIfNeeded(timerServiceOnFailure,
                                                  processingTimeOnFailure,
                                                  activationCooldownDelayMillis)
          }
        }
        logger.error(s"Error in giga tile eviction for groupBy=${groupBy.getMetaData.getName}", e)
        eventProcessingErrorCounter.inc()
    }
  }

  private def isCurrentProcessingEvictionTimer(timestamp: Long): Boolean =
    Option(nextProcessingEvictionTimerState.value()).exists(_.longValue() == timestamp)

  private def isUnmarkedLegacyEvictionTimer(timestamp: Long): Boolean = {
    val interval = processor.minEvictionInterval
    nextProcessingEvictionTimerState.value() == null && interval > 0L && timestamp == TsUtils.round(timestamp, interval)
  }

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
  * between batchEndDay and the current materialized day.
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
  private var cachedSmallWindowAsOfTsState: ValueState[java.lang.Long] = _
  private var lastLargeRecomputeAsOfTsState: ValueState[java.lang.Long] = _

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
                     runningLarge: ValueState[Array[Byte]],
                     cachedSmallWindowAsOfTs: ValueState[java.lang.Long],
                     lastLargeRecomputeAsOfTs: ValueState[java.lang.Long]): Unit = {
    tileState = tiles
    megaTileIrState = megaTileIr
    dailyLargeIrState = dailyLargeIr
    currentDayStartState = dayStart
    earliestTileStartState = earliest
    batchIrState = batchIr
    batchEndTsState = batchEndTs
    runningLargeIrState = runningLarge
    cachedSmallWindowAsOfTsState = cachedSmallWindowAsOfTs
    lastLargeRecomputeAsOfTsState = lastLargeRecomputeAsOfTs
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

  override def getCachedSmallWindowAsOfTs: Long =
    Option(cachedSmallWindowAsOfTsState.value()).map(_.longValue()).getOrElse(-1L)
  override def putCachedSmallWindowAsOfTs(ts: Long): Unit = cachedSmallWindowAsOfTsState.update(ts)

  override def getLastLargeRecomputeAsOfTs: Long =
    Option(lastLargeRecomputeAsOfTsState.value()).map(_.longValue()).getOrElse(Long.MinValue)
  override def putLastLargeRecomputeAsOfTs(ts: Long): Unit = lastLargeRecomputeAsOfTsState.update(ts)

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
