package ai.chronon.flink.test.window

import ai.chronon.aggregator.windowing.{
  EvictionTimes,
  GigaEmitResult,
  GigaTileStreamProcessor,
  InMemoryGigaTileStore,
  MegaTileAggregator,
  SawtoothOnlineAggregator
}
import ai.chronon.api._
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.flink.FlinkJob
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.types.{BatchIrRow, TimestampedTile}
import ai.chronon.flink.window.GigaTileProcessFunction
import ai.chronon.online.{GigaTileCodec, MegaTileCodec}
import ai.chronon.online.serde.{ArrayRow, AvroCodec}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.KeyedStateBackend
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness
import org.apache.flink.util.Collector
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util
import scala.collection.JavaConverters._

class GigaTileProcessFunctionTest extends AnyFlatSpec with Matchers {
  import GigaTileProcessFunctionTest._

  "GigaTileProcessFunction" should "use processing time for current-value versions and eviction timers" in {
    val testHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val retainedLateEventTs = processingTs - 30 * 60 * 1000L
    val carriedProcessingTs = processingTs - 10 * 60 * 1000L

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      advanceToHealthyLiveWatermark(testHarness, processingTs)
      testHarness.processElement1(
        ProjectedEvent(Map(Constants.TimeColumn -> retainedLateEventTs, "num" -> 5L), carriedProcessingTs),
        retainedLateEventTs)

      val output = testHarness.extractOutputValues().get(0)
      decodeSum(output) shouldEqual 5L
      output.latestTsMillis shouldEqual processingTs
      testHarness.numProcessingTimeTimers() shouldEqual 1
      testHarness.numEventTimeTimers() shouldEqual 0
    } finally testHarness.close()
  }

  it should "evict small-window state on processing time" in {
    val testHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val windowMillis = new Window(1, TimeUnit.HOURS).millis
    val justBeforeExpiry = eventTs + windowMillis - 1L
    val afterExpiry = TsUtils.round(justBeforeExpiry, new Window(5, TimeUnit.MINUTES).millis) +
      new Window(5, TimeUnit.MINUTES).millis

    try {
      testHarness.open()
      testHarness.setProcessingTime(justBeforeExpiry)
      advanceToHealthyLiveWatermark(testHarness, justBeforeExpiry)
      testHarness.processElement1(event(eventTs, 5L), eventTs)
      advanceToHealthyLiveWatermark(testHarness, afterExpiry)
      testHarness.setProcessingTime(afterExpiry)

      val outputs = testHarness.extractOutputValues()
      val evictionOutput = outputs.get(outputs.size() - 1)
      decodeSum(evictionOutput) shouldBe null
      evictionOutput.latestTsMillis shouldEqual afterExpiry
      testHarness.numProcessingTimeTimers() shouldEqual 0
    } finally testHarness.close()
  }

  it should "roll the current day on processing time while retaining an eligible late event" in {
    val testHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    val beforeMidnightProcessingTs = dayMillis - 60 * 1000L
    val afterMidnightProcessingTs = dayMillis + 60 * 1000L
    val initialEventTs = dayMillis - 30 * 60 * 1000L
    val delayedEventTs = dayMillis - 10 * 60 * 1000L

    try {
      testHarness.open()
      testHarness.setProcessingTime(beforeMidnightProcessingTs)
      advanceToHealthyLiveWatermark(testHarness, beforeMidnightProcessingTs)
      testHarness.processElement1(event(initialEventTs, 5L), initialEventTs)
      advanceToHealthyLiveWatermark(testHarness, afterMidnightProcessingTs)
      testHarness.setProcessingTime(afterMidnightProcessingTs)
      // Zipline suppresses redundant timer writes. Crossing midnight must still leave the
      // retained pre-midnight value available to the next event.
      testHarness.extractOutputValues().size() shouldEqual 1

      testHarness.processElement1(event(delayedEventTs, 7L), delayedEventTs)
      val outputs = testHarness.extractOutputValues()
      val delayedEventOutput = outputs.get(outputs.size() - 1)
      decodeSum(delayedEventOutput) shouldEqual 12L
      delayedEventOutput.latestTsMillis shouldEqual afterMidnightProcessingTs
    } finally testHarness.close()
  }

  it should "use processing time for batch refresh versions" in {
    val batchGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "gigatile-process-function-batch-test"),
      aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(7, TimeUnit.DAYS)))))
    val testHarness = harness(new GigaTileProcessFunction(batchGroupBy, inputSchema))
    val batchCodec = new GigaTileCodec(batchGroupBy, inputSchema)
    val batchEndTs = TsUtils.round(eventTs, new Window(1, TimeUnit.DAYS).millis)
    val batchEventTs = batchEndTs - 60 * 1000L
    val batchAggregator =
      new SawtoothOnlineAggregator(batchEndTs, batchGroupBy.getAggregations.asScala.toSeq, inputSchema)
    val batchIr = batchAggregator.update(
      batchAggregator.init,
      new ArrayRow(Array[Any](batchEventTs, 7L), batchEventTs))
    val finalBatchIr = batchAggregator.denormalizeBatchIr(batchAggregator.normalizeBatchIr(batchIr))

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      advanceToHealthyLiveWatermark(testHarness, processingTs)
      testHarness.processElement2(
        new BatchIrRow(entityKey(), batchCodec.encodeBatchIr(finalBatchIr), batchEndTs),
        batchEndTs)

      val output = testHarness.extractOutputValues().get(0)
      decodeSum(output, batchGroupBy) shouldEqual 7L
      output.latestTsMillis shouldEqual processingTs
    } finally testHarness.close()
  }

  it should "schedule decay when a batch refresh leaves the current value unchanged" in {
    val batchGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "gigatile-process-function-empty-batch-test"),
      aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(7, TimeUnit.DAYS)))))
    val testHarness = harness(new GigaTileProcessFunction(batchGroupBy, inputSchema))
    val batchCodec = new GigaTileCodec(batchGroupBy, inputSchema)
    val batchEndTs = TsUtils.round(eventTs, new Window(1, TimeUnit.DAYS).millis)
    val batchAggregator =
      new SawtoothOnlineAggregator(batchEndTs, batchGroupBy.getAggregations.asScala.toSeq, inputSchema)
    val emptyBatchIr = batchAggregator.denormalizeBatchIr(batchAggregator.normalizeBatchIr(batchAggregator.init))

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      advanceToHealthyLiveWatermark(testHarness, processingTs)
      testHarness.processElement2(
        new BatchIrRow(entityKey(), batchCodec.encodeBatchIr(emptyBatchIr), batchEndTs),
        batchEndTs)

      testHarness.extractOutputValues().asScala shouldBe empty
      testHarness.numProcessingTimeTimers() shouldEqual 1
    } finally testHarness.close()
  }

  it should "keep an empty key scheduled until a deferred future batch activates" in {
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    val hourMillis = new Window(1, TimeUnit.HOURS).millis
    val batchGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "gigatile-process-function-future-batch-test"),
      aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(7, TimeUnit.DAYS)))))
    val testHarness = harness(new GigaTileProcessFunction(batchGroupBy, inputSchema))
    val batchCodec = new GigaTileCodec(batchGroupBy, inputSchema)
    val currentBatchEnd = 10 * dayMillis
    val currentProcessingTs = currentBatchEnd + hourMillis
    val futureBatchEnd = currentBatchEnd + dayMillis
    val futureEventTs = futureBatchEnd - hourMillis
    val batchAggregator =
      new SawtoothOnlineAggregator(futureBatchEnd, batchGroupBy.getAggregations.asScala.toSeq, inputSchema)
    val futureBatchIr = batchAggregator.update(
      batchAggregator.init,
      new ArrayRow(Array[Any](futureEventTs, 50L), futureEventTs))
    val futureBatchRow = new BatchIrRow(
      entityKey(),
      batchCodec.encodeBatchIr(batchAggregator.denormalizeBatchIr(batchAggregator.normalizeBatchIr(futureBatchIr))),
      futureBatchEnd)

    try {
      testHarness.open()
      testHarness.setProcessingTime(currentProcessingTs)
      testHarness.processElement2(emptyBatchRow(batchGroupBy, currentBatchEnd), currentBatchEnd)
      testHarness.processElement2(futureBatchRow, futureBatchEnd)

      testHarness.extractOutputValues().asScala shouldBe empty
      // A short watermark retry and the normal eviction timer are both live initially.
      testHarness.numProcessingTimeTimers() shouldEqual 2

      testHarness.setProcessingTime(currentProcessingTs + hourMillis)

      testHarness.extractOutputValues().asScala shouldBe empty
      testHarness.numProcessingTimeTimers() shouldEqual 1

      advanceToHealthyLiveWatermark(testHarness, futureBatchEnd + hourMillis)
      testHarness.setProcessingTime(futureBatchEnd + hourMillis)

      val outputs = testHarness.extractOutputValues()
      outputs.size() shouldEqual 1
      decodeSum(outputs.get(0), batchGroupBy) shouldEqual 50L
      outputs.get(0).latestTsMillis shouldEqual futureBatchEnd + hourMillis
    } finally testHarness.close()
  }

  it should "drop a future event before it can poison batch-backed large-window state" in {
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    val batchGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "gigatile-process-function-future-event-test"),
      aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(7, TimeUnit.DAYS)))))
    val testHarness = harness(new GigaTileProcessFunction(batchGroupBy, inputSchema))
    val currentProcessingTime = 10 * dayMillis + new Window(1, TimeUnit.HOURS).millis
    val batchCallbackTime = currentProcessingTime - 2L
    val batchEnd = 9 * dayMillis

    try {
      testHarness.open()
      testHarness.setProcessingTime(batchCallbackTime)
      advanceToHealthyLiveWatermark(testHarness, batchCallbackTime)
      testHarness.processElement2(batchRow(batchGroupBy, batchEnd, batchEnd - 1000L, 3L), batchEnd)
      decodeSum(testHarness.extractOutputValues().get(0), batchGroupBy) shouldEqual 3L

      testHarness.setProcessingTime(currentProcessingTime)
      advanceToHealthyLiveWatermark(testHarness, currentProcessingTime)
      testHarness.processElement1(event(currentProcessingTime + 1L, 11L), currentProcessingTime + 1L)
      testHarness.processElement1(event(currentProcessingTime - 1000L, 5L), currentProcessingTime - 1000L)

      val outputs = testHarness.extractOutputValues()
      outputs.size() shouldEqual 2
      decodeSum(outputs.get(1), batchGroupBy) shouldEqual 8L
      outputs.get(1).latestTsMillis shouldEqual currentProcessingTime
    } finally testHarness.close()
  }

  it should "stay fenced while a source watermark implies a future event" in {
    val testHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val futureEventTime = processingTs + 1L
    val historicalEventTime = processingTs - 1000L
    val poisonedWatermark = processingTs - FlinkJob.AllowedOutOfOrderness.toMillis
    val retryTime = processingTs + 2L * FlinkJob.AutoWatermarkInterval

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      testHarness.processWatermarkStatus2(WatermarkStatus.IDLE)
      testHarness.processElement1(event(futureEventTime, 11L), futureEventTime)
      testHarness.processWatermark1(new Watermark(poisonedWatermark))
      testHarness.processElement1(event(historicalEventTime, 5L), historicalEventTime)

      testHarness.extractOutputValues() shouldBe empty
      testHarness.setProcessingTime(retryTime)

      val outputs = testHarness.extractOutputValues()
      outputs should have size 1
      decodeSum(outputs.get(0)) shouldEqual 5L
      outputs.get(0).latestTsMillis shouldEqual retryTime
    } finally testHarness.close()
  }

  it should "wait once at the exact safe boundary for a finite future watermark" in {
    val function = new GigaTileProcessFunction(groupBy, inputSchema)
    val testHarness = harness(function)
    val futureWatermark = processingTs + 1000L
    val shortRetryTime = processingTs + 2L * FlinkJob.AutoWatermarkInterval
    val safeProcessingTime = futureWatermark + FlinkJob.AllowedOutOfOrderness.toMillis + 1L
    val eventTime = processingTs - 1000L

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      advanceToLiveWatermark(testHarness, futureWatermark)
      testHarness.processElement1(event(eventTime, 5L), eventTime)

      testHarness.extractOutputValues() shouldBe empty
      testHarness.setProcessingTime(shortRetryTime)
      publicationRetryTimestamp(function) shouldEqual safeProcessingTime

      testHarness.setProcessingTime(shortRetryTime + FlinkJob.IdlenessTimeout.toMillis)
      testHarness.extractOutputValues() shouldBe empty
      publicationRetryTimestamp(function) shouldEqual safeProcessingTime

      testHarness.setProcessingTime(safeProcessingTime)
      val outputs = testHarness.extractOutputValues()
      outputs should have size 1
      decodeSum(outputs.get(0)) shouldEqual 5L
      outputs.get(0).latestTsMillis shouldEqual safeProcessingTime
    } finally testHarness.close()
  }

  it should "migrate restored legacy event-time timers without publishing twice" in {
    val firstLegacyTimer = eventTs + 1000L
    val secondLegacyTimer = firstLegacyTimer + 1000L
    val originalHarness = harness(new LegacyEventTimeTimerFunction(Seq(firstLegacyTimer, secondLegacyTimer)))
    originalHarness.open()
    originalHarness.processElement1(event(eventTs, 1L), eventTs)
    originalHarness.numEventTimeTimers() shouldEqual 2
    val snapshot = originalHarness.snapshot(7L, processingTs)
    originalHarness.close()

    val restoredHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(snapshot)
      restoredHarness.open()
      restoredHarness.setProcessingTime(processingTs)
      restoredHarness.processWatermark1(new Watermark(secondLegacyTimer))
      restoredHarness.processWatermark2(new Watermark(secondLegacyTimer))

      restoredHarness.extractOutputValues().asScala shouldBe empty
      restoredHarness.numEventTimeTimers() shouldEqual 0
      restoredHarness.numProcessingTimeTimers() shouldEqual 1
    } finally restoredHarness.close()
  }

  it should "advance only by event time while the batch input holds the watermark at MIN" in {
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    val currentProcessingTime = 10 * dayMillis + new Window(1, TimeUnit.HOURS).millis
    val firstEventTime = dayMillis + new Window(1, TimeUnit.HOURS).millis
    val laterEventTime = 4 * dayMillis + new Window(1, TimeUnit.HOURS).millis
    val firstFutureEventTime = currentProcessingTime + 1L
    val farFutureEventTime = currentProcessingTime + 2 * dayMillis
    val function = new GigaTileProcessFunction(groupBy, inputSchema)
    val testHarness = harness(function)

    try {
      testHarness.open()
      testHarness.setProcessingTime(currentProcessingTime)
      // Input 2 remains active, so this input-1 watermark does not initialize the connected one.
      testHarness.processWatermark1(new Watermark(currentProcessingTime))
      testHarness.processElement1(event(firstEventTime, 5L), firstEventTime)
      testHarness.processElement1(event(laterEventTime, 7L), laterEventTime)

      testHarness.extractOutputValues() shouldBe empty
      currentFinalizedSum(function) shouldEqual 7L
      currentDayStart(function) shouldEqual 4 * dayMillis

      // Any future event is rejected before it can mutate either small- or large-window state.
      testHarness.processElement1(event(firstFutureEventTime, 11L), firstFutureEventTime)
      testHarness.processElement1(event(farFutureEventTime, 11L), farFutureEventTime)
      currentFinalizedSum(function) shouldEqual 7L
      testHarness.setProcessingTime(nextEvictionHop(currentProcessingTime))
      testHarness.extractOutputValues() shouldBe empty
      currentDayStart(function) shouldEqual 4 * dayMillis
      testHarness.numProcessingTimeTimers() shouldEqual 1
    } finally testHarness.close()
  }

  it should "remain fail-closed when both inputs idle before any finite watermark" in {
    val batchGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "gigatile-process-function-idle-bootstrap-test"),
      aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(7, TimeUnit.DAYS)))))
    val function = new GigaTileProcessFunction(batchGroupBy, inputSchema)
    val testHarness = harness(function)
    val batchEnd = TsUtils.round(eventTs, new Window(1, TimeUnit.DAYS).millis)

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      testHarness.processElement2(batchRow(batchGroupBy, batchEnd, eventTs - 1000L, 3L), batchEnd)
      testHarness.processWatermarkStatus1(WatermarkStatus.IDLE)
      testHarness.processWatermarkStatus2(WatermarkStatus.IDLE)
      testHarness.setProcessingTime(nextEvictionHop(processingTs))

      // Marking both inputs idle does not synthesize a finite watermark. Without a previously
      // observed finite event watermark, retain state rather than publish a partial row.
      testHarness.extractOutputValues() shouldBe empty
      testHarness.numProcessingTimeTimers() shouldEqual 1
    } finally testHarness.close()
  }

  it should "bound finite batch-backed state during a long replay with an uninitialized watermark" in {
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    val replayGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "gigatile-process-function-long-replay-test"),
      aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(7, TimeUnit.DAYS)))))
    val function = new GigaTileProcessFunction(replayGroupBy, inputSchema)
    val testHarness = harness(function)
    // Keep the serving horizon day-aligned so the daily large-IR boundary is exact.
    val currentProcessingTime = 41 * dayMillis

    try {
      testHarness.open()
      testHarness.setProcessingTime(currentProcessingTime)
      testHarness.processElement2(emptyBatchRow(replayGroupBy, dayMillis), dayMillis)
      (1 to 40).foreach { day =>
        val eventTime = day * dayMillis + new Window(1, TimeUnit.HOURS).millis
        testHarness.processElement1(event(eventTime, 1L), eventTime)
      }

      testHarness.extractOutputValues() shouldBe empty
      currentDayStart(function) shouldEqual 40 * dayMillis
      dailyLargeSlotCount(function) should be <= 8

      val liveTimer = nextEvictionHop(currentProcessingTime)
      advanceToHealthyLiveWatermark(testHarness, liveTimer)
      testHarness.setProcessingTime(liveTimer)
      val outputs = testHarness.extractOutputValues()
      outputs should not be empty
      decodeSum(outputs.get(outputs.size() - 1), replayGroupBy) shouldEqual 7L
    } finally testHarness.close()
  }

  it should "use source watermark readiness across five-minute, hourly, and daily aggregation hops" in {
    val hourMillis = new Window(1, TimeUnit.HOURS).millis
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    val staleWatermark = processingTs - 30 * 60 * 1000L
    val liveWatermark = processingTs - FlinkJob.LiveWatermarkLagToleranceMillis
    val cases = Seq(
      ("five-minute", new Window(1, TimeUnit.HOURS), 5 * 60 * 1000L, false),
      ("hourly", new Window(7, TimeUnit.DAYS), hourMillis, true),
      ("daily", new Window(30, TimeUnit.DAYS), dayMillis, true)
    )

    cases.foreach { case (name, window, expectedHopMillis, needsBatch) =>
      val testGroupBy = Builders.GroupBy(metaData = Builders.MetaData(name = s"gigatile-source-readiness-$name-test"),
                                         aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window))))
      val function = new GigaTileProcessFunction(testGroupBy, inputSchema)
      val testHarness = harness(function)

      try {
        testHarness.open()
        testHarness.setProcessingTime(processingTs)
        currentProcessor(function).minSmallWindowTileSize shouldEqual expectedHopMillis
        if (needsBatch) {
          testHarness.processElement2(batchRow(testGroupBy, 0L, -1000L, 3L), 0L)
        }
        advanceToLiveWatermark(testHarness, staleWatermark)
        testHarness.processElement1(event(staleWatermark - 1000L, 5L), staleWatermark - 1000L)

        withClue(s"$name aggregation hop: ") {
          testHarness.extractOutputValues() shouldBe empty
        }

        testHarness.processWatermark1(new Watermark(liveWatermark))
        testHarness.processElement1(event(liveWatermark, 7L), liveWatermark)

        val outputs = testHarness.extractOutputValues()
        withClue(s"$name aggregation hop: ") {
          outputs should have size 1
          decodeSum(outputs.get(0), testGroupBy) shouldEqual (if (needsBatch) 15L else 12L)
        }
      } finally testHarness.close()
    }
  }

  it should "keep a sparse key fenced until the global watermark is live" in {
    val testHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val staleWatermark = processingTs - 10 * 60 * 1000L
    val secondTimer = nextEvictionHop(nextEvictionHop(processingTs))

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      advanceToLiveWatermark(testHarness, staleWatermark)
      testHarness.processElement1(event(staleWatermark - 1000L, 5L), staleWatermark - 1000L)
      testHarness.setProcessingTime(secondTimer)
      testHarness.extractOutputValues() shouldBe empty
      testHarness.numProcessingTimeTimers() shouldEqual 1

      testHarness.processElement1(event(staleWatermark + 1000L, 7L), staleWatermark + 1000L)
      testHarness.extractOutputValues() shouldBe empty

      val liveTimer = nextEvictionHop(secondTimer)
      testHarness.processWatermark1(new Watermark(healthyLiveWatermark(liveTimer)))
      testHarness.setProcessingTime(liveTimer)
      val outputs = testHarness.extractOutputValues()
      outputs.size() shouldEqual 1
      decodeSum(outputs.get(0)) shouldEqual 12L
      outputs.get(0).latestTsMillis shouldEqual liveTimer
    } finally testHarness.close()
  }

  it should "publish a lone event after its periodic watermark catches up" in {
    val testHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val eventTime = processingTs - 1000L
    val retryTime = processingTs + 2L * FlinkJob.AutoWatermarkInterval

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      testHarness.processWatermarkStatus2(WatermarkStatus.IDLE)
      testHarness.processElement1(event(eventTime, 5L), eventTime)

      testHarness.extractOutputValues() shouldBe empty
      testHarness.numProcessingTimeTimers() shouldEqual 2

      testHarness.setProcessingTime(retryTime)
      testHarness.extractOutputValues() shouldBe empty
      testHarness.numEventTimeTimers() shouldEqual 1

      val periodicWatermark = healthyLiveWatermark(processingTs)
      testHarness.processWatermark1(new Watermark(periodicWatermark))

      val outputs = testHarness.extractOutputValues()
      outputs.size() shouldEqual 1
      decodeSum(outputs.get(0)) shouldEqual 5L
      outputs.get(0).latestTsMillis shouldEqual retryTime
      testHarness.numProcessingTimeTimers() shouldEqual 1
    } finally testHarness.close()
  }

  it should "park a catch-up publication on one event-time activation without hot retries" in {
    val testHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val staleWatermark = processingTs - new Window(1, TimeUnit.HOURS).millis
    val retryTime = processingTs + 2L * FlinkJob.AutoWatermarkInterval

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      advanceToLiveWatermark(testHarness, staleWatermark)
      testHarness.processElement1(event(staleWatermark - 1000L, 5L), staleWatermark - 1000L)

      testHarness.numProcessingTimeTimers() shouldEqual 2
      testHarness.setProcessingTime(retryTime)

      testHarness.extractOutputValues() shouldBe empty
      testHarness.numProcessingTimeTimers() shouldEqual 1
      testHarness.numEventTimeTimers() shouldEqual 1
      testHarness.processElement1(event(staleWatermark + 1000L, 7L), staleWatermark + 1000L)
      testHarness.setProcessingTime(retryTime + 5L * FlinkJob.AutoWatermarkInterval)
      testHarness.extractOutputValues() shouldBe empty
      testHarness.numProcessingTimeTimers() shouldEqual 1
      testHarness.numEventTimeTimers() shouldEqual 1
    } finally testHarness.close()
  }

  it should "publish a daily sparse key after watermark-only activation" in {
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    val dailyGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "gigatile-daily-sparse-activation-test"),
      aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(30, TimeUnit.DAYS)))))
    val function = new GigaTileProcessFunction(dailyGroupBy, inputSchema)
    val testHarness = harness(function)
    val staleWatermark = processingTs - new Window(1, TimeUnit.HOURS).millis
    val retryDelay = 2L * FlinkJob.AutoWatermarkInterval
    val retryTime = processingTs + retryDelay

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      testHarness.processElement2(batchRow(dailyGroupBy, 0L, -1000L, 3L), 0L)
      advanceToLiveWatermark(testHarness, staleWatermark)
      testHarness.processElement1(event(staleWatermark - 1000L, 5L), staleWatermark - 1000L)
      currentProcessor(function).minEvictionInterval shouldEqual dayMillis

      testHarness.setProcessingTime(retryTime)
      testHarness.extractOutputValues() shouldBe empty
      testHarness.numEventTimeTimers() shouldEqual 1

      testHarness.processWatermark1(new Watermark(healthyLiveWatermark(retryTime)))

      val outputs = testHarness.extractOutputValues()
      outputs should have size 1
      decodeSum(outputs.get(0), dailyGroupBy) shouldEqual 8L
      outputs.get(0).latestTsMillis shouldEqual retryTime
      retryTime should be < nextDay(processingTs)
    } finally testHarness.close()
  }

  it should "advance a catch-up day before publishing a batch-ahead sparse key" in {
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    val dailyGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "gigatile-batch-ahead-activation-test"),
      aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(30, TimeUnit.DAYS)))))
    val function = new GigaTileProcessFunction(dailyGroupBy, inputSchema)
    val testHarness = harness(function)
    val currentProcessingTime = 10L * dayMillis + new Window(1, TimeUnit.HOURS).millis
    val staleWatermark = 8L * dayMillis + new Window(1, TimeUnit.HOURS).millis
    val batchEnd = 9L * dayMillis
    val retryTime = currentProcessingTime + 2L * FlinkJob.AutoWatermarkInterval

    try {
      testHarness.open()
      testHarness.setProcessingTime(currentProcessingTime)
      advanceToLiveWatermark(testHarness, staleWatermark)
      testHarness.processElement1(event(staleWatermark - 1000L, 5L), staleWatermark - 1000L)
      testHarness.processElement2(batchRow(dailyGroupBy, batchEnd, batchEnd - 1000L, 3L), batchEnd)

      currentDayStart(function) should be < batchEnd
      testHarness.extractOutputValues() shouldBe empty
      testHarness.setProcessingTime(retryTime)
      testHarness.numEventTimeTimers() shouldEqual 1

      testHarness.processWatermark1(new Watermark(liveActivationWatermark(retryTime)))

      val outputs = testHarness.extractOutputValues()
      outputs should have size 1
      decodeSum(outputs.get(0), dailyGroupBy) shouldEqual 3L
      currentDayStart(function) should be >= batchEnd
    } finally testHarness.close()
  }

  it should "publish at the exact lower Live watermark boundary" in {
    val testHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val staleWatermark = processingTs - new Window(1, TimeUnit.HOURS).millis
    val retryTime = processingTs + 2L * FlinkJob.AutoWatermarkInterval

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      advanceToLiveWatermark(testHarness, staleWatermark)
      testHarness.processElement1(event(staleWatermark - 1000L, 5L), staleWatermark - 1000L)
      testHarness.setProcessingTime(retryTime)

      testHarness.processWatermark1(new Watermark(liveActivationWatermark(retryTime)))

      val outputs = testHarness.extractOutputValues()
      outputs should have size 1
      decodeSum(outputs.get(0)) shouldEqual 5L
      outputs.get(0).latestTsMillis shouldEqual retryTime
    } finally testHarness.close()
  }

  it should "move activation forward when processing time consumes the live slack" in {
    val testHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val staleWatermark = processingTs - new Window(1, TimeUnit.HOURS).millis
    val retryDelay = 2L * FlinkJob.AutoWatermarkInterval
    val retryTime = processingTs + retryDelay
    val firstActivationWatermark = liveActivationWatermark(retryTime)
    val delayedProcessingTime = retryTime + FlinkJob.CatchupWatermarkLagSlackMillis + 1L
    val cooldownDelay = math.max(retryDelay, FlinkJob.CatchupWatermarkLagSlackMillis / 2L)
    val cooldownTime = delayedProcessingTime + cooldownDelay

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      advanceToLiveWatermark(testHarness, staleWatermark)
      testHarness.processElement1(event(staleWatermark - 1000L, 5L), staleWatermark - 1000L)
      testHarness.setProcessingTime(retryTime)

      testHarness.setProcessingTime(delayedProcessingTime)
      testHarness.processWatermark1(new Watermark(firstActivationWatermark))
      testHarness.extractOutputValues() shouldBe empty
      testHarness.numEventTimeTimers() shouldEqual 0

      testHarness.setProcessingTime(cooldownTime)
      testHarness.extractOutputValues() shouldBe empty
      testHarness.numEventTimeTimers() shouldEqual 1

      testHarness.processWatermark1(new Watermark(healthyLiveWatermark(cooldownTime)))

      val outputs = testHarness.extractOutputValues()
      outputs should have size 1
      decodeSum(outputs.get(0)) shouldEqual 5L
    } finally testHarness.close()
  }

  it should "preserve an eviction timer that shares a cleared publication-retry timestamp" in {
    val testHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val retryDelay = 2L * FlinkJob.AutoWatermarkInterval
    val evictionTime = nextEvictionHop(processingTs)
    val firstProcessingTime = evictionTime - retryDelay
    val firstEventTime = firstProcessingTime - 1000L

    try {
      testHarness.open()
      testHarness.setProcessingTime(firstProcessingTime)
      testHarness.processWatermarkStatus2(WatermarkStatus.IDLE)
      testHarness.processElement1(event(firstEventTime, 5L), firstEventTime)
      // The retry and normal eviction markers share one physical Flink timer.
      testHarness.numProcessingTimeTimers() shouldEqual 1

      testHarness.setProcessingTime(evictionTime)
      testHarness.extractOutputValues() shouldBe empty
      // Both roles fired: normal eviction remains scheduled and publication waits on watermark.
      testHarness.numProcessingTimeTimers() shouldEqual 1
      testHarness.numEventTimeTimers() shouldEqual 1
    } finally testHarness.close()
  }

  it should "preserve a pending publication retry while draining an earlier callback" in {
    val eventTime = processingTs - 1000L
    val injectedTimer = processingTs + 1L
    val retryTime = processingTs + 2L * FlinkJob.AutoWatermarkInterval
    injectedTimer should be < retryTime
    val originalHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))

    originalHarness.open()
    originalHarness.setProcessingTime(processingTs)
    originalHarness.processWatermarkStatus2(WatermarkStatus.IDLE)
    originalHarness.processElement1(event(eventTime, 5L), eventTime)
    val snapshot = originalHarness.snapshot(10L, processingTs)
    originalHarness.close()

    val injectorHarness = harness(new ProcessingTimerInjector(injectedTimer))
    injectorHarness.setup()
    injectorHarness.initializeState(snapshot)
    injectorHarness.open()
    injectorHarness.setProcessingTime(processingTs)
    injectorHarness.processElement1(event(eventTime, 0L), eventTime)
    val injectedSnapshot = injectorHarness.snapshot(11L, processingTs)
    injectorHarness.close()

    val restoredHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(injectedSnapshot)
      restoredHarness.open()
      restoredHarness.setProcessingTime(processingTs)
      // The injected callback runs first while Flink already reports retryTime. It must not
      // clear the still-queued publication retry marker.
      restoredHarness.numProcessingTimeTimers() shouldEqual 3
      advanceToHealthyLiveWatermark(restoredHarness, retryTime)
      restoredHarness.setProcessingTime(retryTime)

      val outputs = restoredHarness.extractOutputValues()
      outputs.size() shouldEqual 1
      decodeSum(outputs.get(0)) shouldEqual 5L
      outputs.get(0).latestTsMillis shouldEqual retryTime
      restoredHarness.numProcessingTimeTimers() shouldEqual 1
    } finally restoredHarness.close()
  }

  it should "restore a watermark activation without another element" in {
    val staleWatermark = processingTs - new Window(10, TimeUnit.MINUTES).millis
    val retryTime = processingTs + 2L * FlinkJob.AutoWatermarkInterval
    val originalHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))

    originalHarness.open()
    originalHarness.setProcessingTime(processingTs)
    advanceToLiveWatermark(originalHarness, staleWatermark)
    originalHarness.processElement1(event(staleWatermark - 1000L, 5L), staleWatermark - 1000L)
    originalHarness.setProcessingTime(retryTime)
    originalHarness.extractOutputValues() shouldBe empty
    originalHarness.numProcessingTimeTimers() shouldEqual 1
    originalHarness.numEventTimeTimers() shouldEqual 1
    val snapshot = originalHarness.snapshot(11L, retryTime)
    originalHarness.close()

    val restoredHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(snapshot)
      restoredHarness.open()
      restoredHarness.setProcessingTime(retryTime)
      advanceToHealthyLiveWatermark(restoredHarness, retryTime)
      val outputs = restoredHarness.extractOutputValues()
      outputs.size() shouldEqual 1
      decodeSum(outputs.get(0)) shouldEqual 5L
      outputs.get(0).latestTsMillis shouldEqual retryTime
    } finally restoredHarness.close()
  }

  it should "restore a delayed activation cooldown without another element" in {
    val staleWatermark = processingTs - new Window(10, TimeUnit.MINUTES).millis
    val retryDelay = 2L * FlinkJob.AutoWatermarkInterval
    val retryTime = processingTs + retryDelay
    val firstActivationWatermark = liveActivationWatermark(retryTime)
    val delayedProcessingTime = retryTime + FlinkJob.CatchupWatermarkLagSlackMillis + 1L
    val cooldownDelay = math.max(retryDelay, FlinkJob.CatchupWatermarkLagSlackMillis / 2L)
    val cooldownTime = delayedProcessingTime + cooldownDelay
    val originalHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))

    originalHarness.open()
    originalHarness.setProcessingTime(processingTs)
    advanceToLiveWatermark(originalHarness, staleWatermark)
    originalHarness.processElement1(event(staleWatermark - 1000L, 5L), staleWatermark - 1000L)
    originalHarness.setProcessingTime(retryTime)
    originalHarness.setProcessingTime(delayedProcessingTime)
    originalHarness.processWatermark1(new Watermark(firstActivationWatermark))
    originalHarness.extractOutputValues() shouldBe empty
    originalHarness.numEventTimeTimers() shouldEqual 0
    val snapshot = originalHarness.snapshot(12L, delayedProcessingTime)
    originalHarness.close()

    val restoredHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(snapshot)
      restoredHarness.open()
      restoredHarness.setProcessingTime(delayedProcessingTime)
      advanceToLiveWatermark(restoredHarness, firstActivationWatermark)
      restoredHarness.setProcessingTime(cooldownTime)

      restoredHarness.extractOutputValues() shouldBe empty
      restoredHarness.numEventTimeTimers() shouldEqual 1
      restoredHarness.processWatermark1(new Watermark(healthyLiveWatermark(cooldownTime)))

      val outputs = restoredHarness.extractOutputValues()
      outputs should have size 1
      decodeSum(outputs.get(0)) shouldEqual 5L
      outputs.get(0).latestTsMillis shouldEqual cooldownTime
    } finally restoredHarness.close()
  }

  it should "recover an expired retry marker on the next keyed callback" in {
    val staleWatermark = processingTs - new Window(10, TimeUnit.MINUTES).millis
    val retryDelay = 2L * FlinkJob.AutoWatermarkInterval
    val retryTime = processingTs + retryDelay
    val originalFunction = new GigaTileProcessFunction(groupBy, inputSchema)
    val originalHarness = harness(originalFunction)

    originalHarness.open()
    originalHarness.setProcessingTime(processingTs)
    advanceToLiveWatermark(originalHarness, staleWatermark)
    originalHarness.processElement1(event(staleWatermark - 1000L, 5L), staleWatermark - 1000L)
    originalHarness.setProcessingTime(retryTime)
    originalHarness.extractOutputValues() shouldBe empty
    setPublicationRetryTimestamp(originalFunction, retryTime)
    clearPublicationActivationTimestamp(originalFunction)
    val snapshot = originalHarness.snapshot(12L, retryTime)
    originalHarness.close()

    val restoredFunction = new GigaTileProcessFunction(groupBy, inputSchema)
    val restoredHarness = harness(restoredFunction)
    val restoredProcessingTime = retryTime + 1L
    val restoredRetryTime = restoredProcessingTime + retryDelay
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(snapshot)
      restoredHarness.open()
      restoredHarness.setProcessingTime(restoredProcessingTime)
      restoredHarness.processElement1(event(staleWatermark + 1000L, 7L), staleWatermark + 1000L)

      publicationRetryTimestamp(restoredFunction) shouldEqual restoredRetryTime
      advanceToHealthyLiveWatermark(restoredHarness, restoredRetryTime)
      restoredHarness.setProcessingTime(restoredRetryTime)

      val outputs = restoredHarness.extractOutputValues()
      outputs should have size 1
      decodeSum(outputs.get(0)) shouldEqual 12L
      outputs.get(0).latestTsMillis shouldEqual restoredRetryTime
    } finally restoredHarness.close()
  }

  it should "coalesce event and batch publications from the same processing millisecond" in {
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    val batchGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "gigatile-process-function-collision-batch-test"),
      aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(7, TimeUnit.DAYS)))))
    val function = new GigaTileProcessFunction(batchGroupBy, inputSchema)
    val testHarness = harness(function)
    val callbackProcessingTime = 3 * dayMillis + new Window(1, TimeUnit.HOURS).millis
    val initialBatchEnd = dayMillis
    val replacementBatchEnd = 2 * dayMillis
    val streamEventTime = replacementBatchEnd + new Window(1, TimeUnit.HOURS).millis

    try {
      testHarness.open()
      testHarness.setProcessingTime(callbackProcessingTime - 1L)
      advanceToHealthyLiveWatermark(testHarness, callbackProcessingTime - 1L)
      testHarness.processElement2(
        batchRow(batchGroupBy, initialBatchEnd, initialBatchEnd - 1000L, 1L),
        initialBatchEnd)
      decodeSum(testHarness.extractOutputValues().get(0), batchGroupBy) shouldEqual 1L

      testHarness.setProcessingTime(callbackProcessingTime)
      testHarness.processElement1(event(streamEventTime, 5L), streamEventTime)
      testHarness.processElement2(
        batchRow(batchGroupBy, replacementBatchEnd, replacementBatchEnd - 1000L, 7L),
        replacementBatchEnd)

      val initialOutputs = testHarness.extractOutputValues()
      initialOutputs.size() shouldEqual 2
      decodeSum(initialOutputs.get(1), batchGroupBy) shouldEqual 6L
      setCurrentKey(testHarness, entityKey())
      valueState[java.lang.Boolean](function, "pendingPublicationState").value() shouldEqual
        java.lang.Boolean.TRUE

      testHarness.setProcessingTime(callbackProcessingTime + 1L)
      val outputs = testHarness.extractOutputValues()
      outputs.size() shouldEqual 3
      decodeSum(outputs.get(2), batchGroupBy) shouldEqual 12L
      outputs.get(2).latestTsMillis shouldEqual callbackProcessingTime + 1L
      setCurrentKey(testHarness, entityKey())
      valueState[java.lang.Boolean](function, "pendingPublicationState").value() shouldBe null
    } finally testHarness.close()
  }

  it should "keep a restored version collision fenced without a timer storm" in {
    val originalHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val firstEventTs = processingTs - 1000L
    originalHarness.open()
    originalHarness.setProcessingTime(processingTs)
    advanceToHealthyLiveWatermark(originalHarness, processingTs)
    originalHarness.processElement1(event(firstEventTs, 5L), firstEventTs)
    originalHarness.processElement1(event(firstEventTs + 1L, 7L), firstEventTs + 1L)
    originalHarness.processElement1(event(firstEventTs + 2L, 3L), firstEventTs + 2L)

    val initialOutputs = originalHarness.extractOutputValues()
    initialOutputs.size() shouldEqual 1
    decodeSum(initialOutputs.get(0)) shouldEqual 5L
    initialOutputs.get(0).latestTsMillis shouldEqual processingTs
    // One timer keeps normal eviction cadence; the other flushes the version collision.
    originalHarness.numProcessingTimeTimers() shouldEqual 2
    val snapshot = originalHarness.snapshot(8L, processingTs)
    originalHarness.close()

    val restoredHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(snapshot)
      restoredHarness.open()
      restoredHarness.setProcessingTime(processingTs + 100L)
      restoredHarness.extractOutputValues() shouldBe empty
      // The collision marker shares the next normal eviction timer while the connected
      // watermark is uninitialized; it does not retry every millisecond.
      restoredHarness.numProcessingTimeTimers() shouldEqual 1

      val retryTimer = nextEvictionHop(processingTs + 100L)
      advanceToHealthyLiveWatermark(restoredHarness, retryTimer)
      restoredHarness.setProcessingTime(retryTimer)
      val outputs = restoredHarness.extractOutputValues()
      outputs.size() shouldEqual 1
      decodeSum(outputs.get(0)) shouldEqual 15L
      outputs.get(0).latestTsMillis shouldEqual retryTimer
    } finally restoredHarness.close()
  }

  it should "repair collision and eviction markers after a state-blind timer consumer" in {
    val firstEventTs = processingTs - 1000L
    val evictionTs = nextEvictionHop(processingTs)
    val originalFunction = new GigaTileProcessFunction(groupBy, inputSchema)
    val originalHarness = harness(originalFunction)
    originalHarness.open()
    originalHarness.setProcessingTime(processingTs)
    advanceToHealthyLiveWatermark(originalHarness, processingTs)
    originalHarness.processElement1(event(firstEventTs, 5L), firstEventTs)
    originalHarness.processElement1(event(firstEventTs + 1L, 7L), firstEventTs + 1L)
    val originalSnapshot = originalHarness.snapshot(31L, processingTs)
    originalHarness.close()

    val legacyHarness = harness(new StateBlindProcessingTimerConsumer)
    legacyHarness.setup()
    legacyHarness.initializeState(originalSnapshot)
    legacyHarness.open()
    legacyHarness.setProcessingTime(processingTs)
    legacyHarness.setProcessingTime(evictionTs)
    legacyHarness.numProcessingTimeTimers() shouldEqual 0
    val legacySnapshot = legacyHarness.snapshot(32L, evictionTs)
    legacyHarness.close()

    val restoredFunction = new GigaTileProcessFunction(groupBy, inputSchema)
    val restoredHarness = harness(restoredFunction)
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(legacySnapshot)
      restoredHarness.open()
      restoredHarness.setProcessingTime(evictionTs)
      // With no keyed callback, there is no way for open() to enumerate and repair this key.
      restoredHarness.numProcessingTimeTimers() shouldEqual 0

      advanceToHealthyLiveWatermark(restoredHarness, evictionTs)
      restoredHarness.processElement1(event(firstEventTs + 2L, 3L), firstEventTs + 2L)
      setCurrentKey(restoredHarness, entityKey())
      val repairedEviction = valueState[java.lang.Long](restoredFunction,
                                                        "nextProcessingEvictionTimerState").value().longValue()
      val repairedCollision = valueState[java.lang.Long](restoredFunction,
                                                         "pendingVersionCollisionState").value().longValue()
      repairedEviction should be > evictionTs
      repairedCollision shouldEqual repairedEviction
      restoredHarness.numProcessingTimeTimers() shouldEqual 1

      advanceToHealthyLiveWatermark(restoredHarness, repairedCollision)
      restoredHarness.setProcessingTime(repairedCollision)

      val outputs = restoredHarness.extractOutputValues()
      outputs should have size 1
      decodeSum(outputs.get(0)) shouldEqual 15L
      outputs.get(0).latestTsMillis shouldEqual repairedCollision
      valueState[java.lang.Long](restoredFunction, "pendingVersionCollisionState").value() shouldBe null
      restoredHarness.numProcessingTimeTimers() shouldEqual 1
    } finally restoredHarness.close()
  }

  it should "keep a later owned timer while draining an earlier due callback" in {
    val windowMillis = new Window(1, TimeUnit.HOURS).millis
    val justBeforeExpiry = eventTs + windowMillis - 1L
    val injectedTimer = justBeforeExpiry + 1L
    val evictionTimer = nextEvictionHop(justBeforeExpiry)
    injectedTimer should be < evictionTimer

    val originalHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    originalHarness.open()
    originalHarness.setProcessingTime(justBeforeExpiry)
    advanceToHealthyLiveWatermark(originalHarness, justBeforeExpiry)
    originalHarness.processElement1(event(eventTs, 5L), eventTs)
    val originalSnapshot = originalHarness.snapshot(33L, justBeforeExpiry)
    originalHarness.close()

    val injectorHarness = harness(new ProcessingTimerInjector(injectedTimer))
    injectorHarness.setup()
    injectorHarness.initializeState(originalSnapshot)
    injectorHarness.open()
    injectorHarness.setProcessingTime(justBeforeExpiry)
    injectorHarness.processElement1(event(eventTs, 0L), eventTs)
    val injectedSnapshot = injectorHarness.snapshot(34L, justBeforeExpiry)
    injectorHarness.close()

    val restoredHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(injectedSnapshot)
      restoredHarness.open()
      // Flink reports this jump target while it first drains injectedTimer. That earlier
      // callback must not steal ownership from the still-queued evictionTimer.
      advanceToHealthyLiveWatermark(restoredHarness, evictionTimer)
      restoredHarness.setProcessingTime(evictionTimer)

      val outputs = restoredHarness.extractOutputValues()
      outputs should have size 1
      decodeSum(outputs.get(0)) shouldBe null
      outputs.get(0).latestTsMillis shouldEqual evictionTimer
      restoredHarness.numProcessingTimeTimers() shouldEqual 0
    } finally restoredHarness.close()
  }

  it should "share a queued eviction while draining an earlier collision repair callback" in {
    val firstEventTs = processingTs - 1000L
    val consumedCollisionTimer = processingTs + 1L
    val injectedTimer = processingTs + 2L
    val evictionTimer = nextEvictionHop(processingTs)
    injectedTimer should be < evictionTimer

    val originalHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    originalHarness.open()
    originalHarness.setProcessingTime(processingTs)
    advanceToHealthyLiveWatermark(originalHarness, processingTs)
    originalHarness.processElement1(event(firstEventTs, 5L), firstEventTs)
    originalHarness.processElement1(event(firstEventTs + 1L, 7L), firstEventTs + 1L)
    val originalSnapshot = originalHarness.snapshot(35L, processingTs)
    originalHarness.close()

    val legacyHarness = harness(new StateBlindProcessingTimerConsumer)
    legacyHarness.setup()
    legacyHarness.initializeState(originalSnapshot)
    legacyHarness.open()
    legacyHarness.setProcessingTime(processingTs)
    legacyHarness.setProcessingTime(consumedCollisionTimer)
    legacyHarness.numProcessingTimeTimers() shouldEqual 1
    val legacySnapshot = legacyHarness.snapshot(36L, consumedCollisionTimer)
    legacyHarness.close()

    val injectorHarness = harness(new ProcessingTimerInjector(injectedTimer))
    injectorHarness.setup()
    injectorHarness.initializeState(legacySnapshot)
    injectorHarness.open()
    injectorHarness.setProcessingTime(consumedCollisionTimer)
    injectorHarness.processElement1(event(firstEventTs, 0L), firstEventTs)
    val injectedSnapshot = injectorHarness.snapshot(37L, consumedCollisionTimer)
    injectorHarness.close()

    val restoredFunction = new GigaTileProcessFunction(groupBy, inputSchema)
    val restoredHarness = harness(restoredFunction)
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(injectedSnapshot)
      restoredHarness.open()
      restoredHarness.setProcessingTime(consumedCollisionTimer)
      // The injected callback runs first while Flink already reports evictionTimer as the
      // current processing time. The stale collision must join that still-queued eviction.
      advanceToHealthyLiveWatermark(restoredHarness, evictionTimer)
      restoredHarness.setProcessingTime(evictionTimer)

      val outputs = restoredHarness.extractOutputValues()
      outputs should have size 1
      decodeSum(outputs.get(0)) shouldEqual 12L
      outputs.get(0).latestTsMillis shouldEqual evictionTimer
      setCurrentKey(restoredHarness, entityKey())
      valueState[java.lang.Long](restoredFunction, "pendingVersionCollisionState").value() shouldBe null
      restoredHarness.numProcessingTimeTimers() shouldEqual 1
    } finally restoredHarness.close()
  }

  it should "retry the current snapshot when the first publication fails" in {
    val function = new GigaTileProcessFunction(groupBy, inputSchema)
    val testHarness = harness(function)
    val firstEventTs = processingTs - 1000L

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      advanceToHealthyLiveWatermark(testHarness, processingTs)
      failNextEncode(function)
      testHarness.processElement1(event(firstEventTs, 5L), firstEventTs)

      testHarness.extractOutputValues() shouldBe empty
      // Normal eviction and the failed-snapshot retry both remain live.
      testHarness.numProcessingTimeTimers() shouldEqual 2

      testHarness.setProcessingTime(processingTs + 1L)

      val outputs = testHarness.extractOutputValues()
      outputs.size() shouldEqual 1
      decodeSum(outputs.get(0)) shouldEqual 5L
      outputs.get(0).latestTsMillis shouldEqual processingTs + 1L
      testHarness.numProcessingTimeTimers() shouldEqual 1
    } finally testHarness.close()
  }

  it should "keep a failed collision on a queued eviction while draining delayed callbacks" in {
    val function = new GigaTileProcessFunction(groupBy, inputSchema)
    val testHarness = harness(function)
    val firstEventTs = processingTs - 1000L
    val retryTimer = nextEvictionHop(processingTs)

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      advanceToHealthyLiveWatermark(testHarness, processingTs)
      testHarness.processElement1(event(firstEventTs, 5L), firstEventTs)
      testHarness.processElement1(event(firstEventTs + 1L, 7L), firstEventTs + 1L)

      testHarness.extractOutputValues().size() shouldEqual 1
      failNextSnapshot(function)

      // Flink drains the earlier collision callback while already reporting retryTimer as the
      // wall clock. A transient failure must keep the collision on the queued eviction timer.
      advanceToHealthyLiveWatermark(testHarness, retryTimer)
      testHarness.setProcessingTime(retryTimer)

      val outputs = testHarness.extractOutputValues()
      outputs.size() shouldEqual 2
      decodeSum(outputs.get(1)) shouldEqual 12L
      outputs.get(1).latestTsMillis shouldEqual retryTimer
      testHarness.numProcessingTimeTimers() shouldEqual 1
    } finally testHarness.close()
  }

  it should "evict before flushing when version and eviction timers coincide" in {
    val testHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val hopMillis = new Window(5, TimeUnit.MINUTES).millis
    val evictionTs = TsUtils.round(processingTs, hopMillis) + hopMillis
    val collisionProcessingTs = evictionTs - 1L
    val expiringEventTs = evictionTs - new Window(1, TimeUnit.HOURS).millis - 1L

    try {
      testHarness.open()
      testHarness.setProcessingTime(collisionProcessingTs)
      advanceToHealthyLiveWatermark(testHarness, collisionProcessingTs)
      testHarness.processElement1(event(expiringEventTs, 5L), expiringEventTs)
      testHarness.processElement1(event(expiringEventTs, 7L), expiringEventTs)

      testHarness.setProcessingTime(evictionTs)

      val outputs = testHarness.extractOutputValues()
      outputs.size() shouldEqual 2
      decodeSum(outputs.get(1)) shouldBe null
      outputs.get(1).latestTsMillis shouldEqual evictionTs
      testHarness.numProcessingTimeTimers() shouldEqual 0
    } finally testHarness.close()
  }

  it should "delay a shared collision until a failed eviction is retried" in {
    val function = new GigaTileProcessFunction(groupBy, inputSchema)
    val testHarness = harness(function)
    val hopMillis = new Window(5, TimeUnit.MINUTES).millis
    val evictionTs = TsUtils.round(processingTs, hopMillis) + hopMillis
    val retryTs = evictionTs + hopMillis
    val collisionProcessingTs = evictionTs - 1L
    val expiringEventTs = retryTs - new Window(1, TimeUnit.HOURS).millis - 1L

    try {
      testHarness.open()
      testHarness.setProcessingTime(collisionProcessingTs)
      advanceToHealthyLiveWatermark(testHarness, collisionProcessingTs)
      testHarness.processElement1(event(expiringEventTs, 5L), expiringEventTs)
      testHarness.processElement1(event(expiringEventTs, 7L), expiringEventTs)
      failNextEviction(function)

      testHarness.setProcessingTime(evictionTs)

      testHarness.extractOutputValues().size() shouldEqual 1
      testHarness.numProcessingTimeTimers() shouldEqual 1

      testHarness.setProcessingTime(evictionTs + 1L)
      testHarness.extractOutputValues().size() shouldEqual 1

      advanceToHealthyLiveWatermark(testHarness, retryTs)
      testHarness.setProcessingTime(retryTs)

      val outputs = testHarness.extractOutputValues()
      outputs.size() shouldEqual 2
      decodeSum(outputs.get(1)) shouldBe null
      outputs.get(1).latestTsMillis shouldEqual retryTs
      testHarness.numProcessingTimeTimers() shouldEqual 0
    } finally testHarness.close()
  }

  it should "isolate same-millisecond publication collisions by key" in {
    val testHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val firstEventTs = processingTs - 1000L

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      advanceToHealthyLiveWatermark(testHarness, processingTs)
      testHarness.processElement1(keyedEvent("campaign-1", firstEventTs, 5L), firstEventTs)
      testHarness.processElement1(keyedEvent("campaign-1", firstEventTs + 1L, 7L), firstEventTs + 1L)
      testHarness.processElement1(keyedEvent("campaign-2", firstEventTs, 11L), firstEventTs)
      testHarness.processElement1(keyedEvent("campaign-2", firstEventTs + 1L, 13L), firstEventTs + 1L)

      testHarness.extractOutputValues().size() shouldEqual 2
      testHarness.setProcessingTime(processingTs + 1L)

      val flushedByKey = testHarness.extractOutputValues().asScala
        .filter(_.latestTsMillis == processingTs + 1L)
        .map(tile => tile.keys.get(0).toString -> decodeSum(tile))
        .toMap
      flushedByKey shouldEqual Map("campaign-1" -> 12L, "campaign-2" -> 24L)
    } finally testHarness.close()
  }

  it should "include an event that arrives exactly on a small-window hop" in {
    val testHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val hopMillis = new Window(5, TimeUnit.MINUTES).millis
    val exactHopTs = 30 * hopMillis

    try {
      testHarness.open()
      testHarness.setProcessingTime(exactHopTs)
      advanceToHealthyLiveWatermark(testHarness, exactHopTs)
      testHarness.processElement1(event(exactHopTs, 5L), exactHopTs)

      val output = testHarness.extractOutputValues().get(0)
      decodeSum(output) shouldEqual 5L
      output.latestTsMillis shouldEqual exactHopTs
    } finally testHarness.close()
  }

  it should "restore state from before the horizon markers existed" in {
    val eventTimestamp = 4 * new Window(1, TimeUnit.DAYS).millis + new Window(1, TimeUnit.HOURS).millis
    val legacyHarness = harness(new LegacyHorizonStateFunction(groupBy, inputSchema))
    legacyHarness.open()
    legacyHarness.processElement1(event(eventTimestamp, 5L), eventTimestamp)
    val snapshot = legacyHarness.snapshot(9L, eventTimestamp)
    legacyHarness.close()

    val restoredHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val batchCallbackTs = eventTimestamp + new Window(2, TimeUnit.HOURS).millis
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(snapshot)
      restoredHarness.open()
      restoredHarness.setProcessingTime(batchCallbackTs)
      advanceToHealthyLiveWatermark(restoredHarness, batchCallbackTs)
      restoredHarness.processElement2(emptyBatchRow(groupBy, batchEndTs = 0L), 0L)

      val outputs = restoredHarness.extractOutputValues()
      outputs.size() shouldEqual 1
      decodeSum(outputs.get(0)) shouldBe null
      outputs.get(0).latestTsMillis shouldEqual batchCallbackTs
    } finally restoredHarness.close()
  }

  it should "rebuild large state when a restored checkpoint has no recompute marker" in {
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    val hourMillis = new Window(1, TimeUnit.HOURS).millis
    val batchEndTs = 10 * dayMillis
    val batchEventTs = batchEndTs - 48 * hourMillis
    val beforeExpiry = batchEndTs + 59 * 60 * 1000L
    val delayedCallbackTs = batchEndTs + 3 * hourMillis + 60 * 1000L
    val batchGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "gigatile-process-function-legacy-large-marker-test"),
      aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(49, TimeUnit.HOURS)))))
    val batchAggregator =
      new SawtoothOnlineAggregator(batchEndTs, batchGroupBy.getAggregations.asScala.toSeq, inputSchema)
    val batchIr = batchAggregator.update(
      batchAggregator.init,
      new ArrayRow(Array[Any](batchEventTs, 5L), batchEventTs))
    val finalBatchIr = batchAggregator.denormalizeBatchIr(batchAggregator.normalizeBatchIr(batchIr))
    val batchRow = new BatchIrRow(
      entityKey(),
      new GigaTileCodec(batchGroupBy, inputSchema).encodeBatchIr(finalBatchIr),
      batchEndTs)

    val legacyHarness = harness(new LegacyHorizonStateFunction(batchGroupBy, inputSchema))
    legacyHarness.open()
    legacyHarness.setProcessingTime(beforeExpiry)
    legacyHarness.processElement2(batchRow, batchEndTs)
    val snapshot = legacyHarness.snapshot(10L, beforeExpiry)
    legacyHarness.close()

    val restoredHarness = harness(new GigaTileProcessFunction(batchGroupBy, inputSchema))
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(snapshot)
      restoredHarness.open()
      restoredHarness.setProcessingTime(delayedCallbackTs)
      advanceToHealthyLiveWatermark(restoredHarness, delayedCallbackTs)
      restoredHarness.processElement1(event(delayedCallbackTs - 1L, 7L), delayedCallbackTs - 1L)

      val outputs = restoredHarness.extractOutputValues()
      outputs.size() shouldEqual 1
      decodeSum(outputs.get(0), batchGroupBy) shouldEqual 7L
      outputs.get(0).latestTsMillis shouldEqual delayedCallbackTs
    } finally restoredHarness.close()
  }
}

object GigaTileProcessFunctionTest {
  private val eventTs = 1000000L
  private val processingTs = eventTs + new Window(2, TimeUnit.HOURS).millis
  private val inputSchema: Seq[(String, DataType)] =
    Seq(Constants.TimeColumn -> LongType, "num" -> LongType)
  private val groupBy = Builders.GroupBy(
    metaData = Builders.MetaData(name = "gigatile-process-function-test"),
    aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(1, TimeUnit.HOURS)))))

  private def harness(
      function: KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile]
  ): KeyedTwoInputStreamOperatorTestHarness[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile] =
    new KeyedTwoInputStreamOperatorTestHarness[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile](
      new KeyedCoProcessOperator(function),
      new KeySelector[ProjectedEvent, util.List[Any]] {
        override def getKey(value: ProjectedEvent): util.List[Any] =
          entityKey(value.fields.getOrElse("entity", "campaign-1").toString)
      },
      new KeySelector[BatchIrRow, util.List[Any]] {
        override def getKey(value: BatchIrRow): util.List[Any] = value.entityKeys
      },
      TypeInformation
        .of(classOf[util.List[_]])
        .asInstanceOf[TypeInformation[util.List[Any]]])

  private def event(timestamp: Long, value: Long): ProjectedEvent =
    ProjectedEvent(Map(Constants.TimeColumn -> timestamp, "num" -> value), timestamp)

  private def advanceToLiveWatermark(
      testHarness: KeyedTwoInputStreamOperatorTestHarness[
        util.List[Any],
        ProjectedEvent,
        BatchIrRow,
        TimestampedTile],
      watermark: Long
  ): Unit = {
    testHarness.processWatermarkStatus2(WatermarkStatus.IDLE)
    testHarness.processWatermark1(new Watermark(watermark))
  }

  private def healthyLiveWatermark(processingTime: Long): Long =
    processingTime - FlinkJob.AllowedOutOfOrderness.toMillis - 1L

  private def liveActivationWatermark(processingTime: Long): Long =
    processingTime - FlinkJob.LiveWatermarkLagToleranceMillis

  private def advanceToHealthyLiveWatermark(
      testHarness: KeyedTwoInputStreamOperatorTestHarness[
        util.List[Any],
        ProjectedEvent,
        BatchIrRow,
        TimestampedTile],
      processingTime: Long
  ): Unit =
    advanceToLiveWatermark(testHarness, healthyLiveWatermark(processingTime))

  private def nextDay(timestamp: Long): Long = {
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    TsUtils.round(timestamp, dayMillis) + dayMillis
  }

  private def currentProcessor(function: GigaTileProcessFunction): GigaTileStreamProcessor = {
    val processorField = classOf[GigaTileProcessFunction].getDeclaredField("processor")
    processorField.setAccessible(true)
    processorField.get(function).asInstanceOf[GigaTileStreamProcessor]
  }

  private def publicationTimerState(
      function: GigaTileProcessFunction,
      fieldName: String
  ): ValueState[java.lang.Long] = {
    val stateField = classOf[GigaTileProcessFunction].getDeclaredField(fieldName)
    stateField.setAccessible(true)
    stateField.get(function).asInstanceOf[ValueState[java.lang.Long]]
  }

  private def publicationRetryTimestamp(function: GigaTileProcessFunction): Long =
    publicationTimerState(function, "pendingPublicationRetryTimerState").value().longValue()

  private def setPublicationRetryTimestamp(function: GigaTileProcessFunction, timestamp: Long): Unit =
    publicationTimerState(function, "pendingPublicationRetryTimerState").update(timestamp)

  private def clearPublicationActivationTimestamp(function: GigaTileProcessFunction): Unit =
    publicationTimerState(function, "pendingPublicationActivationTimerState").clear()

  private def currentFinalizedSum(function: GigaTileProcessFunction): AnyRef =
    currentProcessor(function).currentSnapshot.finalizedVector(0).asInstanceOf[AnyRef]

  private def currentDayStart(function: GigaTileProcessFunction): Long =
    currentProcessor(function).store.getCurrentDayStart

  private def dailyLargeSlotCount(function: GigaTileProcessFunction): Int =
    currentProcessor(function).store.dailyLargeIrIterator.size

  private def keyedEvent(entity: String, timestamp: Long, value: Long): ProjectedEvent =
    ProjectedEvent(Map("entity" -> entity, Constants.TimeColumn -> timestamp, "num" -> value), timestamp)

  private def entityKey(entity: String = "campaign-1"): util.List[Any] = {
    val result = new util.ArrayList[Any](1)
    result.add(entity)
    result
  }

  private def nextEvictionHop(timestamp: Long): Long = {
    val hopMillis = new Window(5, TimeUnit.MINUTES).millis
    TsUtils.round(timestamp, hopMillis) + hopMillis
  }

  private def setCurrentKey(
      testHarness: KeyedTwoInputStreamOperatorTestHarness[
        util.List[Any],
        ProjectedEvent,
        BatchIrRow,
        TimestampedTile],
      key: util.List[Any]
  ): Unit =
    testHarness.getOperator.getKeyedStateBackend
      .asInstanceOf[KeyedStateBackend[util.List[Any]]]
      .setCurrentKey(key)

  private def valueState[T](function: GigaTileProcessFunction, fieldName: String): ValueState[T] = {
    val field = classOf[GigaTileProcessFunction].getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(function).asInstanceOf[ValueState[T]]
  }

  private def decodeSum(tile: TimestampedTile, decodeGroupBy: GroupBy = groupBy): AnyRef = {
    val outputCodec = new GigaTileCodec(decodeGroupBy, inputSchema)
    val fieldName = outputCodec.outputSchema.fields.head.name
    AvroCodec
      .of(ai.chronon.online.serde.AvroConversions.fromChrononSchema(outputCodec.outputSchema).toString)
      .decodeMap(tile.tileBytes)(fieldName)
  }

  private def failNextSnapshot(function: GigaTileProcessFunction): Unit = {
    val processorField = classOf[GigaTileProcessFunction].getDeclaredField("processor")
    processorField.setAccessible(true)
    val delegate = processorField.get(function).asInstanceOf[GigaTileStreamProcessor]
    processorField.set(
      function,
      new GigaTileStreamProcessor(
        delegate.megaTileAgg,
        delegate.store,
        delegate.irEqual,
        delegate.maxBatchStalenessDays
      ) {
        private var shouldFail = true

        override private[chronon] def currentSnapshot: GigaEmitResult = {
          if (shouldFail) {
            shouldFail = false
            throw new RuntimeException("expected snapshot failure")
          }
          delegate.currentSnapshot
        }
      }
    )
  }

  private def failNextEncode(function: GigaTileProcessFunction): Unit = {
    val codecField = classOf[GigaTileProcessFunction].getDeclaredField("gigaTileCodec")
    codecField.setAccessible(true)
    codecField.set(
      function,
      new GigaTileCodec(groupBy, inputSchema) {
        private var shouldFail = true

        override def encodeOutput(finalizedVector: Array[Any]): Array[Byte] = {
          if (shouldFail) {
            shouldFail = false
            throw new RuntimeException("expected publication encoding failure")
          }
          super.encodeOutput(finalizedVector)
        }
      })
  }

  private def failNextEviction(function: GigaTileProcessFunction): Unit = {
    val processorField = classOf[GigaTileProcessFunction].getDeclaredField("processor")
    processorField.setAccessible(true)
    val delegate = processorField.get(function).asInstanceOf[GigaTileStreamProcessor]
    processorField.set(
      function,
      new GigaTileStreamProcessor(
        delegate.megaTileAgg,
        delegate.store,
        delegate.irEqual,
        delegate.maxBatchStalenessDays
      ) {
        private var shouldFail = true

        override private[chronon] def onEviction(evictionTimes: EvictionTimes): GigaEmitResult = {
          if (shouldFail) {
            shouldFail = false
            throw new RuntimeException("expected eviction failure")
          }
          delegate.onEviction(evictionTimes)
        }
      }
    )
  }

  private def emptyBatchRow(batchGroupBy: GroupBy, batchEndTs: Long): BatchIrRow = {
    val batchAggregator =
      new SawtoothOnlineAggregator(batchEndTs, batchGroupBy.getAggregations.asScala.toSeq, inputSchema)
    val emptyBatchIr = batchAggregator.denormalizeBatchIr(batchAggregator.normalizeBatchIr(batchAggregator.init))
    new BatchIrRow(entityKey(), new GigaTileCodec(batchGroupBy, inputSchema).encodeBatchIr(emptyBatchIr), batchEndTs)
  }

  private def batchRow(
      batchGroupBy: GroupBy,
      batchEndTs: Long,
      batchEventTs: Long,
      value: Long
  ): BatchIrRow = {
    val batchAggregator =
      new SawtoothOnlineAggregator(batchEndTs, batchGroupBy.getAggregations.asScala.toSeq, inputSchema)
    val batchIr = batchAggregator.update(
      batchAggregator.init,
      new ArrayRow(Array[Any](batchEventTs, value), batchEventTs))
    val finalBatchIr = batchAggregator.denormalizeBatchIr(batchAggregator.normalizeBatchIr(batchIr))
    new BatchIrRow(entityKey(), new GigaTileCodec(batchGroupBy, inputSchema).encodeBatchIr(finalBatchIr), batchEndTs)
  }

  private class LegacyEventTimeTimerFunction(timerTimestamps: Seq[Long])
      extends KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile] {
    override def processElement1(
        event: ProjectedEvent,
        ctx: KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile]#Context,
        out: Collector[TimestampedTile]
    ): Unit =
      timerTimestamps.foreach(ctx.timerService().registerEventTimeTimer)

    override def processElement2(
        batchRow: BatchIrRow,
        ctx: KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile]#Context,
        out: Collector[TimestampedTile]
    ): Unit = ()
  }

  /** Models a rollback binary that consumes physical timers without binding the additive
    * collision and eviction ownership descriptors.
    */
  private class StateBlindProcessingTimerConsumer
      extends KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile] {
    override def processElement1(
        event: ProjectedEvent,
        ctx: KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile]#Context,
        out: Collector[TimestampedTile]
    ): Unit = ()

    override def processElement2(
        batchRow: BatchIrRow,
        ctx: KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile]#Context,
        out: Collector[TimestampedTile]
    ): Unit = ()

    override def onTimer(
        timestamp: Long,
        ctx: KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile]#OnTimerContext,
        out: Collector[TimestampedTile]
    ): Unit = ()
  }

  private class ProcessingTimerInjector(timerTimestamp: Long)
      extends KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile] {
    override def processElement1(
        event: ProjectedEvent,
        ctx: KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile]#Context,
        out: Collector[TimestampedTile]
    ): Unit = ctx.timerService().registerProcessingTimeTimer(timerTimestamp)

    override def processElement2(
        batchRow: BatchIrRow,
        ctx: KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile]#Context,
        out: Collector[TimestampedTile]
    ): Unit = ()
  }

  /** Seeds only descriptors that existed before the cached-as-of markers were introduced. */
  private class LegacyHorizonStateFunction(groupBy: GroupBy, inputSchema: Seq[(String, DataType)])
      extends KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile] {
    private var tileState: MapState[String, Array[Byte]] = _
    private var megaTileIrState: ValueState[Array[Byte]] = _
    private var currentDayStartState: ValueState[java.lang.Long] = _
    private var earliestTileStartState: ValueState[java.lang.Long] = _
    private var batchIrState: ValueState[Array[Byte]] = _
    private var batchEndTsState: ValueState[java.lang.Long] = _
    private var runningLargeIrState: ValueState[Array[Byte]] = _
    private var megaTileAgg: MegaTileAggregator = _
    private var codec: MegaTileCodec = _
    private var gigaCodec: GigaTileCodec = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      val aggregations = groupBy.getAggregations.asScala.toSeq
      megaTileAgg = new MegaTileAggregator(aggregations, inputSchema)
      codec = new MegaTileCodec(groupBy, inputSchema)
      gigaCodec = new GigaTileCodec(groupBy, inputSchema)
      tileState = getRuntimeContext.getMapState(
        new MapStateDescriptor[String, Array[Byte]]("giga-tile-tiles", classOf[String], classOf[Array[Byte]]))
      megaTileIrState = getRuntimeContext.getState(
        new ValueStateDescriptor[Array[Byte]]("giga-tile-ir", classOf[Array[Byte]]))
      currentDayStartState = getRuntimeContext.getState(
        new ValueStateDescriptor[java.lang.Long]("giga-tile-day-start", classOf[java.lang.Long]))
      earliestTileStartState = getRuntimeContext.getState(
        new ValueStateDescriptor[java.lang.Long]("giga-tile-earliest-tile", classOf[java.lang.Long]))
      batchIrState = getRuntimeContext.getState(
        new ValueStateDescriptor[Array[Byte]]("giga-tile-batch-ir", classOf[Array[Byte]]))
      batchEndTsState = getRuntimeContext.getState(
        new ValueStateDescriptor[java.lang.Long]("giga-tile-batch-end", classOf[java.lang.Long]))
      runningLargeIrState = getRuntimeContext.getState(
        new ValueStateDescriptor[Array[Byte]]("giga-tile-running-large", classOf[Array[Byte]]))
    }

    override def processElement1(
        event: ProjectedEvent,
        ctx: KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile]#Context,
        out: Collector[TimestampedTile]
    ): Unit = {
      val timestamp = event.fields(Constants.TimeColumn).asInstanceOf[Long]
      val row = new ArrayRow(inputSchema.map { case (name, _) => event.fields(name) }.toArray, timestamp)
      val baseIr = megaTileAgg.baseAggregator.init
      megaTileAgg.baseAggregator.update(baseIr, row)
      val tileStarts = megaTileAgg.tileStartsForEvent(timestamp)
      tileStarts.foreach { case (hopSize, tileStart) =>
        tileState.put(s"$hopSize:$tileStart", codec.encodeBaseIr(baseIr))
      }

      val cachedIr = megaTileAgg.windowedAggregator.init
      megaTileAgg.windowedAggregator.columnAggregators(0).update(cachedIr, row)
      megaTileIrState.update(codec.encode(cachedIr))
      currentDayStartState.update(TsUtils.round(timestamp, new Window(1, TimeUnit.DAYS).millis))
      earliestTileStartState.update(tileStarts.map(_._2).min)
    }

    override def processElement2(
        batchRow: BatchIrRow,
        ctx: KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile]#Context,
        out: Collector[TimestampedTile]
    ): Unit = {
      val batchIr = gigaCodec.decodeBatchIr(batchRow.valueBytes)
      val seedStore = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
      val seedProcessor = new GigaTileStreamProcessor(megaTileAgg, seedStore)
      seedProcessor.onBatchUpdate(
        batchIr,
        batchRow.batchEndTs,
        ctx.timerService().currentProcessingTime())

      batchIrState.update(gigaCodec.encodeBatchIr(seedStore.getBatchIr))
      batchEndTsState.update(batchRow.batchEndTs)
      runningLargeIrState.update(codec.encode(seedStore.getRunningLargeIr))
      currentDayStartState.update(TsUtils.round(batchRow.batchEndTs, new Window(1, TimeUnit.DAYS).millis))
    }
  }
}
