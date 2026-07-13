package ai.chronon.flink.test.window

import ai.chronon.aggregator.windowing.{
  GigaEmitResult,
  GigaTileStreamProcessor,
  SawtoothOnlineAggregator
}
import ai.chronon.api._
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.types.{BatchIrRow, TimestampedTile}
import ai.chronon.flink.window.GigaTileProcessFunction
import ai.chronon.online.GigaTileCodec
import ai.chronon.online.serde.{ArrayRow, AvroCodec}
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.runtime.state.KeyedStateBackend
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
import org.apache.flink.streaming.api.watermark.Watermark
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
      testHarness.processElement1(event(eventTs, 5L), eventTs)
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
      testHarness.processElement1(event(initialEventTs, 5L), initialEventTs)
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
      testHarness.processElement2(
        new BatchIrRow(entityKey(), batchCodec.encodeBatchIr(emptyBatchIr), batchEndTs),
        batchEndTs)

      testHarness.extractOutputValues().asScala shouldBe empty
      testHarness.numProcessingTimeTimers() shouldEqual 1
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

  it should "restore and flush same-millisecond updates with a strictly newer version" in {
    val originalHarness = harness(new GigaTileProcessFunction(groupBy, inputSchema))
    val firstEventTs = processingTs - 1000L
    originalHarness.open()
    originalHarness.setProcessingTime(processingTs)
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
      // A delayed callback still uses the reserved strictly-newer version without moving
      // aggregation time merely to flush the current snapshot.
      restoredHarness.setProcessingTime(processingTs + 100L)

      val outputs = restoredHarness.extractOutputValues()
      outputs.size() shouldEqual 1
      decodeSum(outputs.get(0)) shouldEqual 15L
      outputs.get(0).latestTsMillis shouldEqual processingTs + 1L
    } finally restoredHarness.close()
  }

  it should "repair collision and eviction markers after a state-blind timer consumer" in {
    val firstEventTs = processingTs - 1000L
    val evictionTs = nextEvictionHop(processingTs)
    val originalFunction = new GigaTileProcessFunction(groupBy, inputSchema)
    val originalHarness = harness(originalFunction)
    originalHarness.open()
    originalHarness.setProcessingTime(processingTs)
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

      restoredHarness.processElement1(event(firstEventTs + 2L, 3L), firstEventTs + 2L)
      setCurrentKey(restoredHarness, entityKey())
      val repairedEviction = valueState[java.lang.Long](restoredFunction,
                                                        "nextProcessingEvictionTimerState").value().longValue()
      val repairedCollision = valueState[java.lang.Long](restoredFunction,
                                                         "pendingVersionCollisionState").value().longValue()
      repairedEviction should be > evictionTs
      repairedCollision shouldEqual repairedEviction
      restoredHarness.numProcessingTimeTimers() shouldEqual 1

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

  it should "re-arm a version collision when snapshot construction fails" in {
    val function = new GigaTileProcessFunction(groupBy, inputSchema)
    val testHarness = harness(function)
    val firstEventTs = processingTs - 1000L

    try {
      testHarness.open()
      testHarness.setProcessingTime(processingTs)
      testHarness.processElement1(event(firstEventTs, 5L), firstEventTs)
      testHarness.processElement1(event(firstEventTs + 1L, 7L), firstEventTs + 1L)

      testHarness.extractOutputValues().size() shouldEqual 1
      failNextSnapshot(function)

      testHarness.setProcessingTime(processingTs + 1L)

      testHarness.extractOutputValues().size() shouldEqual 1
      // The normal eviction timer and the re-armed collision timer must both remain live.
      testHarness.numProcessingTimeTimers() shouldEqual 2

      testHarness.setProcessingTime(processingTs + 2L)

      val outputs = testHarness.extractOutputValues()
      outputs.size() shouldEqual 2
      decodeSum(outputs.get(1)) shouldEqual 12L
      outputs.get(1).latestTsMillis shouldEqual processingTs + 2L
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
      testHarness.processElement1(event(expiringEventTs, 5L), expiringEventTs)
      testHarness.processElement1(event(expiringEventTs, 7L), expiringEventTs)
      failNextScheduledEviction(function)

      testHarness.setProcessingTime(evictionTs)

      testHarness.extractOutputValues().size() shouldEqual 1
      testHarness.numProcessingTimeTimers() shouldEqual 1

      testHarness.setProcessingTime(evictionTs + 1L)
      testHarness.extractOutputValues().size() shouldEqual 1

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

  private def failNextScheduledEviction(function: GigaTileProcessFunction): Unit = {
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

        override def onScheduledEviction(timerTs: Long): GigaEmitResult = {
          if (shouldFail) {
            shouldFail = false
            throw new RuntimeException("expected scheduled eviction failure")
          }
          delegate.onScheduledEviction(timerTs)
        }
      }
    )
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
}
