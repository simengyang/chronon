package ai.chronon.flink.test.window

import ai.chronon.aggregator.windowing.{GigaEmitResult, SawtoothOnlineAggregator}
import ai.chronon.api._
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.flink.FlinkJob
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.types.{BatchIrRow, TimestampedTile}
import ai.chronon.flink.window.GigaTileProcessFunction
import ai.chronon.online.GigaTileCodec
import ai.chronon.online.serde.{ArrayRow, AvroCodec}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
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

class GigaTileFirstSeenKeyGraceTest extends AnyFlatSpec with Matchers {
  import GigaTileFirstSeenKeyGraceTest._

  "GigaTileProcessFunction first-seen grace" should "initialize during catch-up but remain fenced until Live" in {
    val graceMillis = 1000L
    val function = new GigaTileProcessFunction(batchBackedGroupBy,
                                               inputSchema,
                                               firstSeenKeyGraceMillis = graceMillis)
    val testHarness = harness(function)
    val startProcessingTime = tenDays + oneHour
    val catchupWatermark = startProcessingTime - 3L * dayMillis
    val eventTime = catchupWatermark - 1000L
    val graceTimer = nextDay(startProcessingTime)

    try {
      testHarness.open()
      testHarness.setProcessingTime(startProcessingTime)
      advanceConnectedWatermark(testHarness, catchupWatermark)
      testHarness.processElement1(event(DefaultEntity, eventTime, 5L), eventTime)

      testHarness.setProcessingTime(graceTimer)
      testHarness.extractOutputValues() shouldBe empty
      setCurrentKey(testHarness, entityKey(DefaultEntity))
      state[Array[Byte]](function, "batchIrState").value() shouldBe null
      state[java.lang.Boolean](function, "syntheticEmptyBatchBaselineState").value() shouldEqual
        java.lang.Boolean.TRUE
      state[java.lang.Boolean](function, "pendingEmptyBatchBaselineState").value() shouldBe null

      val liveProcessingTime = graceTimer + 1000L
      testHarness.setProcessingTime(liveProcessingTime)
      advanceConnectedHealthyLiveWatermark(testHarness, liveProcessingTime)
      testHarness.processElement1(event(DefaultEntity, liveProcessingTime - 1L, 0L), liveProcessingTime - 1L)

      val outputs = testHarness.extractOutputValues()
      outputs should have size 1
      decodeSum(outputs.get(0), batchBackedGroupBy) shouldEqual 5L
    } finally testHarness.close()
  }

  it should "not initialize, age, or publish from a NoWatermark processing timer" in {
    val graceMillis = dayMillis + oneHour
    val function = new GigaTileProcessFunction(batchBackedGroupBy,
                                               inputSchema,
                                               firstSeenKeyGraceMillis = graceMillis)
    val testHarness = harness(function)
    val startProcessingTime = tenDays + oneHour
    val eventTime = startProcessingTime - 1000L
    val eventDay = TsUtils.round(eventTime, dayMillis)

    try {
      testHarness.open()
      testHarness.setProcessingTime(startProcessingTime)
      testHarness.processElement1(event(DefaultEntity, eventTime, 5L), eventTime)

      // This crosses two daily processing timers, including one after the grace expires.
      testHarness.setProcessingTime(startProcessingTime + 2L * dayMillis)

      testHarness.extractOutputValues() shouldBe empty
      setCurrentKey(testHarness, entityKey(DefaultEntity))
      state[Array[Byte]](function, "batchIrState").value() shouldBe null
      state[java.lang.Boolean](function, "pendingEmptyBatchBaselineState").value() shouldEqual java.lang.Boolean.TRUE
      state[java.lang.Long](function, "currentDayStartState").value().longValue() shouldEqual eventDay
    } finally testHarness.close()
  }

  it should "not initialize from a NoWatermark event after grace" in {
    val graceMillis = 1000L
    val function = new GigaTileProcessFunction(
      batchBackedGroupBy,
      inputSchema,
      firstSeenKeyGraceMillis = graceMillis)
    val testHarness = harness(function)
    val startProcessingTime = tenDays + oneHour
    val firstEventTime = startProcessingTime - 1000L
    val afterGraceProcessingTime = startProcessingTime + graceMillis + 1L

    try {
      testHarness.open()
      testHarness.setProcessingTime(startProcessingTime)
      testHarness.processElement1(event(DefaultEntity, firstEventTime, 5L), firstEventTime)

      testHarness.setProcessingTime(afterGraceProcessingTime)
      testHarness.processElement1(event(DefaultEntity, firstEventTime + 1L, 7L), firstEventTime + 1L)

      testHarness.extractOutputValues() shouldBe empty
      setCurrentKey(testHarness, entityKey(DefaultEntity))
      state[Array[Byte]](function, "batchIrState").value() shouldBe null
      state[java.lang.Boolean](function, "pendingEmptyBatchBaselineState").value() shouldEqual java.lang.Boolean.TRUE
    } finally testHarness.close()
  }

  it should "let a real batch row win during the grace" in {
    val graceMillis = 2000L
    val function = new GigaTileProcessFunction(batchBackedGroupBy,
                                               inputSchema,
                                               firstSeenKeyGraceMillis = graceMillis)
    val testHarness = harness(function)
    val startProcessingTime = tenDays + oneHour
    val eventTime = startProcessingTime - 1000L
    val batchEnd = tenDays

    try {
      testHarness.open()
      testHarness.setProcessingTime(startProcessingTime)
      advanceConnectedHealthyLiveWatermark(testHarness, startProcessingTime)
      testHarness.processElement1(event(DefaultEntity, eventTime, 5L), eventTime)
      testHarness.extractOutputValues() shouldBe empty

      val batchProcessingTime = startProcessingTime + graceMillis / 2L
      testHarness.setProcessingTime(batchProcessingTime)
      advanceConnectedHealthyLiveWatermark(testHarness, batchProcessingTime)
      testHarness.processElement2(batchRow(DefaultEntity, batchBackedGroupBy, batchEnd, 7L), batchEnd)

      val outputs = testHarness.extractOutputValues()
      decodeSum(outputs.get(outputs.size() - 1), batchBackedGroupBy) shouldEqual 12L
      setCurrentKey(testHarness, entityKey(DefaultEntity))
      state[java.lang.Boolean](function, "pendingEmptyBatchBaselineState").value() shouldBe null
      state[java.lang.Long](function, "batchEndTsState").value().longValue() shouldEqual batchEnd
    } finally testHarness.close()
  }

  it should "let a real batch row replace a synthetic empty baseline" in {
    val function = new GigaTileProcessFunction(batchBackedGroupBy,
                                               inputSchema,
                                               firstSeenKeyGraceMillis = 1L)
    val testHarness = harness(function)
    val startProcessingTime = tenDays + oneHour
    val eventTime = startProcessingTime - 1000L
    val batchEnd = tenDays

    try {
      testHarness.open()
      testHarness.setProcessingTime(startProcessingTime)
      advanceConnectedHealthyLiveWatermark(testHarness, startProcessingTime)
      testHarness.processElement1(event(DefaultEntity, eventTime, 5L), eventTime)
      testHarness.extractOutputValues() shouldBe empty

      val baselineProcessingTime = startProcessingTime + 1L
      testHarness.setProcessingTime(baselineProcessingTime)
      advanceConnectedHealthyLiveWatermark(testHarness, baselineProcessingTime)
      testHarness.processElement1(event(DefaultEntity, eventTime + 1L, 0L), eventTime + 1L)
      val baselineOutputs = testHarness.extractOutputValues()
      decodeSum(baselineOutputs.get(baselineOutputs.size() - 1), batchBackedGroupBy) shouldEqual 5L

      setCurrentKey(testHarness, entityKey(DefaultEntity))
      state[Array[Byte]](function, "batchIrState").value() shouldBe null
      state[java.lang.Boolean](function, "syntheticEmptyBatchBaselineState").value() shouldEqual
        java.lang.Boolean.TRUE
      state[java.lang.Long](function, "batchEndTsState").value() shouldBe null

      testHarness.processElement2(batchRow(DefaultEntity, batchBackedGroupBy, batchEnd, 7L), batchEnd)
      // The event and batch callbacks share one processing-time millisecond, so the
      // monotonic publisher flushes the authoritative batch result at the next millisecond.
      testHarness.setProcessingTime(baselineProcessingTime + 1L)
      val outputs = testHarness.extractOutputValues()
      decodeSum(outputs.get(outputs.size() - 1), batchBackedGroupBy) shouldEqual 12L
      state[Array[Byte]](function, "batchIrState").value() should not be null
      state[java.lang.Boolean](function, "syntheticEmptyBatchBaselineState").value() shouldBe null
      state[java.lang.Long](function, "batchEndTsState").value().longValue() shouldEqual batchEnd
    } finally testHarness.close()
  }

  it should "fail batch value decoding closed when the fallback is enabled" in {
    val function = new GigaTileProcessFunction(batchBackedGroupBy,
                                               inputSchema,
                                               firstSeenKeyGraceMillis = 1L)
    val testHarness = harness(function)
    val startProcessingTime = tenDays + oneHour

    try {
      testHarness.open()
      testHarness.setProcessingTime(startProcessingTime)
      advanceConnectedHealthyLiveWatermark(testHarness, startProcessingTime)

      an[Exception] should be thrownBy testHarness.processElement2(
        new BatchIrRow(entityKey(DefaultEntity), Array[Byte](1, 2, 3), tenDays),
        tenDays)
    } finally testHarness.close()
  }

  it should "ignore a stale pending fallback when batch state already exists" in {
    val function = new GigaTileProcessFunction(batchBackedGroupBy,
                                               inputSchema,
                                               firstSeenKeyGraceMillis = 1L)
    val testHarness = harness(function)
    val startProcessingTime = tenDays + oneHour
    val batchEnd = tenDays

    try {
      testHarness.open()
      testHarness.setProcessingTime(startProcessingTime)
      advanceConnectedHealthyLiveWatermark(testHarness, startProcessingTime)
      testHarness.processElement2(batchRow(DefaultEntity, batchBackedGroupBy, batchEnd, 7L), batchEnd)

      setCurrentKey(testHarness, entityKey(DefaultEntity))
      state[java.lang.Boolean](function, "pendingEmptyBatchBaselineState").update(java.lang.Boolean.TRUE)

      val noOpInitialization = initializeEmptyBatchBaseline(function,
                                                            startProcessingTime + 1L,
                                                            startProcessingTime)
      noOpInitialization shouldBe None
      state[java.lang.Boolean](function, "pendingEmptyBatchBaselineState").value() shouldBe null

      // A no-op initialization must not replace callback metadata. This is the shape used
      // by batch callbacks that need to retain the normal eviction schedule without emitting.
      val callbackResult = GigaEmitResult(null, needsEvictionTimer = true)
      val mergedResult = noOpInitialization.getOrElse(callbackResult)
      mergedResult.finalizedVector shouldBe null
      mergedResult.needsEvictionTimer shouldBe true

      // Exercise the same coexistence through the normal event path and retain its value.
      state[java.lang.Boolean](function, "pendingEmptyBatchBaselineState").update(java.lang.Boolean.TRUE)
      val eventProcessingTime = startProcessingTime + 1L
      testHarness.setProcessingTime(eventProcessingTime)
      advanceConnectedHealthyLiveWatermark(testHarness, eventProcessingTime)
      testHarness.processElement1(event(DefaultEntity, eventProcessingTime - 1L, 5L), eventProcessingTime - 1L)

      val outputs = testHarness.extractOutputValues()
      decodeSum(outputs.get(outputs.size() - 1), batchBackedGroupBy) shouldEqual 12L
      state[java.lang.Boolean](function, "pendingEmptyBatchBaselineState").value() shouldBe null
    } finally testHarness.close()
  }

  it should "restore pending grace and cadence state without bypassing a fresh grace" in {
    val graceMillis = 1000L
    val cadenceMillis = 100L
    val startProcessingTime = tenDays + oneHour
    val eventTime = startProcessingTime - 1000L
    val originalFunction = new GigaTileProcessFunction(batchBackedGroupBy,
                                                       inputSchema,
                                                       bufferingOutputTimeMillis = cadenceMillis,
                                                       firstSeenKeyGraceMillis = graceMillis)
    val originalHarness = harness(originalFunction)

    originalHarness.open()
    originalHarness.setProcessingTime(startProcessingTime)
    advanceConnectedHealthyLiveWatermark(originalHarness, startProcessingTime)
    originalHarness.processElement1(event(DefaultEntity, eventTime, 5L), eventTime)
    val snapshot = originalHarness.snapshot(31L, startProcessingTime)
    originalHarness.close()

    val restoredFunction = new GigaTileProcessFunction(batchBackedGroupBy,
                                                       inputSchema,
                                                       bufferingOutputTimeMillis = cadenceMillis,
                                                       firstSeenKeyGraceMillis = graceMillis)
    val restoredHarness = harness(restoredFunction)
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(snapshot)
      restoredHarness.open()
      restoredHarness.setProcessingTime(startProcessingTime)
      advanceConnectedHealthyLiveWatermark(restoredHarness, startProcessingTime)
      restoredHarness.processElement1(event(DefaultEntity, eventTime + 1L, 0L), eventTime + 1L)

      // Restoring the pending key starts a fresh operator-local grace. It must not publish
      // before a baseline is available, install synthetic history, or clear the keyed marker.
      restoredHarness.extractOutputValues() shouldBe empty
      setCurrentKey(restoredHarness, entityKey(DefaultEntity))
      state[Array[Byte]](restoredFunction, "batchIrState").value() shouldBe null
      state[java.lang.Boolean](restoredFunction, "pendingEmptyBatchBaselineState").value() shouldEqual
        java.lang.Boolean.TRUE

      val readyProcessingTime = startProcessingTime + graceMillis
      restoredHarness.setProcessingTime(readyProcessingTime)
      advanceConnectedHealthyLiveWatermark(restoredHarness, readyProcessingTime)
      restoredHarness.processElement1(event(DefaultEntity, readyProcessingTime - 1L, 0L), readyProcessingTime - 1L)
      val finalCadence = nextBufferedWriteTick(batchBackedGroupBy,
                                               entityKey(DefaultEntity),
                                               readyProcessingTime,
                                               cadenceMillis)
      restoredHarness.setProcessingTime(finalCadence)

      val outputs = restoredHarness.extractOutputValues()
      decodeSum(outputs.get(outputs.size() - 1), batchBackedGroupBy) shouldEqual 5L
      state[Array[Byte]](restoredFunction, "batchIrState").value() shouldBe null
      state[java.lang.Boolean](restoredFunction, "syntheticEmptyBatchBaselineState").value() shouldEqual
        java.lang.Boolean.TRUE
      state[java.lang.Boolean](restoredFunction, "pendingPublicationState").value() shouldEqual
        java.lang.Boolean.TRUE
      state[java.lang.Boolean](restoredFunction, "syntheticRollbackCorrectionState").value() shouldEqual
        java.lang.Boolean.TRUE
    } finally restoredHarness.close()
  }

  it should "remove every restored synthetic baseline when the option is disabled" in {
    val graceMillis = 1000L
    val startProcessingTime = tenDays + oneHour
    val eventTime = startProcessingTime - 1000L
    val firstKey = "entity-a"
    val secondKey = "entity-b"
    val keys = Seq(firstKey, secondKey)
    val originalFunction = new GigaTileProcessFunction(batchBackedGroupBy,
                                                       inputSchema,
                                                       firstSeenKeyGraceMillis = graceMillis)
    val originalHarness = harness(originalFunction)

    originalHarness.open()
    originalHarness.setProcessingTime(startProcessingTime)
    advanceConnectedHealthyLiveWatermark(originalHarness, startProcessingTime)
    originalHarness.processElement1(event(firstKey, eventTime, 5L), eventTime)
    originalHarness.processElement1(event(secondKey, eventTime, 7L), eventTime)
    val baselineProcessingTime = startProcessingTime + graceMillis + 1L
    originalHarness.setProcessingTime(baselineProcessingTime)
    advanceConnectedHealthyLiveWatermark(originalHarness, baselineProcessingTime)
    originalHarness.processElement1(event(firstKey, eventTime + 1L, 0L), eventTime + 1L)
    originalHarness.processElement1(event(secondKey, eventTime + 1L, 0L), eventTime + 1L)
    originalHarness.extractOutputValues().asScala
      .map(output => output.keys.get(0).toString -> decodeSum(output, batchBackedGroupBy))
      .toMap shouldEqual Map(firstKey -> 5L, secondKey -> 7L)
    keys.foreach { key =>
      setCurrentKey(originalHarness, entityKey(key))
      state[Array[Byte]](originalFunction, "batchIrState").value() shouldBe null
      state[java.lang.Boolean](originalFunction, "syntheticEmptyBatchBaselineState").value() shouldEqual
        java.lang.Boolean.TRUE
      state[java.lang.Boolean](originalFunction, "pendingPublicationState").value() shouldEqual
        java.lang.Boolean.TRUE
      state[java.lang.Boolean](originalFunction, "syntheticRollbackCorrectionState").value() shouldEqual
        java.lang.Boolean.TRUE
    }
    setCurrentKey(originalHarness, entityKey(firstKey))
    val firstCorrectionTimer =
      state[java.lang.Long](originalFunction, "nextProcessingEvictionTimerState").value().longValue()
    val snapshot = originalHarness.snapshot(32L, baselineProcessingTime)
    originalHarness.close()

    val restoredFunction = new GigaTileProcessFunction(batchBackedGroupBy, inputSchema)
    val restoredHarness = harness(restoredFunction)
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(snapshot)
      restoredHarness.open()
      // No new event arrives. The first restored timer disables synthetic availability while
      // the connected watermark is still catching up; the next live timer must publish the
      // null-fenced correction so a C4 rollback cannot leave the old KV value serving.
      restoredHarness.setProcessingTime(firstCorrectionTimer)
      advanceConnectedHealthyLiveWatermark(restoredHarness, firstCorrectionTimer)
      val liveCorrectionTimers = keys.map { key =>
        setCurrentKey(restoredHarness, entityKey(key))
        state[java.lang.Boolean](restoredFunction, "syntheticEmptyBatchBaselineState").value() shouldBe null
        state[java.lang.Boolean](restoredFunction, "syntheticRollbackCorrectionState").value() shouldEqual
          java.lang.Boolean.TRUE
        state[java.lang.Boolean](restoredFunction, "pendingPublicationState").value() shouldEqual
          java.lang.Boolean.TRUE
        state[java.lang.Long](restoredFunction, "nextProcessingEvictionTimerState").value().longValue()
      }
      liveCorrectionTimers.foreach(_ should be > firstCorrectionTimer)
      liveCorrectionTimers.distinct.sorted.foreach { liveCorrectionTimer =>
        restoredHarness.setProcessingTime(liveCorrectionTimer - 1L)
        advanceConnectedHealthyLiveWatermark(restoredHarness, liveCorrectionTimer)
        restoredHarness.setProcessingTime(liveCorrectionTimer)
      }

      val corrections = restoredHarness.extractOutputValues().asScala
      corrections should have size 2
      corrections.map(_.keys.get(0).toString).toSet shouldEqual keys.toSet
      corrections.foreach { correction =>
        decodeSum(correction, batchBackedGroupBy) shouldBe null
      }
      keys.foreach { key =>
        setCurrentKey(restoredHarness, entityKey(key))
        state[Array[Byte]](restoredFunction, "batchIrState").value() shouldBe null
        state[java.lang.Boolean](restoredFunction, "pendingEmptyBatchBaselineState").value() shouldBe null
        state[java.lang.Boolean](restoredFunction, "syntheticEmptyBatchBaselineState").value() shouldBe null
        state[java.lang.Boolean](restoredFunction, "pendingPublicationState").value() shouldBe null
        state[java.lang.Boolean](restoredFunction, "syntheticRollbackCorrectionState").value() shouldBe null
      }
    } finally restoredHarness.close()
  }

  it should "publish only baseline-safe corrections for live keys when the option is disabled" in {
    val graceMillis = 1L
    val startProcessingTime = tenDays + oneHour
    val eventTime = startProcessingTime - 1000L
    val firstKey = "entity-a"
    val secondKey = "entity-b"
    val initialValues = Map(firstKey -> 5L, secondKey -> 7L)
    val mixedGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "gigatile-first-seen-mixed-window"),
      aggregations = Seq(
        Builders.Aggregation(Operation.MAX, "num", Seq(new Window(1, TimeUnit.HOURS))),
        Builders.Aggregation(Operation.SUM, "num", Seq(new Window(7, TimeUnit.DAYS)))
      ))
    val originalFunction = new GigaTileProcessFunction(mixedGroupBy,
                                                       inputSchema,
                                                       firstSeenKeyGraceMillis = graceMillis)
    val originalHarness = harness(originalFunction)

    originalHarness.open()
    originalHarness.setProcessingTime(startProcessingTime)
    advanceConnectedHealthyLiveWatermark(originalHarness, startProcessingTime)
    initialValues.foreach { case (key, value) =>
      originalHarness.processElement1(event(key, eventTime, value), eventTime)
    }
    val baselineProcessingTime = startProcessingTime + graceMillis + 1L
    originalHarness.setProcessingTime(baselineProcessingTime)
    advanceConnectedHealthyLiveWatermark(originalHarness, baselineProcessingTime)
    initialValues.keys.foreach { key =>
      originalHarness.processElement1(event(key, eventTime + 1L, 0L), eventTime + 1L)
    }
    originalHarness.setProcessingTime(baselineProcessingTime + 1L)
    val snapshot = originalHarness.snapshot(33L, baselineProcessingTime + 1L)
    originalHarness.close()

    val restoredFunction = new GigaTileProcessFunction(mixedGroupBy, inputSchema)
    val restoredHarness = harness(restoredFunction)
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(snapshot)
      restoredHarness.open()
      val restoredProcessingTime = baselineProcessingTime + 2L
      restoredHarness.setProcessingTime(restoredProcessingTime)
      advanceConnectedHealthyLiveWatermark(restoredHarness, restoredProcessingTime)
      initialValues.keys.foreach { key =>
        restoredHarness.processElement1(event(key, restoredProcessingTime - 1L, 11L),
                                        restoredProcessingTime - 1L)
      }

      val corrections = restoredHarness.extractOutputValues().asScala
      corrections should have size 2
      val correctionsByKey = corrections.map(correction => correction.keys.get(0).toString -> correction).toMap
      correctionsByKey.keySet shouldEqual initialValues.keySet
      initialValues.keys.foreach { key =>
        decodeOutputField(correctionsByKey(key), mixedGroupBy, 0) shouldEqual 11L
        decodeOutputField(correctionsByKey(key), mixedGroupBy, 1) shouldBe null
      }

      restoredHarness.setProcessingTime(restoredProcessingTime + 1L)
      advanceConnectedHealthyLiveWatermark(restoredHarness, restoredProcessingTime + 1L)
      initialValues.keys.foreach { key =>
        restoredHarness.processElement1(event(key, restoredProcessingTime, 13L), restoredProcessingTime)
      }
      restoredHarness.extractOutputValues() should have size 2
    } finally restoredHarness.close()
  }

  it should "not resurrect a synthetic baseline after a marker-blind rollback checkpoint" in {
    val graceMillis = 1L
    val startProcessingTime = tenDays + oneHour
    val eventTime = startProcessingTime - 1000L
    val originalFunction = new GigaTileProcessFunction(batchBackedGroupBy,
                                                       inputSchema,
                                                       firstSeenKeyGraceMillis = graceMillis)
    val originalHarness = harness(originalFunction)
    originalHarness.open()
    originalHarness.setProcessingTime(startProcessingTime)
    advanceConnectedHealthyLiveWatermark(originalHarness, startProcessingTime)
    originalHarness.processElement1(event(DefaultEntity, eventTime, 5L), eventTime)
    val baselineProcessingTime = startProcessingTime + graceMillis + 1L
    originalHarness.setProcessingTime(baselineProcessingTime)
    advanceConnectedHealthyLiveWatermark(originalHarness, baselineProcessingTime)
    originalHarness.processElement1(event(DefaultEntity, eventTime + 1L, 0L), eventTime + 1L)
    setCurrentKey(originalHarness, entityKey(DefaultEntity))
    state[Array[Byte]](originalFunction, "batchIrState").value() shouldBe null
    state[java.lang.Boolean](originalFunction, "syntheticEmptyBatchBaselineState").value() shouldEqual
      java.lang.Boolean.TRUE
    state[java.lang.Boolean](originalFunction, "syntheticRollbackCorrectionState").value() shouldEqual
      java.lang.Boolean.TRUE
    state[java.lang.Boolean](originalFunction, "pendingPublicationState").value() shouldEqual
      java.lang.Boolean.TRUE
    val correctionTimer =
      state[java.lang.Long](originalFunction, "nextProcessingEvictionTimerState").value().longValue()
    val originalSnapshot = originalHarness.snapshot(40L, baselineProcessingTime)
    originalHarness.close()

    val markerBlindFunction = new MarkerBlindCorrectionConsumer(expectedTimer = Some(correctionTimer))
    val markerBlindHarness = harness(markerBlindFunction)
    markerBlindHarness.setup()
    markerBlindHarness.initializeState(originalSnapshot)
    markerBlindHarness.open()
    markerBlindHarness.setProcessingTime(correctionTimer)
    markerBlindFunction.correctionConsumed shouldBe true
    val markerBlindSnapshot = markerBlindHarness.snapshot(41L, correctionTimer)
    markerBlindHarness.close()

    val restoredFunction = new GigaTileProcessFunction(batchBackedGroupBy,
                                                       inputSchema,
                                                       firstSeenKeyGraceMillis = graceMillis)
    val restoredHarness = harness(restoredFunction)
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(markerBlindSnapshot)
      restoredHarness.open()
      restoredHarness.setProcessingTime(correctionTimer)
      restoredHarness.processElement1(event(DefaultEntity, eventTime + 2L, 0L), eventTime + 2L)

      setCurrentKey(restoredHarness, entityKey(DefaultEntity))
      state[Array[Byte]](restoredFunction, "batchIrState").value() shouldBe null
      state[java.lang.Boolean](restoredFunction, "syntheticEmptyBatchBaselineState").value() shouldBe null
      state[java.lang.Boolean](restoredFunction, "syntheticRollbackCorrectionState").value() shouldBe null
      state[java.lang.Boolean](restoredFunction, "pendingEmptyBatchBaselineState").value() shouldEqual
        java.lang.Boolean.TRUE
    } finally restoredHarness.close()
  }

  it should "reconcile every key's synthetic baseline after a marker-blind rollback checkpoint" in {
    // Regression: the per-key rollback reconciliation must run for every key on the subtask,
    // not only the first callback that seeds the operator-wide grace clock. Two keys both hold a
    // published synthetic baseline; a marker-blind binary serves the null correction for both and
    // clears their pending markers; on roll-forward both must drop the stale synthetic baseline.
    val graceMillis = 1L
    val firstKey = "entity-a"
    val secondKey = "entity-b"
    val startProcessingTime = tenDays + oneHour
    val eventTime = startProcessingTime - 1000L
    val originalFunction = new GigaTileProcessFunction(batchBackedGroupBy,
                                                       inputSchema,
                                                       firstSeenKeyGraceMillis = graceMillis)
    val originalHarness = harness(originalFunction)
    originalHarness.open()
    originalHarness.setProcessingTime(startProcessingTime)
    advanceConnectedHealthyLiveWatermark(originalHarness, startProcessingTime)
    originalHarness.processElement1(event(firstKey, eventTime, 5L), eventTime)
    originalHarness.processElement1(event(secondKey, eventTime, 9L), eventTime)
    val baselineProcessingTime = startProcessingTime + graceMillis + 1L
    originalHarness.setProcessingTime(baselineProcessingTime)
    advanceConnectedHealthyLiveWatermark(originalHarness, baselineProcessingTime)
    originalHarness.processElement1(event(firstKey, eventTime + 1L, 0L), eventTime + 1L)
    originalHarness.processElement1(event(secondKey, eventTime + 1L, 0L), eventTime + 1L)

    Seq(firstKey, secondKey).foreach { key =>
      setCurrentKey(originalHarness, entityKey(key))
      state[java.lang.Boolean](originalFunction, "syntheticEmptyBatchBaselineState").value() shouldEqual
        java.lang.Boolean.TRUE
      state[java.lang.Boolean](originalFunction, "syntheticRollbackCorrectionState").value() shouldEqual
        java.lang.Boolean.TRUE
      state[java.lang.Boolean](originalFunction, "pendingPublicationState").value() shouldEqual
        java.lang.Boolean.TRUE
    }
    // Both keys share the day-aligned eviction cadence, so their correction timers coincide.
    setCurrentKey(originalHarness, entityKey(firstKey))
    val correctionTimer =
      state[java.lang.Long](originalFunction, "nextProcessingEvictionTimerState").value().longValue()
    val originalSnapshot = originalHarness.snapshot(50L, baselineProcessingTime)
    originalHarness.close()

    val markerBlindFunction = new MarkerBlindCorrectionConsumer(expectedTimer = Some(correctionTimer))
    val markerBlindHarness = harness(markerBlindFunction)
    markerBlindHarness.setup()
    markerBlindHarness.initializeState(originalSnapshot)
    markerBlindHarness.open()
    markerBlindHarness.setProcessingTime(correctionTimer)
    markerBlindFunction.correctionConsumed shouldBe true
    val markerBlindSnapshot = markerBlindHarness.snapshot(51L, correctionTimer)
    markerBlindHarness.close()

    val restoredFunction = new GigaTileProcessFunction(batchBackedGroupBy,
                                                       inputSchema,
                                                       firstSeenKeyGraceMillis = graceMillis)
    val restoredHarness = harness(restoredFunction)
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(markerBlindSnapshot)
      restoredHarness.open()
      restoredHarness.setProcessingTime(correctionTimer)
      advanceConnectedHealthyLiveWatermark(restoredHarness, correctionTimer)
      restoredHarness.processElement1(event(firstKey, eventTime + 2L, 0L), eventTime + 2L)
      restoredHarness.processElement1(event(secondKey, eventTime + 2L, 0L), eventTime + 2L)

      Seq(firstKey, secondKey).foreach { key =>
        setCurrentKey(restoredHarness, entityKey(key))
        withClue(s"synthetic baseline for $key should be dropped after marker-blind rollback: ") {
          state[Array[Byte]](restoredFunction, "batchIrState").value() shouldBe null
          state[java.lang.Boolean](restoredFunction, "syntheticEmptyBatchBaselineState").value() shouldBe null
          state[java.lang.Boolean](restoredFunction, "syntheticRollbackCorrectionState").value() shouldBe null
          state[java.lang.Boolean](restoredFunction, "pendingEmptyBatchBaselineState").value() shouldEqual
            java.lang.Boolean.TRUE
        }
      }
    } finally restoredHarness.close()
  }

  it should "restart grace after marker-blind rollback consumes a newer fenced synthetic update" in {
    val graceMillis = oneHour
    val startProcessingTime = tenDays + oneHour
    val eventTime = startProcessingTime - 1000L
    val originalFunction = new GigaTileProcessFunction(batchBackedGroupBy,
                                                       inputSchema,
                                                       firstSeenKeyGraceMillis = graceMillis)
    val originalHarness = harness(originalFunction)
    originalHarness.open()
    originalHarness.setProcessingTime(startProcessingTime)
    advanceConnectedHealthyLiveWatermark(originalHarness, startProcessingTime)
    originalHarness.processElement1(event(DefaultEntity, eventTime, 5L), eventTime)
    originalHarness.extractOutputValues() shouldBe empty

    val baselineProcessingTime = startProcessingTime + graceMillis + 1L
    originalHarness.setProcessingTime(baselineProcessingTime)
    advanceConnectedHealthyLiveWatermark(originalHarness, baselineProcessingTime)
    originalHarness.processElement1(event(DefaultEntity, eventTime + 1L, 0L), eventTime + 1L)
    // Watermark activation and the event share one processing-time version. Flush their
    // coalesced snapshot before asserting that the synthetic handoff is complete.
    originalHarness.setProcessingTime(baselineProcessingTime + 1L)
    decodeSum(originalHarness.extractOutputValues().asScala.last, batchBackedGroupBy) shouldEqual 5L
    setCurrentKey(originalHarness, entityKey(DefaultEntity))
    state[java.lang.Boolean](originalFunction, "syntheticEmptyBatchBaselineState").value() shouldEqual
      java.lang.Boolean.TRUE
    state[java.lang.Boolean](originalFunction, "syntheticRollbackCorrectionState").value() shouldEqual
      java.lang.Boolean.TRUE

    // Let the connected watermark become stale, then mutate the synthetic view while fenced.
    // This is the checkpoint shape that clears the correction marker but still owns a pending
    // publication through the legacy descriptor.
    val catchupProcessingTime = baselineProcessingTime + 10L * oneHour
    originalHarness.setProcessingTime(catchupProcessingTime)
    originalHarness.processElement1(event(DefaultEntity, eventTime + 2L, 7L), eventTime + 2L)
    setCurrentKey(originalHarness, entityKey(DefaultEntity))
    state[java.lang.Boolean](originalFunction, "syntheticEmptyBatchBaselineState").value() shouldEqual
      java.lang.Boolean.TRUE
    state[java.lang.Boolean](originalFunction, "pendingPublicationState").value() shouldEqual
      java.lang.Boolean.TRUE
    state[java.lang.Boolean](originalFunction, "syntheticRollbackCorrectionState").value() shouldBe null
    val correctionTimer =
      state[java.lang.Long](originalFunction, "nextProcessingEvictionTimerState").value().longValue()
    val originalSnapshot = originalHarness.snapshot(42L, catchupProcessingTime)
    originalHarness.close()

    val markerBlindFunction = new MarkerBlindCorrectionConsumer(expectedTimer = Some(correctionTimer))
    val markerBlindHarness = harness(markerBlindFunction)
    markerBlindHarness.setup()
    markerBlindHarness.initializeState(originalSnapshot)
    markerBlindHarness.open()
    markerBlindHarness.setProcessingTime(catchupProcessingTime)
    markerBlindHarness.setProcessingTime(correctionTimer)
    markerBlindFunction.correctionConsumed shouldBe true
    val markerBlindSnapshot = markerBlindHarness.snapshot(43L, correctionTimer)
    markerBlindHarness.close()

    val restoredFunction = new GigaTileProcessFunction(batchBackedGroupBy,
                                                       inputSchema,
                                                       firstSeenKeyGraceMillis = graceMillis)
    val restoredHarness = harness(restoredFunction)
    try {
      restoredHarness.setup()
      restoredHarness.initializeState(markerBlindSnapshot)
      restoredHarness.open()
      restoredHarness.setProcessingTime(correctionTimer)
      advanceConnectedHealthyLiveWatermark(restoredHarness, correctionTimer)
      restoredHarness.processElement1(event(DefaultEntity, correctionTimer - 1L, 0L), correctionTimer - 1L)

      restoredHarness.extractOutputValues().asScala.foreach { output =>
        decodeSum(output, batchBackedGroupBy) shouldBe null
      }
      setCurrentKey(restoredHarness, entityKey(DefaultEntity))
      state[java.lang.Boolean](restoredFunction, "syntheticEmptyBatchBaselineState").value() shouldBe null
      state[java.lang.Boolean](restoredFunction, "syntheticRollbackCorrectionState").value() shouldBe null
      state[java.lang.Boolean](restoredFunction, "pendingEmptyBatchBaselineState").value() shouldEqual
        java.lang.Boolean.TRUE
      transientLong(restoredFunction, "emptyBatchBaselineSafeAfterMillis") shouldEqual
        correctionTimer + graceMillis
    } finally restoredHarness.close()
  }

  it should "share the operator warm clock while isolating per-key baseline decisions" in {
    val graceMillis = 1000L
    val firstKey = "entity-a"
    val secondKey = "entity-b"
    val function = new GigaTileProcessFunction(batchBackedGroupBy,
                                               inputSchema,
                                               firstSeenKeyGraceMillis = graceMillis)
    val testHarness = harness(function)
    val startProcessingTime = tenDays + oneHour

    try {
      testHarness.open()
      testHarness.setProcessingTime(startProcessingTime)
      advanceConnectedHealthyLiveWatermark(testHarness, startProcessingTime)
      testHarness.processElement1(event(firstKey, startProcessingTime - 1000L, 5L), startProcessingTime - 1000L)

      val warmProcessingTime = startProcessingTime + graceMillis + 1L
      testHarness.setProcessingTime(warmProcessingTime)
      advanceConnectedHealthyLiveWatermark(testHarness, warmProcessingTime)
      testHarness.processElement1(event(secondKey, warmProcessingTime - 1L, 7L), warmProcessingTime - 1L)

      val secondKeyOutput = testHarness.extractOutputValues().asScala
        .filter(_.keys.get(0).toString == secondKey)
        .last
      decodeSum(secondKeyOutput, batchBackedGroupBy) shouldEqual 7L

      testHarness.processElement2(batchRow(firstKey, batchBackedGroupBy, tenDays, 11L), tenDays)

      setCurrentKey(testHarness, entityKey(firstKey))
      state[java.lang.Long](function, "batchEndTsState").value().longValue() shouldEqual tenDays
      state[java.lang.Boolean](function, "pendingEmptyBatchBaselineState").value() shouldBe null
      setCurrentKey(testHarness, entityKey(secondKey))
      state[Array[Byte]](function, "batchIrState").value() shouldBe null
      state[java.lang.Boolean](function, "syntheticEmptyBatchBaselineState").value() shouldEqual
        java.lang.Boolean.TRUE
      state[java.lang.Long](function, "batchEndTsState").value() shouldBe null
    } finally testHarness.close()
  }

  it should "initialize before flushing a coincident eviction and cadence timer" in {
    val cadenceMillis = 1L
    val collisionTime = 11L * dayMillis
    val eventProcessingTime = collisionTime - 1L
    val function = new GigaTileProcessFunction(batchBackedGroupBy,
                                               inputSchema,
                                               bufferingOutputTimeMillis = cadenceMillis,
                                               firstSeenKeyGraceMillis = 1L)
    val testHarness = harness(function)

    try {
      testHarness.open()
      testHarness.setProcessingTime(eventProcessingTime)
      advanceConnectedHealthyLiveWatermark(testHarness, eventProcessingTime)
      testHarness.processElement1(event(DefaultEntity, eventProcessingTime - 1000L, 5L), eventProcessingTime - 1000L)
      testHarness.extractOutputValues() shouldBe empty

      testHarness.setProcessingTime(collisionTime)
      testHarness.extractOutputValues() shouldBe empty
      testHarness.setProcessingTime(collisionTime + cadenceMillis)

      val outputs = testHarness.extractOutputValues()
      outputs should have size 1
      decodeSum(outputs.get(0), batchBackedGroupBy) shouldEqual 5L
      outputs.get(0).latestTsMillis shouldEqual collisionTime
    } finally testHarness.close()
  }

  it should "not initialize from collision-only or retry-only timers" in {
    val collisionFunction = new GigaTileProcessFunction(batchBackedGroupBy,
                                                        inputSchema,
                                                        firstSeenKeyGraceMillis = 1L)
    val collisionHarness = harness(collisionFunction)
    val startProcessingTime = tenDays + oneHour
    val eventTime = startProcessingTime - 1000L

    try {
      collisionHarness.open()
      collisionHarness.setProcessingTime(startProcessingTime)
      advanceConnectedHealthyLiveWatermark(collisionHarness, startProcessingTime)
      collisionHarness.processElement1(event(DefaultEntity, eventTime, 5L), eventTime)
      collisionHarness.processElement1(event(DefaultEntity, eventTime + 1L, 0L), eventTime + 1L)
      collisionHarness.setProcessingTime(startProcessingTime + 1L)

      setCurrentKey(collisionHarness, entityKey(DefaultEntity))
      state[Array[Byte]](collisionFunction, "batchIrState").value() shouldBe null
    } finally collisionHarness.close()

    val retryDelay = math.max(1L, 2L * FlinkJob.AutoWatermarkInterval)
    val retryFunction = new GigaTileProcessFunction(batchBackedGroupBy,
                                                    inputSchema,
                                                    firstSeenKeyGraceMillis = retryDelay)
    val retryHarness = harness(retryFunction)
    val catchupWatermark = startProcessingTime - 3L * dayMillis
    try {
      retryHarness.open()
      retryHarness.setProcessingTime(startProcessingTime)
      advanceConnectedWatermark(retryHarness, catchupWatermark)
      retryHarness.processElement1(event(DefaultEntity, catchupWatermark - 1L, 5L), catchupWatermark - 1L)
      retryHarness.setProcessingTime(startProcessingTime + retryDelay)

      retryHarness.extractOutputValues() shouldBe empty
      setCurrentKey(retryHarness, entityKey(DefaultEntity))
      state[Array[Byte]](retryFunction, "batchIrState").value() shouldBe null
    } finally retryHarness.close()
  }

  it should "leave a no-batch-only GroupBy outside the fallback path" in {
    val function = new GigaTileProcessFunction(noBatchGroupBy,
                                               inputSchema,
                                               firstSeenKeyGraceMillis = 1L)
    val testHarness = harness(function)
    val startProcessingTime = tenDays + oneHour

    try {
      testHarness.open()
      testHarness.setProcessingTime(startProcessingTime)
      advanceConnectedHealthyLiveWatermark(testHarness, startProcessingTime)
      testHarness.processElement1(event(DefaultEntity, startProcessingTime - 1000L, 5L), startProcessingTime - 1000L)

      decodeSum(testHarness.extractOutputValues().get(0), noBatchGroupBy) shouldEqual 5L
      setCurrentKey(testHarness, entityKey(DefaultEntity))
      state[Array[Byte]](function, "batchIrState").value() shouldBe null
      state[java.lang.Boolean](function, "pendingEmptyBatchBaselineState").value() shouldBe null
    } finally testHarness.close()
  }

  it should "leave the fallback disabled by default" in {
    GigaTileProcessFunction.DefaultFirstSeenKeyGraceMillis shouldEqual 0L
    val function = new GigaTileProcessFunction(batchBackedGroupBy, inputSchema)
    val testHarness = harness(function)
    val startProcessingTime = tenDays + oneHour

    try {
      testHarness.open()
      testHarness.setProcessingTime(startProcessingTime)
      advanceConnectedHealthyLiveWatermark(testHarness, startProcessingTime)
      testHarness.processElement1(event(DefaultEntity, startProcessingTime - 1000L, 5L), startProcessingTime - 1000L)

      testHarness.extractOutputValues() shouldBe empty
      setCurrentKey(testHarness, entityKey(DefaultEntity))
      state[Array[Byte]](function, "batchIrState").value() shouldBe null
      state[java.lang.Boolean](function, "pendingEmptyBatchBaselineState").value() shouldBe null
    } finally testHarness.close()
  }

  it should "reject a negative grace at the operator boundary" in {
    an[IllegalArgumentException] should be thrownBy new GigaTileProcessFunction(
      batchBackedGroupBy,
      inputSchema,
      firstSeenKeyGraceMillis = -1L)
  }

  it should "saturate the operator grace deadline instead of overflowing" in {
    val function = new GigaTileProcessFunction(batchBackedGroupBy,
                                               inputSchema,
                                               firstSeenKeyGraceMillis = 10L)
    val testHarness = harness(function)
    val eventTime = tenDays + oneHour

    try {
      testHarness.open()
      testHarness.setProcessingTime(Long.MaxValue - 5L)
      testHarness.processElement1(event(DefaultEntity, eventTime, 5L), eventTime)

      transientLong(function, "emptyBatchBaselineSafeAfterMillis") shouldEqual Long.MaxValue
      setCurrentKey(testHarness, entityKey(DefaultEntity))
      state[Array[Byte]](function, "batchIrState").value() shouldBe null
    } finally testHarness.close()
  }
}

object GigaTileFirstSeenKeyGraceTest {
  private val DefaultEntity = "entity-1"
  private val dayMillis = new Window(1, TimeUnit.DAYS).millis
  private val oneHour = new Window(1, TimeUnit.HOURS).millis
  private val tenDays = 10L * dayMillis
  private val inputSchema: Seq[(String, DataType)] =
    Seq(Constants.TimeColumn -> LongType, "num" -> LongType)
  private val batchBackedGroupBy = groupBy("gigatile-first-seen-batch-backed", new Window(7, TimeUnit.DAYS))
  private val noBatchGroupBy = groupBy("gigatile-first-seen-no-batch", new Window(1, TimeUnit.HOURS))

  private def groupBy(name: String, window: Window): GroupBy =
    Builders.GroupBy(
      metaData = Builders.MetaData(name = name),
      aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window))))

  private def harness(
      function: KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile]
  ): KeyedTwoInputStreamOperatorTestHarness[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile] =
    new KeyedTwoInputStreamOperatorTestHarness[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile](
      new KeyedCoProcessOperator(function),
      new KeySelector[ProjectedEvent, util.List[Any]] {
        override def getKey(value: ProjectedEvent): util.List[Any] =
          entityKey(value.fields.getOrElse("entity", DefaultEntity).toString)
      },
      new KeySelector[BatchIrRow, util.List[Any]] {
        override def getKey(value: BatchIrRow): util.List[Any] = value.entityKeys
      },
      TypeInformation
        .of(classOf[util.List[_]])
        .asInstanceOf[TypeInformation[util.List[Any]]])

  /** Models a rollback binary that binds only the legacy correction descriptors and
    * checkpoints without knowing the additive synthetic markers.
    */
  private class MarkerBlindCorrectionConsumer(expectedTimer: Option[Long] = None)
      extends KeyedCoProcessFunction[util.List[Any], ProjectedEvent, BatchIrRow, TimestampedTile] {
    @transient private var batchIrState: ValueState[Array[Byte]] = _
    @transient private var pendingPublicationState: ValueState[java.lang.Boolean] = _
    @transient private var nextEvictionTimerState: ValueState[java.lang.Long] = _
    var correctionConsumed: Boolean = false

    override def open(parameters: org.apache.flink.configuration.Configuration): Unit = {
      batchIrState = getRuntimeContext.getState(
        new ValueStateDescriptor[Array[Byte]]("giga-tile-batch-ir", classOf[Array[Byte]]))
      pendingPublicationState = getRuntimeContext.getState(
        new ValueStateDescriptor[java.lang.Boolean]("giga-tile-pending-publication", classOf[java.lang.Boolean]))
      nextEvictionTimerState = getRuntimeContext.getState(
        new ValueStateDescriptor[java.lang.Long]("giga-tile-next-evict-pt-timer", classOf[java.lang.Long]))
    }

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
    ): Unit = {
      if (expectedTimer.forall(_ == timestamp)) {
        require(batchIrState.value() == null)
        require(java.lang.Boolean.TRUE.equals(pendingPublicationState.value()))
        pendingPublicationState.clear()
        nextEvictionTimerState.clear()
        correctionConsumed = true
      }
    }
  }

  private def event(entity: String, timestamp: Long, value: Long): ProjectedEvent =
    ProjectedEvent(Map("entity" -> entity, Constants.TimeColumn -> timestamp, "num" -> value), timestamp)

  private def entityKey(entity: String): util.List[Any] = {
    val result = new util.ArrayList[Any](1)
    result.add(entity)
    result
  }

  private def batchRow(entity: String, targetGroupBy: GroupBy, batchEnd: Long, value: Long): BatchIrRow = {
    val batchEventTime = batchEnd - 1000L
    val aggregator = new SawtoothOnlineAggregator(
      batchEnd,
      targetGroupBy.getAggregations.asScala.toSeq,
      inputSchema)
    val batchIr = aggregator.update(
      aggregator.init,
      new ArrayRow(Array[Any](batchEventTime, value), batchEventTime))
    val finalBatchIr = aggregator.denormalizeBatchIr(aggregator.normalizeBatchIr(batchIr))
    val codec = new GigaTileCodec(targetGroupBy, inputSchema)
    new BatchIrRow(entityKey(entity), codec.encodeBatchIr(finalBatchIr), batchEnd)
  }

  private def advanceConnectedWatermark(
      testHarness: KeyedTwoInputStreamOperatorTestHarness[
        util.List[Any],
        ProjectedEvent,
        BatchIrRow,
        TimestampedTile],
      watermark: Long
  ): Unit = {
    testHarness.processWatermark1(new Watermark(watermark))
    testHarness.processWatermark2(new Watermark(watermark))
  }

  private def advanceConnectedHealthyLiveWatermark(
      testHarness: KeyedTwoInputStreamOperatorTestHarness[
        util.List[Any],
        ProjectedEvent,
        BatchIrRow,
        TimestampedTile],
      processingTime: Long
  ): Unit =
    advanceConnectedWatermark(testHarness,
                              processingTime - FlinkJob.AllowedOutOfOrderness.toMillis - 1L)

  private def nextDay(timestamp: Long): Long = TsUtils.round(timestamp, dayMillis) + dayMillis

  private def nextBufferedWriteTick(
      targetGroupBy: GroupBy,
      key: util.List[Any],
      currentProcessingTime: Long,
      cadenceMillis: Long
  ): Long = {
    val phase =
      Math.floorMod((targetGroupBy.getMetaData.getName :: key.iterator().asScala.toList).hashCode().toLong,
                    cadenceMillis)
    val elapsedSincePhase = Math.floorMod(Math.floorMod(currentProcessingTime, cadenceMillis) - phase, cadenceMillis)
    val delay = if (elapsedSincePhase == 0L) cadenceMillis else cadenceMillis - elapsedSincePhase
    currentProcessingTime + delay
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

  private def state[T](function: GigaTileProcessFunction, fieldName: String): ValueState[T] = {
    val field = classOf[GigaTileProcessFunction].getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(function).asInstanceOf[ValueState[T]]
  }

  private def transientLong(function: GigaTileProcessFunction, fieldName: String): Long = {
    val field = classOf[GigaTileProcessFunction].getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(function).asInstanceOf[java.lang.Long].longValue()
  }

  private def initializeEmptyBatchBaseline(
      function: GigaTileProcessFunction,
      currentProcessingTime: Long,
      largeWindowAsOfMillis: Long
  ): Option[GigaEmitResult] = {
    val method = classOf[GigaTileProcessFunction]
      .getDeclaredMethod("initializeEmptyBatchBaselineIfReady", java.lang.Long.TYPE, java.lang.Long.TYPE)
    method.setAccessible(true)
    method
      .invoke(function, Long.box(currentProcessingTime), Long.box(largeWindowAsOfMillis))
      .asInstanceOf[Option[GigaEmitResult]]
  }

  private def decodeSum(tile: TimestampedTile, targetGroupBy: GroupBy): AnyRef = {
    decodeOutputField(tile, targetGroupBy, 0)
  }

  private def decodeOutputField(tile: TimestampedTile, targetGroupBy: GroupBy, fieldIndex: Int): AnyRef = {
    val outputCodec = new GigaTileCodec(targetGroupBy, inputSchema)
    val fieldName = outputCodec.outputSchema.fields(fieldIndex).name
    AvroCodec
      .of(ai.chronon.online.serde.AvroConversions.fromChrononSchema(outputCodec.outputSchema).toString)
      .decodeMap(tile.tileBytes)(fieldName)
  }
}
