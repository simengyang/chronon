package ai.chronon.aggregator.test

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.windowing._
import ai.chronon.api._
import ai.chronon.api.Extensions.{AggregationOps, WindowOps}
import com.google.gson.Gson
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Tests the full GigaTile streaming pipeline:
  * GigaTileStreamProcessor replays events + batch IR, emitting finalized feature vectors.
  * Results compared against NaiveAggregator.
  *
  * Key differences from MegaTileStreamProcessorTest:
  * - Batch IR is loaded into the processor (simulating Iceberg connected stream)
  * - Output is a finalized vector (not a windowed IR entry)
  * - No separate fetcher merge step — the processor IS the merge
  */
class GigaTileStreamProcessorTest extends AnyFlatSpec {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val gson = new Gson

  val AllWindows: Seq[Window] = Seq(
    new Window(6, TimeUnit.HOURS),
    new Window(1, TimeUnit.DAYS),
    new Window(47, TimeUnit.HOURS),
    new Window(2, TimeUnit.DAYS),
    new Window(49, TimeUnit.HOURS),
    new Window(3, TimeUnit.DAYS),
    new Window(7, TimeUnit.DAYS)
  )

  val TailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis
  val DayMillis: Long = new Window(1, TimeUnit.DAYS).millis
  val Epsilon = 1e-6

  def approxEqual(a: Any, b: Any, sketchTolerance: Double = 0.0): Boolean = (a, b) match {
    case (null, null)                         => true
    case (null, _) | (_, null)                => false
    case (x: Double, y: Double)               => Math.abs(x - y) <= Epsilon * Math.max(1.0, Math.max(Math.abs(x), Math.abs(y)))
    case (x: Float, y: Float)                 => Math.abs(x - y) <= Epsilon.toFloat * Math.max(1.0f, Math.max(Math.abs(x), Math.abs(y)))
    case (x: Long, y: Long) if sketchTolerance > 0 =>
      x == y || Math.abs(x - y).toDouble <= sketchTolerance * Math.max(1.0, Math.max(Math.abs(x), Math.abs(y)).toDouble)
    case (x: java.util.List[_], y: java.util.List[_]) =>
      x.size() == y.size() && (0 until x.size()).forall(i => approxEqual(x.get(i), y.get(i), sketchTolerance))
    case (x: java.util.Map[_, _], y: java.util.Map[_, _]) =>
      x.size() == y.size() && x.keySet().toArray.forall(k => approxEqual(x.get(k), y.get(k), sketchTolerance))
    case (x: Array[_], y: Array[_]) =>
      x.length == y.length && x.zip(y).forall { case (a, b) => approxEqual(a, b, sketchTolerance) }
    case _ => a == b
  }

  def compareResults(actual: Array[Array[Any]], expected: Array[Array[Any]], queryTimes: Array[Long],
                     label: String, sketchTolerance: Double = 0.0): Unit = {
    assertEquals(s"$label: result count mismatch", expected.length, actual.length)
    for (i <- queryTimes.indices) {
      if (!approxEqual(actual(i), expected(i), sketchTolerance)) {
        val expStr = gson.toJson(expected(i))
        val actStr = gson.toJson(actual(i))
        fail(s"$label: mismatch at query ${queryTimes(i)} (index $i)\n  expected: $expStr\n  got:      $actStr")
      }
    }
  }

  def generateEvents(windowDays: Int, count: Int): (Array[TestRow], Seq[(String, DataType)]) = {
    val columns = Seq(Column("ts", LongType, windowDays), Column("num", LongType, 1000), Column("amount", DoubleType, 500))
    val data = CStream.gen(columns, count)
    (data.rows, columns.map(_.schema))
  }

  def naiveAggregate(allEvents: Array[TestRow], queryTimes: Array[Long], aggregations: Seq[Aggregation],
                     schema: Seq[(String, DataType)]): Array[Array[Any]] = {
    val unpackedParts = aggregations.flatMap(_.unpack)
    val unpacked = unpackedParts.map(_.window).toArray
    val tailHops = unpacked.map(w => FiveMinuteResolution.calculateTailHop(w))
    val rowAgg = new RowAggregator(schema, unpackedParts)
    val naiveAgg = new NaiveAggregator(rowAgg, unpacked, tailHops)
    naiveAgg.aggregate(allEvents, queryTimes).map(ir => rowAgg.finalize(ir))
  }

  /** Build a FinalBatchIr from events before batchEnd using the standard Sawtooth pipeline. */
  def buildBatchIr(allEvents: Array[TestRow], batchEnd: Long, aggregations: Seq[Aggregation],
                   schema: Seq[(String, DataType)]): FinalBatchIr = {
    val batchEvents = allEvents.filter(_.ts < batchEnd)
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    batchEvents.foreach(row => batchIr = onlineAgg.update(batchIr, row))
    onlineAgg.normalizeBatchIr(batchIr)
  }

  /** Denormalize a FinalBatchIr for use in the processor (which operates on denormalized IRs). */
  def denormalizeBatchIr(batchIr: FinalBatchIr, aggregations: Seq[Aggregation],
                         schema: Seq[(String, DataType)], batchEnd: Long): FinalBatchIr = {
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)
    onlineAgg.denormalizeBatchIr(batchIr)
  }

  /**
    * Full GigaTile simulation:
    * 1. Build batch IR from pre-batchEnd events
    * 2. Create GigaTileStreamProcessor, load batch via onBatchUpdate
    * 3. Replay streaming events through onEvent
    * 4. Simulate watermark advancement + eviction at regular intervals
    * 5. At each query: read the last emitted finalized vector
    * 6. Compare with naive
    */
  def gigaTileAggregate(allEvents: Array[TestRow],
                        queryTimes: Array[Long],
                        aggregations: Seq[Aggregation],
                        schema: Seq[(String, DataType)],
                        batchEnd: Long): Array[Array[Any]] = {

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    // Build and load batch IR
    val normalizedBatchIr = buildBatchIr(allEvents, batchEnd, aggregations, schema)
    val batchIr = denormalizeBatchIr(normalizedBatchIr, aggregations, schema, batchEnd)

    // Flink processes ALL events (sorted by timestamp)
    val streamingEvents = allEvents.sortBy(_.ts)
    val sortedQueries = queryTimes.sorted

    val evictionInterval = processor.minEvictionInterval
    var nextEvictionTs = Long.MaxValue
    var lastEmitted: Array[Any] = null

    def firePendingEvictions(upToTs: Long): Unit = {
      while (nextEvictionTs <= upToTs) {
        processor.advanceWatermark(nextEvictionTs)
        val result = processor.onEviction(nextEvictionTs)
        if (result.finalizedVector != null) lastEmitted = result.finalizedVector
        nextEvictionTs += evictionInterval
      }
    }

    var eventIdx = 0
    val resultsByQueryTs = mutable.Map[Long, Array[Any]]()

    // Load batch IR before processing events (simulates Iceberg scan on startup)
    val batchResult = processor.onBatchUpdate(batchIr, batchEnd, batchEnd)
    if (batchResult.finalizedVector != null) lastEmitted = batchResult.finalizedVector
    if (batchResult.needsEvictionTimer && nextEvictionTs == Long.MaxValue) {
      nextEvictionTs = TsUtils.round(batchEnd, evictionInterval) + evictionInterval
    }

    for (queryTs <- sortedQueries) {
      // Process all streaming events up to queryTs
      while (eventIdx < streamingEvents.length && streamingEvents(eventIdx).ts <= queryTs) {
        val event = streamingEvents(eventIdx)

        firePendingEvictions(event.ts)
        processor.advanceWatermark(event.ts)

        val result = processor.onEvent(event, event.ts)
        if (result.finalizedVector != null) lastEmitted = result.finalizedVector

        if (nextEvictionTs == Long.MaxValue) {
          nextEvictionTs = TsUtils.round(event.ts, evictionInterval) + evictionInterval
        }

        eventIdx += 1
      }

      // Fire pending evictions up to query time
      firePendingEvictions(queryTs)
      processor.advanceWatermark(queryTs)

      // Final eviction at query time to correct sawtooth
      val evictResult = processor.onEviction(queryTs)
      if (evictResult.finalizedVector != null) lastEmitted = evictResult.finalizedVector

      resultsByQueryTs(queryTs) = lastEmitted
    }

    queryTimes.map(resultsByQueryTs)
  }

  it should "match naive with batch fresh (14 day sim)" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )

    val queryTimes = Array(
      batchEnd + 6 * 3600 * 1000L,
      batchEnd + 14 * 3600 * 1000L,
      batchEnd + 23 * 3600 * 1000L
    ).filter(_ <= maxTs)

    val results = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "giga_batch_fresh")
  }

  it should "match naive with batch delayed (14 day sim)" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - 2 * DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val queryTimes = Array(
      batchEnd + DayMillis + 2 * 3600 * 1000L,
      batchEnd + DayMillis + 18 * 3600 * 1000L
    ).filter(_ <= maxTs)

    val results = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "giga_batch_delayed")
  }

  it should "handle idle entities (batch only, no streaming events)" in {
    val (events, schema) = generateEvents(14, 10000)
    val maxTs = events.map(_.ts).max
    val batchEnd = maxTs + DayMillis

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val queryTimes = Array(batchEnd + 14 * 3600 * 1000L)

    val results = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "giga_idle")
  }

  it should "match naive with all aggregation types" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows),
      Builders.Aggregation(Operation.MIN, "num", AllWindows),
      Builders.Aggregation(Operation.MAX, "num", AllWindows),
      Builders.Aggregation(Operation.LAST, "num", AllWindows),
      Builders.Aggregation(Operation.FIRST, "num", AllWindows)
    )

    val queryTimes = Array(batchEnd + 14 * 3600 * 1000L).filter(_ <= maxTs)

    val results = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "giga_multi_agg")
  }

  it should "handle day boundary transitions across multiple days" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - 2 * DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )

    val queryTimes = Array(
      batchEnd + 6 * 3600 * 1000L,
      batchEnd + 18 * 3600 * 1000L,
      batchEnd + 30 * 3600 * 1000L,
      batchEnd + 42 * 3600 * 1000L
    ).filter(_ <= maxTs)

    val results = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "giga_day_boundary")
  }

  it should "handle multi-day watermark gap (3+ day idle then resume)" in {
    val (allEvents, schema) = generateEvents(14, 20000)
    val maxTs = allEvents.map(_.ts).max
    val gapEnd = TsUtils.round(maxTs - 2 * DayMillis, DayMillis)
    val gapStart = gapEnd - 3 * DayMillis

    val events = allEvents.filter(e => e.ts < gapStart || e.ts >= gapEnd)
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val queryTimes = Array(
      batchEnd + 6 * 3600 * 1000L,
      batchEnd + 14 * 3600 * 1000L
    ).filter(_ <= maxTs)

    val results = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "giga_multi_day_gap")
  }

  it should "handle new entity (streaming only, no batch IR)" in {
    val (events, schema) = generateEvents(2, 5000)
    val maxTs = events.map(_.ts).max
    val now = TsUtils.round(maxTs, DayMillis) + 14 * 3600 * 1000L

    // Only small windows — these are fully covered by streaming tiles
    val smallWindows = Seq(
      new Window(6, TimeUnit.HOURS),
      new Window(1, TimeUnit.DAYS),
      new Window(2, TimeUnit.DAYS)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", smallWindows),
      Builders.Aggregation(Operation.COUNT, "num", smallWindows)
    )

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    // No batch loaded — entity type C
    val streamingEvents = events.filter(_.ts <= now).sortBy(_.ts)
    val evictionInterval = processor.minEvictionInterval
    var nextEvictionTs = Long.MaxValue
    var lastEmitted: Array[Any] = null

    for (event <- streamingEvents) {
      while (nextEvictionTs <= event.ts) {
        processor.advanceWatermark(nextEvictionTs)
        val r = processor.onEviction(nextEvictionTs)
        if (r.finalizedVector != null) lastEmitted = r.finalizedVector
        nextEvictionTs += evictionInterval
      }
      processor.advanceWatermark(event.ts)
      val result = processor.onEvent(event, event.ts)
      if (result.finalizedVector != null) lastEmitted = result.finalizedVector
      if (nextEvictionTs == Long.MaxValue) {
        nextEvictionTs = TsUtils.round(event.ts, evictionInterval) + evictionInterval
      }
    }

    // Final eviction at query time
    while (nextEvictionTs <= now) {
      processor.advanceWatermark(nextEvictionTs)
      val r = processor.onEviction(nextEvictionTs)
      if (r.finalizedVector != null) lastEmitted = r.finalizedVector
      nextEvictionTs += evictionInterval
    }
    processor.advanceWatermark(now)
    val evictResult = processor.onEviction(now)
    if (evictResult.finalizedVector != null) lastEmitted = evictResult.finalizedVector

    val naive = naiveAggregate(events.filter(_.ts <= now), Array(now), aggregations, schema)
    if (!approxEqual(lastEmitted, naive(0))) {
      val expStr = gson.toJson(naive(0))
      val actStr = gson.toJson(lastEmitted)
      fail(s"giga_new_entity: mismatch\n  expected: $expStr\n  got:      $actStr")
    }
  }

  it should "handle batch refresh (old batch → events → new batch)" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val oldBatchEnd = TsUtils.round(maxTs - 3 * DayMillis, DayMillis)
    val newBatchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    // Build both batch IRs
    val oldNormalizedBatchIr = buildBatchIr(events, oldBatchEnd, aggregations, schema)
    val oldBatchIr = denormalizeBatchIr(oldNormalizedBatchIr, aggregations, schema, oldBatchEnd)
    val newNormalizedBatchIr = buildBatchIr(events, newBatchEnd, aggregations, schema)
    val newBatchIr = denormalizeBatchIr(newNormalizedBatchIr, aggregations, schema, newBatchEnd)

    // Load old batch
    processor.onBatchUpdate(oldBatchIr, oldBatchEnd, oldBatchEnd)

    // Process events
    val streamingEvents = events.sortBy(_.ts)
    val evictionInterval = processor.minEvictionInterval
    var nextEvictionTs = TsUtils.round(oldBatchEnd, evictionInterval) + evictionInterval
    var lastEmitted: Array[Any] = null

    val queryTs = newBatchEnd + 14 * 3600 * 1000L
    for (event <- streamingEvents if event.ts <= queryTs) {
      while (nextEvictionTs <= event.ts) {
        processor.advanceWatermark(nextEvictionTs)
        val r = processor.onEviction(nextEvictionTs)
        if (r.finalizedVector != null) lastEmitted = r.finalizedVector
        nextEvictionTs += evictionInterval
      }
      processor.advanceWatermark(event.ts)
      val result = processor.onEvent(event, event.ts)
      if (result.finalizedVector != null) lastEmitted = result.finalizedVector
    }

    // Load new batch (simulates daily Iceberg refresh)
    val batchResult = processor.onBatchUpdate(newBatchIr, newBatchEnd, queryTs)
    if (batchResult.finalizedVector != null) lastEmitted = batchResult.finalizedVector

    // Final eviction
    while (nextEvictionTs <= queryTs) {
      processor.advanceWatermark(nextEvictionTs)
      val r = processor.onEviction(nextEvictionTs)
      if (r.finalizedVector != null) lastEmitted = r.finalizedVector
      nextEvictionTs += evictionInterval
    }
    processor.advanceWatermark(queryTs)
    val evictResult = processor.onEviction(queryTs)
    if (evictResult.finalizedVector != null) lastEmitted = evictResult.finalizedVector

    val naive = naiveAggregate(events, Array(queryTs), aggregations, schema)
    if (!approxEqual(lastEmitted, naive(0))) {
      val expStr = gson.toJson(naive(0))
      val actStr = gson.toJson(lastEmitted)
      fail(s"giga_batch_refresh: mismatch\n  expected: $expStr\n  got:      $actStr")
    }
  }

  it should "accept sequential batch updates with advancing batchEnd (Bug 1 regression)" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val day1End = TsUtils.round(maxTs - 3 * DayMillis, DayMillis)
    val day2End = day1End + DayMillis
    val day3End = day2End + DayMillis

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    // Day 1 batch — initializes currentDayStart = day1End
    val batch1 = denormalizeBatchIr(buildBatchIr(events, day1End, aggregations, schema), aggregations, schema, day1End)
    val result1 = processor.onBatchUpdate(batch1, day1End, day1End)
    assertNotNull("first batch should emit", result1.finalizedVector)
    assertEquals("store should track day1 batchEnd", day1End, store.getBatchEndTs)

    // Advance watermark past day2End so currentDayStart catches up (avoids defer)
    processor.advanceWatermark(day2End + 6 * 3600 * 1000L)

    // Day 2 batch — batchEnd advances, must NOT be rejected as stale
    val batch2 = denormalizeBatchIr(buildBatchIr(events, day2End, aggregations, schema), aggregations, schema, day2End)
    val result2 = processor.onBatchUpdate(batch2, day2End, day2End)
    assertNotNull("second batch should emit (not rejected as stale)", result2.finalizedVector)
    assertEquals("store should track day2 batchEnd", day2End, store.getBatchEndTs)

    // Advance watermark past day3End
    processor.advanceWatermark(day3End + 6 * 3600 * 1000L)

    // Day 3 batch
    val batch3 = denormalizeBatchIr(buildBatchIr(events, day3End, aggregations, schema), aggregations, schema, day3End)
    val result3 = processor.onBatchUpdate(batch3, day3End, day3End)
    assertNotNull("third batch should emit", result3.finalizedVector)
    assertEquals("store should track day3 batchEnd", day3End, store.getBatchEndTs)
  }

  it should "suppress redundant eviction emits when nothing changed (Bug 2 regression)" in {
    val (events, schema) = generateEvents(14, 10000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    // Large windows only — no small windows, no tile rebuilding.
    // This ensures eviction only recomputes runningLargeIr from batch hops.
    // With 1hr hops, two evictions 5 min apart produce the same tail hop selection.
    val largeWindows = Seq(
      new Window(49, TimeUnit.HOURS),
      new Window(3, TimeUnit.DAYS),
      new Window(7, TimeUnit.DAYS)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", largeWindows),
      Builders.Aggregation(Operation.COUNT, "num", largeWindows)
    )

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    // irEqual that actually compares values
    val irEqual: (Array[Any], Array[Any]) => Boolean = { (a, b) =>
      if (a == null && b == null) true
      else if (a == null || b == null) false
      else a.length == b.length && a.zip(b).forall {
        case (null, null) => true
        case (null, _) | (_, null) => false
        case (x, y) => x == y
      }
    }
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store, irEqual)

    assertFalse("should have no small windows", processor.hasSmallWindows)

    // Load batch
    val batchIr = denormalizeBatchIr(buildBatchIr(events, batchEnd, aggregations, schema), aggregations, schema, batchEnd)
    processor.onBatchUpdate(batchIr, batchEnd, batchEnd)

    // Process some events
    val streamEvents = events.filter(e => e.ts >= batchEnd && e.ts < batchEnd + 12 * 3600 * 1000L).sortBy(_.ts)
    for (event <- streamEvents) {
      processor.advanceWatermark(event.ts)
      processor.onEvent(event, event.ts)
    }

    // Event-path recomputes already kept the large view current. Both forced evictions
    // must suppress their write because recomputing produces the same packed IR.
    val evictionTs = TsUtils.round(batchEnd + 12 * 3600 * 1000L, 3600 * 1000L)
    processor.advanceWatermark(evictionTs)
    val result1 = processor.onEviction(evictionTs)
    assertNull("an already-current eviction should not emit", result1.finalizedVector)

    // Second eviction 5 min later — same hour boundary, no events, nothing changed
    val nextEviction = evictionTs + 5 * 60 * 1000L
    processor.advanceWatermark(nextEviction)
    val result2 = processor.onEviction(nextEviction)
    assertNull("redundant eviction should not emit when nothing changed", result2.finalizedVector)
  }

  // ==========================================================================
  // Targeted edge case tests — steady state focus
  // ==========================================================================

  private def row(ts: Long, num: Long, amount: Double): TestRow = new TestRow(ts, num, amount)()

  /** Helper: build a processor with batch loaded AND events replayed through onEvent.
    * Flink processes ALL events (including pre-batch) to populate tiles for small windows.
    * Without this, small window columns are empty (tiles not populated by onBatchUpdate).
    */
  private def buildProcessor(
      aggregations: Seq[Aggregation],
      schema: Seq[(String, DataType)],
      batchEvents: Array[TestRow],
      batchEnd: Long
  ): (GigaTileStreamProcessor, InMemoryGigaTileStore) = {
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)
    val normalizedBatchIr = buildBatchIr(batchEvents, batchEnd, aggregations, schema)
    val batchIr = denormalizeBatchIr(normalizedBatchIr, aggregations, schema, batchEnd)
    // Replay all events through onEvent FIRST to populate tiles (small window state).
    // In production, Flink processes ALL events from Kafka — tiles capture them for small windows.
    // Must happen BEFORE onBatchUpdate to avoid double-counting in largeTodayIr.
    for (event <- batchEvents.sortBy(_.ts)) {
      processor.advanceWatermark(event.ts)
      processor.onEvent(event, event.ts)
    }
    // Then load batch IR — onBatchUpdate recomputes runningLargeIr from batch + streaming.
    // Since events are pre-batchEnd, they route to yesterday/today accumulators but the
    // onBatchUpdate clears yesterday (batchEnd >= currentDayStart) and recomputes from batch.
    processor.advanceWatermark(batchEnd)
    processor.onBatchUpdate(batchIr, batchEnd, batchEnd)
    (processor, store)
  }

  it should "not lose an event at exactly batchEnd timestamp after eviction" in {
    // Event at exactly midnight = batchEnd. Batch is exclusive [0, batchEnd), so event is NOT in batch.
    // onEvent clamps it to today (>= nextDayStart branch). After day rotation, it lands in
    // largeYesterdayIr. Eviction with batchEnd == currentDayStart skips yesterday merge.
    // The event must still be in the final answer.
    val batchEnd = 1743033600000L // Mar 27 00:00 UTC (arbitrary fixed point)
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    // Batch events: everything before batchEnd
    val batchEvents = Array(
      row(batchEnd - 3 * 3600 * 1000L, 10L, 1.0),
      row(batchEnd - 6 * 3600 * 1000L, 20L, 2.0)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", Seq(new Window(3, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.COUNT, "num", Seq(new Window(3, TimeUnit.DAYS)))
    )

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)

    // Event at exactly batchEnd — this is the edge case
    val midnightEvent = row(batchEnd, 100L, 5.0)
    processor.advanceWatermark(batchEnd)
    val eventResult = processor.onEvent(midnightEvent, batchEnd)
    assertNotNull("event at batchEnd should emit", eventResult.finalizedVector)

    // Advance watermark past midnight — triggers day rotation
    processor.advanceWatermark(batchEnd + DayMillis + 60000L)

    // Eviction after rotation
    val evictResult = processor.onEviction(batchEnd + DayMillis + 60000L)
    assertNotNull("eviction should emit", evictResult.finalizedVector)

    // The 100 from the midnight event must be present in the 3d SUM
    // Batch has 10 + 20 = 30. Midnight event adds 100. Total = 130.
    val naive = naiveAggregate(
      batchEvents :+ midnightEvent,
      Array(batchEnd + DayMillis + 60000L),
      aggregations,
      schema
    )
    if (!approxEqual(evictResult.finalizedVector, naive(0))) {
      val expStr = gson.toJson(naive(0))
      val actStr = gson.toJson(evictResult.finalizedVector)
      fail(s"midnight_event: expected $expStr got $actStr — event at batchEnd likely lost after eviction")
    }
  }

  it should "detect incremental vs eviction divergence for late yesterday event after fresh batch" in {
    // Fresh batch (batchEnd = currentDayStart). A late event from yesterday arrives AFTER
    // batch was loaded. The event was NOT in the batch source data.
    // onEvent adds it to largeYesterdayIr + runningLargeIr (incremental).
    // Eviction recomputes: yesterday not merged (batchEnd >= currentDayStart).
    // This tests whether the two paths agree.
    val batchEnd = 1743033600000L // Mar 27 00:00 UTC
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    // Batch events: does NOT include the late event
    val batchEvents = Array(
      row(batchEnd - 12 * 3600 * 1000L, 10L, 1.0)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", Seq(new Window(3, TimeUnit.DAYS)))
    )

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)

    // Advance watermark to today
    processor.advanceWatermark(batchEnd + 3600 * 1000L)

    // Late event from yesterday — NOT in batch
    val lateEvent = row(batchEnd - 2 * 3600 * 1000L, 50L, 3.0)
    val eventResult = processor.onEvent(lateEvent, lateEvent.ts)
    assertNotNull("late event should emit", eventResult.finalizedVector)

    // Capture incremental value (from onEvent)
    val incrementalVector = eventResult.finalizedVector.clone()

    // Eviction recomputes from scratch
    val evictResult = processor.onEviction(batchEnd + 3600 * 1000L)
    assertNotNull("eviction should emit", evictResult.finalizedVector)
    val evictionVector = evictResult.finalizedVector

    // Document the divergence: incremental includes the late event, eviction may not.
    // Both are compared against naive (which DOES include the event).
    val allEvents = batchEvents :+ lateEvent
    val naive = naiveAggregate(allEvents, Array(batchEnd + 3600 * 1000L), aggregations, schema)

    val incrementalMatch = approxEqual(incrementalVector, naive(0))
    val evictionMatch = approxEqual(evictionVector, naive(0))

    // At minimum one of these should match. If neither matches, there's a bug.
    // The known design trade-off: eviction may drop the late event to avoid double-counting.
    assertTrue(
      s"at least one path should match naive. incremental=$incrementalMatch eviction=$evictionMatch",
      incrementalMatch || evictionMatch
    )

    // Log which path diverges for visibility
    if (incrementalMatch && !evictionMatch) {
      logger.warn("KNOWN TRADE-OFF: eviction drops late yesterday event not in batch " +
        "(avoids double-count, causes transient value flip)")
    }
    if (!incrementalMatch) {
      fail(s"incremental path should always include the late event: " +
        s"expected ${gson.toJson(naive(0))} got ${gson.toJson(incrementalVector)}")
    }
  }

  it should "not double-count events present in both batch and streaming after eviction" in {
    // Event at batchEnd - 1hr is in both batch IR and streaming.
    // Between event and eviction, runningLargeIr double-counts it.
    // After eviction, it must be counted exactly once.
    val batchEnd = 1743033600000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    val overlapTs = batchEnd - 3600 * 1000L // 1hr before batchEnd
    val batchEvents = Array(
      row(overlapTs, 100L, 5.0),
      row(batchEnd - 12 * 3600 * 1000L, 10L, 1.0)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", Seq(new Window(3, TimeUnit.DAYS)))
    )

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)
    processor.advanceWatermark(batchEnd + 3600 * 1000L)

    // Replay the overlap event (simulates Kafka delivering it after batch)
    val overlapEvent = row(overlapTs, 100L, 5.0)
    processor.onEvent(overlapEvent, overlapEvent.ts)

    // Eviction should correct the double-count
    val evictResult = processor.onEviction(batchEnd + 3600 * 1000L)
    assertNotNull("eviction should emit", evictResult.finalizedVector)

    val naive = naiveAggregate(batchEvents, Array(batchEnd + 3600 * 1000L), aggregations, schema)
    if (!approxEqual(evictResult.finalizedVector, naive(0))) {
      val expStr = gson.toJson(naive(0))
      val actStr = gson.toJson(evictResult.finalizedVector)
      fail(s"double_count: expected $expStr got $actStr — overlap event likely counted twice")
    }
  }

  it should "return null for large windows when entire window has moved past batch data" in {
    // Batch-only entity queried >windowSize after batchEnd. The 49h window covers
    // [queryTs - 49h, queryTs) which is entirely past batchEnd — no data in window.
    // recomputeRunningLargeIr starts from clone(batchIr.collapsed) which is non-null.
    // The collapsed value is stale — it covers data before batchEnd, outside the window.
    // After eviction, the result should match naive (which returns null).
    val batchEnd = 1743033600000L
    val hourMillis = 3600 * 1000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    val batchEvents = (0 until 240).map { i =>
      row(batchEnd - (240 - i) * hourMillis, (i + 1).toLong, 1.0)
    }.toArray

    val largeWindows = Seq(new Window(49, TimeUnit.HOURS))
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", largeWindows),
      Builders.Aggregation(Operation.COUNT, "num", largeWindows)
    )

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)

    // Query 50 hours after batchEnd — entire 49h window is past all batch data
    val queryTs = batchEnd + 50 * hourMillis
    processor.advanceWatermark(queryTs)
    val result = processor.onEviction(queryTs)

    val naive = naiveAggregate(batchEvents, Array(queryTs), aggregations, schema)
    // naive returns [null, null] — no events in [queryTs - 49h, queryTs)

    if (result.finalizedVector != null) {
      if (!approxEqual(result.finalizedVector, naive(0))) {
        fail(s"stale_collapsed: expected ${gson.toJson(naive(0))} got ${gson.toJson(result.finalizedVector)} " +
          s"— collapsed included when window has moved entirely past batch data")
      }
    }
  }

  it should "survive day rotation with no events and produce correct values" in {
    // Batch loaded, events processed, then entity goes idle across a day boundary.
    // Verify eviction produces correct results after rotation.
    val batchEnd = 1743033600000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    val batchEvents = (0 until 100).map { i =>
      row(batchEnd - (100 - i) * 3600 * 1000L, (i + 1).toLong, 1.0)
    }.toArray

    // A few streaming events on day 1 only
    val streamEvents = Array(
      row(batchEnd + 3600 * 1000L, 200L, 10.0),
      row(batchEnd + 6 * 3600 * 1000L, 300L, 15.0)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)

    // Process streaming events
    for (event <- streamEvents) {
      processor.advanceWatermark(event.ts)
      processor.onEvent(event, event.ts)
    }

    // Eviction before day boundary
    val preRotation = batchEnd + 23 * 3600 * 1000L
    processor.advanceWatermark(preRotation)
    processor.onEviction(preRotation)

    // Day rotation
    processor.advanceWatermark(batchEnd + DayMillis + 60000L)

    // Eviction after rotation — no new events
    val postRotation = batchEnd + DayMillis + 3600 * 1000L
    processor.advanceWatermark(postRotation)
    val result = processor.onEviction(postRotation)
    assertNotNull("post-rotation eviction should emit", result.finalizedVector)

    val allEvents = batchEvents ++ streamEvents
    val naive = naiveAggregate(allEvents, Array(postRotation), aggregations, schema)
    if (!approxEqual(result.finalizedVector, naive(0))) {
      fail(s"day_rotation: expected ${gson.toJson(naive(0))} got ${gson.toJson(result.finalizedVector)}")
    }
  }

  it should "handle MIN correctly when min value is at the sawtooth boundary" in {
    // The global MIN is in the oldest tail hop. As the window slides forward,
    // that hop should fall off and MIN should increase.
    val batchEnd = 1743033600000L
    val hourMillis = 3600 * 1000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    // Place the minimum value exactly at the 3d window's oldest hop boundary
    val oldHopTs = batchEnd - 71 * hourMillis // ~71 hours before batchEnd
    val batchEvents = Array(
      row(oldHopTs, 1L, 1.0), // the min
      row(batchEnd - 24 * hourMillis, 100L, 10.0),
      row(batchEnd - 12 * hourMillis, 200L, 20.0),
      row(batchEnd - 1 * hourMillis, 150L, 15.0)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.MIN, "num", Seq(new Window(3, TimeUnit.DAYS)))
    )

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)

    // Query while old hop is still in window — MIN should be 1
    val earlyQuery = batchEnd + 60000L
    processor.advanceWatermark(earlyQuery)
    val r1 = processor.onEviction(earlyQuery)
    val naive1 = naiveAggregate(batchEvents, Array(earlyQuery), aggregations, schema)
    if (!approxEqual(r1.finalizedVector, naive1(0))) {
      fail(s"min_boundary early: expected ${gson.toJson(naive1(0))} got ${gson.toJson(r1.finalizedVector)}")
    }

    // Query after old hop falls off the 3d window — MIN should increase to 100
    val lateQuery = batchEnd + 2 * hourMillis
    processor.advanceWatermark(lateQuery)
    val r2 = processor.onEviction(lateQuery)
    assertNotNull(r2.finalizedVector)
    val naive2 = naiveAggregate(batchEvents, Array(lateQuery), aggregations, schema)
    if (!approxEqual(r2.finalizedVector, naive2(0))) {
      fail(s"min_boundary late: expected ${gson.toJson(naive2(0))} got ${gson.toJson(r2.finalizedVector)}")
    }
  }

  it should "transiently double-count pre-batch events replayed after batch load, corrected by eviction" in {
    // Production scenario: batch loads, then Kafka replays pre-batchEnd events.
    // onEvent adds these to largeTodayIr + runningLargeIr (incremental).
    // But batch already has them → large window columns are transiently double-counted.
    // Eviction recomputes from scratch and corrects.
    val batchEnd = 1743033600000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    // Batch events that will be in both batch IR and Kafka replay
    val batchEvents = Array(
      row(batchEnd - 3600 * 1000L, 10L, 1.0),  // 1hr before batchEnd
      row(batchEnd - 7200 * 1000L, 20L, 2.0)   // 2hr before batchEnd
    )

    // Only large windows to isolate the effect (small windows use tiles, not runningLargeIr)
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", Seq(new Window(3, TimeUnit.DAYS)))
    )

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    // Step 1: Load batch first
    val normalizedBatchIr = buildBatchIr(batchEvents, batchEnd, aggregations, schema)
    val batchIr = denormalizeBatchIr(normalizedBatchIr, aggregations, schema, batchEnd)
    processor.onBatchUpdate(batchIr, batchEnd, batchEnd)

    // Step 2: Kafka replays the SAME events (this is what happens in production)
    processor.advanceWatermark(batchEnd + 3600 * 1000L)
    for (event <- batchEvents.sortBy(_.ts)) {
      processor.onEvent(event, event.ts)
    }

    // The incremental path now has batch(10+20) + largeTodayIr(10+20) = 60 for SUM_3d
    // But correct answer is 30 (each event counted once)
    val incrementalResult = processor.onEvent(row(batchEnd + 1000L, 0L, 0.0), batchEnd + 1000L)
    assertNotNull("should emit", incrementalResult.finalizedVector)

    val naive = naiveAggregate(
      batchEvents :+ row(batchEnd + 1000L, 0L, 0.0),
      Array(batchEnd + 1000L),
      aggregations,
      schema
    )

    // Large window SUM should be 30 (10+20+0) but incremental path has double-counted
    val incrementalMatchesNaive = approxEqual(incrementalResult.finalizedVector, naive(0))

    // Eviction corrects: recomputes runningLargeIr = batch + hops + todayIr.
    // Yesterday is cleared (batchEnd >= currentDayStart), so the pre-batch events
    // in largeYesterdayIr are dropped. Only batch contributes.
    val evictResult = processor.onEviction(batchEnd + 3600 * 1000L)
    assertNotNull("eviction should emit", evictResult.finalizedVector)
    val evictionMatchesNaive = approxEqual(evictResult.finalizedVector, naive(0))

    // Assert: eviction MUST correct to match naive
    if (!evictionMatchesNaive) {
      fail(s"eviction should correct double-count: expected ${gson.toJson(naive(0))} " +
        s"got ${gson.toJson(evictResult.finalizedVector)}")
    }

    // Document: the incremental path transiently double-counts (this is the sawtooth)
    if (!incrementalMatchesNaive) {
      logger.warn(s"EXPECTED: incremental path transiently double-counts pre-batch events. " +
        s"naive=${gson.toJson(naive(0))} incremental=${gson.toJson(incrementalResult.finalizedVector)} " +
        s"eviction(corrected)=${gson.toJson(evictResult.finalizedVector)}")
    }
  }

  it should "handle all-small-window GroupBy with batch loaded correctly" in {
    // Only small windows (all <= tailBuffer). Large window paths should be no-ops.
    // Batch IR is loaded but only small window columns matter (from tiles).
    val batchEnd = 1743033600000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    val smallWindows = Seq(
      new Window(6, TimeUnit.HOURS),
      new Window(1, TimeUnit.DAYS),
      new Window(2, TimeUnit.DAYS)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", smallWindows),
      Builders.Aggregation(Operation.COUNT, "num", smallWindows)
    )

    val batchEvents = (0 until 200).map { i =>
      row(batchEnd - (200 - i) * 5 * 60 * 1000L, (i + 1).toLong, 1.0)
    }.toArray

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)

    // Streaming events
    val streamEvents = Array(
      row(batchEnd + 3600 * 1000L, 500L, 25.0),
      row(batchEnd + 2 * 3600 * 1000L, 600L, 30.0)
    )
    for (event <- streamEvents) {
      processor.advanceWatermark(event.ts)
      processor.onEvent(event, event.ts)
    }

    val queryTs = batchEnd + 3 * 3600 * 1000L
    processor.advanceWatermark(queryTs)
    val result = processor.onEviction(queryTs)
    assertNotNull("all-small-window eviction should emit", result.finalizedVector)

    val allEvents = batchEvents ++ streamEvents
    val naive = naiveAggregate(allEvents, Array(queryTs), aggregations, schema)
    if (!approxEqual(result.finalizedVector, naive(0))) {
      fail(s"all_small_windows: expected ${gson.toJson(naive(0))} got ${gson.toJson(result.finalizedVector)}")
    }
  }

  it should "exclude an expired small-window tile when a hop timer callback is delayed" in {
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(1, TimeUnit.HOURS))))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new GigaTileStreamProcessor(megaTileAgg, new InMemoryGigaTileStore(megaTileAgg.windowedAggregator))
    val hopMillis = 5 * 60 * 1000L
    val expiredTileEventTs = 10 * hopMillis
    val previousHopTs = expiredTileEventTs + 12 * hopMillis + 1L
    val delayedEventTs = previousHopTs + hopMillis + 9 * 1000L

    // Seed the left-edge 1h tile, then stop before the next hop eviction fires.
    // The delayed event path must rebuild the stale cache before adding the fresh 7.
    processor.onEvent(row(expiredTileEventTs, 5L, 1.0), expiredTileEventTs + 1L)
    processor.onEviction(previousHopTs)

    val result = processor.onEvent(row(delayedEventTs, 7L, 1.0), delayedEventTs)
    assertEquals("expired left-edge tile must not survive a delayed hop timer", 7L, result.finalizedVector(0))
  }

  it should "rebuild a sparse small window when a timer skips multiple hops" in {
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(1, TimeUnit.HOURS))))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)
    val hopMillis = processor.minSmallWindowTileSize

    processor.onEvent(row(0L, 5L, 1.0), 0L, hopMillis + 1L)
    assertEquals(hopMillis, store.getCachedSmallWindowAsOfTs)

    // The 0-minute tile expires before this callback, but the immediately preceding
    // hop tile is empty. The full skipped range still has to trigger a rebuild.
    val delayedCallbackTs = 14 * hopMillis + 1L
    val result = processor.onEviction(delayedCallbackTs)

    assertNotNull("the delayed timer must publish the sparse-key correction", result.finalizedVector)
    assertNull("the old sparse tile must not survive the skipped hops", result.finalizedVector(0))
    assertTrue("the corrected sparse key must be empty", result.isEmpty)

    // A +1 exclusive bound can cache the tile that starts exactly at the rounded marker.
    // The bounded gap scan must include that marker tile as well.
    val markerStore = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val markerProcessor = new GigaTileStreamProcessor(megaTileAgg, markerStore)
    markerProcessor.onEvent(row(hopMillis, 7L, 1.0), hopMillis, hopMillis + 1L)
    assertEquals(hopMillis, markerStore.getCachedSmallWindowAsOfTs)

    val markerResult = markerProcessor.onEviction(delayedCallbackTs)
    assertNotNull("the delayed timer must publish the exact-marker correction", markerResult.finalizedVector)
    assertNull("the exact-marker tile must expire across the skipped hops", markerResult.finalizedVector(0))
    assertTrue("the corrected exact-marker key must be empty", markerResult.isEmpty)
  }

  it should "exclude an expired batch tail for a large-window-only delayed callback" in {
    val batchEnd = 10 * DayMillis
    val hourMillis = 60 * 60 * 1000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(49, TimeUnit.HOURS))))
    val batchEvents = Array(row(batchEnd - 48 * hourMillis, 5L, 1.0))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator) {
      var runningLargeWrites: Int = 0

      override def putRunningLargeIr(ir: Array[Any]): Unit = {
        runningLargeWrites += 1
        super.putRunningLargeIr(ir)
      }
    }
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)
    val batchIr = denormalizeBatchIr(buildBatchIr(batchEvents, batchEnd, aggregations, schema),
                                     aggregations,
                                     schema,
                                     batchEnd)
    val beforeExpiry = batchEnd + 59 * 60 * 1000L
    // Skip past the +2h boundary where the -48h tail hop leaves this 49h sawtooth.
    val delayedCallbackTs = batchEnd + 3 * hourMillis + 60 * 1000L
    val freshEvent = row(delayedCallbackTs - 1L, 7L, 1.0)

    processor.onBatchUpdate(batchIr, batchEnd, batchEnd)
    processor.onEviction(beforeExpiry)
    val delayedTimerResult = processor.onEviction(delayedCallbackTs)
    assertNull("the delayed timer must remove the expired batch tail", delayedTimerResult.finalizedVector(0))
    assertEquals(delayedCallbackTs, store.getLastLargeRecomputeAsOfTs)

    val writesBeforeDelayedCallback = store.runningLargeWrites
    val result = processor.onEvent(freshEvent,
                                   freshEvent.ts,
                                   largeWindowAsOfTs = delayedCallbackTs,
                                   smallWindowAsOfTs = delayedCallbackTs)

    assertEquals("the expired batch tail must be removed before adding the fresh event",
                 7L,
                 result.finalizedVector(0))
    assertEquals("the event must reuse the timer-corrected horizon and then apply once",
                 writesBeforeDelayedCallback + 1,
                 store.runningLargeWrites)
    assertEquals(delayedCallbackTs, store.getLastLargeRecomputeAsOfTs)

    val writesAfterRebuild = store.runningLargeWrites
    val sameHopCallbackTs = delayedCallbackTs + 30 * 60 * 1000L
    val sameHopEvent = row(sameHopCallbackTs - 1L, 3L, 1.0)
    val sameHopResult = processor.onEvent(sameHopEvent,
                                         sameHopEvent.ts,
                                         largeWindowAsOfTs = sameHopCallbackTs,
                                         smallWindowAsOfTs = sameHopCallbackTs)

    assertEquals(10L, sameHopResult.finalizedVector(0))
    assertEquals("a second event in the same large-window hop must not rebuild again",
                 writesAfterRebuild + 1,
                 store.runningLargeWrites)
    assertEquals("the last recompute marker must remain at the first callback in the hop",
                 delayedCallbackTs,
                 store.getLastLargeRecomputeAsOfTs)
  }

  it should "rebuild at an offset tail boundary inside one eviction hop" in {
    val batchEnd = 10 * DayMillis
    val hourMillis = 60 * 60 * 1000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val window = new Window(49 * 60 + 20, TimeUnit.MINUTES)
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window)))
    val batchEvents = Array(row(batchEnd - 48 * hourMillis, 5L, 1.0))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator) {
      var runningLargeWrites: Int = 0

      override def putRunningLargeIr(ir: Array[Any]): Unit = {
        runningLargeWrites += 1
        super.putRunningLargeIr(ir)
      }
    }
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)
    val batchIr = denormalizeBatchIr(buildBatchIr(batchEvents, batchEnd, aggregations, schema),
                                     aggregations,
                                     schema,
                                     batchEnd)
    val beforeOffsetBoundary = batchEnd + 2 * hourMillis + 5 * 60 * 1000L
    val afterOffsetBoundary = batchEnd + 2 * hourMillis + 30 * 60 * 1000L

    processor.onBatchUpdate(batchIr, batchEnd, batchEnd)
    processor.onEviction(beforeOffsetBoundary)
    assertEquals(5L, processor.currentSnapshot.finalizedVector(0))

    val writesBeforeEvent = store.runningLargeWrites
    val result = processor.onEvent(row(afterOffsetBoundary - 1L, 7L, 1.0),
                                   afterOffsetBoundary - 1L,
                                   largeWindowAsOfTs = afterOffsetBoundary,
                                   smallWindowAsOfTs = afterOffsetBoundary)

    assertEquals("the tail that expired at +2h20 must be removed before the event", 7L, result.finalizedVector(0))
    assertEquals("the callback must rebuild once and then apply the event",
                 writesBeforeEvent + 2,
                 store.runningLargeWrites)
    assertEquals(afterOffsetBoundary, store.getLastLargeRecomputeAsOfTs)
  }

  it should "rebuild at an offset daily-slot boundary inside one eviction hop" in {
    val hourMillis = 60 * 60 * 1000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val window = new Window(49 * 60 + 20, TimeUnit.MINUTES)
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window)))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)
    val emptyBatch = denormalizeBatchIr(buildBatchIr(Array.empty[TestRow], 0L, aggregations, schema),
                                        aggregations,
                                        schema,
                                        0L)
    val oldEventTs = hourMillis
    val beforeOffsetBoundary = 3 * DayMillis + hourMillis + 5 * 60 * 1000L
    val afterOffsetBoundary = 3 * DayMillis + hourMillis + 30 * 60 * 1000L

    processor.onBatchUpdate(emptyBatch, 0L, 0L)
    processor.onEvent(row(oldEventTs, 5L, 1.0), oldEventTs)
    processor.onEviction(beforeOffsetBoundary)
    assertEquals(5L, processor.currentSnapshot.finalizedVector(0))

    val result = processor.onEvent(row(afterOffsetBoundary - 1L, 7L, 1.0),
                                   afterOffsetBoundary - 1L,
                                   largeWindowAsOfTs = afterOffsetBoundary,
                                   smallWindowAsOfTs = afterOffsetBoundary)

    assertEquals("the expired day slot must be removed before the fresh event", 7L, result.finalizedVector(0))
    assertEquals(afterOffsetBoundary, store.getLastLargeRecomputeAsOfTs)
  }

  it should "keep an out-of-order daily slot out of an expired shorter window" in {
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM,
                           "num",
                           Seq(new Window(3, TimeUnit.DAYS), new Window(7, TimeUnit.DAYS))))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new GigaTileStreamProcessor(megaTileAgg, new InMemoryGigaTileStore(megaTileAgg.windowedAggregator))
    val materializedAsOfTs = 10 * DayMillis
    val oldEventTs = 4 * DayMillis + 60 * 60 * 1000L
    val emptyBatch = denormalizeBatchIr(buildBatchIr(Array.empty[TestRow], 0L, aggregations, schema),
                                        aggregations,
                                        schema,
                                        0L)

    processor.onBatchUpdate(emptyBatch, 0L, 0L)
    processor.advanceWatermark(materializedAsOfTs)
    val result = processor.onEvent(row(oldEventTs, 5L, 1.0),
                                   oldEventTs,
                                   largeWindowAsOfTs = oldEventTs,
                                   smallWindowAsOfTs = oldEventTs)

    assertNull("the old slot must not re-inflate the already-expired 3d window", result.finalizedVector(0))
    assertEquals("the same slot remains eligible for the 7d window", 5L, result.finalizedVector(1))
    assertEquals(materializedAsOfTs, processor.store.getLastLargeRecomputeAsOfTs)
  }

  it should "keep a deferred future batch inactive until its day" in {
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(7, TimeUnit.DAYS))))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new GigaTileStreamProcessor(megaTileAgg, new InMemoryGigaTileStore(megaTileAgg.windowedAggregator))
    val futureBatchEnd = 2 * DayMillis
    val oldBatch = denormalizeBatchIr(
      buildBatchIr(Array(row(-60 * 60 * 1000L, 5L, 1.0)), 0L, aggregations, schema),
      aggregations,
      schema,
      0L)
    val futureBatch = denormalizeBatchIr(
      buildBatchIr(Array(row(futureBatchEnd - 60 * 60 * 1000L, 50L, 1.0)),
                   futureBatchEnd,
                   aggregations,
                   schema),
      aggregations,
      schema,
      futureBatchEnd)

    processor.onBatchUpdate(oldBatch, 0L, 0L)
    val deferred = processor.onBatchUpdate(futureBatch,
                                           futureBatchEnd,
                                           largeWindowAsOfTs = DayMillis,
                                           smallWindowAsOfTs = DayMillis)
    assertNull(deferred.finalizedVector)

    val beforeActivation = processor.onEvent(row(DayMillis + 60 * 60 * 1000L, 7L, 1.0),
                                             DayMillis + 60 * 60 * 1000L)
    assertEquals("the event path must retain the prior large view", 12L, beforeActivation.finalizedVector(0))
    assertNull(processor.onEviction(DayMillis + 2 * 60 * 60 * 1000L).finalizedVector)
    processor.advanceWatermark(DayMillis + 2 * 60 * 60 * 1000L)
    assertEquals(12L, processor.currentSnapshot.finalizedVector(0))

    processor.advanceWatermark(futureBatchEnd + 60 * 60 * 1000L)
    assertEquals("the deferred batch becomes active only once its day is current",
                 50L,
                 processor.currentSnapshot.finalizedVector(0))
  }

  it should "keep eviction live for an empty view with a deferred future batch" in {
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(7, TimeUnit.DAYS))))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new GigaTileStreamProcessor(megaTileAgg, new InMemoryGigaTileStore(megaTileAgg.windowedAggregator))
    val currentBatchEnd = 10 * DayMillis
    val futureBatchEnd = currentBatchEnd + DayMillis
    val emptyBatch = denormalizeBatchIr(buildBatchIr(Array.empty[TestRow], currentBatchEnd, aggregations, schema),
                                        aggregations,
                                        schema,
                                        currentBatchEnd)
    val futureBatch = denormalizeBatchIr(
      buildBatchIr(Array(row(futureBatchEnd - 60 * 60 * 1000L, 50L, 1.0)),
                   futureBatchEnd,
                   aggregations,
                   schema),
      aggregations,
      schema,
      futureBatchEnd)

    processor.onBatchUpdate(emptyBatch,
                            currentBatchEnd,
                            largeWindowAsOfTs = currentBatchEnd,
                            smallWindowAsOfTs = currentBatchEnd)
    processor.onBatchUpdate(futureBatch,
                            futureBatchEnd,
                            largeWindowAsOfTs = currentBatchEnd + 60 * 60 * 1000L,
                            smallWindowAsOfTs = currentBatchEnd + 60 * 60 * 1000L)

    val waiting = processor.onEviction(currentBatchEnd + 2 * 60 * 60 * 1000L)
    assertNull(waiting.finalizedVector)
    assertTrue("the empty key must retain a timer until its future batch activates", waiting.needsEvictionTimer)
    assertTrue(waiting.isEmpty)

    val activated = processor.advanceWatermark(futureBatchEnd + 60 * 60 * 1000L)
    assertEquals(50L, activated.finalizedVector(0))
  }

  it should "not rewind the cached small-window horizon for an older event" in {
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(1, TimeUnit.HOURS))))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)
    val hopMillis = 5 * 60 * 1000L
    val expiredEventTs = 0L
    val liveAsOfTs = 13 * hopMillis + 1L

    processor.onEvent(row(expiredEventTs, 5L, 1.0), expiredEventTs + 1L)
    val eviction = processor.onEviction(liveAsOfTs)
    assertNull("the original tile must be expired at the live horizon", eviction.finalizedVector(0))

    val result = processor.onEvent(row(expiredEventTs, 7L, 1.0), expiredEventTs)
    assertNull("an older event must not resurrect the expired tile", result.finalizedVector(0))
    assertEquals("the cache marker must not move backwards", 13 * hopMillis, store.getCachedSmallWindowAsOfTs)
  }

  it should "surface a cache-only correction from an adjacent day rollover" in {
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(1, TimeUnit.HOURS))))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new GigaTileStreamProcessor(megaTileAgg, new InMemoryGigaTileStore(megaTileAgg.windowedAggregator))
    val eventTs = DayMillis - 90 * 60 * 1000L

    processor.onEvent(row(eventTs, 5L, 1.0), eventTs + 1L)
    val rollover = processor.advanceWatermark(DayMillis + 60 * 1000L)

    assertNotNull("the rollover correction must be publishable", rollover.finalizedVector)
    assertNull("the event must expire from the 1h window", rollover.finalizedVector(0))
    assertTrue("the corrected vector must be marked empty", rollover.isEmpty)
  }

  it should "publish a stale-cache correction when a delayed event is fully dropped" in {
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val aggregations =
      Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(1, TimeUnit.HOURS), new Window(7, TimeUnit.DAYS))))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new GigaTileStreamProcessor(megaTileAgg, new InMemoryGigaTileStore(megaTileAgg.windowedAggregator))
    val hopMillis = 5 * 60 * 1000L
    val baseDay = 40 * DayMillis
    val oldLargeWindowEventTs = baseDay - 8 * DayMillis + 60 * 60 * 1000L
    val expiredTileEventTs = baseDay + 10 * hopMillis
    val delayedProcessingTs = expiredTileEventTs + 13 * hopMillis + 9 * 1000L
    val droppedEventTs = baseDay - 33 * DayMillis
    val emptyBatchIr =
      denormalizeBatchIr(buildBatchIr(Array.empty[TestRow], 0L, aggregations, schema), aggregations, schema, 0L)

    // Keep a 7d value live while the 1h tile expires, then trigger the stale-cache rebuild
    // with an event so old that it is dropped. The rebuild still changed the PUSH value.
    processor.onBatchUpdate(emptyBatchIr, 0L, 0L)
    processor.advanceWatermark(baseDay)
    processor.onEvent(row(oldLargeWindowEventTs, 5L, 1.0), oldLargeWindowEventTs)
    processor.onEvent(row(expiredTileEventTs, 11L, 1.0), expiredTileEventTs + 1L)

    val result = processor.onEvent(row(droppedEventTs, 7L, 1.0), droppedEventTs, delayedProcessingTs)
    assertTrue("event outside the retained daily range must be reported as dropped", result.droppedStaleEvent)
    assertNotNull("stale-cache correction must publish even when the triggering event is dropped",
                  result.finalizedVector)
    assertNull("expired left-edge tile must not survive a fully dropped delayed event", result.finalizedVector(0))
    assertEquals("the correction must also expire the stale large-window tail", 11L, result.finalizedVector(1))
  }

  it should "retain an exact-hop event when a delayed scheduled eviction rebuilds the cache" in {
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(1, TimeUnit.HOURS))))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new GigaTileStreamProcessor(megaTileAgg, new InMemoryGigaTileStore(megaTileAgg.windowedAggregator))
    val hopMillis = 5 * 60 * 1000L
    val exactHopTs = 20 * hopMillis
    val expiredBufferTileTs = exactHopTs - 13 * hopMillis

    processor.onEvent(row(expiredBufferTileTs, 3L, 1.0), expiredBufferTileTs + 1L)
    processor.onEvent(row(exactHopTs - 1L, 0L, 1.0), exactHopTs - 1L, exactHopTs + 1L)
    processor.onEvent(row(exactHopTs, 5L, 1.0), exactHopTs, exactHopTs + 1L)

    val result = processor.onEviction(EvictionTimes(timerTs = exactHopTs, smallWindowAsOfTs = exactHopTs + 1L))
    assertNotNull("the scheduled boundary rebuild should emit", result.finalizedVector)
    assertEquals("the just-opened exact-hop tile must remain visible", 5L, result.finalizedVector(0))
  }

  it should "rebuild mixed small and large state across a day rollover" in {
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val aggregations =
      Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(1, TimeUnit.HOURS), new Window(7, TimeUnit.DAYS))))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new GigaTileStreamProcessor(megaTileAgg, new InMemoryGigaTileStore(megaTileAgg.windowedAggregator))
    val rolloverDay = 10 * DayMillis
    val beforeRollover = rolloverDay - 60 * 1000L
    val afterRollover = rolloverDay + 60 * 1000L
    val oldLargeEventTs = rolloverDay - 8 * DayMillis + 60 * 60 * 1000L
    val recentEventTs = rolloverDay - 30 * 60 * 1000L
    val emptyBatchIr =
      denormalizeBatchIr(buildBatchIr(Array.empty[TestRow], 0L, aggregations, schema), aggregations, schema, 0L)

    processor.onBatchUpdate(emptyBatchIr, 0L, 0L)
    processor.advanceWatermark(beforeRollover)
    processor.onEvent(row(oldLargeEventTs, 5L, 1.0), oldLargeEventTs, beforeRollover)
    processor.onEvent(row(recentEventTs, 7L, 1.0), recentEventTs, beforeRollover)

    val rollover = processor.advanceWatermark(afterRollover)
    assertNotNull("the mixed-window correction must be publishable", rollover.finalizedVector)
    assertEquals("the recent value remains in the 1h window", 7L, rollover.finalizedVector(0))
    assertEquals("the expired daily slot must leave the 7d window", 7L, rollover.finalizedVector(1))
  }

  it should "keep the large-window horizon separate from the small-window boundary" in {
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val aggregations =
      Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(1, TimeUnit.HOURS), new Window(7, TimeUnit.DAYS))))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new GigaTileStreamProcessor(megaTileAgg, new InMemoryGigaTileStore(megaTileAgg.windowedAggregator))
    val baseDay = 40 * DayMillis
    val largeWindowAsOfTs = baseDay + 7 * DayMillis
    val smallWindowAsOfTs = largeWindowAsOfTs + DayMillis
    val oldLargeEventTs = baseDay + 60 * 60 * 1000L
    val recentEventTs = largeWindowAsOfTs - 30 * 60 * 1000L
    val droppedEventTs = largeWindowAsOfTs - 33 * DayMillis
    val emptyBatchIr =
      denormalizeBatchIr(buildBatchIr(Array.empty[TestRow], 0L, aggregations, schema), aggregations, schema, 0L)

    processor.onBatchUpdate(emptyBatchIr, 0L, 0L)
    processor.advanceWatermark(largeWindowAsOfTs)
    processor.onEvent(row(oldLargeEventTs, 5L, 1.0), oldLargeEventTs, largeWindowAsOfTs)
    processor.onEvent(row(recentEventTs, 11L, 1.0), recentEventTs, largeWindowAsOfTs)

    val result = processor.onEvent(row(droppedEventTs, 7L, 1.0),
                                   droppedEventTs,
                                   largeWindowAsOfTs = largeWindowAsOfTs,
                                   smallWindowAsOfTs = smallWindowAsOfTs)
    assertTrue("the trigger must be outside retained history", result.droppedStaleEvent)
    assertNull("the small horizon should expire both streaming tiles", result.finalizedVector(0))
    assertEquals("the large horizon must retain both 7d contributions", 16L, result.finalizedVector(1))
  }

  it should "rebuild stale small state when a batch row is the next callback" in {
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(new Window(1, TimeUnit.HOURS))))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new GigaTileStreamProcessor(megaTileAgg, new InMemoryGigaTileStore(megaTileAgg.windowedAggregator))
    val eventTs = 4 * DayMillis + 60 * 60 * 1000L
    val batchCallbackTs = eventTs + 2 * 60 * 60 * 1000L
    val emptyBatchIr =
      denormalizeBatchIr(buildBatchIr(Array.empty[TestRow], 0L, aggregations, schema), aggregations, schema, 0L)

    processor.onEvent(row(eventTs, 5L, 1.0), eventTs, eventTs + 1L)
    val result = processor.onBatchUpdate(emptyBatchIr,
                                         newBatchEnd = 0L,
                                         largeWindowAsOfTs = batchCallbackTs,
                                         smallWindowAsOfTs = batchCallbackTs)

    assertNotNull("the batch callback must surface the stale-cache correction", result.finalizedVector)
    assertNull("the expired 1h value must not remain published", result.finalizedVector(0))
    assertTrue("the correction is an encoded all-null value", result.isEmpty)
  }
}
