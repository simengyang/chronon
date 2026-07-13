package ai.chronon.aggregator.test

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.windowing._
import ai.chronon.api._
import ai.chronon.api.Extensions.WindowOps
import com.google.gson.Gson
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

/** Failing tests demonstrating bugs in GigaTile (PR #1645) that Mick already fixed in MegaTile (PRs #1680/#1776).
  *
  * Each test reproduces a specific bug at the processor / aggregator level. When a fix is ported
  * from MegaTile, the corresponding test should turn green.
  */
class GigaTileBugRegressionTest extends AnyFlatSpec {
  val gson = new Gson
  val DayMillis: Long = new Window(1, TimeUnit.DAYS).millis
  val HourMillis: Long = 3600 * 1000L
  val MinuteMillis: Long = 60 * 1000L
  val TailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis
  val schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "num" -> LongType)

  private def irEqual(a: Array[Any], b: Array[Any]): Boolean = {
    if (a == null && b == null) true
    else if (a == null || b == null) false
    else a.length == b.length && a.zip(b).forall { case (x, y) => x == y }
  }

  private class SwitchableGigaTileStore(windowedAgg: RowAggregator) extends GigaTileStore {
    private val stores = mutable.Map.empty[String, InMemoryGigaTileStore]
    private var currentKey: String = _

    def bind(key: String): Unit = {
      currentKey = key
      stores.getOrElseUpdate(key, new InMemoryGigaTileStore(windowedAgg))
    }

    private def currentStore: InMemoryGigaTileStore = stores(currentKey)

    override def getTile(hopSize: Long, tileStart: Long): Array[Any] = currentStore.getTile(hopSize, tileStart)
    override def putTile(hopSize: Long, tileStart: Long, ir: Array[Any]): Unit =
      currentStore.putTile(hopSize, tileStart, ir)
    override def removeTile(hopSize: Long, tileStart: Long): Unit = currentStore.removeTile(hopSize, tileStart)
    override def tileIterator: Iterator[(Long, Long, Array[Any])] = currentStore.tileIterator

    override def getCachedSmallWindowIr: Array[Any] = currentStore.getCachedSmallWindowIr
    override def putCachedSmallWindowIr(ir: Array[Any]): Unit = currentStore.putCachedSmallWindowIr(ir)

    override def getLargeTodayIr: Array[Any] = currentStore.getLargeTodayIr
    override def putLargeTodayIr(ir: Array[Any]): Unit = currentStore.putLargeTodayIr(ir)
    override def getLargeYesterdayIr: Array[Any] = currentStore.getLargeYesterdayIr
    override def putLargeYesterdayIr(ir: Array[Any]): Unit = currentStore.putLargeYesterdayIr(ir)

    override def getCurrentDayStart: Long = currentStore.getCurrentDayStart
    override def putCurrentDayStart(ts: Long): Unit = currentStore.putCurrentDayStart(ts)
    override def getEarliestTileStart: Long = currentStore.getEarliestTileStart
    override def putEarliestTileStart(ts: Long): Unit = currentStore.putEarliestTileStart(ts)

    override def getBatchIr: FinalBatchIr = currentStore.getBatchIr
    override def putBatchIr(ir: FinalBatchIr): Unit = currentStore.putBatchIr(ir)
    override def getBatchEndTs: Long = currentStore.getBatchEndTs
    override def putBatchEndTs(ts: Long): Unit = currentStore.putBatchEndTs(ts)
    override def getRunningLargeIr: Array[Any] = currentStore.getRunningLargeIr
    override def putRunningLargeIr(ir: Array[Any]): Unit = currentStore.putRunningLargeIr(ir)

    override def getDailyLargeIr(dayStart: Long): Array[Any] = currentStore.getDailyLargeIr(dayStart)
    override def putDailyLargeIr(dayStart: Long, ir: Array[Any]): Unit =
      currentStore.putDailyLargeIr(dayStart, ir)
    override def removeDailyLargeIr(dayStart: Long): Unit = currentStore.removeDailyLargeIr(dayStart)
    override def dailyLargeIrIterator: Iterator[(Long, Array[Any])] = currentStore.dailyLargeIrIterator

  }

  private class CountingGigaTileStore(windowedAgg: RowAggregator) extends InMemoryGigaTileStore(windowedAgg) {
    var runningLargeWrites: Int = 0
    var tileScans: Int = 0

    override def putRunningLargeIr(ir: Array[Any]): Unit = {
      runningLargeWrites += 1
      super.putRunningLargeIr(ir)
    }

    override def tileIterator: Iterator[(Long, Long, Array[Any])] = {
      tileScans += 1
      super.tileIterator
    }
  }

  private def mixedBoundaryAggregations: Seq[Aggregation] =
    Seq(
      Builders.Aggregation(Operation.SUM, "num", Seq(new Window(1, TimeUnit.HOURS), new Window(3, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.SUM, "num")
    )

  private def emptyBatchIr(batchEnd: Long, aggregations: Seq[Aggregation]): FinalBatchIr = {
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)
    onlineAgg.denormalizeBatchIr(onlineAgg.finalizeSnapshot(onlineAgg.init))
  }

  private def batchIrFromEvents(events: Array[TestRow], batchEnd: Long, aggregations: Seq[Aggregation]): FinalBatchIr = {
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    events.foreach(row => batchIr = onlineAgg.update(batchIr, row))
    onlineAgg.denormalizeBatchIr(onlineAgg.finalizeSnapshot(batchIr))
  }

  // -----------------------------------------------------------------
  // Gap N — eviction suppression must be keyed and based on the key's current full IR.
  //
  // Flink creates one GigaTileStreamProcessor per operator subtask and rebinds only the
  // GigaTileStore to the current key. A processor field such as lastEvictionPackedIr is
  // therefore shared across keys. If key A evicts to [7, 12, 12], key B evicting to the
  // same vector must still emit because B's external KV row may still hold [12, 12, 12].
  // The same test also covers a large-window batch boundary while an unwindowed column
  // remains non-null, so all three column classes are represented.
  // -----------------------------------------------------------------
  it should "FAIL: eviction suppression must be keyed across small, large, and unwindowed columns" in {
    val aggregations = mixedBoundaryAggregations
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new SwitchableGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store, irEqual)
    val baseDay = TsUtils.round(1700000000000L, DayMillis)
    val emptyBatch = emptyBatchIr(baseDay, aggregations)
    val queryTs = baseDay + 12 * HourMillis

    def loadStreamingKey(key: String): Unit = {
      store.bind(key)
      processor.onBatchUpdate(emptyBatch, baseDay, baseDay)
      processor.advanceWatermark(queryTs - 2 * HourMillis)
      processor.onEvent(new TestRow(queryTs - 2 * HourMillis, 5L)(0), queryTs - 2 * HourMillis)
      processor.advanceWatermark(queryTs - 30 * MinuteMillis)
      processor.onEvent(new TestRow(queryTs - 30 * MinuteMillis, 7L)(0), queryTs - 30 * MinuteMillis)
    }

    loadStreamingKey("a")
    loadStreamingKey("b")

    store.bind("a")
    val aSmallBoundary = processor.onEviction(queryTs)
    assertNotNull("key a must emit when the 1h small-window tile expires", aSmallBoundary.finalizedVector)
    assertEquals(7L, aSmallBoundary.finalizedVector(0))
    assertEquals(12L, aSmallBoundary.finalizedVector(1))
    assertEquals(12L, aSmallBoundary.finalizedVector(2))

    store.bind("b")
    val bSmallBoundary = processor.onEviction(queryTs)
    assertNotNull(
      "key b must emit the same post-eviction vector; another key's memo cannot suppress it",
      bSmallBoundary.finalizedVector
    )
    assertEquals(7L, bSmallBoundary.finalizedVector(0))
    assertEquals(12L, bSmallBoundary.finalizedVector(1))
    assertEquals(12L, bSmallBoundary.finalizedVector(2))

    val batchEnd = baseDay
    val batchEvents = Array(new TestRow(batchEnd - HourMillis, 12L)(0))
    val batchIr = batchIrFromEvents(batchEvents, batchEnd, aggregations)
    val largeBoundaryTs = batchEnd + 4 * DayMillis

    def loadBatchKey(key: String): Unit = {
      store.bind(key)
      processor.onBatchUpdate(batchIr, batchEnd, batchEnd)
    }

    loadBatchKey("batch-a")
    loadBatchKey("batch-b")

    store.bind("batch-a")
    val aLargeBoundary = processor.onEviction(largeBoundaryTs)
    assertNotNull("key batch-a must emit when the 3d large window moves past batch data",
                  aLargeBoundary.finalizedVector)
    assertNull(aLargeBoundary.finalizedVector(0))
    assertNull(aLargeBoundary.finalizedVector(1))
    assertEquals(12L, aLargeBoundary.finalizedVector(2))

    store.bind("batch-b")
    val bLargeBoundary = processor.onEviction(largeBoundaryTs)
    assertNotNull(
      "key batch-b must emit the same large-window decay vector; another key's memo cannot suppress it",
      bLargeBoundary.finalizedVector
    )
    assertNull(bLargeBoundary.finalizedVector(0))
    assertNull(bLargeBoundary.finalizedVector(1))
    assertEquals(12L, bLargeBoundary.finalizedVector(2))
  }

  it should "FAIL: scheduled eviction before the next real boundary must not scan or rewrite full state" in {
    val aggregations = mixedBoundaryAggregations
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new CountingGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store, irEqual)
    val baseDay = TsUtils.round(1700000000000L, DayMillis)
    val eventTs = baseDay + 10 * HourMillis + 30 * MinuteMillis
    val earlyTimer = eventTs + 5 * MinuteMillis
    val expiryTimer = TsUtils.round(eventTs, 5 * MinuteMillis) + HourMillis + 5 * MinuteMillis

    processor.onBatchUpdate(emptyBatchIr(baseDay, aggregations), baseDay, baseDay)
    processor.advanceWatermark(eventTs)
    processor.onEvent(new TestRow(eventTs, 3L)(0), eventTs)
    val writesAfterEvent = store.runningLargeWrites
    val scansAfterEvent = store.tileScans

    val earlyEviction = processor.onScheduledEviction(earlyTimer)
    assertNull("timer before any small/large boundary should not emit", earlyEviction.finalizedVector)
    assertEquals("timer before a real boundary should not rewrite runningLargeIr",
                 writesAfterEvent,
                 store.runningLargeWrites)
    assertEquals("timer before a real boundary should not scan tile state", scansAfterEvent, store.tileScans)

    val boundaryEviction = processor.onEviction(expiryTimer)
    assertNotNull("timer at the 1h small-window expiry should recompute and emit", boundaryEviction.finalizedVector)
    assertNull(boundaryEviction.finalizedVector(0))
    assertEquals(3L, boundaryEviction.finalizedVector(1))
    assertEquals(3L, boundaryEviction.finalizedVector(2))
  }

  // -----------------------------------------------------------------
  // Bug A — Late retained event re-inflates the cached 1h window.
  //
  // Mirrors MegaTile fix `cedf6f4 — Fix MegaTile small-window as-of ingestion`.
  // GigaTile's onEvent updates cachedSmallWindowIr for any column whose tier-aligned
  // tile is within retention, regardless of whether the event lies inside the column's
  // own as-of horizon. A late event valid for the 1d window therefore also bumps 1h.
  //
  // Both events share the same as-of (the current serving horizon). The retained late
  // event must update the base tile (so a future eviction can rebuild correctly) but
  // must NOT bump the live cached IR for columns whose horizon excludes it.
  // -----------------------------------------------------------------
  it should "FAIL: late retained event must not re-inflate 1h cached small-window IR" in {
    val aggregations = Seq(
      Builders.Aggregation(Operation.COUNT, "num", Seq(new Window(1, TimeUnit.HOURS)))
    )
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    val baseDay = TsUtils.round(1700000000000L, DayMillis)
    val tNow = baseDay + 12 * HourMillis
    val asOfTs = tNow + 5 * MinuteMillis // current serving horizon, just past tNow
    val tLate = tNow - 2 * HourMillis    // outside 1h horizon, inside 2d retention

    processor.advanceWatermark(tNow)
    val r1 = processor.onEvent(new TestRow(tNow, 1L)(0), tNow, asOfTs)
    assertNotNull("first event should emit", r1.finalizedVector)
    assertEquals("baseline: 1h count after first event must be 1", 1L, r1.finalizedVector(0))

    val r2 = processor.onEvent(new TestRow(tLate, 1L)(0), tLate, asOfTs)
    assertEquals(
      "1h count must remain 1 — the late event is outside the 1h horizon at the current as-of",
      1L,
      Option(r2.finalizedVector).map(_(0)).getOrElse(r1.finalizedVector(0))
    )
  }

  // -----------------------------------------------------------------
  // Bug B — cachedSmallWindowIr is carried across day rollover unchanged.
  //
  // Mirrors MegaTile fix `eedd34a — Fix MegaTile small-window cache on day rollover`.
  // advanceWatermark rotates large-IR slots but does not rebuild cachedSmallWindowIr,
  // so events older than the new as-of horizon survive into the next day's serves
  // until the next eviction timer fires.
  // -----------------------------------------------------------------
  it should "FAIL: cachedSmallWindowIr must not include yesterday's events after rollover" in {
    val aggregations = Seq(
      Builders.Aggregation(Operation.COUNT, "num", Seq(new Window(1, TimeUnit.HOURS)))
    )
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    val baseDay = TsUtils.round(1700000000000L, DayMillis)
    val day0_22h = baseDay + 22 * HourMillis
    Seq(day0_22h, day0_22h + 5 * MinuteMillis, day0_22h + 10 * MinuteMillis).foreach { ts =>
      processor.advanceWatermark(ts)
      val r = processor.onEvent(new TestRow(ts, 1L)(0), ts)
      assertNotNull(s"event at $ts should emit", r.finalizedVector)
    }

    // Roll over — wall clock has moved to day+1, three hours past midnight.
    val day1_3h = baseDay + DayMillis + 3 * HourMillis
    val rollover = processor.advanceWatermark(day1_3h)
    assertNotNull("day rollover should surface the cache correction", rollover.finalizedVector)
    assertNull("expired values should be removed at rollover", rollover.finalizedVector(0))

    // Single fresh event 55 minutes later. Only this event lies inside the 1h horizon now.
    val day1_3h_55m = day1_3h + 55 * MinuteMillis
    processor.advanceWatermark(day1_3h_55m)
    val r4 = processor.onEvent(new TestRow(day1_3h_55m, 1L)(0), day1_3h_55m)
    assertNotNull("post-rollover event should emit", r4.finalizedVector)
    assertEquals(
      "1h count after rollover must reflect only the new event — yesterday's events fell out of the window",
      1L,
      r4.finalizedVector(0)
    )
  }

  // -----------------------------------------------------------------
  // Bug C — bulkMerge mutates the cached batch tail-hop IR.
  //
  // Mirrors MegaTile fix `c311a0a — Port MegaTile PT semantics and tail-hop fix`.
  // mergeTailHopsForBatchColumns passes a tail-hop slot directly into bulkMerge as the
  // (initially null) accumulator's first non-null entry. bulkMerge mutates that entry
  // for non-trivial aggregators (e.g. AVERAGE → (sum,count) Array[Any]). Repeated serves
  // therefore re-merge against a corrupted tail-hop and drift.
  //
  // This test goes through MegaTileAggregator.serveMegaTile — the same path GigaTile's
  // GigaTileStreamProcessor.recomputeRunningLargeIr takes via mergeTailHopsForBatchColumns.
  // -----------------------------------------------------------------
  it should "FAIL: serveMegaTile must not mutate batch tail hops across repeated calls" in {
    val avgSchema: Seq[(String, DataType)] = Seq("ts" -> LongType, "amount" -> DoubleType)
    val batchEnd = TsUtils.round(1700000000000L, DayMillis)
    val queryTs = batchEnd + HourMillis
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.AVERAGE, "amount", Seq(new Window(3, TimeUnit.DAYS)))
    )

    val megaTileAgg = new MegaTileAggregator(aggregations, avgSchema, tailBufferMillis = TailBufferMillis)
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, avgSchema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    batchIr = onlineAgg.update(batchIr, new TestRow(batchEnd - 26 * HourMillis, 2.0)(0))
    batchIr = onlineAgg.update(batchIr, new TestRow(batchEnd - 25 * HourMillis, 4.0)(0))
    val finalBatchIr = onlineAgg.finalizeSnapshot(batchIr)

    val emptyMegaTileIr = megaTileAgg.buildMegaTileIr(
      Map.empty[Long, collection.Map[Long, Array[Any]]],
      now = queryTs,
      batchEnd = batchEnd
    )
    val first = megaTileAgg.serveMegaTile(finalBatchIr, emptyMegaTileIr, queryTs, batchEnd)
    val second = megaTileAgg.serveMegaTile(finalBatchIr, emptyMegaTileIr, queryTs, batchEnd)

    assertEquals(
      "first serve should report the true 3-day average over (2.0, 4.0)",
      3.0,
      first(0).asInstanceOf[Double],
      1e-6
    )
    assertEquals(
      "second serve must equal first — tail hops in batchIr must be untouched between calls",
      first(0).asInstanceOf[Double],
      second(0).asInstanceOf[Double],
      1e-6
    )
  }

  // -----------------------------------------------------------------
  // Bug D — Delayed batch (>1d stale) loses streaming events from intermediate days.
  //
  // Analogous to MegaTile fix `ced0185` (the multi-day fetch part), but the architectural
  // shape is different: GigaTile keeps only `largeTodayIr` and `largeYesterdayIr` per
  // entity. When the batch upload is more than 1 day stale and the watermark advances
  // through sequential day rollovers, each rollover overwrites yesterday with today and
  // the previous yesterday is silently dropped. recomputeRunningLargeIr (run on every
  // eviction and batch update) then merges only batch + today + yesterday and the
  // intermediate streaming day disappears from the finalized vector.
  //
  // Setup: 7d SUM, batch covers up to day0 with no events. Stream 5 events × 1 each on
  // days 0, 1, 2. After eviction on day 2, the running large IR should contain all 15
  // events; with the bug it contains 10 (day 0 lost between rollovers).
  // -----------------------------------------------------------------
  it should "FAIL: delayed batch (>1d) must not lose streaming events from intermediate days" in {
    val window = new Window(7, TimeUnit.DAYS)
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", Seq(window))
    )
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    val day0 = TsUtils.round(1700000000000L, DayMillis)
    val day1 = day0 + DayMillis
    val day2 = day1 + DayMillis

    // Empty batch covering up to day0 — keeps the assertion clean (all 15 events come from streaming).
    val onlineAgg = new SawtoothOnlineAggregator(day0, aggregations, schema, tailBufferMillis = TailBufferMillis)
    val emptyFinalized = onlineAgg.finalizeSnapshot(onlineAgg.init)
    val batchIr = onlineAgg.denormalizeBatchIr(emptyFinalized)
    processor.onBatchUpdate(batchIr, day0, day0)

    def streamDay(dayStart: Long): Unit = {
      (1 to 5).foreach { i =>
        val ts = dayStart + i * HourMillis
        processor.advanceWatermark(ts)
        processor.onEvent(new TestRow(ts, 1L)(0), ts)
      }
    }

    // Sequential adjacent rollovers — this is what surfaces the bug. A single multi-day
    // jump would zero yesterday out via the `wmDay == currentDayStart + DayMillis` branch,
    // so the only way to lose day0 is to walk through day1 in between.
    streamDay(day0)
    processor.advanceWatermark(day1)
    streamDay(day1)
    processor.advanceWatermark(day2)
    streamDay(day2)

    val queryTs = day2 + 12 * HourMillis
    processor.advanceWatermark(queryTs)
    val r = processor.onEviction(queryTs)
    assertNotNull("eviction at day2 should emit", r.finalizedVector)
    val sum = r.finalizedVector(0).asInstanceOf[Long]
    assertEquals(
      "7d sum after delayed batch must include all 15 streaming events — batch is 2d stale, day0 must not be lost",
      15L,
      sum
    )
  }

  // -----------------------------------------------------------------
  // Gap E — Bootstrap underreport: emits before batch IR is loaded report a 7d window as
  // if it equalled the streaming-since-startup partial sum. The fetcher's PUSH branch
  // already returns null for "no streaming data", but as soon as Flink emits one vector
  // (with batchIr still null), that under-counted vector overwrites the KV row and is
  // served indefinitely.
  //
  // Correct behavior: don't emit a finalized vector for any column whose contract requires
  // batch (large/unwindowed). Either suppress the emit entirely, or null out the batch-
  // dependent columns until onBatchUpdate has run.
  // -----------------------------------------------------------------
  it should "FAIL: must not emit batch-dependent columns before batch IR has loaded" in {
    val window = new Window(7, TimeUnit.DAYS)
    val aggregations: Seq[Aggregation] = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window)))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    // Deliberately skip onBatchUpdate — Iceberg connected stream hasn't fired yet.
    val day0 = TsUtils.round(1700000000000L, DayMillis)
    val event = new TestRow(day0 + HourMillis, 5L)(0)
    processor.advanceWatermark(event.ts)
    val r = processor.onEvent(event, event.ts)

    assertNotNull("event must produce some emit", r)
    val sumIr = r.finalizedVector
    val sumValue = if (sumIr == null) null else sumIr(0)
    assertNull(
      "7d SUM must not be emitted as the streaming-only partial sum while batchIr is unloaded — " +
        "fetcher will serve this under-counted value indefinitely from the PUSH KV row",
      sumValue
    )
  }

  // -----------------------------------------------------------------
  // Gap F — Silent drop of events older than the staleness bound. With
  // maxBatchStalenessDays=2 and an event 5 days old, the event is dropped from BOTH the
  // small-window path (out of retention) and the daily-slot path (older than oldestAcceptedDay).
  // Nothing surfaces — no exception, no metric, no return signal. A consumer downstream
  // cannot tell that data was silently lost.
  //
  // Correct behavior at minimum: signal the drop on the GigaEmitResult so the Flink wiring
  // can bump a counter / log. The simplest API surface is a `droppedStaleEvent: Boolean`
  // field on GigaEmitResult. This test asserts that signal exists; it currently doesn't.
  // -----------------------------------------------------------------
  it should "FAIL: events older than maxBatchStalenessDays must surface a drop signal" in {
    val window = new Window(7, TimeUnit.DAYS)
    val aggregations: Seq[Aggregation] = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window)))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store, maxBatchStalenessDays = 2)

    val today = TsUtils.round(1700000000000L, DayMillis) + 12 * HourMillis
    processor.advanceWatermark(today)
    processor.onEvent(new TestRow(today, 1L)(0), today)

    val staleEvent = new TestRow(today - 5 * DayMillis, 100L)(0)
    val r = processor.onEvent(staleEvent, staleEvent.ts)

    assertTrue(
      "stale event drop must be observable on GigaEmitResult so the Flink wiring can " +
        "bump a counter / log instead of silently losing data",
      r.droppedStaleEvent
    )
    // Sanity: a fresh event at `today` must not be flagged as a drop.
    val freshResult = processor.onEvent(new TestRow(today + MinuteMillis, 1L)(0), today + MinuteMillis)
    assertTrue("a fresh event must not be flagged as dropped", !freshResult.droppedStaleEvent)
  }

  // -----------------------------------------------------------------
  // Gap G — Idle-entity stale finalized vector. After events arrive, the small-window
  // cache reflects the (eventTs - window, eventTs] interval. As wall-clock advances with
  // no further events, the cache should age out: events whose ts falls outside the
  // [now - window, now] interval should no longer contribute. GigaTile's PUSH KV row
  // is whatever was last emitted, and `onTimer` deliberately doesn't re-register a
  // processing-time eviction timer ("Don't re-register from onTimer" comment), so the
  // KV value stays at the last-emit forever — even after the events should have aged
  // out of the small window.
  //
  // The processor itself decays correctly when onEviction is called with a later ts —
  // the bug is at the Flink wiring level (no PT timer for idle entities). Here we
  // assert the desired contract at the processor level: a serving emit issued at
  // (last_event + 2h) for a 1h SUM must show 0/null because the events are outside
  // the 1h horizon. The test simulates "idle Flink wiring" by NOT calling onEviction
  // and asks the processor for the as-of value via a no-op trigger.
  //
  // Without a "serve at time T" API on the processor (which the Flink wiring would
  // need too), we exercise the proxy: the last in-state cached IR is the value the
  // PUSH KV would serve. The test fails until either (a) the processor exposes a way
  // to compute the as-of vector without an event, or (b) the Flink wiring keeps the
  // cache fresh via PT timers.
  // -----------------------------------------------------------------
  it should "FAIL: idle entity must serve a vector that decays with wall-clock, not the last emit" in {
    val window = new Window(1, TimeUnit.HOURS)
    val aggregations: Seq[Aggregation] = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window)))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    val day0 = TsUtils.round(1700000000000L, DayMillis)
    val tEvent = day0 + 6 * HourMillis
    processor.advanceWatermark(tEvent)
    val r1 = processor.onEvent(new TestRow(tEvent, 1L)(0), tEvent)
    assertEquals("baseline: 1h SUM after the event is 1", 1L, r1.finalizedVector(0))

    // Wall clock advances 2 hours past the event with no further events — the event is
    // now well outside the 1h serving horizon. The Flink wiring should be able to refresh
    // the KV row without waiting for a new event by calling serveAsOf(asOfTs).
    val tServe = tEvent + 2 * HourMillis
    val served = processor.serveAsOf(tServe)
    assertNotNull("serveAsOf must produce an emit result", served)
    val sumValue = if (served.finalizedVector == null) null else served.finalizedVector(0)
    assertNull(
      "serveAsOf 2h after the event must decay the 1h SUM to null — the event is outside " +
        "the 1h horizon at the new as-of",
      sumValue
    )
    assertTrue(
      "serveAsOf result must signal isEmpty=true when every column has decayed",
      served.isEmpty
    )
  }

  // -----------------------------------------------------------------
  // Gap I — Tombstone signal when the finalized vector is fully empty.
  //
  // After a 1h-windowed entity goes idle for 2h, the rebuilt cache is all-null. The
  // resulting finalized vector encodes to a row of nulls. GigaTile writes that row to the
  // PUSH KV table with no signal that it is semantically "empty". Two consequences:
  //   1. Idle keys accumulate as zero-rows in KV indefinitely (no TTL on the values).
  //   2. The fetcher cannot distinguish "Flink emitted an empty row for this key" from
  //      "Flink never emitted for this key" — both serve nulls — so a freshly-decayed key
  //      cannot be tombstoned via DELETE.
  //
  // The contract: GigaEmitResult should expose `isEmpty` (or equivalent) when the packed
  // IR has every column null, so the Flink wiring can choose to issue a KV DELETE instead
  // of a PUT-with-nulls. Currently no such field exists.
  // -----------------------------------------------------------------
  it should "FAIL: emit must signal a tombstone when the finalized vector is fully empty" in {
    val window = new Window(1, TimeUnit.HOURS)
    val aggregations: Seq[Aggregation] = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window)))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    val day0 = TsUtils.round(1700000000000L, DayMillis)
    val tEvent = day0 + 6 * HourMillis
    processor.advanceWatermark(tEvent)
    processor.onEvent(new TestRow(tEvent, 1L)(0), tEvent)

    // Eviction well past the window — every retained tile is stale, cache rebuilt to empty.
    val tDecay = tEvent + 2 * HourMillis
    processor.advanceWatermark(tDecay)
    val emit = processor.onEviction(tDecay)
    assertNotNull("eviction past window must still produce an emit", emit.finalizedVector)
    val allNull = emit.finalizedVector.forall(_ == null)
    assertTrue("baseline: every column should be null after the window decays", allNull)

    assertTrue(
      "GigaEmitResult must expose isEmpty=true for fully-decayed emits so Flink can DELETE " +
        "the PUSH KV row instead of writing a row of nulls that lives forever",
      emit.isEmpty
    )
  }

  // -----------------------------------------------------------------
  // Gap J — Idle entity post-batch-advance: stale row served indefinitely.
  //
  // Architectural gap, not a processor-internal bug. The Iceberg connected stream emits a
  // BatchIrRow only for entities present in the new batch. An entity X that had events
  // weeks ago and went silent has no row in the new batch, so processElement2 is never
  // called for X. processElement1 also never fires (no Kafka events). Result:
  //   - X's `batchEndTs` in Flink state stays at the OLD batch end.
  //   - X's daily-slot map keeps the old streaming events.
  //   - recomputeRunningLargeIr (when an eviction does fire) merges the OLD batch + OLD
  //     slots, reproducing the stale value.
  //
  // The fetcher then serves X's stale row — even though every event for X is now outside
  // every retained window. Worst case: a 7d window keeps reporting X's events from a year
  // ago because the entity never received a per-key signal that the world moved on.
  //
  // Correct behavior requires a *broadcast* batch-advance signal that reaches every key
  // (or some equivalent: scheduled per-key TTL, periodic null heartbeat from a side
  // input). This test asserts a `onGlobalBatchAdvance` API exists on the processor; it
  // currently does not.
  // -----------------------------------------------------------------
  it should "FAIL: idle entity must receive a global batch-advance signal so its KV row decays after weeks of silence" in {
    val window = new Window(7, TimeUnit.DAYS)
    val aggregations: Seq[Aggregation] = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window)))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    val day0 = TsUtils.round(1700000000000L, DayMillis)
    val day20 = day0 + 20 * DayMillis

    // Batch covers up to day0; this entity has events on day 0 and then goes silent.
    val onlineAgg = new SawtoothOnlineAggregator(day0, aggregations, schema, tailBufferMillis = TailBufferMillis)
    val emptyBatch = onlineAgg.denormalizeBatchIr(onlineAgg.finalizeSnapshot(onlineAgg.init))
    processor.onBatchUpdate(emptyBatch, day0, day0)

    val event = new TestRow(day0 + HourMillis, 5L)(0)
    processor.advanceWatermark(event.ts)
    processor.onEvent(event, event.ts)

    // 20 days pass globally. Other entities trigger batch advances; this entity never does.
    // No per-key signal reaches the processor — exactly the production gap.
    processor.advanceWatermark(day20 + 12 * HourMillis)

    // Simulate the global signal: advance the per-key batchEndTs without a per-key BatchIrRow.
    val newBatchEnd = day20 - 5 * DayMillis // global batch is now ~15 days fresher
    val advanced = processor.onGlobalBatchAdvance(newBatchEnd, day20 + 12 * HourMillis)
    assertNotNull("onGlobalBatchAdvance must produce an emit", advanced)

    // After advancement, the in-state batchEndTs must reflect the new boundary so the next
    // recompute uses the correct horizon.
    assertEquals(
      "store.batchEndTs must advance to the new boundary so future eviction filters slots correctly",
      newBatchEnd,
      store.getBatchEndTs
    )
    // And the day-0 slot (now well behind the new batchEnd) must be pruned from state.
    assertNull(
      "daily slot for day 0 must be pruned once it falls before the new batchEndDay",
      store.getDailyLargeIr(TsUtils.round(event.ts, DayMillis))
    )
  }

  // -----------------------------------------------------------------
  // Gap K — Decay produces a stable empty-emit only when irEqual is configured.
  //
  // With the default `irEqual = (_, _) => false`, every eviction past window decay emits
  // the same all-null vector — burning O(idle_entities × eviction_interval) KV writes
  // per day. With a value-aware irEqual, the second decay emit is suppressed correctly.
  //
  // This is more efficiency than correctness, but it ALSO masks a write-amplification bug
  // that's user-visible (KV write traffic, write quota exhaustion). The processor should
  // suppress consecutive all-empty emits even under the default no-op irEqual — those
  // are guaranteed equivalent by construction.
  // -----------------------------------------------------------------
  // -----------------------------------------------------------------
  // Gap L — Multi-window per-column merge for *large* windows. Slots that fall outside a
  // smaller large-window must not contribute to that column even when a larger large-window
  // on the same key still needs them.
  //
  // Both windows must be > tailBufferMillis (2d) so they go through the daily-slot path; a
  // small window (≤ 2d) goes through the tile path and never reads slots.
  //
  // Scenario: aggregations {SUM(num,3d), SUM(num,14d)} on the same input. Batch is frozen
  // at day 0. Events arrive on day 0. Query at day 4 12:00. The 3d window covers
  // [day 1 12:00, day 4 12:00] — slot day 0 is entirely outside, must contribute 0 to 3d.
  // The 14d window covers [day -9 12:00, day 4 12:00] — slot day 0 is inside, must
  // contribute its events to 14d.
  //
  // Without per-column window-overlap filtering, slot day 0 is merged into BOTH columns,
  // over-counting 3d. The fix in recomputeRunningLargeIr applies a per-column predicate.
  // -----------------------------------------------------------------
  it should "FAIL: must not over-count 3d when slot is in 14d but outside 3d (multi-window stale-batch)" in {
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", Seq(new Window(3, TimeUnit.DAYS), new Window(14, TimeUnit.DAYS)))
    )
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    val day0 = TsUtils.round(1700000000000L, DayMillis)
    // Empty batch at day0 to wire batchEndTs without contributing values.
    val onlineAgg = new SawtoothOnlineAggregator(day0, aggregations, schema, tailBufferMillis = TailBufferMillis)
    val emptyBatch = onlineAgg.denormalizeBatchIr(onlineAgg.finalizeSnapshot(onlineAgg.init))
    processor.onBatchUpdate(emptyBatch, day0, day0)

    // Three day-0 events at 09:00, 10:00, 11:00 (each value 2 → sum=6).
    Seq(day0 + 9 * HourMillis, day0 + 10 * HourMillis, day0 + 11 * HourMillis).foreach { ts =>
      processor.advanceWatermark(ts)
      processor.onEvent(new TestRow(ts, 2L)(0), ts)
    }

    // Batch never advances. Query at day 4 12:00 — slot day 0 is in the 14d window but
    // outside the 3d window (3d window starts day 1 12:00, slot ends day 1 00:00).
    val queryTs = day0 + 4 * DayMillis + 12 * HourMillis
    processor.advanceWatermark(queryTs)
    val r = processor.onEviction(queryTs)
    assertNotNull("eviction must emit", r.finalizedVector)

    // SUM(num,3d) = col 0, SUM(num,14d) = col 1.
    val sum3d = r.finalizedVector(0)
    val sum14d = r.finalizedVector(1)
    assertEquals(
      "3d SUM at day 4 12:00 must be null — events on day 0 are entirely before day 1 12:00 " +
        "(3d window start). Without per-column window-overlap filtering, slot day 0's events " +
        "leak into the 3d column.",
      null,
      sum3d
    )
    assertEquals(
      "14d SUM at day 4 12:00 must include the day 0 events (within 14d window)",
      6L,
      sum14d
    )
  }

  // -----------------------------------------------------------------
  // Gap M — Slot eviction by largest window. A slot whose [dayStart, dayStart+1d) is
  // entirely older than (queryTs - maxWindowMillis) cannot contribute to any column under
  // any circumstance — pure pollution that grows Flink state unboundedly when batch never
  // refreshes (gap J's broken-batch scenario).
  //
  // Pure window-bound check, independent of batchEndDay. Eviction must drop these slots
  // from state every time recompute runs so total state per entity is bounded by the
  // largest window, not by maxBatchStalenessDays.
  // -----------------------------------------------------------------
  it should "FAIL: slots older than the largest window must be evicted from state" in {
    val largestWindow = new Window(7, TimeUnit.DAYS)
    val aggregations: Seq[Aggregation] = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(largestWindow)))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    // Permissive staleness bound so we can demonstrate eviction is driven by window size,
    // not by the staleness bound.
    val processor = new GigaTileStreamProcessor(megaTileAgg, store, maxBatchStalenessDays = 100)

    val day0 = TsUtils.round(1700000000000L, DayMillis)
    // Empty batch at day0; batch never refreshes after this.
    val onlineAgg = new SawtoothOnlineAggregator(day0, aggregations, schema, tailBufferMillis = TailBufferMillis)
    val emptyBatch = onlineAgg.denormalizeBatchIr(onlineAgg.finalizeSnapshot(onlineAgg.init))
    processor.onBatchUpdate(emptyBatch, day0, day0)

    // One event on day 0 → slot day 0 created.
    processor.advanceWatermark(day0 + HourMillis)
    processor.onEvent(new TestRow(day0 + HourMillis, 1L)(0), day0 + HourMillis)
    assertNotNull("slot day 0 must exist after the event", store.getDailyLargeIr(day0))

    // Query 10 days later — slot day 0 ends at day 1 00:00, which is well before
    // (queryTs - 7d) = day 3 12:00. Slot is useless to any column. Recompute must evict it.
    val queryTs = day0 + 10 * DayMillis + 12 * HourMillis
    processor.advanceWatermark(queryTs)
    processor.onEviction(queryTs)

    assertNull(
      "slot day 0 must be evicted from state once it's older than the largest window — " +
        "without this, slots accumulate unboundedly when batch never advances (Gap J's broken-batch case)",
      store.getDailyLargeIr(day0)
    )
  }

  it should "FAIL: consecutive all-empty eviction emits must be suppressed even under default irEqual" in {
    val window = new Window(1, TimeUnit.HOURS)
    val aggregations: Seq[Aggregation] = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window)))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store) // default irEqual

    val day0 = TsUtils.round(1700000000000L, DayMillis)
    val tEvent = day0 + 6 * HourMillis
    processor.advanceWatermark(tEvent)
    processor.onEvent(new TestRow(tEvent, 1L)(0), tEvent)

    // Two evictions past the window — both rebuild to all-null.
    val firstDecay = processor.onEviction(tEvent + 2 * HourMillis)
    val secondDecay = processor.onEviction(tEvent + 3 * HourMillis)

    assertNotNull("first decay emit must exist (transitions from non-null to all-null)", firstDecay.finalizedVector)
    assertTrue("first decay emit should be all-null", firstDecay.finalizedVector.forall(_ == null))
    assertNull(
      "second decay emit must be suppressed — both rebuilds produce identical all-null vectors and " +
        "burning a KV write per eviction past idle adds up to traffic quota exhaustion at scale",
      secondDecay.finalizedVector
    )
  }
}
