package ai.chronon.online.test

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.test.NaiveAggregator
import ai.chronon.aggregator.windowing._
import ai.chronon.api._
import ai.chronon.api.Extensions.{AggregationOps, WindowOps}
import ai.chronon.online.{GigaTileCodec, MegaTileCodec}
import ai.chronon.online.serde.ArrayRow
import com.google.gson.Gson
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Random

/** Tests GigaTileStreamProcessor with serde round-trips on every state access,
  * simulating what FlinkGigaTileStore does (encode on put, decode on get).
  * Also tests GigaTileCodec.encodeBatchIr/decodeBatchIr round-trip.
  */
class GigaTileCodecRoundTripTest extends AnyFlatSpec {
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
  val Schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "num" -> LongType, "amount" -> DoubleType)

  def approxEqual(a: Any, b: Any): Boolean = (a, b) match {
    case (null, null)                         => true
    case (null, _) | (_, null)                => false
    case (x: Double, y: Double)               => Math.abs(x - y) <= Epsilon * Math.max(1.0, Math.max(Math.abs(x), Math.abs(y)))
    case (x: java.util.List[_], y: java.util.List[_]) =>
      x.size() == y.size() && (0 until x.size()).forall(i => approxEqual(x.get(i), y.get(i)))
    case (x: java.util.Map[_, _], y: java.util.Map[_, _]) =>
      x.size() == y.size() && x.keySet().toArray.forall(k => approxEqual(x.get(k), y.get(k)))
    case (x: Array[_], y: Array[_]) =>
      x.length == y.length && x.zip(y).forall { case (a, b) => approxEqual(a, b) }
    case _ => a == b
  }

  def generateEvents(daySpan: Int, count: Int): Array[Row] = {
    val rng = new Random(42)
    val baseTs = 1774000000000L
    val spanMillis = daySpan.toLong * DayMillis
    (0 until count).map { _ =>
      val ts = baseTs + (rng.nextDouble() * spanMillis).toLong
      val num = rng.nextInt(1000).toLong
      val amount = rng.nextDouble() * 500.0
      new ArrayRow(Array(ts, num, amount), ts): Row
    }.toArray
  }

  def naiveAggregate(allEvents: Array[Row], queryTimes: Array[Long],
                     aggregations: Seq[Aggregation]): Array[Array[Any]] = {
    val unpackedParts = aggregations.flatMap(_.unpack)
    val unpacked = unpackedParts.map(_.window).toArray
    val tailHops = unpacked.map(w => FiveMinuteResolution.calculateTailHop(w))
    val rowAgg = new RowAggregator(Schema, unpackedParts)
    val naiveAgg = new NaiveAggregator(rowAgg, unpacked, tailHops)
    naiveAgg.aggregate(allEvents, queryTimes).map(ir => rowAgg.finalize(ir))
  }

  def buildGroupBy(aggregations: Seq[Aggregation]): GroupBy = {
    import scala.collection.JavaConverters._
    val gb = new GroupBy()
    gb.setAggregations(aggregations.asJava)
    val meta = new MetaData()
    meta.setName("test_giga_tile_codec")
    gb.setMetaData(meta)
    gb
  }

  /** GigaTileStore that encodes/decodes on every access via GigaTileCodec + MegaTileCodec. */
  class SerdeGigaTileStore(windowedAgg: RowAggregator, codec: MegaTileCodec, gigaCodec: GigaTileCodec)
      extends GigaTileStore {
    private val tileBytes = mutable.Map[(Long, Long), Array[Byte]]()
    private var cachedSmallBytes: Array[Byte] = codec.encode(windowedAgg.init)
    private val dailyLargeBytes = mutable.Map[Long, Array[Byte]]()
    private var dayStart: Long = -1L
    private var earliest: Long = Long.MaxValue
    private var batchIrBytes: Array[Byte] = _
    private var batchEnd: Long = -1L
    private var runningLargeBytes: Array[Byte] = codec.encode(windowedAgg.init)
    private var cachedSmallWindowAsOfTs: Long = -1L
    private var lastLargeRecomputeAsOfTs: Long = Long.MinValue

    override def getTile(h: Long, t: Long): Array[Any] = tileBytes.get((h, t)).map(codec.decodeBaseIr).orNull
    override def putTile(h: Long, t: Long, ir: Array[Any]): Unit = tileBytes((h, t)) = codec.encodeBaseIr(ir)
    override def removeTile(h: Long, t: Long): Unit = tileBytes.remove((h, t))
    override def tileIterator: Iterator[(Long, Long, Array[Any])] =
      tileBytes.iterator.map { case ((h, t), b) => (h, t, codec.decodeBaseIr(b)) }

    override def getCachedSmallWindowIr: Array[Any] = codec.decode(cachedSmallBytes)
    override def putCachedSmallWindowIr(ir: Array[Any]): Unit = cachedSmallBytes = codec.encode(ir)
    // Today/yesterday are required by the parent TileStore trait but unused by GigaTile —
    // routing happens through the per-day map below.
    override def getLargeTodayIr: Array[Any] = windowedAgg.init
    override def putLargeTodayIr(ir: Array[Any]): Unit = ()
    override def getLargeYesterdayIr: Array[Any] = windowedAgg.init
    override def putLargeYesterdayIr(ir: Array[Any]): Unit = ()
    override def getCurrentDayStart: Long = dayStart
    override def putCurrentDayStart(ts: Long): Unit = dayStart = ts
    override def getEarliestTileStart: Long = earliest
    override def putEarliestTileStart(ts: Long): Unit = earliest = ts

    override def getBatchIr: FinalBatchIr =
      if (batchIrBytes != null) gigaCodec.decodeBatchIr(batchIrBytes) else null
    override def putBatchIr(ir: FinalBatchIr): Unit =
      batchIrBytes = gigaCodec.encodeBatchIr(ir)
    override def getBatchEndTs: Long = batchEnd
    override def putBatchEndTs(ts: Long): Unit = batchEnd = ts
    override def getRunningLargeIr: Array[Any] = codec.decode(runningLargeBytes)
    override def putRunningLargeIr(ir: Array[Any]): Unit = runningLargeBytes = codec.encode(ir)
    override def getCachedSmallWindowAsOfTs: Long = cachedSmallWindowAsOfTs
    override def putCachedSmallWindowAsOfTs(ts: Long): Unit = cachedSmallWindowAsOfTs = ts
    override def getLastLargeRecomputeAsOfTs: Long = lastLargeRecomputeAsOfTs
    override def putLastLargeRecomputeAsOfTs(ts: Long): Unit = lastLargeRecomputeAsOfTs = ts

    // Daily slot IRs are base-aggregator-shaped (one column per (op, input)) — fanned out
    // to per-window columns at recompute time via baseIrIndices.
    override def getDailyLargeIr(ds: Long): Array[Any] = dailyLargeBytes.get(ds).map(codec.decodeBaseIr).orNull
    override def putDailyLargeIr(ds: Long, ir: Array[Any]): Unit = dailyLargeBytes(ds) = codec.encodeBaseIr(ir)
    override def removeDailyLargeIr(ds: Long): Unit = dailyLargeBytes.remove(ds)
    override def dailyLargeIrIterator: Iterator[(Long, Array[Any])] =
      dailyLargeBytes.iterator.map { case (ds, b) => (ds, codec.decodeBaseIr(b)) }
  }

  /** Full giga tile pipeline with serde round-trips at every state boundary. */
  def gigaTileWithSerdeAggregate(allEvents: Array[Row],
                                  queryTimes: Array[Long],
                                  aggregations: Seq[Aggregation],
                                  batchEnd: Long): Array[Array[Any]] = {

    val megaTileAgg = new MegaTileAggregator(aggregations, Schema, tailBufferMillis = TailBufferMillis)
    val gb = buildGroupBy(aggregations)
    val codec = new MegaTileCodec(gb, Schema)
    val gigaCodec = new GigaTileCodec(gb, Schema)
    val store = new SerdeGigaTileStore(megaTileAgg.windowedAggregator, codec, gigaCodec)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    // Build and load batch IR
    val batchEvents = allEvents.filter(_.ts < batchEnd)
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, Schema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    batchEvents.foreach(row => batchIr = onlineAgg.update(batchIr, row))
    val normalizedBatchIr = onlineAgg.normalizeBatchIr(batchIr)
    val denormalizedBatchIr = onlineAgg.denormalizeBatchIr(normalizedBatchIr)

    val sortedEvents = allEvents.sortBy(_.ts)
    val sortedQueries = queryTimes.sorted
    val evictionInterval = processor.minEvictionInterval
    var nextEvictionTs = Long.MaxValue
    var lastEmitted: Array[Any] = null
    var eventIdx = 0
    val resultsByQueryTs = mutable.Map[Long, Array[Any]]()

    // Load batch via onBatchUpdate (goes through serde)
    val batchResult = processor.onBatchUpdate(denormalizedBatchIr, batchEnd, batchEnd)
    if (batchResult.finalizedVector != null) lastEmitted = batchResult.finalizedVector
    if (batchResult.needsEvictionTimer && nextEvictionTs == Long.MaxValue) {
      nextEvictionTs = TsUtils.round(batchEnd, evictionInterval) + evictionInterval
    }

    def firePendingEvictions(upToTs: Long): Unit = {
      while (nextEvictionTs <= upToTs) {
        processor.advanceWatermark(nextEvictionTs)
        val r = processor.onEviction(nextEvictionTs)
        if (r.finalizedVector != null) lastEmitted = r.finalizedVector
        nextEvictionTs += evictionInterval
      }
    }

    for (queryTs <- sortedQueries) {
      while (eventIdx < sortedEvents.length && sortedEvents(eventIdx).ts <= queryTs) {
        val event = sortedEvents(eventIdx)
        firePendingEvictions(event.ts)
        processor.advanceWatermark(event.ts)
        val result = processor.onEvent(event, event.ts)
        if (result.finalizedVector != null) lastEmitted = result.finalizedVector
        if (nextEvictionTs == Long.MaxValue) {
          nextEvictionTs = TsUtils.round(event.ts, evictionInterval) + evictionInterval
        }
        eventIdx += 1
      }

      firePendingEvictions(queryTs)
      processor.advanceWatermark(queryTs)
      val evictResult = processor.onEviction(queryTs)
      if (evictResult.finalizedVector != null) lastEmitted = evictResult.finalizedVector

      resultsByQueryTs(queryTs) = lastEmitted
    }

    queryTimes.map(resultsByQueryTs)
  }

  it should "match naive with serde round-trips (batch fresh)" in {
    val events = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )

    val queryTimes = Array(batchEnd + 6 * 3600 * 1000L, batchEnd + 14 * 3600 * 1000L).filter(_ <= maxTs)
    val results = gigaTileWithSerdeAggregate(events, queryTimes, aggregations, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations)
    assertEquals("result count", naive.length, results.length)
    for (i <- queryTimes.indices) {
      assertTrue(s"giga_serde_fresh: mismatch at query ${queryTimes(i)}\n  expected: ${gson.toJson(naive(i))}\n  got:      ${gson.toJson(results(i))}",
                 approxEqual(results(i), naive(i)))
    }
  }

  it should "match naive with serde round-trips (all agg types)" in {
    val events = generateEvents(14, 20000)
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
    val results = gigaTileWithSerdeAggregate(events, queryTimes, aggregations, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations)
    assertEquals("result count", naive.length, results.length)
    for (i <- queryTimes.indices) {
      assertTrue(s"giga_serde_all_agg: mismatch at query ${queryTimes(i)}\n  expected: ${gson.toJson(naive(i))}\n  got:      ${gson.toJson(results(i))}",
                 approxEqual(results(i), naive(i)))
    }
  }

  it should "round-trip batch IR through encode/decode" in {
    val events = generateEvents(14, 10000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )

    val gb = buildGroupBy(aggregations)
    val gigaCodec = new GigaTileCodec(gb, Schema)

    // Build a real FinalBatchIr
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, Schema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    events.filter(_.ts < batchEnd).foreach(row => batchIr = onlineAgg.update(batchIr, row))
    val normalizedBatchIr = onlineAgg.normalizeBatchIr(batchIr)
    val denormalized = onlineAgg.denormalizeBatchIr(normalizedBatchIr)

    // Round-trip: encode → decode
    val encoded = gigaCodec.encodeBatchIr(denormalized)
    assertNotNull("encoded bytes should not be null", encoded)
    assertTrue("encoded bytes should be non-empty", encoded.length > 0)

    val decoded = gigaCodec.decodeBatchIr(encoded)
    assertNotNull("decoded batch IR should not be null", decoded)

    // Verify collapsed values match
    assertEquals("collapsed length", denormalized.collapsed.length, decoded.collapsed.length)
    for (i <- denormalized.collapsed.indices) {
      assertTrue(s"collapsed[$i] mismatch: orig=${denormalized.collapsed(i)} decoded=${decoded.collapsed(i)}",
                 approxEqual(denormalized.collapsed(i), decoded.collapsed(i)))
    }

    // Verify tail hops structure
    assertEquals("tailHops length", denormalized.tailHops.length, decoded.tailHops.length)
    for (i <- denormalized.tailHops.indices) {
      assertEquals(s"tailHops[$i] length", denormalized.tailHops(i).length, decoded.tailHops(i).length)
    }
  }

  // -----------------------------------------------------------------
  // Gap H — GigaTileCodec.irCodec is `@transient private lazy val avroCodec = AvroCodec.of(...)`.
  // AvroCodec.of is backed by a per-thread cache; caching the result in a lazy val means the
  // first thread's codec is reused across all threads, leaking that thread's mutable decoder
  // state. Mick's MegaTile fix `ced0185` switched to `private def avroCodec = ...` so each
  // thread resolves its own codec. The same pattern in GigaTileCodec is currently safe only
  // because Flink calls decodeBatchIr single-threaded per key — moving any decode onto a
  // shared (e.g. fetcher / parallel-batch-ingest) path will hit this race.
  //
  // Reproduce by spinning N threads and decoding the same encoded batchIr concurrently. With
  // a thread-safe codec the decoded results are all equal and no decode throws. With the
  // lazy-val pattern, repeated runs surface either an exception or inconsistent decodes.
  // -----------------------------------------------------------------
  it should "FAIL: GigaTileCodec.decodeBatchIr must be thread-safe" in {
    val batchEnd = TsUtils.round(1700000000000L, DayMillis)
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )
    val gb = buildGroupBy(aggregations)
    val gigaCodec = new GigaTileCodec(gb, Schema)

    val events = generateEvents(14, 5000).filter(_.ts < batchEnd)
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, Schema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    events.foreach(row => batchIr = onlineAgg.update(batchIr, row))
    val denormalized = onlineAgg.denormalizeBatchIr(onlineAgg.normalizeBatchIr(batchIr))
    val encoded = gigaCodec.encodeBatchIr(denormalized)

    val nThreads = 16
    val iterations = 200
    val pool = java.util.concurrent.Executors.newFixedThreadPool(nThreads)
    val errors = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()
    val collapsedHashes = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()
    val barrier = new java.util.concurrent.CyclicBarrier(nThreads)
    val futures = (0 until nThreads).map { _ =>
      pool.submit(new Runnable {
        override def run(): Unit = {
          barrier.await()
          var i = 0
          while (i < iterations) {
            try {
              val decoded = gigaCodec.decodeBatchIr(encoded)
              val collapsedFingerprint = decoded.collapsed.map(v => if (v == null) "null" else v.toString).mkString(",")
              collapsedHashes.add(collapsedFingerprint)
            } catch {
              case t: Throwable => errors.add(s"${t.getClass.getSimpleName}: ${t.getMessage}")
            }
            i += 1
          }
        }
      })
    }
    futures.foreach(_.get(60, java.util.concurrent.TimeUnit.SECONDS))
    pool.shutdown()

    assertTrue(s"concurrent decodes must not throw — observed: ${errors.toArray.mkString("; ")}", errors.isEmpty)
    assertEquals(
      s"all concurrent decodes must return the same collapsed fingerprint — observed ${collapsedHashes.size} distinct values",
      1,
      collapsedHashes.size
    )

    // Defense-in-depth assertion: prefer that irCodec resolves per call (def, not lazy val),
    // matching Mick's MegaTileCodec fix. The runtime test above can flake; this static check
    // pins the fix shape.
    val irCodecField = classOf[GigaTileCodec].getDeclaredFields.find(_.getName == "irCodec")
    assertTrue(
      "GigaTileCodec.irCodec must be resolved per-call (def) so AvroCodec.of's per-thread cache " +
        "isn't bypassed — it is currently a `lazy val`",
      irCodecField.isEmpty
    )
  }
}
