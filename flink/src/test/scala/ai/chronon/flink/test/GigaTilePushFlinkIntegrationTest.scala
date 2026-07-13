package ai.chronon.flink.test

import ai.chronon.api._
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.{FlinkJob, GigaTileAvroCodecFn, SparkExpressionEval, SparkExpressionEvalFn}
import ai.chronon.flink.types.{BatchIrRow, TimestampedTile, WriteResponse}
import ai.chronon.flink.window.GigaTileProcessFunction
import ai.chronon.online.{Api, GigaTileCodec, GroupByServingInfoParsed}
import ai.chronon.online.serde.{AvroCodec, SparkConversions}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.spark.sql.Encoders
import org.mockito.Mockito.withSettings
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util
import java.util.concurrent.{CountDownLatch, TimeUnit}

/** Flink MiniCluster integration test for the PUSH (giga tile) pipeline.
  *
  * Exercises the full Flink wiring that isn't covered by aggregator-level tests:
  * - GigaTileProcessFunction (CoProcessFunction) with connected streams
  * - FlinkGigaTileStore with real Flink keyed state (ValueState/MapState)
  * - Key switching across entities
  * - Timer registration and processing-time eviction
  * - GigaTileAvroCodecFn output encoding
  *
  * Does NOT test Iceberg source (requires a real Iceberg catalog).
  */
class GigaTilePushFlinkIntegrationTest extends AnyFlatSpec with BeforeAndAfter {

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(8)
      .setNumberTaskManagers(1)
      .build)

  before {
    flinkCluster.before()
    CollectSink.values.clear()
    GigaTilePushIntegrationSourceGate.reset()
  }

  after {
    flinkCluster.after()
    CollectSink.values.clear()
  }

  /** Like FlinkTestUtils.makeTestGroupByServingInfoParsed but sets batchEndDate/dateFormat
    * BEFORE constructing GroupByServingInfoParsed (needed by outputCodec → aggregator → batchEndTsMillis).
    */
  private def makePushGroupByServingInfoParsed(groupBy: GroupBy,
                                                inputSchema: org.apache.spark.sql.types.StructType,
                                                outputSchema: org.apache.spark.sql.types.StructType): GroupByServingInfoParsed = {
    import ai.chronon.online.Extensions.StructTypeOps
    val servingInfo = new GroupByServingInfo()
    servingInfo.setGroupBy(groupBy)
    servingInfo.setBatchEndDate("2023-11-08")
    servingInfo.setDateFormat("yyyy-MM-dd")
    servingInfo.setInputAvroSchema(inputSchema.toAvroSchema("Input").toString(true))
    servingInfo.setKeyAvroSchema(
      org.apache.spark.sql.types.StructType(
        groupBy.keyColumns.toScala.map(col => outputSchema.fields.find(_.name == col).get))
        .toAvroSchema("Key").toString(true))
    val aggInputCols = groupBy.aggregations.toScala.map(_.inputColumn).toList
    servingInfo.setSelectedAvroSchema(
      org.apache.spark.sql.types.StructType(outputSchema.fields.filter(f => aggInputCols.contains(f.name)))
        .toAvroSchema("Value").toString(true))
    new GroupByServingInfoParsed(servingInfo)
  }

  private def makePushGroupBy(keyColumns: Seq[String]): GroupBy = {
    val gb = FlinkTestUtils.makeGroupBy(keyColumns)
    gb.setOnlineStrategy(OnlineStrategy.PUSH)
    gb
  }

  private def buildPushPipeline(groupBy: GroupBy, elements: Seq[E2ETestEvent])(
      implicit env: StreamExecutionEnvironment
  ): (DataStream[WriteResponse], GroupByServingInfoParsed) = {
    val query = SparkExpressionEval.queryFromGroupBy(groupBy)
    val sparkExprEvalFn =
      new SparkExpressionEvalFn(Encoders.product[E2ETestEvent], query, groupBy.metaData.name, groupBy.dataModel)
    val encoder = Encoders.product[E2ETestEvent]
    val outputSchema =
      new SparkExpressionEval(encoder, query, groupBy.getMetaData.getName, groupBy.dataModel).getOutputSchema
    val outputSchemaDataTypes = outputSchema.fields.map { field =>
      (field.name, SparkConversions.toChrononType(field.name, field.dataType))
    }

    val groupByServingInfoParsed =
      makePushGroupByServingInfoParsed(groupBy, encoder.schema, outputSchema)

    // The production sources are unbounded, but this fixture is bounded and would otherwise
    // finish before a periodic watermark or processing-time timer can flush fenced state.
    // Establish a finite watermark before records after the empty batch input is ready.
    val preparedStream = env
      .addSource(new LiveWatermarkedE2EEventSource(elements))
      .uid(s"source-${groupBy.metaData.name}")
      .flatMap(sparkExprEvalFn)
      .map(e => ProjectedEvent(e, System.currentTimeMillis()))

    // Empty batch stream: completes immediately (so env.execute returns),
    // watermark goes to MAX on completion (doesn't stall event stream's watermark).
    val batchStream: DataStream[BatchIrRow] = env
      .addSource(new EmptyBatchIrSource())
      .uid(s"empty-batch-${groupBy.metaData.name}")

    // Connect event + batch streams, key by entity, process
    val eventKeySelector =
      ai.chronon.flink.window.KeySelectorBuilder.build(groupBy)
    val batchKeySelector =
      new org.apache.flink.api.java.functions.KeySelector[BatchIrRow, java.util.List[Any]] {
        override def getKey(row: BatchIrRow): java.util.List[Any] = row.entityKeys
      }

    val gigaTileDS: DataStream[TimestampedTile] = preparedStream
      .connect(batchStream)
      .keyBy(eventKeySelector, batchKeySelector)
      .process(new GigaTileProcessFunction(groupBy, outputSchemaDataTypes, enableDebug = true))
      .uid(s"push-process-${groupBy.metaData.name}")
      .setParallelism(2)

    // Skip the AsyncKVStoreWriter — collect TimestampedTile directly as WriteResponse
    // to isolate the CoProcessFunction from async I/O hangs.
    val writeDS: DataStream[WriteResponse] = gigaTileDS
      .flatMap(GigaTileAvroCodecFn(groupByServingInfoParsed, enableDebug = true))
      .uid(s"push-codec-${groupBy.metaData.name}")
      .setParallelism(2)
      .map { codec: ai.chronon.flink.types.AvroCodecOutput =>
        new WriteResponse(codec.keyBytes, codec.valueBytes, codec.dataset,
          codec.tsMillis, true, codec.startProcessingTime)
      }
      .uid(s"push-to-response-${groupBy.metaData.name}")

    (writeDS, groupByServingInfoParsed)
  }

  it should "process events through GigaTileProcessFunction and emit finalized vectors" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val now = System.currentTimeMillis()

    // 3 events across 2 entities. Each entity should produce at least one finalized vector.
    val elements = Seq(
      E2ETestEvent("test1", 12, 1.5, now - 3000L),
      E2ETestEvent("test2", 13, 1.6, now - 2000L),
      E2ETestEvent("test1", 14, 2.5, now - 1000L)
    )

    val groupBy = makePushGroupBy(Seq("id"))
    val (writeDS, servingInfo) = buildPushPipeline(groupBy, elements)
    writeDS.addSink(new GigaTileCollectSink(expectedKeyCount = 2))

    env.execute("PushFlinkIntegrationTest")

    val results = CollectSink.values.toScala

    // All writes should succeed
    results.forall(_.status) shouldBe true

    // Same-millisecond coalescing and replay fencing may emit fewer writes, but every entity
    // must publish final state.
    val keyHashes = results.map(result => util.Arrays.hashCode(result.keyBytes)).distinct
    keyHashes.size shouldBe 2

    // Value bytes should be non-empty (finalized vector encoded)
    results.foreach { wr =>
      wr.valueBytes should not be empty
    }
  }

  it should "produce decodable finalized vectors" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val now = System.currentTimeMillis()

    // Single entity, known values
    val elements = Seq(
      E2ETestEvent("entity1", 10, 5.0, now - 2000L),
      E2ETestEvent("entity1", 20, 3.0, now - 1000L)
    )

    val groupBy = makePushGroupBy(Seq("id"))
    val (writeDS, servingInfo) = buildPushPipeline(groupBy, elements)
    writeDS.addSink(new GigaTileCollectSink(servingInfo.outputCodec, Set(8.0)))

    env.execute("PushFlinkDecodableTest")

    val results = CollectSink.values.toScala
    results should not be empty
    results.forall(_.status) shouldBe true

    // Processing-time freshness gates events against the current wall clock, so these test
    // rows are intentionally recent rather than fixed historical timestamps.
    val sumFieldName = servingInfo.outputCodec.decodeMap(results.head.valueBytes).keys
      .find(_.contains("double_val")).get
    val nonEmpty = results.toSeq.flatMap { wr =>
      val decoded = servingInfo.outputCodec.decodeMap(wr.valueBytes)
      Option(decoded(sumFieldName)).map(v => (wr.tsMillis, v.asInstanceOf[Double]))
    }
    nonEmpty should not be empty
    val (_, latestSum) = nonEmpty.maxBy(_._1)
    latestSum shouldBe 8.0
  }

  it should "handle multiple entities with correct key isolation" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val now = System.currentTimeMillis()

    // Two entities with different values — verify state isolation
    val elements = Seq(
      E2ETestEvent("alice", 1, 10.0, now - 3000L),
      E2ETestEvent("bob", 2, 20.0, now - 2000L),
      E2ETestEvent("alice", 3, 5.0, now - 1000L)
    )

    val groupBy = makePushGroupBy(Seq("id"))
    val (writeDS, servingInfo) = buildPushPipeline(groupBy, elements)
    writeDS.addSink(new GigaTileCollectSink(servingInfo.outputCodec, Set(15.0, 20.0)))

    env.execute("PushFlinkKeyIsolationTest")

    val results = CollectSink.values.toScala
    results should not be empty
    results.forall(_.status) shouldBe true

    // Group by key and pick the latest non-empty write per entity.
    val sumFieldName = servingInfo.outputCodec.decodeMap(results.head.valueBytes).keys
      .find(_.contains("double_val")).get
    val latestSumPerKey: Set[Double] = results
      .groupBy(r => util.Arrays.hashCode(r.keyBytes))
      .values
      .flatMap { writes =>
        val nonEmpty = writes.toSeq.flatMap { wr =>
          val decoded = servingInfo.outputCodec.decodeMap(wr.valueBytes)
          Option(decoded(sumFieldName)).map(v => (wr.tsMillis, v.asInstanceOf[Double]))
        }
        if (nonEmpty.isEmpty) None else Some(nonEmpty.maxBy(_._1)._2)
      }
      .toSet
    // Alice: 10.0 + 5.0 = 15.0, Bob: 20.0
    latestSumPerKey shouldBe Set(15.0, 20.0)
  }
}

/** SourceFunction that completes immediately, emitting zero elements.
  * When a bounded source completes, Flink sets its watermark to Long.MAX_VALUE.
  * The CoProcessFunction's combined watermark becomes min(eventWm, MAX) = eventWm,
  * so the event stream drives watermark progression normally.
  */
class EmptyBatchIrSource extends SourceFunction[BatchIrRow] {
  override def run(ctx: SourceFunction.SourceContext[BatchIrRow]): Unit = {
    ctx.emitWatermark(new Watermark(Long.MaxValue))
    GigaTilePushIntegrationSourceGate.batchInputReady()
  }
  override def cancel(): Unit = {}
}

/** Bounded event source that establishes a near-live watermark before emitting records. */
class LiveWatermarkedE2EEventSource(elements: Seq[E2ETestEvent]) extends SourceFunction[E2ETestEvent] {
  @volatile private var running = true

  private def boundedOutOfOrdernessWatermark(maxEventTime: Long): Long = {
    val delta = FlinkJob.AllowedOutOfOrderness.toMillis + 1L
    if (maxEventTime < Long.MinValue + delta) Long.MinValue else maxEventTime - delta
  }

  override def run(ctx: SourceFunction.SourceContext[E2ETestEvent]): Unit = {
    if (!GigaTilePushIntegrationSourceGate.awaitBatchInput()) {
      throw new IllegalStateException("Timed out waiting for the empty batch input")
    }

    val sourceStartTime = System.currentTimeMillis()
    val latestOriginalEventTime = elements.map(_.created).reduceOption((left, right) => math.max(left, right))
      .getOrElse(sourceStartTime)
    val rebasedElements = elements.map { event =>
      event.copy(created = sourceStartTime - (latestOriginalEventTime - event.created))
    }
    val lock = ctx.getCheckpointLock
    rebasedElements.headOption.foreach { firstEvent =>
      val initialWatermark = boundedOutOfOrdernessWatermark(firstEvent.created)
      lock.synchronized(ctx.emitWatermark(new Watermark(initialWatermark)))
    }

    var maxEventTime = Long.MinValue
    rebasedElements.iterator.takeWhile(_ => running).foreach { event =>
      lock.synchronized {
        maxEventTime = math.max(maxEventTime, event.created)
        ctx.collectWithTimestamp(event, event.created)
        ctx.emitWatermark(new Watermark(boundedOutOfOrdernessWatermark(maxEventTime)))
      }
    }

    if (running && !GigaTilePushIntegrationSourceGate.awaitOutput()) {
      throw new IllegalStateException("Timed out waiting for GigaTile output")
    }
  }

  override def cancel(): Unit = running = false
}

private object GigaTilePushIntegrationSourceGate {
  @volatile private var batchReady = new CountDownLatch(1)
  @volatile private var outputReady = new CountDownLatch(1)

  def reset(): Unit = {
    batchReady = new CountDownLatch(1)
    outputReady = new CountDownLatch(1)
  }

  def batchInputReady(): Unit = batchReady.countDown()

  def awaitBatchInput(): Boolean = batchReady.await(10, TimeUnit.SECONDS)

  def outputObserved(): Unit = outputReady.countDown()

  def awaitOutput(): Boolean = outputReady.await(30, TimeUnit.SECONDS)
}

private class GigaTileCollectSink(
    outputCodec: AvroCodec = null,
    expectedFinalSums: Set[Double] = Set.empty,
    expectedKeyCount: Int = 1)
    extends SinkFunction[WriteResponse] {

  private def completionObserved: Boolean = {
    val results = CollectSink.values.synchronized(CollectSink.values.toScala.toSeq)
    if (expectedFinalSums.nonEmpty) {
      val sumFieldName = outputCodec.decodeMap(results.head.valueBytes).keys
        .find(_.contains("double_val"))
        .get
      val latestSumPerKey = results
        .groupBy(result => util.Arrays.hashCode(result.keyBytes))
        .values
        .flatMap { writes =>
          val nonEmpty = writes.flatMap { write =>
            val decoded = outputCodec.decodeMap(write.valueBytes)
            Option(decoded(sumFieldName)).map(value => (write.tsMillis, value.asInstanceOf[Double]))
          }
          if (nonEmpty.isEmpty) None else Some(nonEmpty.maxBy(_._1)._2)
        }
        .toSet
      latestSumPerKey == expectedFinalSums
    } else {
      results.map(result => util.Arrays.hashCode(result.keyBytes)).distinct.size >= expectedKeyCount
    }
  }

  override def invoke(value: WriteResponse, context: SinkFunction.Context): Unit = {
    CollectSink.values.add(value)
    if (completionObserved) GigaTilePushIntegrationSourceGate.outputObserved()
  }
}
