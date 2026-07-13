package ai.chronon.flink.chaining

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.flink.{AsyncKVStoreWriter, BaseFlinkJob, FlinkUtils}
import ai.chronon.flink.FlinkJob.watermarkStrategy
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.source.{BatchIrSourceBuilder, FlinkSource}
import ai.chronon.flink.types.{AvroCodecOutput, WriteResponse}
import ai.chronon.flink.window.{GigaTileProcessFunction, MegaTileEmissionPolicy}
import ai.chronon.online.{Api, GroupByServingInfoParsed, TopicInfo}

import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.api.datastream.{AsyncDataStream, DataStream}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction

/** Flink job implementation for chaining features using JoinSource GroupBys.
  * The job reads from event source (that has already performed projections/filters), performs async enrichment,
  * and another round of Spark expr eval before proceeding with tiled aggregations and writing to KV store.
  * In case of errors during enrichment or query processing, the event is skipped and logged (to ensure we don't
  * poison pill the Flink app)
  */
class ChainedGroupByJob(eventSrc: FlinkSource[ProjectedEvent],
                        inputSchema: Seq[(String, DataType)],
                        sinkFn: RichAsyncFunction[AvroCodecOutput, WriteResponse],
                        val groupByServingInfoParsed: GroupByServingInfoParsed,
                        parallelism: Int,
                        props: Map[String, String],
                        topicInfo: TopicInfo,
                        api: Api,
                        enableDebug: Boolean = false)
    extends BaseFlinkJob {

  val groupByName: String = groupByServingInfoParsed.groupBy.getMetaData.getName
  logger.info(f"Creating Flink JoinSource streaming job. groupByName=${groupByName}")

  // The source of our Flink application is a topic
  val topic: String = topicInfo.name

  private val groupByConf = groupByServingInfoParsed.groupBy

  // Validate that this is a JoinSource configuration
  require(groupByConf.streamingSource.isDefined,
          s"No streaming source present in the groupBy: ${groupByConf.metaData.name}")
  require(groupByConf.streamingSource.get.isSetJoinSource,
          s"No JoinSource found in the groupBy: ${groupByConf.metaData.name}")

  val joinSource: JoinSource = groupByConf.streamingSource.get.getJoinSource
  val leftSource: Source = joinSource.getJoin.getLeft

  // Validate Events-based source
  require(leftSource.isSetEvents,
          s"Only Events-based sources are currently supported. Found: ${leftSource.getSetField}")

  val keyColumns: Array[String] = groupByConf.keyColumns.toScala.toArray
  val valueColumns: Array[String] = groupByConf.aggregationInputs
  val eventTimeColumn = Constants.TimeColumn

  // Configuration properties with defaults
  private val asyncTimeout: Long = FlinkUtils.getProperty("async_timeout_ms", props, topicInfo).getOrElse("5000").toLong
  private val asyncCapacity: Int = FlinkUtils.getProperty("async_capacity", props, topicInfo).getOrElse("100").toInt

  // Configuration properties with defaults
  private val kvStoreCapacity = FlinkUtils
    .getProperty("kv_concurrency", props, topicInfo)
    .map(_.toInt)
    .getOrElse(AsyncKVStoreWriter.kvStoreConcurrency)
  private val bufferingOutputTimeMillis =
    FlinkUtils.getNonNegativeLongProperty("buffering_output_time_millis", props, topicInfo)
  private val bufferingOutputJitterMillis =
    FlinkUtils.getNonNegativeLongProperty("buffering_output_jitter_millis", props, topicInfo)
  private val firstSeenKeyGraceMillis =
    FlinkUtils.getNonNegativeLongProperty(GigaTileProcessFunction.FirstSeenKeyGraceMillisConfig, props, topicInfo)
  private val bufferingOutputPolicy =
    FlinkUtils
      .getProperty("buffering_output_policy", props, topicInfo)
      .map(MegaTileEmissionPolicy.fromString)
      .getOrElse(MegaTileEmissionPolicy.Default)
  private val gigaTileBufferingOutputTimeMillis =
    FlinkUtils.gigaTileBufferingOutputTimeMillis(bufferingOutputTimeMillis, bufferingOutputPolicy)

  /** Build the tiled version of the Flink GroupBy job that chains features using a JoinSource.
    *  The operators are structured as follows:
    *  - Source: Read from Kafka topic into ProjectedEvent stream
    *  - Assign timestamps and watermarks based on event time column
    *  - Async Enrichment: Use JoinEnrichmentAsyncFunction to fetch join data asynchronously
    *  - Join Source Query: Apply join source query transformations using JoinSourceQueryFunction
    *  - Avro Conversion: Convert enriched events to AvroCodecOutput format for KV
    *  - Sink: Write to KV store using AsyncKVStoreWriter
    */
  override def runTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(
      s"Building tiled Flink streaming job for groupBy: $groupByName that chains join: " +
        s"${joinSource.getJoin.getMetaData.getName} using topic: $topic")
    val (processedStream, schema) = buildEnrichedStream(env)
    buildTiledTail(processedStream, schema, parallelism, sinkFn, kvStoreCapacity, props, topicInfo, enableDebug)
  }

  override def runMegaTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(
      s"Building mega tiled Flink streaming job for groupBy: $groupByName that chains join: " +
        s"${joinSource.getJoin.getMetaData.getName} using topic: $topic")
    val (processedStream, schema) = buildEnrichedStream(env)
    buildMegaTiledTail(processedStream,
                       schema,
                       parallelism,
                       sinkFn,
                       kvStoreCapacity,
                       enableDebug,
                       bufferingOutputTimeMillis,
                       bufferingOutputJitterMillis,
                       bufferingOutputPolicy)
  }

  override def runGigaTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(
      s"Building giga tiled (push) Flink streaming job for groupBy: $groupByName that chains join: " +
        s"${joinSource.getJoin.getMetaData.getName} using topic: $topic")
    val (processedStream, schema) = buildEnrichedStream(env)
    val batchIrStream = BatchIrSourceBuilder.build(env,
                                                   groupByServingInfoParsed,
                                                   props,
                                                   requireConfiguredBatchSource = firstSeenKeyGraceMillis > 0L)
    buildGigaTiledTail(processedStream,
                       batchIrStream,
                       schema,
                       parallelism,
                       sinkFn,
                       kvStoreCapacity,
                       enableDebug,
                       gigaTileBufferingOutputTimeMillis,
                       firstSeenKeyGraceMillis)
  }

  /** Build the source → watermark → enrichment → query transform pipeline.
    * Returns the prepared stream and its post-transformation schema.
    */
  private def buildEnrichedStream(
      env: StreamExecutionEnvironment): (DataStream[ProjectedEvent], Seq[(String, DataType)]) = {
    val sourceSparkProjectedStream: DataStream[ProjectedEvent] = eventSrc
      .getDataStream(topic, groupByName)(env, parallelism)
      .uid(s"join-source-$groupByName")
      .name(s"Join Source for $groupByName")

    val watermarkedStream = sourceSparkProjectedStream
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .uid(s"join-source-watermarks-$groupByName")
      .name(s"Spark expression eval with timestamps for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    val enrichmentFunction = new JoinEnrichmentAsyncFunction(
      joinSource.join.metaData.getName,
      groupByName,
      api,
      enableDebug
    )

    val enrichedStream = AsyncDataStream
      .unorderedWait(
        watermarkedStream,
        enrichmentFunction,
        asyncTimeout,
        TimeUnit.MILLISECONDS,
        asyncCapacity
      )
      .uid(s"join-enrichment-$groupByName")
      .name(s"Async Join Enrichment for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    val processedStream =
      if (joinSource.query != null && joinSource.query.selects != null && !joinSource.query.selects.isEmpty) {
        logger.info("Applying join source query transformations")
        val queryFunction = new JoinSourceQueryFunction(joinSource, inputSchema, groupByName, api, enableDebug)
        enrichedStream
          .flatMap(queryFunction)
          .uid(s"join-source-query-$groupByName")
          .name(s"Join Source Query for $groupByName")
          .setParallelism(sourceSparkProjectedStream.getParallelism)
      } else {
        logger.info("No join source query transformations to apply - using enriched stream directly")
        enrichedStream
      }

    val postTransformationSchema = computePostTransformationSchemaWithCatalyst(joinSource, inputSchema)
    (processedStream, postTransformationSchema)
  }

  /** Compute the schema that results after JoinSourceQueryFunction transformations.
    *  If there are no Query transforms defined in the JoinSource, we return the join schema
    *  (which includes the enrichment fields). Else, we get the output schema from CatalystUtil.
    */
  private def computePostTransformationSchemaWithCatalyst(
      joinSource: JoinSource,
      originalInputSchema: Seq[(String, DataType)]): Seq[(String, DataType)] = {
    if (joinSource.query == null || joinSource.query.selects == null || joinSource.query.selects.isEmpty) {
      // No transformations applied, return join schema (includes enrichment)
      val joinSchema = JoinSourceQueryFunction.buildJoinSchema(originalInputSchema, joinSource, api, enableDebug)
      joinSchema.fields.map { field =>
        (field.name, field.fieldType)
      }.toSeq
    } else {
      // Use shared method to determine the exact output schema
      val result = JoinSourceQueryFunction.buildCatalystUtil(joinSource, originalInputSchema, api, enableDebug)
      result.outputSchema
    }
  }
}
