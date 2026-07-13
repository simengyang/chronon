package ai.chronon.flink

import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.api.DataType
import ai.chronon.flink.FlinkJob.watermarkStrategy
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.source.FlinkSource
import ai.chronon.flink.source.BatchIrSourceBuilder
import ai.chronon.flink.types.{AvroCodecOutput, WriteResponse}
import ai.chronon.flink.window.MegaTileEmissionPolicy
import ai.chronon.online.{GroupByServingInfoParsed, TopicInfo}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction

/** Flink job that processes a single streaming GroupBy and writes out the results (in the form of pre-aggregated tiles) to the KV store.
  *
  * @param eventSrc - Provider of a Flink Datastream[ ProjectedEvent ] for the given topic and groupBy. The event
  *                    consists of a field Map as well as metadata columns such as processing start time (to track
  *                    metrics). The Map contains projected columns from the source data based on projections and filters
  *                    in the GroupBy.
  * @param sinkFn - Async Flink writer function to help us write to the KV store
  * @param groupByServingInfoParsed - The GroupBy we are working with
  * @param parallelism - Parallelism to use for the Flink job
  * @param enableDebug - If enabled will log additional debug info per processed event
  */
class FlinkGroupByStreamingJob(eventSrc: FlinkSource[ProjectedEvent],
                               inputSchema: Seq[(String, DataType)],
                               sinkFn: RichAsyncFunction[AvroCodecOutput, WriteResponse],
                               val groupByServingInfoParsed: GroupByServingInfoParsed,
                               parallelism: Int,
                               props: Map[String, String],
                               topicInfo: TopicInfo,
                               enableDebug: Boolean = false)
    extends BaseFlinkJob {

  val groupByName: String = groupByServingInfoParsed.groupBy.getMetaData.getName
  logger.info(f"Creating Flink GroupBy streaming job. groupByName=${groupByName}")

  if (groupByServingInfoParsed.groupBy.streamingSource.isEmpty) {
    throw new IllegalArgumentException(
      s"Invalid groupBy: $groupByName. No streaming source"
    )
  }

  private val kvStoreCapacity = FlinkUtils
    .getProperty("kv_concurrency", props, topicInfo)
    .map(_.toInt)
    .getOrElse(AsyncKVStoreWriter.kvStoreConcurrency)
  private val bufferingOutputTimeMillis =
    FlinkUtils.getNonNegativeLongProperty("buffering_output_time_millis", props, topicInfo)
  private val bufferingOutputJitterMillis =
    FlinkUtils.getNonNegativeLongProperty("buffering_output_jitter_millis", props, topicInfo)
  private val bufferingOutputPolicy =
    FlinkUtils
      .getProperty("buffering_output_policy", props, topicInfo)
      .map(MegaTileEmissionPolicy.fromString)
      .getOrElse(MegaTileEmissionPolicy.Default)
  private val gigaTileBufferingOutputTimeMillis =
    FlinkUtils.gigaTileBufferingOutputTimeMillis(bufferingOutputTimeMillis, bufferingOutputPolicy)

  // The source of our Flink application is a  topic
  val topic: String = groupByServingInfoParsed.groupBy.streamingSource.get.topic

  /** The "untiled" version of the Flink app.
    *
    *  At a high level, the operators are structured as follows:
    *    source -> Spark expression eval -> Avro conversion -> KV store writer
    *    source - Reads objects of type T (specific case class, Thrift / Proto) from a  topic
    *   Spark expression eval - Evaluates the Spark SQL expression in the GroupBy and projects and filters the input data
    *   Avro conversion - Converts the Spark expr eval output to a form that can be written out to the KV store
    *      (PutRequest object)
    *   KV store writer - Writes the PutRequest objects to the KV store using the AsyncDataStream API
    *
    *  In this untiled version, there are no shuffles and thus this ends up being a single node in the Flink DAG
    *  (with the above 4 operators and parallelism as injected by the user).
    */
  def runGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {

    logger.info(
      f"Running Flink job for groupByName=${groupByName}, Topic=${topic}. " +
        "Tiling is disabled.")

    // we expect parallelism on the source stream to be set by the source provider
    val sourceSparkProjectedStream: DataStream[ProjectedEvent] =
      eventSrc
        .getDataStream(topic, groupByName)(env, parallelism)
        .uid(s"source-$groupByName")
        .name(s"Source for $groupByName")

    val sparkExprEvalDSWithWatermarks: DataStream[ProjectedEvent] = sourceSparkProjectedStream
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .uid(s"spark-expr-eval-timestamps-$groupByName")
      .name(s"Spark expression eval with timestamps for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    val putRecordDS: DataStream[AvroCodecOutput] = sparkExprEvalDSWithWatermarks
      .flatMap(AvroCodecFn(groupByServingInfoParsed))
      .uid(s"avro-conversion-$groupByName")
      .name(s"Avro conversion for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    AsyncKVStoreWriter.withUnorderedWaits(
      putRecordDS,
      sinkFn,
      groupByName,
      capacity = kvStoreCapacity
    )
  }

  /** The "tiled" version of the Flink app.
    *
    * The operators are structured as follows:
    *  1.  source - Reads objects of type T (specific case class, Thrift / Proto) from a  topic
    *  2. Spark expression eval - Evaluates the Spark SQL expression in the GroupBy and projects and filters the input
    *      data
    *  3. Window/tiling - This window aggregates incoming events, keeps track of the IRs, and sends them forward so
    *      they are written out to the KV store
    *  4. Avro conversion - Finishes converting the output of the window (the IRs) to a form that can be written out
    *      to the KV store (PutRequest object)
    *  5. KV store writer - Writes the PutRequest objects to the KV store using the AsyncDataStream API
    *
    *  The window causes a split in the Flink DAG, so there are two nodes, (1+2) and (3+4+5).
    */
  override def runTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(f"Running Flink job for groupByName=${groupByName}, Topic=${topic}. Tiling is enabled.")
    val preparedStream = buildSourceStream(env)
    buildTiledTail(preparedStream,
                   inputSchema,
                   parallelism,
                   sinkFn,
                   kvStoreCapacity,
                   props,
                   topicInfo,
                   enableDebug,
                   uidSuffix = "-01")
  }

  override def runMegaTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(f"Running Mega Tiled Flink job for groupByName=${groupByName}, Topic=${topic}.")
    val preparedStream = buildSourceStream(env)
    buildMegaTiledTail(preparedStream,
                       inputSchema,
                       parallelism,
                       sinkFn,
                       kvStoreCapacity,
                       enableDebug,
                       bufferingOutputTimeMillis,
                       bufferingOutputJitterMillis,
                       bufferingOutputPolicy)
  }

  override def runGigaTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(f"Running Giga Tiled (push) Flink job for groupByName=${groupByName}, Topic=${topic}.")
    val preparedStream = buildSourceStream(env)
    val batchIrStream = BatchIrSourceBuilder.build(env, groupByServingInfoParsed, props)
    buildGigaTiledTail(preparedStream,
                       batchIrStream,
                       inputSchema,
                       parallelism,
                       sinkFn,
                       kvStoreCapacity,
                       enableDebug,
                       gigaTileBufferingOutputTimeMillis)
  }

  private def buildSourceStream(env: StreamExecutionEnvironment): DataStream[ProjectedEvent] = {
    val sourceSparkProjectedStream = eventSrc
      .getDataStream(topic, groupByName)(env, parallelism)
      .uid(s"source-$groupByName")
      .name(s"Source for $groupByName")

    sourceSparkProjectedStream
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .uid(s"spark-expr-eval-timestamps-$groupByName")
      .name(s"Spark expression eval with timestamps for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)
  }

}
