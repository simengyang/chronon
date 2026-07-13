package ai.chronon.flink

import ai.chronon.aggregator.windowing.ResolutionUtils
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.DataType
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.api.{Constants, DataModel}
import ai.chronon.flink.{AsyncKVStoreWriter, FlinkGroupByStreamingJob}
import ai.chronon.flink.deser.{DeserializationSchemaBuilder, FlinkSerDeProvider, ProjectedEvent, SourceProjection}
import ai.chronon.flink.chaining.ChainedGroupByJob
import ai.chronon.flink.source.FlinkSourceProvider
import ai.chronon.flink.types.{AvroCodecOutput, BatchIrRow, TimestampedTile, WriteResponse}
import ai.chronon.flink.validation.ValidationFlinkJob
import ai.chronon.flink.window.{
  AlwaysFireOnElementTrigger,
  BufferedProcessingTimeTrigger,
  FlinkRowAggProcessFunction,
  FlinkRowAggregationFunction,
  GigaTileProcessFunction,
  KeySelectorBuilder,
  MegaTileEmissionPolicy,
  MegaTileProcessFunction
}
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.online.{Api, GroupByServingInfoParsed, TopicInfo}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.OutputTag
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.rogach.scallop.{ScallopConf, ScallopOption, Serialization}
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** Base abstract class for all Flink streaming jobs in Chronon.
  * Defines the common interface and shared functionality for different job types.
  */
abstract class BaseFlinkJob {

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  def groupByName: String

  def groupByServingInfoParsed: GroupByServingInfoParsed

  /** Run the streaming job with tiling enabled (default mode).
    * This is the main execution method that should be implemented by subclasses.
    */
  def runTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse]

  /** Run the streaming job with mega tiling enabled.
    *
    * Default implementation throws — opt-in for any subclass that wants MegaTile. Concrete
    * BaseFlinkJob subclasses in this repo (FlinkGroupByStreamingJob, ChainedGroupByJob)
    * override and delegate to `buildMegaTiledTail`. Custom subclasses outside this repo
    * that don't override stay source-compatible (no compile break) but will fail loudly
    * at job startup if their GroupBy has `onlineStrategy = STREAMING_MEGATILES` — which
    * matches chronon's serving-correctness model: a misconfigured job should not silently
    * fall back to DEFAULT-mode output when the user explicitly asked for MegaTile.
    */
  def runMegaTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] =
    throw new UnsupportedOperationException(
      s"${getClass.getSimpleName} does not implement runMegaTiledGroupByJob. " +
        s"GroupBy '$groupByName' has onlineStrategy=STREAMING_MEGATILES, but the Flink job " +
        "subclass dispatched for it doesn't support mega-tiled execution. Override " +
        "runMegaTiledGroupByJob to opt in, or unset STREAMING_MEGATILES on this GroupBy."
    )

  /** Run the streaming job with giga tiling (push-based) enabled.
    * Emits finalized feature vectors — fetcher does a single point get.
    */
  def runGigaTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse]

  /** Shared tail for tiled pipeline: keyBy → window aggregate → tile codec → KV write.
    * Both FlinkGroupByStreamingJob and ChainedGroupByJob use this with their respective
    * prepared streams and schemas.
    */
  protected def buildTiledTail(
      preparedStream: DataStream[ProjectedEvent],
      schema: Seq[(String, DataType)],
      parallelism: Int,
      sinkFn: RichAsyncFunction[AvroCodecOutput, WriteResponse],
      kvStoreCapacity: Int,
      props: Map[String, String],
      topicInfo: TopicInfo,
      enableDebug: Boolean,
      uidSuffix: String = ""
  ): DataStream[WriteResponse] = {
    val tilingWindowSizeInMillis = ResolutionUtils.getSmallestTailHopMillis(groupByServingInfoParsed.groupBy)

    val window = TumblingEventTimeWindows
      .of(Time.milliseconds(tilingWindowSizeInMillis))
      .asInstanceOf[WindowAssigner[ProjectedEvent, TimeWindow]]

    val trigger = FlinkUtils
      .getProperty("trigger", props, topicInfo)
      .map {
        case "always_fire" => new AlwaysFireOnElementTrigger(): Trigger[ProjectedEvent, TimeWindow]
        case "buffered"    => new BufferedProcessingTimeTrigger(100L): Trigger[ProjectedEvent, TimeWindow]
        case t             => throw new IllegalArgumentException(s"Unsupported trigger type: $t")
      }
      .getOrElse(new AlwaysFireOnElementTrigger())

    val allowedLatenessMs = FlinkUtils.getAllowedLatenessMs(props, topicInfo)
    val tilingLateEventsTag = new OutputTag[ProjectedEvent]("tiling-late-events") {}

    val tilingDS: SingleOutputStreamOperator[TimestampedTile] =
      preparedStream
        .keyBy(KeySelectorBuilder.build(groupByServingInfoParsed.groupBy))
        .window(window)
        .allowedLateness(Time.milliseconds(allowedLatenessMs))
        .trigger(trigger)
        .sideOutputLateData(tilingLateEventsTag)
        .aggregate(
          new FlinkRowAggregationFunction(groupByServingInfoParsed.groupBy, schema, enableDebug),
          new FlinkRowAggProcessFunction(groupByServingInfoParsed.groupBy, schema, enableDebug)
        )
        .uid(s"tiling${uidSuffix}-$groupByName")
        .name(s"Tiling for $groupByName")
        .setParallelism(parallelism)

    tilingDS
      .getSideOutput(tilingLateEventsTag)
      .flatMap(new LateEventCounter(groupByName))
      .uid(s"tiling-side-output${uidSuffix}-$groupByName")
      .name(s"Tiling Side Output Late Data for $groupByName")
      .setParallelism(parallelism)

    val putRecordDS = tilingDS
      .flatMap(TiledAvroCodecFn(groupByServingInfoParsed, tilingWindowSizeInMillis, enableDebug))
      .uid(s"avro-conversion${uidSuffix}-$groupByName")
      .name(s"Avro conversion for $groupByName")
      .setParallelism(parallelism)

    AsyncKVStoreWriter.withUnorderedWaits(putRecordDS, sinkFn, groupByName, capacity = kvStoreCapacity)
  }

  /** Shared tail for mega tiled pipeline: keyBy → MegaTileProcessFunction → mega tile codec → KV write. */
  protected def buildMegaTiledTail(
      preparedStream: DataStream[ProjectedEvent],
      schema: Seq[(String, DataType)],
      parallelism: Int,
      sinkFn: RichAsyncFunction[AvroCodecOutput, WriteResponse],
      kvStoreCapacity: Int,
      enableDebug: Boolean,
      bufferingOutputTimeMillis: Long,
      bufferingOutputJitterMillis: Long,
      emissionPolicy: MegaTileEmissionPolicy
  ): DataStream[WriteResponse] = {
    val megaTileDS = preparedStream
      .keyBy(KeySelectorBuilder.build(groupByServingInfoParsed.groupBy))
      .process(
        new MegaTileProcessFunction(groupByServingInfoParsed.groupBy,
                                    schema,
                                    enableDebug,
                                    bufferingOutputTimeMillis,
                                    bufferingOutputJitterMillis,
                                    emissionPolicy))
      .uid(s"mega-tiling-$groupByName")
      .name(s"Mega Tiling for $groupByName")
      .setParallelism(parallelism)

    val putRecordDS = megaTileDS
      .flatMap(MegaTileAvroCodecFn(groupByServingInfoParsed, enableDebug))
      .uid(s"mega-avro-conversion-$groupByName")
      .name(s"Mega Tile Avro conversion for $groupByName")
      .setParallelism(parallelism)

    AsyncKVStoreWriter.withUnorderedWaits(putRecordDS, sinkFn, groupByName, capacity = kvStoreCapacity)
  }

  /** Shared tail for giga tiled pipeline: connect event + batch streams → GigaTileProcessFunction → codec → KV write.
    * Emits finalized feature vectors with plain entity keys.
    *
    * @param batchIrStream batch IR rows from Iceberg upload table (keyed by entity).
    *                      Use an idle/empty stream if Iceberg source is not yet configured.
    */
  protected def buildGigaTiledTail(
      preparedStream: DataStream[ProjectedEvent],
      batchIrStream: DataStream[BatchIrRow],
      schema: Seq[(String, DataType)],
      parallelism: Int,
      sinkFn: RichAsyncFunction[AvroCodecOutput, WriteResponse],
      kvStoreCapacity: Int,
      enableDebug: Boolean,
      bufferingOutputTimeMillis: Long
  ): DataStream[WriteResponse] = {
    val eventKeySelector = KeySelectorBuilder.build(groupByServingInfoParsed.groupBy)
    val batchKeySelector = new org.apache.flink.api.java.functions.KeySelector[BatchIrRow, java.util.List[Any]] {
      override def getKey(row: BatchIrRow): java.util.List[Any] = row.entityKeys
    }

    val gigaTileDS = preparedStream
      .connect(batchIrStream)
      .keyBy(eventKeySelector, batchKeySelector)
      .process(
        new GigaTileProcessFunction(groupByServingInfoParsed.groupBy,
                                    schema,
                                    enableDebug = enableDebug,
                                    bufferingOutputTimeMillis = bufferingOutputTimeMillis))
      .uid(s"giga-tiling-$groupByName")
      .name(s"Giga Tiling for $groupByName")
      .setParallelism(parallelism)

    val putRecordDS = gigaTileDS
      .flatMap(GigaTileAvroCodecFn(groupByServingInfoParsed, enableDebug))
      .uid(s"giga-avro-conversion-$groupByName")
      .name(s"Giga Tile Avro conversion for $groupByName")
      .setParallelism(parallelism)

    AsyncKVStoreWriter.withUnorderedWaits(putRecordDS, sinkFn, groupByName, capacity = kvStoreCapacity)
  }
}

object FlinkJob {
  // we set an explicit max parallelism to ensure if we do make parallelism setting updates, there's still room
  // to restore the job from prior state. Number chosen does have perf ramifications if too high (can impact rocksdb perf)
  // so we've chosen one that should allow us to scale to jobs in the 10K-50K events / s range.
  val MaxParallelism: Int = 1260 // highly composite number

  def runWriteInternalManifestJob(env: StreamExecutionEnvironment,
                                  manifestPath: String,
                                  parentJobId: String,
                                  groupByName: String): DataStreamSink[String] = {

    // check that the last character is a slash
    val outputPath = if (!manifestPath.endsWith("/")) {
      s"$manifestPath/$groupByName/manifest.txt"
    } else {
      manifestPath + s"$groupByName/manifest.txt"
    }

    env
      .fromElements(parentJobId)
      .uid(s"$groupByName-manifest-source-operator-$parentJobId")
      .name("Manifest source to map Flink job id to parent job id")
      .map(new RichMapFunction[String, String] {
        def map(parentJobId: String): String = {
          val flinkJobId = getRuntimeContext.getJobId
          s"flinkJobId=$flinkJobId,parentJobId=$parentJobId"
        }
      })
      .uid(s"$groupByName-manifest-map-operator-$parentJobId")
      .name("Create manifest string mapping Flink job id to parent job id")
      .setParallelism(1)
      .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE)
      .uid(s"$groupByName-manifest-sink-operator-$parentJobId")
      .name(s"Write manifest mapping to $outputPath")
      .setParallelism(1) // Use parallelism 1 to get a single output file
  }

  // We choose to checkpoint frequently to ensure the incremental checkpoints are small in size
  // as well as ensuring the catch-up backlog is fairly small in case of failures
  val CheckPointInterval: FiniteDuration = 10.seconds

  // We set a more lenient checkpoint timeout to guard against large backlog / catchup scenarios where checkpoints
  // might be slow and a tight timeout will set us on a snowball restart loop
  val CheckpointTimeout: FiniteDuration = 5.minutes

  // how many consecutive checkpoint failures can we tolerate - default is 0, we choose a more lenient value
  // to allow us a few tries before we give up
  val TolerableCheckpointFailures: Int = 5

  // Keep windows open for a bit longer before closing to ensure we don't lose data due to late arrivals (needed in case of
  // tiling implementation)
  val AllowedOutOfOrderness: Duration = Duration.ofMinutes(5)

  val CatchupWatermarkLagSlackMillis: Long = 30 * 1000L

  // The event-source watermark intentionally trails the newest observed event by
  // AllowedOutOfOrderness. Publication is safe once only that expected lag plus a small
  // operational slack remains; aggregation hop sizes do not describe source readiness.
  val LiveWatermarkLagToleranceMillis: Long =
    AllowedOutOfOrderness.toMillis + CatchupWatermarkLagSlackMillis

  // Set an idleness timeout to keep time moving in case of very low traffic event streams as well as late events during
  // large backlog catchups
  val IdlenessTimeout: Duration = Duration.ofSeconds(30)

  // default is 100ms, we set it to 10ms to reduce latency at the expense of throughput
  val NetworkBufferTimeout: Long = 10L

  // default is 200ms, we set it to 100ms to reduce latency at the expense of throughput
  val AutoWatermarkInterval: Long = 100L

  // We wire up the watermark strategy post the spark expr eval to be able to leverage the user's timestamp column (which is
  // ETLed to Contants.TimeColumn) as the event timestamp and watermark
  val watermarkStrategy: WatermarkStrategy[ProjectedEvent] = WatermarkStrategy
    .forBoundedOutOfOrderness[ProjectedEvent](AllowedOutOfOrderness)
    .withIdleness(IdlenessTimeout)
    .withTimestampAssigner(new SerializableTimestampAssigner[ProjectedEvent] {
      override def extractTimestamp(element: ProjectedEvent, recordTimestamp: Long): Long = {
        element.fields.get(Constants.TimeColumn).map(_.asInstanceOf[Long]).getOrElse(recordTimestamp)
      }
    })

  // Pull in the Serialization trait to sidestep: https://github.com/scallop/scallop/issues/137
  class JobArgs(args: Seq[String]) extends ScallopConf(args) with Serialization {

    val onlineClass: ScallopOption[String] =
      opt[String](required = true,
                  descr = "Fully qualified Online.Api based class. We expect the jar to be on the class path")
    val groupbyName: ScallopOption[String] =
      opt[String](required = true, descr = "The name of the groupBy to process")
    val mockSource: ScallopOption[Boolean] =
      opt[Boolean](required = false, descr = "Use a mocked data source instead of a real source", default = Some(false))
    // Kafka config is optional as we can support other sources in the future
    val kafkaBootstrap: ScallopOption[String] =
      opt[String](required = false, descr = "Kafka bootstrap server in host:port format")
    // Run in validate mode - We read rows using Kafka and run them through Spark Df and compare against CatalystUtil output
    val validate: ScallopOption[Boolean] =
      opt[Boolean](required = false, descr = "Run in validate mode", default = Some(false))
    // Number of rows to use for validation
    val validateRows: ScallopOption[Int] =
      opt[Int](required = false, descr = "Number of rows to use for validation", default = Some(10000))
    val parentJobId: ScallopOption[String] =
      opt[String](required = false,
                  descr = "Parent job id that invoked the Flink job. For example, the Dataproc job id.")

    val apiProps: Map[String, String] = props[String]('Z', descr = "Props to configure API / KV Store")

    val streamingManifestPath: ScallopOption[String] =
      opt[String](required = true, descr = "Bucket to write the manifest to")

    val enableDebug: ScallopOption[Boolean] =
      opt[Boolean](required = false, descr = "Enable debug logging mode", default = Some(false))

    val topicOverride: ScallopOption[String] =
      opt[String](required = false,
                  descr = "Optional topic URI override. If set, used instead of topic from GroupByServingInfo.")

    verify()
  }

  def main(args: Array[String]): Unit = {
    val jobArgs = new JobArgs(args)
    val groupByName = jobArgs.groupbyName()
    val onlineClassName = jobArgs.onlineClass()
    val props = jobArgs.apiProps.map(identity)
    val validateMode = jobArgs.validate()
    val validateRows = jobArgs.validateRows()
    val maybeParentJobId = jobArgs.parentJobId.toOption
    val enableDebug = jobArgs.enableDebug()

    val api = buildApi(onlineClassName, props)
    val kvStore = api.genKvStore
    val metadataStore = new MetadataStore(FetchContext(kvStore, MetadataDataset))

    if (validateMode) {
      val validationResults = ValidationFlinkJob.run(metadataStore, props, groupByName, validateRows)
      if (validationResults.map(_.totalMismatches).sum > 0) {
        val validationSummary = s"Total records: ${validationResults.map(_.totalRecords).sum}, " +
          s"Total matches: ${validationResults.map(_.totalMatches).sum}, " +
          s"Total mismatches: ${validationResults.map(_.totalMismatches).sum}"
        throw new IllegalStateException(
          s"Spark DF vs Catalyst util validation failed. Validation summary: $validationSummary")
      }
    }

    val maybeServingInfo = metadataStore.getGroupByServingInfo(groupByName)
    val flinkJob =
      maybeServingInfo
        .map { servingInfo =>
          // create the groupby dataset on the KV store if it doesn't exist prior to starting up the job
          if (new GroupByOps(servingInfo.groupBy).isPushEnabled)
            kvStore.create(servingInfo.groupByOps.pushDataset)
          else
            kvStore.create(servingInfo.groupBy.streamingDataset)
          buildFlinkJob(groupByName, props, api, servingInfo, enableDebug, jobArgs.topicOverride.toOption)
        }
        .recover { case e: Exception =>
          throw new IllegalArgumentException(s"Unable to lookup serving info for GroupBy: '$groupByName'", e)
        }
        .get

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(CheckPointInterval.toMillis, CheckpointingMode.AT_LEAST_ONCE)
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setMinPauseBetweenCheckpoints(CheckPointInterval.toMillis)
    checkpointConfig.setCheckpointTimeout(CheckpointTimeout.toMillis)
    checkpointConfig.setMaxConcurrentCheckpoints(1)
    checkpointConfig.setTolerableCheckpointFailureNumber(TolerableCheckpointFailures)
    // for now we retain our checkpoints even when we can cancel to allow us to resume from where we left off
    // post orchestrator, we will trigger savepoints on deploys and we can switch to delete on cancel
    checkpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // Flink default is to restart indefinitely on failure. We want to cap this to be able to
    // bubble up errors to the orchestrator
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(
        20,
        30.seconds.toMillis
      ))

    val config = new Configuration()

    env.setMaxParallelism(MaxParallelism)

    env.getConfig.setAutoWatermarkInterval(AutoWatermarkInterval)
    env.setBufferTimeout(NetworkBufferTimeout)
    env.getConfig.disableAutoGeneratedUIDs() // we generate UIDs manually to ensure consistency across runs
    env.getConfig
      .enableForceKryo() // use kryo for complex types that Flink's default ser system doesn't support (e.g case classes)
    env.getConfig.enableGenericTypes() // more permissive type checks

    env.configure(config)

    // Store the mapping between parent job id and flink job id to bucket
    if (maybeParentJobId.isDefined) {
      FlinkJob.runWriteInternalManifestJob(env, jobArgs.streamingManifestPath(), maybeParentJobId.get, groupByName)
    }

    val groupByOpsVal = new GroupByOps(flinkJob.groupByServingInfoParsed.groupBy)
    val jobDatastream =
      if (groupByOpsVal.isPushEnabled) flinkJob.runGigaTiledGroupByJob(env)
      else if (groupByOpsVal.isMegaTilingEnabled) flinkJob.runMegaTiledGroupByJob(env)
      else flinkJob.runTiledGroupByJob(env)

    jobDatastream
      .addSink(new MetricsSink(groupByName))
      .uid(s"metrics-sink - $groupByName")
      .name(s"Metrics Sink for $groupByName")
      .setParallelism(jobDatastream.getParallelism)

    env.execute(s"$groupByName")
  }

  private def buildFlinkJob(groupByName: String,
                            props: Map[String, String],
                            api: Api,
                            servingInfo: GroupByServingInfoParsed,
                            enableDebug: Boolean = false,
                            maybeTopicOverride: Option[String] = None): BaseFlinkJob = {
    // Check if this is a JoinSource GroupBy
    if (servingInfo.groupBy.streamingSource.get.isSetJoinSource) {
      buildJoinSourceFlinkJob(groupByName, props, api, servingInfo, enableDebug, maybeTopicOverride)
    } else {
      buildGroupByStreamingJob(groupByName, props, api, servingInfo, enableDebug, maybeTopicOverride)
    }
  }

  private def buildJoinSourceFlinkJob(groupByName: String,
                                      props: Map[String, String],
                                      api: Api,
                                      servingInfo: GroupByServingInfoParsed,
                                      enableDebug: Boolean,
                                      maybeTopicOverride: Option[String] = None): ChainedGroupByJob = {
    val logger = LoggerFactory.getLogger(getClass)

    val joinSource = servingInfo.groupBy.streamingSource.get.getJoinSource
    val leftSource = joinSource.getJoin.getLeft
    require(
      leftSource.dataModel == DataModel.EVENTS,
      s"Data model on the left source for chaining jobs must be EVENTs. " +
        s"Found: ${leftSource.dataModel} for groupBy: $groupByName"
    )

    val rawTopic = maybeTopicOverride.getOrElse(leftSource.topic)
    val topicInfo = TopicInfo.parse(rawTopic)
    val schemaProvider = FlinkSerDeProvider.build(topicInfo)

    // Use left source query for deserialization schema - this is the topic & schema we use to drive
    // the JoinSource processing
    val leftSourceQuery = leftSource.query
    val leftSourceGroupByName = s"left_source_${joinSource.getJoin.getMetaData.getName}"

    val deserializationSchema = DeserializationSchemaBuilder.buildSourceProjectionDeserSchema(
      schemaProvider,
      leftSourceQuery,
      leftSourceGroupByName,
      DataModel.EVENTS,
      enableDebug
    )

    require(
      deserializationSchema.isInstanceOf[SourceProjection],
      s"Expect created deserialization schema for left source: $leftSourceGroupByName with $topicInfo to mixin SourceProjection. " +
        s"We got: ${deserializationSchema.getClass.getSimpleName}"
    )

    val projectedSchema =
      try {
        deserializationSchema.asInstanceOf[SourceProjection].projectedSchema
      } catch {
        case _: Exception =>
          throw new RuntimeException(
            s"Failed to perform projection via Spark SQL eval for groupBy: $groupByName. Retrieved event schema: \n${schemaProvider.schema}\n" +
              s"Make sure the Spark SQL expressions are valid (e.g. column names match the source event schema).")
      }

    val source = FlinkSourceProvider.build(props, deserializationSchema, topicInfo)
    val sinkFn = new AsyncKVStoreWriter(api, servingInfo.groupBy.metaData.name, enableDebug)

    logger.info(s"Building JoinSource GroupBy: $groupByName. Using FlinkJoinSourceJob.")
    new ChainedGroupByJob(
      eventSrc = source,
      inputSchema = projectedSchema,
      sinkFn = sinkFn,
      groupByServingInfoParsed = servingInfo,
      parallelism = source.parallelism,
      props = props,
      topicInfo = topicInfo,
      api = api,
      enableDebug = enableDebug
    )
  }

  private def buildGroupByStreamingJob(groupByName: String,
                                       props: Map[String, String],
                                       api: Api,
                                       servingInfo: GroupByServingInfoParsed,
                                       enableDebug: Boolean,
                                       maybeTopicOverride: Option[String] = None): FlinkGroupByStreamingJob = {
    val logger = LoggerFactory.getLogger(getClass)

    val rawTopic = maybeTopicOverride.getOrElse(servingInfo.groupBy.streamingSource.get.topic)
    val topicInfo = TopicInfo.parse(rawTopic)
    val schemaProvider = FlinkSerDeProvider.build(topicInfo)

    // Use the existing GroupBy-based interface for regular GroupBys
    val deserializationSchema = DeserializationSchemaBuilder.buildSourceProjectionDeserSchema(
      schemaProvider,
      servingInfo.groupBy,
      enableDebug
    )

    require(
      deserializationSchema.isInstanceOf[SourceProjection],
      s"Expect created deserialization schema for groupBy: $groupByName with $topicInfo to mixin SourceProjection. " +
        s"We got: ${deserializationSchema.getClass.getSimpleName}"
    )

    val projectedSchema =
      try {
        deserializationSchema.asInstanceOf[SourceProjection].projectedSchema
      } catch {
        case _: Exception =>
          throw new RuntimeException(
            s"Failed to perform projection via Spark SQL eval for groupBy: $groupByName. Retrieved event schema: \n${schemaProvider.schema}\n" +
              s"Make sure the Spark SQL expressions are valid (e.g. column names match the source event schema).")
      }

    val source = FlinkSourceProvider.build(props, deserializationSchema, topicInfo)
    val sinkFn = new AsyncKVStoreWriter(api, servingInfo.groupBy.metaData.name, enableDebug)

    logger.info(s"Building regular GroupBy: $groupByName. Using FlinkGroupByStreamingJob.")
    new FlinkGroupByStreamingJob(
      eventSrc = source,
      inputSchema = projectedSchema,
      sinkFn = sinkFn,
      groupByServingInfoParsed = servingInfo,
      parallelism = source.parallelism,
      props = props,
      topicInfo = topicInfo,
      enableDebug = enableDebug
    )
  }

  private def buildApi(onlineClass: String, props: Map[String, String]): Api = {
    val cl = Thread.currentThread().getContextClassLoader // Use Flink's classloader
    val cls = cl.loadClass(onlineClass)
    val constructor = cls.getConstructors.apply(0)
    val onlineImpl = constructor.newInstance(props)
    onlineImpl.asInstanceOf[Api]
  }
}
