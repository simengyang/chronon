package ai.chronon.flink.source

import ai.chronon.api.Extensions.{GroupByOps, MetadataOps}
import ai.chronon.api.Constants
import ai.chronon.flink.types.BatchIrRow
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.serde.AvroConversions
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.data.RowData
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.text.{ParseException, ParsePosition}
import java.time.Duration

/** Builds a DataStream[BatchIrRow] from the GroupBy's upload Iceberg table.
  *
  * The Iceberg source operates in streaming monitor mode: it scans the latest partition
  * on startup, then monitors for new snapshots every `monitorInterval`.
  *
  * Configuration via props:
  *   - `iceberg.catalog.warehouse`: warehouse path (required for Iceberg source)
  *   - `iceberg.catalog.uri`: metastore URI (for hive catalog)
  *   - `iceberg.monitor.interval.minutes`: snapshot monitor interval (default: 30)
  *
  * Falls back to an idle (empty) stream if Iceberg is not configured unless the caller has
  * explicitly enabled behavior that depends on distinguishing an absent batch row.
  */
object BatchIrSourceBuilder {

  private val logger = LoggerFactory.getLogger(getClass)

  def build(env: StreamExecutionEnvironment,
            servingInfo: GroupByServingInfoParsed,
            props: Map[String, String],
            requireConfiguredBatchSource: Boolean = false): DataStream[BatchIrRow] = {

    val warehouse = props.getOrElse("iceberg.catalog.warehouse", "")
    val groupByName = servingInfo.groupByOps.metaData.getName

    if (warehouse.isEmpty) {
      val message = s"No iceberg.catalog.warehouse configured for $groupByName"
      if (requireConfiguredBatchSource) {
        throw new IllegalArgumentException(
          s"$message; a configured batch source is required when gigatile_first_seen_key_grace_millis is enabled")
      }
      logger.warn(s"$message. Returning idle batch IR stream (batch loading disabled).")
      return buildIdleStream(env, groupByName)
    }

    try {
      val monitorIntervalMin = props.getOrElse("iceberg.monitor.interval.minutes", "30").toLong
      buildIcebergStream(env,
                         servingInfo,
                         props,
                         warehouse,
                         monitorIntervalMin,
                         failOnDecodeError = requireConfiguredBatchSource)
    } catch {
      case e: Exception =>
        if (requireConfiguredBatchSource) {
          throw new IllegalStateException(s"Failed to create required Iceberg source for $groupByName", e)
        }
        logger.error(s"Failed to create Iceberg source for $groupByName. Falling back to idle stream.", e)
        buildIdleStream(env, groupByName)
    }
  }

  private def buildIcebergStream(env: StreamExecutionEnvironment,
                                 servingInfo: GroupByServingInfoParsed,
                                 props: Map[String, String],
                                 warehouse: String,
                                 monitorIntervalMin: Long,
                                 failOnDecodeError: Boolean): DataStream[BatchIrRow] = {
    import org.apache.iceberg.catalog.TableIdentifier
    import org.apache.iceberg.flink.{CatalogLoader, TableLoader}
    import org.apache.iceberg.flink.source.IcebergSource

    val uploadTable = servingInfo.groupByOps.metaData.uploadTable
    val groupByName = servingInfo.groupByOps.metaData.getName
    val catalogProps = new java.util.HashMap[String, String]()
    catalogProps.put("type", props.getOrElse("iceberg.catalog.type", "hadoop"))
    catalogProps.put("warehouse", warehouse)
    props.get("iceberg.catalog.uri").foreach(catalogProps.put("uri", _))

    val catalogLoader =
      CatalogLoader.hadoop("chronon_catalog", new org.apache.hadoop.conf.Configuration(), catalogProps)
    val tableId = TableIdentifier.parse(uploadTable)
    val tableLoader = TableLoader.fromCatalog(catalogLoader, tableId)

    val icebergSource = IcebergSource
      .forRowData()
      .tableLoader(tableLoader)
      .streaming(true)
      .monitorInterval(Duration.ofMinutes(monitorIntervalMin))
      .build()

    // Iceberg source with idleness — prevents stalling the global watermark
    val batchWatermark = WatermarkStrategy
      .noWatermarks[RowData]()
      .withIdleness(Duration.ofMinutes(monitorIntervalMin + 5))

    val rawStream = env
      .fromSource(icebergSource, batchWatermark, s"Iceberg batch source for $uploadTable")
      .uid(s"iceberg-batch-source-$groupByName")
      .setParallelism(1)

    rawStream
      .flatMap(new BatchIrRowDecoder(servingInfo, failOnDecodeError))
      .uid(s"batch-ir-decode-$groupByName")
      .name(s"Decode batch IR for $groupByName")
      .setParallelism(1)
  }

  /** Returns an empty stream that completes immediately. Used as fallback when Iceberg is not configured. */
  private def buildIdleStream(env: StreamExecutionEnvironment, groupByName: String): DataStream[BatchIrRow] = {
    env
      .fromElements(new BatchIrRow())
      .filter(_ => false)
      .uid(s"idle-batch-ir-source-$groupByName")
      .name(s"Idle batch IR source for $groupByName")
      .setParallelism(1)
      .returns(classOf[BatchIrRow])
  }
}

/** Decodes Iceberg RowData (key_bytes, value_bytes, key_json, value_json, ds) into BatchIrRow.
  * batchEnd is derived from the ds partition column — NOT from the static servingInfo.batchEndTsMillis.
  * Each new partition (ds=2026-03-29) produces a fresh batchEnd, ensuring onBatchUpdate accepts it.
  */
class BatchIrRowDecoder(servingInfo: GroupByServingInfoParsed, failOnDecodeError: Boolean = false)
    extends RichFlatMapFunction[RowData, BatchIrRow] {

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  @transient private lazy val keyDecoder: Array[Byte] => java.util.List[Any] = {
    val keySchema = servingInfo.keyChrononSchema
    val converter = AvroConversions.genericRecordToChrononRowConverter(keySchema)
    val codec = servingInfo.keyCodec
    bytes: Array[Byte] => {
      val record = codec.decode(bytes)
      val decoded = converter(record)
      val keys = new java.util.ArrayList[Any](decoded.length)
      decoded.foreach(keys.add)
      keys
    }
  }

  @transient private lazy val dsParser: String => Long = {
    val fmt = new java.text.SimpleDateFormat(servingInfo.groupByServingInfo.getDateFormat)
    fmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
    fmt.setLenient(false)
    ds: String => {
      val position = new ParsePosition(0)
      val parsed = fmt.parse(ds, position)
      if (parsed == null || position.getIndex != ds.length) {
        val errorOffset = math.max(position.getErrorIndex, position.getIndex)
        throw new ParseException(s"Invalid ds partition value: $ds", errorOffset)
      }
      parsed.getTime
    }
  }

  // Upload table columns: key_bytes(0), value_bytes(1), key_json(2), value_json(3), ds(4)
  private val KeyJsonColumnIndex = 2
  private val DsColumnIndex = 4

  override def flatMap(row: RowData, out: Collector[BatchIrRow]): Unit = {
    try {
      // GroupByUpload appends one metadata row to every upload partition and filters it by
      // key_json in its own reload path. It is not an entity BatchIrRow, even in strict mode.
      if (
        row.getArity > KeyJsonColumnIndex && !row.isNullAt(KeyJsonColumnIndex) &&
        row.getString(KeyJsonColumnIndex).toString == Constants.GroupByServingInfoKey
      ) return

      val keyBytes = row.getBinary(0)
      val valueBytes = row.getBinary(1)
      if (keyBytes == null || valueBytes == null) {
        if (failOnDecodeError) throw new IllegalArgumentException("Batch IR row has null key_bytes or value_bytes")
        return
      }

      val entityKeys = keyDecoder(keyBytes)

      // Derive batchEnd from the ds partition column. Chronon convention:
      // batchEnd = after(ds) = ds + 1 day. Batch for ds=2026-03-28 covers
      // [Mar 28 00:00, Mar 29 00:00), so batchEnd = Mar 29 00:00.
      // See GroupByUpload: batchEndDate = partitionSpec.after(endDs).
      val DayMillis = 24 * 3600 * 1000L
      val batchEnd =
        if (row.getArity > DsColumnIndex && !row.isNullAt(DsColumnIndex)) {
          Math.addExact(dsParser(row.getString(DsColumnIndex).toString), DayMillis)
        } else if (failOnDecodeError) {
          throw new IllegalArgumentException("Batch IR row is missing required ds partition value")
        } else {
          servingInfo.batchEndTsMillis
        }

      out.collect(new BatchIrRow(entityKeys, valueBytes, batchEnd))
    } catch {
      case e: Exception =>
        logger.error("Error decoding batch IR row from Iceberg", e)
        if (failOnDecodeError) throw e
    }
  }
}
