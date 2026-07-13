package ai.chronon.flink.test

import ai.chronon.api._
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.flink.source.{BatchIrRowDecoder, BatchIrSourceBuilder}
import ai.chronon.flink.types.BatchIrRow
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.serde.AvroConversions
import org.apache.flink.table.data.{GenericRowData, StringData}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

/** Tests BatchIrRowDecoder's batchEnd derivation from the ds partition column.
  * Verifies the +1 day convention: ds=2026-03-28 → batchEnd = Mar 29 00:00 UTC.
  */
class BatchIrRowDecoderTest extends AnyFlatSpec {

  val DayMillis: Long = 24 * 3600 * 1000L

  private def makeServingInfo(batchEndDate: String = "2026-03-29",
                               dateFormat: String = "yyyy-MM-dd"): GroupByServingInfoParsed = {
    val gb = new GroupBy()
    gb.setAggregations(Seq(
      Builders.Aggregation(Operation.COUNT, "num", Seq(new Window(1, TimeUnit.DAYS)))
    ).asJava)
    val meta = new MetaData()
    meta.setName("test_batch_ir_decoder")
    gb.setMetaData(meta)
    gb.setSources(new java.util.ArrayList(util.Arrays.asList(
      Builders.Source.events(
        table = "db.events",
        topic = "events_topic",
        query = Builders.Query(selects = Map("num" -> "num"), timeColumn = "ts")
      )
    )))
    gb.setKeyColumns(new java.util.ArrayList(util.Arrays.asList("entity_id")))

    val keySchema = StructType("Key", Array(StructField("entity_id", StringType)))
    val inputSchema = StructType("Input", Array(
      StructField("entity_id", StringType),
      StructField("num", LongType),
      StructField("ts", LongType)
    ))
    val selectedSchema = StructType("Selected", Array(
      StructField("num", LongType),
      StructField("ts", LongType)
    ))

    val servingInfo = new GroupByServingInfo()
    servingInfo.setGroupBy(gb)
    servingInfo.setBatchEndDate(batchEndDate)
    servingInfo.setDateFormat(dateFormat)
    servingInfo.setKeyAvroSchema(AvroConversions.fromChrononSchema(keySchema).toString(true))
    servingInfo.setInputAvroSchema(AvroConversions.fromChrononSchema(inputSchema).toString(true))
    servingInfo.setSelectedAvroSchema(AvroConversions.fromChrononSchema(selectedSchema).toString(true))
    new GroupByServingInfoParsed(servingInfo)
  }

  /** Builds a mock RowData with 5 columns: key_bytes, value_bytes, key_json, value_json, ds. */
  private def makeRowData(keyBytes: Array[Byte],
                          valueBytes: Array[Byte],
                          ds: String,
                          keyJson: String = ""): GenericRowData = {
    val row = new GenericRowData(5)
    row.setField(0, keyBytes)
    row.setField(1, valueBytes)
    row.setField(2, if (keyJson == null) null else StringData.fromString(keyJson)) // key_json
    row.setField(3, StringData.fromString(""))   // value_json
    row.setField(4, if (ds == null) null else StringData.fromString(ds))
    row
  }

  /** Collecting Collector that captures emitted BatchIrRows. */
  class CollectingCollector extends Collector[BatchIrRow] {
    val collected: mutable.ArrayBuffer[BatchIrRow] = mutable.ArrayBuffer.empty
    override def collect(record: BatchIrRow): Unit = collected += record
    override def close(): Unit = {}
  }

  it should "derive batchEnd = ds + 1 day (not ds) matching after(endDs) convention" in {
    val servingInfo = makeServingInfo(batchEndDate = "2026-03-29")
    val decoder = new BatchIrRowDecoder(servingInfo)

    val keyEncoder = AvroConversions.encodeBytes(servingInfo.keyChrononSchema, null)
    val keyBytes = keyEncoder(Array[Any]("user_123"))
    val valueBytes = Array[Byte](1, 2, 3) // dummy batch IR bytes

    val collector = new CollectingCollector()

    // ds=2026-03-28 → batchEnd should be Mar 29 00:00 (ds + 1 day)
    val row1 = makeRowData(keyBytes, valueBytes, "2026-03-28")
    decoder.flatMap(row1, collector)
    assertEquals("should emit one row", 1, collector.collected.size)

    val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd")
    fmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
    val expectedBatchEnd1 = fmt.parse("2026-03-28").getTime + DayMillis // Mar 29 00:00
    assertEquals("ds=2026-03-28 → batchEnd should be Mar 29 00:00", expectedBatchEnd1, collector.collected(0).batchEndTs)

    // ds=2026-03-29 → batchEnd should be Mar 30 00:00
    val row2 = makeRowData(keyBytes, valueBytes, "2026-03-29")
    decoder.flatMap(row2, collector)
    assertEquals("should emit two rows", 2, collector.collected.size)

    val expectedBatchEnd2 = fmt.parse("2026-03-29").getTime + DayMillis // Mar 30 00:00
    assertEquals("ds=2026-03-29 → batchEnd should be Mar 30 00:00", expectedBatchEnd2, collector.collected(1).batchEndTs)

    // Verify the two batchEnds are different (sequential updates won't be rejected as stale)
    assertTrue("second batchEnd should be > first", collector.collected(1).batchEndTs > collector.collected(0).batchEndTs)
  }

  it should "fall back to servingInfo.batchEndTsMillis when ds column is missing" in {
    val servingInfo = makeServingInfo(batchEndDate = "2026-03-29")
    val decoder = new BatchIrRowDecoder(servingInfo)

    val keyEncoder = AvroConversions.encodeBytes(servingInfo.keyChrononSchema, null)
    val keyBytes = keyEncoder(Array[Any]("user_123"))
    val valueBytes = Array[Byte](1, 2, 3)

    // Row with only 2 columns (no ds)
    val row = new GenericRowData(2)
    row.setField(0, keyBytes)
    row.setField(1, valueBytes)

    val collector = new CollectingCollector()
    decoder.flatMap(row, collector)
    assertEquals("should emit one row", 1, collector.collected.size)
    assertEquals("should use servingInfo.batchEndTsMillis",
                  servingInfo.batchEndTsMillis, collector.collected(0).batchEndTs)
  }

  it should "fail malformed rows closed only when an absent-row fallback requires them" in {
    val servingInfo = makeServingInfo()
    val malformed = makeRowData(Array[Byte](1, 2, 3), Array[Byte](4, 5, 6), "2026-03-28")

    val bestEffortCollector = new CollectingCollector()
    new BatchIrRowDecoder(servingInfo).flatMap(malformed, bestEffortCollector)
    assertTrue(bestEffortCollector.collected.isEmpty)

    val requiredCollector = new CollectingCollector()
    intercept[Exception] {
      new BatchIrRowDecoder(servingInfo, failOnDecodeError = true).flatMap(malformed, requiredCollector)
    }
    assertTrue(requiredCollector.collected.isEmpty)
  }

  it should "skip the upload metadata row even when strict decoding is enabled" in {
    val decoder = new BatchIrRowDecoder(makeServingInfo(), failOnDecodeError = true)
    val collector = new CollectingCollector()
    val metadataRow = makeRowData(Constants.GroupByServingInfoKey.getBytes(Constants.UTF8),
                                  Array[Byte](1, 2, 3),
                                  "2026-03-28",
                                  keyJson = Constants.GroupByServingInfoKey)

    decoder.flatMap(metadataRow, collector)

    assertTrue(collector.collected.isEmpty)
  }

  it should "reject null key or value bytes when an absent-row fallback requires them" in {
    val decoder = new BatchIrRowDecoder(makeServingInfo(), failOnDecodeError = true)

    val error = intercept[IllegalArgumentException] {
      decoder.flatMap(makeRowData(null, Array[Byte](1), "2026-03-28"), new CollectingCollector())
    }
    assertTrue(error.getMessage.contains("null key_bytes or value_bytes"))
  }

  it should "require a ds partition value when the absent-row fallback is enabled" in {
    val servingInfo = makeServingInfo()
    val decoder = new BatchIrRowDecoder(servingInfo, failOnDecodeError = true)
    val keyEncoder = AvroConversions.encodeBytes(servingInfo.keyChrononSchema, null)
    val keyBytes = keyEncoder(Array[Any]("user_123"))
    val valueBytes = Array[Byte](1, 2, 3)

    val missingDs = new GenericRowData(2)
    missingDs.setField(0, keyBytes)
    missingDs.setField(1, valueBytes)
    val missingError = intercept[IllegalArgumentException] {
      decoder.flatMap(missingDs, new CollectingCollector())
    }
    assertTrue(missingError.getMessage.contains("missing required ds"))

    val nullError = intercept[IllegalArgumentException] {
      decoder.flatMap(makeRowData(keyBytes, valueBytes, null), new CollectingCollector())
    }
    assertTrue(nullError.getMessage.contains("missing required ds"))
  }

  it should "reject invalid or partially parsed ds values when the absent-row fallback is enabled" in {
    val servingInfo = makeServingInfo()
    val decoder = new BatchIrRowDecoder(servingInfo, failOnDecodeError = true)
    val keyEncoder = AvroConversions.encodeBytes(servingInfo.keyChrononSchema, null)
    val keyBytes = keyEncoder(Array[Any]("user_123"))
    val valueBytes = Array[Byte](1, 2, 3)

    Seq("2026-02-31", "2026-03-28-junk").foreach { ds =>
      val error = intercept[java.text.ParseException] {
        decoder.flatMap(makeRowData(keyBytes, valueBytes, ds), new CollectingCollector())
      }
      assertTrue(error.getMessage.contains("Invalid ds partition value"))
    }
  }

  "BatchIrSourceBuilder" should "preserve the idle fallback when no batch source is required" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = BatchIrSourceBuilder.build(env, makeServingInfo(), Map.empty)

    assertEquals("Idle batch IR source for test_batch_ir_decoder", stream.getTransformation.getName)
  }

  it should "reject a missing warehouse when an absent-row fallback requires the batch source" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val error = intercept[IllegalArgumentException] {
      BatchIrSourceBuilder.build(env,
                                 makeServingInfo(),
                                 Map.empty,
                                 requireConfiguredBatchSource = true)
    }
    assertTrue(error.getMessage.contains("gigatile_first_seen_key_grace_millis"))
  }

  it should "fail closed on source construction errors only when the batch source is required" in {
    val props = Map(
      "iceberg.catalog.warehouse" -> "/tmp/unused-warehouse",
      "iceberg.monitor.interval.minutes" -> "not-a-number"
    )

    val fallbackEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val fallback = BatchIrSourceBuilder.build(fallbackEnv, makeServingInfo(), props)
    assertEquals("Idle batch IR source for test_batch_ir_decoder", fallback.getTransformation.getName)

    val requiredEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val error = intercept[IllegalStateException] {
      BatchIrSourceBuilder.build(requiredEnv,
                                 makeServingInfo(),
                                 props,
                                 requireConfiguredBatchSource = true)
    }
    assertTrue(error.getMessage.contains("Failed to create required Iceberg source"))
    assertTrue(error.getCause.isInstanceOf[NumberFormatException])
  }
}
