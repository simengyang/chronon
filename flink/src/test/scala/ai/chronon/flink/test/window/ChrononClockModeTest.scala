package ai.chronon.flink.test.window

import ai.chronon.flink.FlinkJob
import ai.chronon.flink.window.ChrononClockMode
import ai.chronon.flink.window.ChrononClockMode.{Catchup, Live, NoWatermark}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ChrononClockModeTest extends AnyFlatSpec with Matchers {
  private val hopMillis = 5 * 60 * 1000L
  private val exactHop = 10 * hopMillis
  private val sourceOutOfOrderness = FlinkJob.AllowedOutOfOrderness.toMillis
  private val liveBoundary = FlinkJob.LiveWatermarkLagToleranceMillis
  private val aggregationHops = Seq(hopMillis, 60 * 60 * 1000L, 24 * 60 * 60 * 1000L)

  "ChrononClockMode" should "fence an uninitialized connected watermark without timer horizons" in {
    val mode = ChrononClockMode.classify(exactHop, Long.MinValue, sourceOutOfOrderness, liveBoundary)

    mode shouldBe NoWatermark
    ChrononClockMode.allowsPublication(mode) shouldBe false
    ChrononClockMode.dayAdvanceAsOfMillis(mode, exactHop, Long.MinValue) shouldBe None
    ChrononClockMode.processingTimerHorizons(mode, exactHop, Long.MinValue, hopMillis) shouldBe None
    ChrononClockMode.streamEventHorizons(mode, exactHop, exactHop, Long.MinValue, hopMillis) shouldBe
      ChrononClockMode.Horizons(exactHop, exactHop + 1L)
  }

  it should "keep every key fenced while the global watermark is behind" in {
    val processingTime = exactHop + liveBoundary + 1L
    val mode = ChrononClockMode.classify(processingTime, exactHop, sourceOutOfOrderness, liveBoundary)

    mode shouldBe Catchup
    ChrononClockMode.allowsPublication(mode) shouldBe false
    ChrononClockMode.dayAdvanceAsOfMillis(mode, processingTime, exactHop) shouldBe Some(exactHop)
    ChrononClockMode.processingTimerHorizons(mode, processingTime, exactHop, hopMillis) shouldBe
      Some(ChrononClockMode.Horizons(exactHop, exactHop + 1L))
  }

  it should "use one source-readiness tolerance for every aggregation hop" in {
    val processingTime = 20L * aggregationHops.last
    val justBehind = processingTime - liveBoundary - 1L
    val atBoundary = processingTime - liveBoundary

    aggregationHops.foreach { aggregationHop =>
      ChrononClockMode.classify(processingTime, justBehind, sourceOutOfOrderness, liveBoundary) shouldBe Catchup
      ChrononClockMode.classify(processingTime, atBoundary, sourceOutOfOrderness, liveBoundary) shouldBe Live
      ChrononClockMode.processingTimerHorizons(Live, processingTime, atBoundary, aggregationHop) shouldBe
        Some(ChrononClockMode.Horizons(processingTime, processingTime + 1L))
    }
  }

  it should "be live at the lag boundary and preserve separate exact-hop horizons" in {
    val processingTime = exactHop
    val watermark = processingTime - liveBoundary
    val mode = ChrononClockMode.classify(processingTime, watermark, sourceOutOfOrderness, liveBoundary)

    mode shouldBe Live
    ChrononClockMode.allowsPublication(mode) shouldBe true
    ChrononClockMode.streamEventHorizons(mode, watermark, processingTime, watermark, hopMillis) shouldBe
      ChrononClockMode.Horizons(processingTime, processingTime + 1L)
  }

  it should "leave non-hop small horizons unchanged" in {
    val processingTime = exactHop + sourceOutOfOrderness + 17L
    val watermark = processingTime - sourceOutOfOrderness - 1L
    val mode = ChrononClockMode.classify(processingTime, watermark, sourceOutOfOrderness, liveBoundary)

    ChrononClockMode.streamEventHorizons(mode, processingTime, processingTime, watermark, hopMillis) shouldBe
      ChrononClockMode.Horizons(processingTime, processingTime)
  }

  it should "fence a watermark that implies a future source timestamp" in {
    val processingTime = exactHop + sourceOutOfOrderness
    val healthyWatermark = processingTime - sourceOutOfOrderness - 1L
    val futureWatermark = healthyWatermark + 1L

    ChrononClockMode.classify(processingTime,
                              healthyWatermark,
                              sourceOutOfOrderness,
                              liveBoundary) shouldBe Live
    ChrononClockMode.classify(processingTime,
                              futureWatermark,
                              sourceOutOfOrderness,
                              liveBoundary) shouldBe NoWatermark
    ChrononClockMode.classify(processingTime,
                              Long.MaxValue,
                              sourceOutOfOrderness,
                              liveBoundary) shouldBe NoWatermark
  }

  it should "avoid overflow near the end of the timestamp range" in {
    val watermark = Long.MaxValue - sourceOutOfOrderness - 1L
    val mode = ChrononClockMode.classify(Long.MaxValue, watermark, sourceOutOfOrderness, liveBoundary)

    mode shouldBe Live
    ChrononClockMode.processingTimerHorizons(mode, Long.MaxValue, watermark, hopMillis) shouldBe
      Some(ChrononClockMode.Horizons(Long.MaxValue, Long.MaxValue))
  }
}
