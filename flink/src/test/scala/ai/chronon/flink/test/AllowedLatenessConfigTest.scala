package ai.chronon.flink.test

import ai.chronon.flink.FlinkUtils
import ai.chronon.flink.window.MegaTileEmissionPolicy
import ai.chronon.online.TopicInfo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AllowedLatenessConfigTest extends AnyFlatSpec with Matchers {

  "allowed_lateness_seconds" should "be parsed from props" in {
    val props = Map("allowed_lateness_seconds" -> "60")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 60000L
  }

  it should "be parsed from topicInfo params" in {
    val props = Map.empty[String, String]
    val topicInfo = TopicInfo("test-topic", "kafka", Map("allowed_lateness_seconds" -> "30"))

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 30000L
  }

  it should "prefer props over topicInfo" in {
    val props = Map("allowed_lateness_seconds" -> "60")
    val topicInfo = TopicInfo("test-topic", "kafka", Map("allowed_lateness_seconds" -> "30"))

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 60000L
  }

  it should "default to 0 when not configured" in {
    val props = Map.empty[String, String]
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 0L
  }

  it should "handle large values" in {
    val props = Map("allowed_lateness_seconds" -> "300")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 300000L
  }

  it should "ignore empty string values from props" in {
    val props = Map("allowed_lateness_seconds" -> "")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 0L
  }

  it should "ignore empty string values from topicInfo" in {
    val props = Map.empty[String, String]
    val topicInfo = TopicInfo("test-topic", "kafka", Map("allowed_lateness_seconds" -> ""))

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 0L
  }

  it should "ignore whitespace-only values" in {
    val props = Map("allowed_lateness_seconds" -> "   ")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 0L
  }

  it should "trim whitespace from valid values" in {
    val props = Map("allowed_lateness_seconds" -> " 60 ")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 60000L
  }

  it should "clamp negative values to 0" in {
    val props = Map("allowed_lateness_seconds" -> "-60")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val result = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

    result shouldBe 0L
  }

  it should "throw IllegalArgumentException for non-numeric values" in {
    val props = Map("allowed_lateness_seconds" -> "abc")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val exception = the[IllegalArgumentException] thrownBy {
      FlinkUtils.getAllowedLatenessMs(props, topicInfo)
    }
    exception.getMessage should include("invalid allowed_lateness_seconds value")
  }

  it should "throw IllegalArgumentException for values that would overflow" in {
    val hugeValue = (Long.MaxValue / 1000 + 1).toString
    val props = Map("allowed_lateness_seconds" -> hugeValue)
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val exception = the[IllegalArgumentException] thrownBy {
      FlinkUtils.getAllowedLatenessMs(props, topicInfo)
    }
    exception.getMessage should include("exceeds maximum")
  }

  "buffering output millis config" should "be parsed from props before topicInfo params" in {
    val props = Map("buffering_output_time_millis" -> "1000")
    val topicInfo = TopicInfo("test-topic", "kafka", Map("buffering_output_time_millis" -> "2000"))

    FlinkUtils.getNonNegativeLongProperty("buffering_output_time_millis", props, topicInfo) shouldBe 1000L
  }

  it should "be parsed from topicInfo params when props are absent" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map("buffering_output_jitter_millis" -> "250"))

    FlinkUtils.getNonNegativeLongProperty("buffering_output_jitter_millis", Map.empty, topicInfo) shouldBe 250L
  }

  it should "default missing empty whitespace and negative values to 0" in {
    val emptyTopicInfo = TopicInfo("test-topic", "kafka", Map("buffering_output_time_millis" -> ""))
    val whitespaceTopicInfo = TopicInfo("test-topic", "kafka", Map.empty)
    val negativeTopicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    FlinkUtils.getNonNegativeLongProperty("buffering_output_time_millis", Map.empty, emptyTopicInfo) shouldBe 0L
    FlinkUtils.getNonNegativeLongProperty(
      "buffering_output_time_millis",
      Map("buffering_output_time_millis" -> "   "),
      whitespaceTopicInfo) shouldBe 0L
    FlinkUtils.getNonNegativeLongProperty(
      "buffering_output_time_millis",
      Map("buffering_output_time_millis" -> "-1"),
      negativeTopicInfo) shouldBe 0L
  }

  it should "throw IllegalArgumentException for non-numeric values" in {
    val props = Map("buffering_output_jitter_millis" -> "nope")
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)

    val exception = the[IllegalArgumentException] thrownBy {
      FlinkUtils.getNonNegativeLongProperty("buffering_output_jitter_millis", props, topicInfo)
    }
    exception.getMessage should include("invalid buffering_output_jitter_millis value")
  }

  "GigaTile output buffering" should "require the wall-clock cadence policy" in {
    FlinkUtils.gigaTileBufferingOutputTimeMillis(1000L, MegaTileEmissionPolicy.Default) shouldBe 0L
    FlinkUtils.gigaTileBufferingOutputTimeMillis(1000L, MegaTileEmissionPolicy.WallClockCadence) shouldBe 1000L
  }
}
