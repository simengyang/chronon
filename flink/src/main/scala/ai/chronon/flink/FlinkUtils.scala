package ai.chronon.flink

import ai.chronon.flink.window.MegaTileEmissionPolicy
import ai.chronon.online.TopicInfo

import scala.concurrent.ExecutionContext
import scala.util.Try

object FlinkUtils {

  def getProperty(key: String, props: Map[String, String], topicInfo: TopicInfo): Option[String] = {
    props
      .get(key)
      .filter(_.nonEmpty)
      .orElse {
        topicInfo.params.get(key).filter(_.nonEmpty)
      }
  }

  private val MaxAllowedLatenessSeconds: Long = Long.MaxValue / 1000

  /** Returns allowed lateness in milliseconds for Flink window configuration.
    *
    * Defaults to 0 when unconfigured or negative to ensure deterministic window-close semantics
    * and avoid undefined watermark behavior. Accepts input from props (preferred) or topicInfo.
    *
    * @return lateness in milliseconds, clamped to 0 for negative/missing values
    * @throws IllegalArgumentException if value is non-numeric or would overflow
    */
  def getAllowedLatenessMs(props: Map[String, String], topicInfo: TopicInfo): Long = {
    getProperty("allowed_lateness_seconds", props, topicInfo)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { value =>
        val seconds = Try(value.toLong).getOrElse {
          throw new IllegalArgumentException(
            s"FlinkUtils.getAllowedLatenessMs: invalid allowed_lateness_seconds value '$value', must be a valid integer"
          )
        }
        if (seconds < 0) {
          0L
        } else if (seconds > MaxAllowedLatenessSeconds) {
          throw new IllegalArgumentException(
            s"FlinkUtils.getAllowedLatenessMs: allowed_lateness_seconds value $seconds exceeds maximum ($MaxAllowedLatenessSeconds)"
          )
        } else {
          java.lang.Math.multiplyExact(seconds, 1000L)
        }
      }
      .getOrElse(0L)
  }

  def getNonNegativeLongProperty(key: String, props: Map[String, String], topicInfo: TopicInfo): Long = {
    getProperty(key, props, topicInfo)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { value =>
        val parsed = Try(value.toLong).getOrElse {
          throw new IllegalArgumentException(
            s"FlinkUtils.getNonNegativeLongProperty: invalid $key value '$value', must be a valid integer"
          )
        }
        if (parsed < 0L) 0L else parsed
      }
      .getOrElse(0L)
  }

  def gigaTileBufferingOutputTimeMillis(
      configuredMillis: Long,
      emissionPolicy: MegaTileEmissionPolicy
  ): Long =
    if (emissionPolicy == MegaTileEmissionPolicy.WallClockCadence) configuredMillis else 0L
}

/** This was moved to flink-rpc-akka in Flink 1.16 and made private, so we reproduce the direct execution context here
  */
private class DirectExecutionContext extends ExecutionContext {
  override def execute(runnable: Runnable): Unit =
    runnable.run()

  override def reportFailure(cause: Throwable): Unit =
    throw new IllegalStateException("Error in direct execution context.", cause)

  override def prepare: ExecutionContext = this
}
