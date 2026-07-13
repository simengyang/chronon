package ai.chronon.flink.window

import ai.chronon.api.TsUtils

/** Chooses replay-safe aggregation horizons for the GigaTile operator. */
private[flink] object ChrononClockMode {
  sealed trait Mode
  case object NoWatermark extends Mode
  case object Catchup extends Mode
  case object Live extends Mode

  final case class Horizons(largeWindowAsOfMillis: Long, smallWindowAsOfMillis: Long)

  def classify(
      processingTimeMillis: Long,
      eventTimeWatermarkMillis: Long,
      sourceOutOfOrdernessMillis: Long,
      liveWatermarkLagToleranceMillis: Long
  ): Mode = {
    require(sourceOutOfOrdernessMillis >= 0L, "source out-of-orderness must be non-negative")
    require(liveWatermarkLagToleranceMillis >= sourceOutOfOrdernessMillis,
            "live watermark tolerance must cover source out-of-orderness")
    if (eventTimeWatermarkMillis == Long.MinValue) NoWatermark
    // Flink emits bounded-out-of-order watermarks as maxEventTs - outOfOrderness - 1.
    // Equality therefore implies that the source has already observed a future event.
    else if (watermarkImpliesFutureEvent(processingTimeMillis, eventTimeWatermarkMillis, sourceOutOfOrdernessMillis))
      NoWatermark
    else if (watermarkIsBehind(processingTimeMillis, eventTimeWatermarkMillis, liveWatermarkLagToleranceMillis)) Catchup
    else Live
  }

  def allowsPublication(mode: Mode): Boolean = mode == Live

  def dayAdvanceAsOfMillis(
      mode: Mode,
      processingTimeMillis: Long,
      eventTimeWatermarkMillis: Long
  ): Option[Long] =
    mode match {
      case NoWatermark => None
      case Catchup     => Some(eventTimeWatermarkMillis)
      case Live        => Some(processingTimeMillis)
    }

  def streamEventHorizons(
      mode: Mode,
      eventTimeMillis: Long,
      processingTimeMillis: Long,
      eventTimeWatermarkMillis: Long,
      smallestHopMillis: Long
  ): Horizons = {
    val asOfMillis = mode match {
      case NoWatermark => eventTimeMillis
      case Catchup     => eventTimeWatermarkMillis
      case Live        => processingTimeMillis
    }
    horizons(asOfMillis, smallestHopMillis)
  }

  def batchUpdateHorizons(
      mode: Mode,
      batchEndMillis: Long,
      currentDayStartMillis: Long,
      processingTimeMillis: Long,
      eventTimeWatermarkMillis: Long,
      smallestHopMillis: Long
  ): Horizons = {
    val asOfMillis = mode match {
      case NoWatermark => if (currentDayStartMillis >= 0L) currentDayStartMillis else batchEndMillis
      case Catchup     => eventTimeWatermarkMillis
      case Live        => processingTimeMillis
    }
    horizons(asOfMillis, smallestHopMillis)
  }

  def processingTimerHorizons(
      mode: Mode,
      processingTimeMillis: Long,
      eventTimeWatermarkMillis: Long,
      smallestHopMillis: Long
  ): Option[Horizons] =
    mode match {
      case NoWatermark => None
      case Catchup     => Some(horizons(eventTimeWatermarkMillis, smallestHopMillis))
      case Live        => Some(horizons(processingTimeMillis, smallestHopMillis))
    }

  private def horizons(asOfMillis: Long, smallestHopMillis: Long): Horizons =
    Horizons(asOfMillis, adjustExclusiveUpperBound(asOfMillis, smallestHopMillis))

  private def adjustExclusiveUpperBound(asOfMillis: Long, hopMillis: Long): Long =
    if (asOfMillis < Long.MaxValue && asOfMillis == TsUtils.round(asOfMillis, hopMillis)) asOfMillis + 1L
    else asOfMillis

  private def watermarkIsBehind(
      processingTimeMillis: Long,
      eventTimeWatermarkMillis: Long,
      liveWatermarkLagToleranceMillis: Long
  ): Boolean =
    processingTimeMillis > saturatingAdd(eventTimeWatermarkMillis, liveWatermarkLagToleranceMillis)

  private def watermarkImpliesFutureEvent(
      processingTimeMillis: Long,
      eventTimeWatermarkMillis: Long,
      sourceOutOfOrdernessMillis: Long
  ): Boolean =
    saturatingAdd(eventTimeWatermarkMillis, sourceOutOfOrdernessMillis) >= processingTimeMillis

  private def saturatingAdd(value: Long, nonNegativeDelta: Long): Long = {
    require(nonNegativeDelta >= 0L, "clock delta must be non-negative")
    if (value > Long.MaxValue - nonNegativeDelta) Long.MaxValue else value + nonNegativeDelta
  }
}
