package org.bigdata.projection

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, SparkSession}

class AttributeAggregator(spark: SparkSession) extends Aggregator[RawSchema, Seq[RawSchema], Seq[Out]] {

  import spark.implicits._

  override def zero: Seq[RawSchema] = Nil

  override def reduce(b: Seq[RawSchema], a: RawSchema): Seq[RawSchema] = b :+ a

  override def merge(b1: Seq[RawSchema], b2: Seq[RawSchema]): Seq[RawSchema] = b1 ++ b2

  override def finish(reduction: Seq[RawSchema]): Seq[Out] = {
    val purchases = reduction.filter(_.purchaseId.isDefined)
    val campaigns = reduction.filter(_.purchaseId.isEmpty)

    // for each purchase find previous (by eventTime) app_open event
    // then bind this event's campaign and channel ids to the purchase
    val withPurchase = for (purchase <- purchases) yield {
      val previousEvent = campaigns
        .filter(_.eventTime.before(purchase.eventTime))
        .sortWith((x1, x2) => x1.eventTime.compareTo(x2.eventTime) > 0)
        .head

      Out(s"${previousEvent.eventId}_${purchase.purchaseId.get}",
        purchase.purchaseId, previousEvent.campaignId.get, previousEvent.channelId.get)
    }

    // find app_open events without purchase and map them to OUT type
    val campaignsWithoutPurchase = campaigns
      .filter(campaign => !withPurchase.exists(_.sessionId.contains(s"${campaign.eventId}_")))
    val campaignsWithoutPurchaseMapped =
      for (campaign <- campaignsWithoutPurchase)
        yield Out(campaign.eventId, None, campaign.campaignId.get, campaign.channelId.get)

    withPurchase ++ campaignsWithoutPurchaseMapped
  }

  override def bufferEncoder: Encoder[Seq[RawSchema]] = newSequenceEncoder[Seq[RawSchema]]

  override def outputEncoder: Encoder[Seq[Out]] = newSequenceEncoder[Seq[Out]]
}
