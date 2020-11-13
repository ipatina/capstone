package org.bigdata.statistics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, rank, sum}
import org.bigdata.utils.ColumnConstants._

class DataframeMarketingStatistics extends MarketingStatistics {

  override def topTenCampaigns(df: DataFrame): DataFrame = {
    df.filter(col(IS_CONFIRMED))
      .groupBy(col(CAMPAIGN_ID))
      .agg(sum(BILLING_COST).as(BILLING_COST))
      .orderBy(col(BILLING_COST).desc)
      .select(CAMPAIGN_ID)
      .limit(10)
  }

  override def channelPerformanceByCampaign(df: DataFrame): DataFrame = {
    val RANK_COLUMN = "rank"
    val CHANNEL_ID_COUNT = "channelIdCount"

    val window = Window.partitionBy(col(CAMPAIGN_ID)).orderBy(col(CHANNEL_ID_COUNT).desc)

    df.groupBy(col(CHANNEL_ID), col(CAMPAIGN_ID))
      .agg(count(col(CHANNEL_ID)).as(CHANNEL_ID_COUNT))
      .withColumn(RANK_COLUMN, rank().over(window))
      .filter(col(RANK_COLUMN) === 1)
      .select(col(CAMPAIGN_ID), col(CHANNEL_ID))
  }
}

object DataframeMarketingStatistics {
  def apply(): DataframeMarketingStatistics = new DataframeMarketingStatistics()
}