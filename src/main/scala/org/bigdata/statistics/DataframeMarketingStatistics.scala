package org.bigdata.statistics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, rank, sum}
import org.bigdata.utils.ColumnConstants._

class DataframeMarketingStatistics(spark: SparkSession) extends MarketingStatistics {
  import spark.implicits._

  override def topTenCampaigns(df: DataFrame): Array[String] = {
    df.filter(col(IS_CONFIRMED))
      .groupBy(col(CAMPAIGN_ID))
      .agg(sum(BILLING_COST).as(BILLING_COST))
      .orderBy(col(BILLING_COST).desc)
      .select(CAMPAIGN_ID)
      .as[String].take(10)
  }

  override def channelPerformanceByCampaign(df: DataFrame): Array[(String, String)] = {
    val RANK_COLUMN = "rank"
    val CHANNEL_ID_COUNT = "channelIdCount"

    val window = Window.partitionBy(col(CAMPAIGN_ID)).orderBy(col(CHANNEL_ID_COUNT).desc)

    df.groupBy(col(CHANNEL_ID), col(CAMPAIGN_ID))
      .agg(count(col(CHANNEL_ID)).as(CHANNEL_ID_COUNT))
      .withColumn(RANK_COLUMN, rank().over(window))
      .filter(col(RANK_COLUMN) === 1)
      .select(col(CAMPAIGN_ID), col(CHANNEL_ID))
      .as[(String, String)].collect()
  }
}

object DataframeMarketingStatistics {
  def apply(spark: SparkSession): DataframeMarketingStatistics = new DataframeMarketingStatistics(spark)
}