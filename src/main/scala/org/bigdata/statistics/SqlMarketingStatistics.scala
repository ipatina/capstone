package org.bigdata.statistics
import org.apache.spark.sql.{DataFrame, SparkSession}

class SqlMarketingStatistics(spark: SparkSession) extends MarketingStatistics {
  import spark.implicits._

  private val VIEW_NAME = "inputDf"

  override def topTenCampaigns(df: DataFrame): Array[String] = {
    val sqlExpression =
      s"""
        |select campaignId
        |from $VIEW_NAME
        |where isConfirmed = true
        |group by campaignId
        |order by sum(billingCost) desc
        |""".stripMargin

    runQuery(df, sqlExpression).as[String].take(10)
  }

  override def channelPerformanceByCampaign(df: DataFrame): Array[(String, String)] = {
    val sqlExpression =
      s"""
         |with channelCount as (
         |  select campaignId, channelId,
         |  rank() over (partition by campaignId order by count(channelId) desc) as rank
         |  from $VIEW_NAME
         |  group by campaignId, channelId
         |)
         |
         |select campaignId, channelId
         |from channelCount
         |where rank = 1
         |""".stripMargin

    runQuery(df, sqlExpression).as[(String, String)].collect()
  }

  private def runQuery(df: DataFrame, sql: String): DataFrame = {
    df.createOrReplaceTempView(VIEW_NAME)
    spark.sql(sql)
  }
}

object SqlMarketingStatistics {
  def apply(spark: SparkSession): SqlMarketingStatistics = new SqlMarketingStatistics(spark)
}