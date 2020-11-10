package org.bigdata.statistics

import org.apache.spark.sql.DataFrame

trait MarketingStatistics {
  /**
   * Task 2.1
   * Find top 10 marketing campaigns that bring the biggest revenue
   * @param df input data
   * @return output Array with 10 Strings - Campaign names
   */
  def topTenCampaigns(df: DataFrame): Array[String]

  /**
   * Task 2.2
   * Find most popular channel that drives the highest amount
   * of unique sessions in each campaign
   * @param df input data
   * @return output Array with tuples (CampaignId, most popular channel)
   */
  def channelPerformanceByCampaign(df: DataFrame): Array[(String, String)]
}
