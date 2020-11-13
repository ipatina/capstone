package org.bigdata.statistics

import org.apache.spark.sql.DataFrame

trait MarketingStatistics {
  /**
   * Task 2.1
   * Find top 10 marketing campaigns that bring the biggest revenue
   * @param df input data
   * @return output DataFrame with 10 top Campaign names
   */
  def topTenCampaigns(df: DataFrame): DataFrame

  /**
   * Task 2.2
   * Find most popular channel that drives the highest amount
   * of unique sessions in each campaign
   * @param df input data
   * @return output DataFrame with pairs CampaignId - most popular channel
   */
  def channelPerformanceByCampaign(df: DataFrame): DataFrame
}
