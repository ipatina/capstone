package org.bigdata.statistics

import org.apache.spark.sql.SparkSession
import org.bigdata.utils.ReadUtils

object MarketingStatisticsTestCases {

  def testTopTenCampaigns(ms: MarketingStatistics, spark: SparkSession): Unit = {
    val input = ReadUtils.readTsv(spark, ReadUtils.TEST_PATH)

    val result = ms.topTenCampaigns(input)
    val expected = Array("cmp1", "cmp2")

    assert(expected.sameElements(result))
  }

  def testChannelPerformanceByCampaign(ms: MarketingStatistics, spark: SparkSession): Unit = {
    val input = ReadUtils.readTsv(spark, ReadUtils.TEST_PATH)

    val result = ms.channelPerformanceByCampaign(input)
    val expected = Array(("cmp2", "Yandex Ads"), ("cmp1", "Google Ads"))

    assert(expected.sameElements(result))
  }
}
