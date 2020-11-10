package org.bigdata.statistics

import org.bigdata.TestBase
import org.bigdata.statistics.MarketingStatisticsTestCases._
import org.junit.Test

class SqlMarketingStatisticsTest extends TestBase {

  @Test
  def test_topTenCampaigns(): Unit = {
    testTopTenCampaigns(SqlMarketingStatistics(spark), spark)
  }

  @Test
  def test_channelPerformanceByCampaign(): Unit = {
    testChannelPerformanceByCampaign(SqlMarketingStatistics(spark), spark)
  }
}
