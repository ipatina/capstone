package org.bigdata.statistics

import org.bigdata.TestBase
import org.bigdata.statistics.MarketingStatisticsTestCases._
import org.junit.Test

class DataframeMarketingStatisticsTest extends TestBase {

  @Test
  def test_topTenCampaigns(): Unit = {
    testTopTenCampaigns(DataframeMarketingStatistics(spark), spark)
  }

  @Test
  def test_channelPerformanceByCampaign(): Unit = {
    testChannelPerformanceByCampaign(DataframeMarketingStatistics(spark), spark)
  }

}
