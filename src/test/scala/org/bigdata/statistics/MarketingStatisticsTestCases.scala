package org.bigdata.statistics

import org.apache.spark.sql.SparkSession
import org.bigdata.utils.ConfigUtils.Config
import org.bigdata.utils.IOUtils

object MarketingStatisticsTestCases {

  def testTopTenCampaigns(ms: MarketingStatistics, conf: Config)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val input = IOUtils.readTsv(conf.attributeProjectionPath, conf.tsvOptions)

    val result = ms.topTenCampaigns(input).as[String].collect()
    val expected = Array("cmp1", "cmp2")

    assert(expected.sameElements(result))
  }

  def testChannelPerformanceByCampaign(ms: MarketingStatistics, conf: Config)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val input = IOUtils.readTsv(conf.attributeProjectionPath, conf.tsvOptions)

    val result = ms.channelPerformanceByCampaign(input).as[(String, String)].collect()
    val expected = Array(("cmp2", "Yandex Ads"), ("cmp1", "Google Ads"))

    assert(expected.sameElements(result))
  }
}
