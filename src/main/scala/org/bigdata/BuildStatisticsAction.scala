package org.bigdata
import org.apache.spark.sql.SparkSession
import org.bigdata.statistics.MarketingStatistics
import org.bigdata.utils.ConfigUtils
import org.bigdata.utils.IOUtils.{readTsv, writeTsv}

class BuildStatisticsAction(impl: MarketingStatistics) extends Action {
  private val TASK_1_PATH = "topTenCampaigns"
  private val TASK_2_PATH = "channelPerformanceByCampaign"

  override def doAction()(implicit spark: SparkSession): Unit = {
    val conf = ConfigUtils.getConfig()

    val inputData = readTsv(conf.attributeProjectionPath, conf.tsvOptions).cache()
    val topTenCampaigns = impl.topTenCampaigns(inputData)
    val channelPerformanceByCampaign = impl.channelPerformanceByCampaign(inputData)

    writeTsv(topTenCampaigns, s"${conf.statisticsRootPath}/$TASK_1_PATH", conf.tsvOptions)
    writeTsv(channelPerformanceByCampaign, s"${conf.statisticsRootPath}/$TASK_2_PATH", conf.tsvOptions)
  }
}
