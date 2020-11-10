package org.bigdata

import org.apache.spark.sql.SparkSession
import org.bigdata.projection.DataframeAttributeProjection
import org.bigdata.statistics.DataframeMarketingStatistics
import org.bigdata.utils.ReadUtils

object CapstoneProject {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Capstone").master("local").getOrCreate()

    // Read datasets
    val clickstream = ReadUtils.readTsv(spark, ReadUtils.CLICKSTREAM_SAMPLE_PATH)
    val purchaseDetails = ReadUtils.readTsv(spark, ReadUtils.PURCHASES_SAMPLE_PATH)

    // Transform datasets into view
    val input = new DataframeAttributeProjection().getProjection(clickstream, purchaseDetails)

    val statistics = new DataframeMarketingStatistics(spark)

    // Task 1 result
    val topTenCampaigns = statistics.topTenCampaigns(input)
    // Task 2 result
    val channelPerformanceByCampaign = statistics.channelPerformanceByCampaign(input)

    // Write results to console output
    println(topTenCampaigns.mkString("Array(", ", ", ")"))
    println(channelPerformanceByCampaign.mkString("Array(", ", ", ")"))

    spark.close()
  }

}