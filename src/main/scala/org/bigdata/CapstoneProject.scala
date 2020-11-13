package org.bigdata

import org.apache.spark.sql.SparkSession
import org.bigdata.projection.{AggregatorAttributeProjection, DataframeAttributeProjection}
import org.bigdata.statistics.{DataframeMarketingStatistics, SqlMarketingStatistics}
import org.bigdata.utils.ConfigUtils._

object CapstoneProject {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .appName("Capstone")
        .getOrCreate()

    val actionType = getSparkConf(ACTION_TYPE_CONF)
    val actionImpl = getSparkConf(ACTION_IMPLEMENTATION_CONF, defaultVal = Some("default"))

    val action = getActionFromConf(actionType, actionImpl)
    action.doAction()

    spark.close()
  }

  /**
   * Get Action instance from configuration provided in spark-submit.
   * For Action implementation DataframeAttributeProjection and DataframeMarketingStatistics
   * will be used by default.
   * @throws RuntimeException in case of wrong actionType
   */
  private def getActionFromConf(actionType: String, actionImpl: String)
                               (implicit spark: SparkSession): Action =
    actionType match {
      case BUILD_PROJECTION =>
        val impl = actionImpl match {
          case AGGREGATOR_IMPL => AggregatorAttributeProjection()
          case _ => DataframeAttributeProjection()
        }
        new BuildProjectionAction(impl)
      case BUILD_STATISTICS =>
        val impl = actionImpl match {
          case SQL_IMPL => SqlMarketingStatistics()
          case _ => DataframeMarketingStatistics()
        }
        new BuildStatisticsAction(impl)
      case _ => throw new RuntimeException(s"No Action found for $ACTION_TYPE_CONF=$actionType")
    }
}