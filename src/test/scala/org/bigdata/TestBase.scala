package org.bigdata

import org.apache.spark.sql.SparkSession
import org.bigdata.TestBase.createSparkSession
import org.bigdata.utils.ConfigUtils.{Config, getConfig}

class TestBase {
  implicit val spark: SparkSession = createSparkSession
  val conf: Config = getConfig(fromResources = true)
}

object TestBase {
  private def createSparkSession: SparkSession =
    SparkSession
      .builder()
      .appName("Capstone tests")
      .config("spark.capstone.config.path", "config.yaml")
      .master("local")
      .getOrCreate()
}